#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Бот-напоминалка для личного рабочего чата (Telegram Business).

Что делает:
  • через Telegram Business видит ваш личный чат с сотрудником;
  • раз в час (в рабочее время) отдаёт новую переписку Claude, который находит
    ваши вопросы и поручения без ответа;
  • если ответа долго нет — пишет в этот же чат от вашего имени
    «🤖 Напоминание: жду ответа — …»;
  • если после нескольких напоминаний ответа всё нет — сообщает вам;
  • голосовые и кружки расшифровывает (faster-whisper на сервере), фото описывает
    (Claude видит изображения); если Ольга ответила голосом/фото — присылает вам
    вопрос + расшифровку + оригинал; если неясно, ответ ли это — спрашивает вас кнопками.

Команды (в личном чате с ботом, работают только у владелицы):
  /status        — открытые вопросы
  /check         — проверить переписку прямо сейчас
  /remind        — отправить напоминания прямо сейчас
  /done 5        — закрыть вопрос №5 вручную (/done all — закрыть все)
  /pause /resume — выключить / включить напоминания
  + отправьте боту файл result.json (экспорт чата из Telegram Desktop),
    чтобы он изучил старую переписку.
"""
import asyncio
import json
import logging
import os
import re
import base64
import sqlite3
import tempfile
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command, CommandObject
from aiogram.types import (BusinessConnection, BusinessMessagesDeleted, CallbackQuery,
                           InlineKeyboardButton, InlineKeyboardMarkup, Message, ReplyParameters)
from anthropic import AsyncAnthropic

load_dotenv()

# ─────────── НАСТРОЙКИ (меняются в файле .env) ───────────
BOT_TOKEN = os.environ["BOT_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-5-5")
TZ = ZoneInfo(os.getenv("TIMEZONE", "Europe/Moscow"))
WORK_START = int(os.getenv("WORK_START", "10"))          # с какого часа можно напоминать
WORK_END = int(os.getenv("WORK_END", "21"))              # до какого часа
CHECK_EVERY_MIN = int(os.getenv("CHECK_EVERY_MIN", "60"))  # как часто проверять
FIRST_REMIND_AFTER_H = float(os.getenv("FIRST_REMIND_AFTER_H", "3"))  # первое напоминание через N часов после вопроса
REMIND_EVERY_H = float(os.getenv("REMIND_EVERY_H", "6"))  # повтор не чаще чем раз в N часов
MAX_REMINDERS = int(os.getenv("MAX_REMINDERS", "3"))      # после стольких напоминаний — сообщить владелице
# выходные дни, когда напоминания не отправляются (по московскому времени)
_DAYS = {"пн": 0, "вт": 1, "ср": 2, "чт": 3, "пт": 4, "сб": 5, "вс": 6}
DAYS_OFF = {_DAYS[d] for d in re.findall(r"[а-я]{2}", os.getenv("DAYS_OFF", "ср,вс").lower()) if d in _DAYS}
QUIET_MIN = int(os.getenv("QUIET_AFTER_ACTIVITY_MIN", "30"))  # не влезать, если в чате переписываются прямо сейчас
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))      # сколько дней переписки анализировать
PREFIX = os.getenv("REMINDER_PREFIX", "🤖 Напоминание")
OWNER_IDS = {int(x) for x in re.findall(r"\d+", os.getenv("OWNER_IDS", ""))}
DB_PATH = os.getenv("DB_PATH", "reminder_bot.db")
WHISPER_MODEL = os.getenv("WHISPER_MODEL", "small")      # off — не расшифровывать голосовые
FORWARD_MEDIA_ANSWERS = os.getenv("FORWARD_MEDIA_ANSWERS", "1") == "1"  # присылать вам ответы голосом/фото

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("reminder-bot")

router = Router()
claude = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

# ─────────── БАЗА ДАННЫХ ───────────
db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.row_factory = sqlite3.Row
db.executescript("""
CREATE TABLE IF NOT EXISTS conn (
  id TEXT PRIMARY KEY, owner_id INTEGER, owner_name TEXT,
  user_chat_id INTEGER, can_reply INTEGER, enabled INTEGER);
CREATE TABLE IF NOT EXISTS chats (
  chat_id INTEGER PRIMARY KEY, title TEXT, conn_id TEXT,
  analyzed_upto INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS msgs (
  chat_id INTEGER, msg_id INTEGER, who TEXT, name TEXT, text TEXT,
  ts INTEGER, deleted INTEGER DEFAULT 0, PRIMARY KEY (chat_id, msg_id));
CREATE TABLE IF NOT EXISTS items (
  id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, question TEXT,
  asked_msg_id INTEGER, asked_ts INTEGER, status TEXT DEFAULT 'open',
  reminders INTEGER DEFAULT 0, last_remind INTEGER DEFAULT 0,
  escalated INTEGER DEFAULT 0, created INTEGER, closed_ts INTEGER);
CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT);
""")
for table, col, decl in [("msgs", "media", "TEXT"), ("msgs", "file_id", "TEXT"),
                         ("msgs", "pending", "INTEGER DEFAULT 0"), ("msgs", "forwarded", "INTEGER DEFAULT 0"),
                         ("items", "answer_ids", "TEXT")]:
    if col not in [r[1] for r in db.execute(f"PRAGMA table_info({table})")]:
        db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
db.commit()


def q(sql, *a):
    return db.execute(sql, a).fetchall()


def q1(sql, *a):
    return db.execute(sql, a).fetchone()


def ex(sql, *a):
    db.execute(sql, a)
    db.commit()


def now() -> int:
    return int(time.time())


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, TZ).strftime("%d.%m %H:%M")


def is_paused() -> bool:
    r = q1("SELECT v FROM kv WHERE k='paused'")
    return bool(r and r["v"] == "1")


def is_owner(uid: int) -> bool:
    if uid in OWNER_IDS:
        return True
    return bool(q1("SELECT 1 FROM conn WHERE owner_id=?", uid))


# ─────────── ТЕКСТ СООБЩЕНИЯ ───────────
def msg_text(m: Message) -> str:
    t = m.text or m.caption or ""
    tag = ""
    if m.voice:
        tag = "[голосовое]"
    elif m.video_note:
        tag = "[видеокружок]"
    elif m.photo:
        tag = "[фото]"
    elif m.video:
        tag = "[видео]"
    elif m.document:
        tag = f"[файл: {m.document.file_name or ''}]"
    elif m.sticker:
        tag = f"[стикер {m.sticker.emoji or ''}]"
    elif m.location:
        tag = "[геолокация]"
    elif m.contact:
        tag = "[контакт]"
    return (tag + " " + t).strip()[:1500] or "[сообщение]"


def media_of(m: Message):
    """(тип, file_id) для голосового / кружка / фото, иначе (None, None)."""
    if m.voice:
        return "voice", m.voice.file_id
    if m.video_note:
        return "video_note", m.video_note.file_id
    if m.photo:
        return "photo", m.photo[-1].file_id
    return None, None


def store_msg(chat_id, msg_id, who, name, text, ts, media=None, file_id=None, pending=0):
    db.execute("""INSERT INTO msgs(chat_id,msg_id,who,name,text,ts,media,file_id,pending)
                  VALUES(?,?,?,?,?,?,?,?,?)
                  ON CONFLICT(chat_id,msg_id) DO UPDATE SET text=excluded.text""",
               (chat_id, msg_id, who, name, text, ts, media, file_id, pending))
    db.commit()


# ─────────── ЗАПРОС К CLAUDE ───────────
async def ask_claude(max_tokens: int = 8000, **kw) -> str:
    """Возвращает текст ответа. Если текста нет — ошибка с причиной (а не тихий пустой ответ)."""
    resp = await claude.messages.create(model=MODEL, max_tokens=max_tokens, **kw)
    text = "".join(getattr(b, "text", "") or "" for b in resp.content if b.type == "text").strip()
    if not text:
        raise RuntimeError(f"Claude вернул пустой ответ (stop_reason={resp.stop_reason}, "
                           f"блоки={[b.type for b in resp.content]})")
    return text


# ─────────── РАСШИФРОВКА ГОЛОСОВЫХ И ОПИСАНИЕ ФОТО ───────────
_whisper = None
_media_lock = asyncio.Lock()


def _transcribe_sync(path: str) -> str:
    global _whisper
    if _whisper is None:
        from faster_whisper import WhisperModel
        log.info("Загружаю модель распознавания речи %s (первый раз скачивается)…", WHISPER_MODEL)
        _whisper = WhisperModel(WHISPER_MODEL, device="cpu", compute_type="int8")
    segments, _ = _whisper.transcribe(path, language="ru", vad_filter=True)
    return " ".join(s.text.strip() for s in segments).strip()


async def transcribe(bot: Bot, file_id: str) -> str | None:
    if WHISPER_MODEL.lower() == "off":
        return None
    with tempfile.NamedTemporaryFile(suffix=".ogg") as tmp:
        await bot.download(file_id, destination=tmp.name, timeout=120)
        return await asyncio.to_thread(_transcribe_sync, tmp.name)


async def describe_photo(bot: Bot, file_id: str, caption: str) -> str:
    buf = await bot.download(file_id, timeout=120)
    b64 = base64.b64encode(buf.read()).decode()
    return await ask_claude(
        max_tokens=2000,
        messages=[{"role": "user", "content": [
            {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": b64}},
            {"type": "text", "text": "Это фото из рабочего чата пиццерии" +
             (f" с подписью «{caption}»" if caption else "") +
             ". Опиши в 1–2 предложениях, что на нём, и перепиши ключевые цифры и текст, если есть. Только описание."}]}])


async def process_media(bot: Bot, chat_id: int, msg_id: int, media: str, file_id: str, caption: str):
    """Расшифровывает голосовое/кружок или описывает фото и сохраняет текст в базу."""
    async with _media_lock:
        try:
            if media in ("voice", "video_note"):
                label = "голосовое" if media == "voice" else "видеокружок"
                t = await transcribe(bot, file_id)
                text = f"[{label}, расшифровка] {t}" if t else f"[{label}, не удалось расшифровать]"
            else:
                d = await describe_photo(bot, file_id, caption)
                text = f"[фото: {d}]" + (f" подпись: {caption}" if caption else "")
        except Exception as e:
            log.warning("Не смог обработать %s: %s", media, e)
            text = f"[{ {'voice': 'голосовое', 'video_note': 'видеокружок'}.get(media, 'фото') }, не удалось обработать]"
        ex("UPDATE msgs SET text=?, pending=0 WHERE chat_id=? AND msg_id=?", text[:3000], chat_id, msg_id)


# ─────────── TELEGRAM BUSINESS: ПОДКЛЮЧЕНИЕ И СООБЩЕНИЯ ───────────
@router.business_connection()
async def on_connection(bc: BusinessConnection, bot: Bot):
    rights = getattr(bc, "rights", None)
    can_reply = bool(getattr(rights, "can_reply", False)) if rights else bool(getattr(bc, "can_reply", False))
    ex("""INSERT INTO conn(id,owner_id,owner_name,user_chat_id,can_reply,enabled) VALUES(?,?,?,?,?,?)
          ON CONFLICT(id) DO UPDATE SET can_reply=excluded.can_reply, enabled=excluded.enabled""",
       bc.id, bc.user.id, bc.user.first_name, bc.user_chat_id, int(can_reply), int(bc.is_enabled))
    ex("UPDATE chats SET conn_id=? WHERE conn_id IS NULL", bc.id)
    if not bc.is_enabled:
        text = "Бот отключён от вашего аккаунта. Напоминания остановлены."
    elif can_reply:
        text = ("✅ Подключено. Слежу за чатами, к которым вы дали доступ.\n"
                "Чтобы я изучил старую переписку — пришлите мне файл result.json "
                "(экспорт чата из Telegram Desktop). Команды: /help")
    else:
        text = ("⚠️ Подключено, но у бота нет права отвечать на сообщения — напоминания отправлять не смогу. "
                "Включите это право в настройках: Telegram для бизнеса → Чат-боты.")
    try:
        await bot.send_message(bc.user_chat_id, text)
    except Exception as e:
        log.warning("Не смог написать владелице: %s", e)


@router.business_message()
async def on_business_message(m: Message, bot: Bot):
    c = q1("SELECT * FROM conn WHERE id=?", m.business_connection_id)
    owner_id = c["owner_id"] if c else None
    if m.sender_business_bot:
        who = "bot"
    elif m.from_user and m.from_user.id == owner_id:
        who = "owner"
    else:
        who = "other"
    name = m.from_user.first_name if m.from_user else "?"
    title = m.chat.full_name or m.chat.first_name or str(m.chat.id)
    ex("""INSERT INTO chats(chat_id,title,conn_id) VALUES(?,?,?)
          ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title, conn_id=excluded.conn_id""",
       m.chat.id, title, m.business_connection_id)
    media, file_id = media_of(m)
    need = media is not None and who != "bot"
    store_msg(m.chat.id, m.message_id, who, name, msg_text(m), int(m.date.timestamp()),
              media, file_id, pending=int(need))
    if need:
        asyncio.create_task(process_media(bot, m.chat.id, m.message_id, media, file_id, m.caption or ""))


@router.edited_business_message()
async def on_edit(m: Message):
    ex("UPDATE msgs SET text=? WHERE chat_id=? AND msg_id=?", msg_text(m), m.chat.id, m.message_id)


@router.deleted_business_messages()
async def on_delete(d: BusinessMessagesDeleted):
    for mid in d.message_ids:
        ex("UPDATE msgs SET deleted=1 WHERE chat_id=? AND msg_id=?", d.chat.id, mid)


# ─────────── АНАЛИЗ ПЕРЕПИСКИ (CLAUDE) ───────────
SYSTEM = """Ты помощник владелицы бизнеса. Тебе дают переписку из личного рабочего чата между владелицей ({owner}) и сотрудником ({other}). Задача — найти вопросы, просьбы и поручения владелицы, на которые {other} ещё не ответил(а).

Пункт ОТКРЫТ, если владелица спросила или попросила что-то, где от собеседника ожидается ответ, информация, решение или подтверждение, и дальше в переписке собеседник на это по существу не ответил.

Пункт ЗАКРЫТ, если:
- собеседник ответил по существу (информация, цифры, фото, решение);
- на поручение с конкретным действием собеседник подтвердил ВЫПОЛНЕНИЕ («сделала», «готово», «отправила», прислал фото или отчёт);
- это правило или инструкция на будущее («если будет X — делай Y», «так больше не делаем») и собеседник подтвердил, что понял («хорошо», «поняла», «ок»);
- ответ дан голосовым, кружком или фото и по расшифровке/описанию понятно, что это ответ на вопрос;
- владелица сама сняла вопрос («уже не надо», «разобралась») или ответ очевиден из дальнейшей переписки.

Голосовые и кружки даны расшифровкой, фото — описанием: используй их как обычные ответы. Если голосовое помечено «без расшифровки» (старая история) — считай его ответом на предыдущий вопрос.
Статус «unclear» ставь, когда после вопроса пришло голосовое, кружок или фото, но непонятно, отвечает ли оно на этот вопрос (расшифровка не удалась или по смыслу неясно).

ВАЖНО: «хорошо», «ок», «да», «поняла», «сделаю», «посмотрю», «уточню», смайлик или реакция НЕ закрывают:
- вопрос, на который нужна информация, цифра, дата, имя или решение — нужен сам ответ по существу;
- поручение с конкретным действием — нужно подтверждение выполнения, а не обещание.
Обещание «сделаю / посмотрю / уточню» без последующего результата — пункт ОТКРЫТ.

Не считай вопросами: приветствия, «как дела», благодарности, риторические вопросы, реплики без ожидания ответа. Если в одном сообщении несколько отдельных вопросов, а ответили только на часть — каждый неотвеченный отдельным пунктом. Свежие вопросы (последние минуты переписки) тоже включай: время напоминания решает программа.

Формулировка нового пункта: кратко, до 15 слов, по-русски, так, как спросила бы владелица, без имени в начале. Пример: «сколько коробок 30 см осталось на складе?»

Ответь ТОЛЬКО JSON, без пояснений и без ```:
{{"existing":[{{"id":<номер>,"status":"open"|"resolved"|"unclear","answer_msg_ids":[<номера сообщений с ответом>]}}],
"new":[{{"question":"...","msg_id":<номер сообщения владелицы с вопросом>,"status":"open"|"resolved"|"unclear","answer_msg_ids":[...]}}]}}
В existing — статус для КАЖДОГО известного открытого пункта. В new — вопросы, которых нет ни среди открытых, ни среди уже разобранных: открытые, неясные, а также закрытые ответом ГОЛОСОМ, КРУЖКОМ или ФОТО (чтобы переслать их владелице). Вопросы, закрытые обычным текстом, в new не добавляй. В answer_msg_ids — номера сообщений собеседника, которые являются ответом."""


def parse_json(text: str) -> dict:
    text = re.sub(r"```(json)?", "", text).strip()
    a, b = text.find("{"), text.rfind("}")
    return json.loads(text[a:b + 1]) if a >= 0 and b > a else {}


def owner_name_for(chat) -> str:
    c = q1("SELECT owner_name FROM conn WHERE id=?", chat["conn_id"]) if chat["conn_id"] else None
    return (c["owner_name"] if c else None) or "Владелица"


async def analyze_chat(chat_id: int, delay_first: bool = False, bot: Bot | None = None) -> int:
    """Анализирует чат, обновляет список открытых вопросов. Возвращает число новых пунктов."""
    chat = q1("SELECT * FROM chats WHERE chat_id=?", chat_id)
    msgs = q("""SELECT * FROM (SELECT * FROM msgs WHERE chat_id=? AND deleted=0 AND who!='bot' AND ts>=?
                ORDER BY ts DESC LIMIT 500) ORDER BY ts""", chat_id, now() - LOOKBACK_DAYS * 86400)
    if not msgs:
        return 0
    owner, other = owner_name_for(chat), chat["title"]
    known = q("SELECT * FROM items WHERE chat_id=? AND status='open'", chat_id)
    done = q("SELECT * FROM items WHERE chat_id=? AND status!='open' AND asked_ts>=?",
             chat_id, now() - LOOKBACK_DAYS * 86400)
    lines = [f"[#{m['msg_id']} {fmt_ts(m['ts'])}] {owner if m['who'] == 'owner' else other}: {m['text']}" for m in msgs]
    known_txt = "\n".join(f"{i['id']}. {i['question']} (сообщение #{i['asked_msg_id']})" for i in known) or "нет"
    user = (f"Сейчас: {datetime.now(TZ).strftime('%d.%m.%Y %H:%M')}\n\n"
            f"Уже известные открытые пункты:\n{known_txt}\n\n"
            f"Уже разобранные (не добавляй заново):\n"
            + ("\n".join(f"- {i['question']} (сообщение #{i['asked_msg_id']})" for i in done) or "нет") + "\n\n"
            f"Переписка (формат: [#номер дата время] Имя: текст):\n" + "\n".join(lines))

    raw = await ask_claude(system=SYSTEM.format(owner=owner, other=other),
                           messages=[{"role": "user", "content": user}])
    try:
        data = parse_json(raw)
    except Exception:
        data = None
    if not isinstance(data, dict) or ("existing" not in data and "new" not in data):
        raise RuntimeError("Не удалось разобрать ответ Claude: " + raw[:300])

    other_ids = {m["msg_id"] for m in msgs if m["who"] == "other"}

    def answers(e):
        return [i for i in (e.get("answer_msg_ids") or []) if i in other_ids]

    known_ids = {i["id"] for i in known}
    to_report = []  # (item_id, ответ-сообщения)
    for e in data.get("existing", []):
        st, iid = e.get("status"), e.get("id")
        if iid not in known_ids or st not in ("resolved", "unclear"):
            continue
        ex("UPDATE items SET status=?, closed_ts=?, answer_ids=? WHERE id=?",
           "resolved" if st == "resolved" else "review", now(), json.dumps(answers(e)), iid)
        to_report.append(iid)

    owner_msgs = {m["msg_id"]: m["ts"] for m in msgs if m["who"] == "owner"}
    added = 0
    for n in data.get("new", []):
        mid, question = n.get("msg_id"), (n.get("question") or "").strip()
        st = n.get("status") if n.get("status") in ("open", "resolved", "unclear") else "open"
        if not question or mid not in owner_msgs:
            continue
        if q1("SELECT 1 FROM items WHERE chat_id=? AND asked_msg_id=? AND question=?", chat_id, mid, question):
            continue
        status = {"open": "open", "resolved": "resolved", "unclear": "review"}[st]
        cur = db.execute("""INSERT INTO items(chat_id,question,asked_msg_id,asked_ts,created,last_remind,
                            status,answer_ids,closed_ts) VALUES(?,?,?,?,?,?,?,?,?)""",
                         (chat_id, question, mid, owner_msgs[mid], now(), now() if delay_first else 0,
                          status, json.dumps(answers(n)), None if status == "open" else now()))
        db.commit()
        if status == "open":
            added += 1
        else:
            to_report.append(cur.lastrowid)
    if bot is not None:
        for iid in to_report:
            await report_media_answer(bot, iid)
    ex("UPDATE chats SET analyzed_upto=? WHERE chat_id=?", max(m["ts"] for m in msgs), chat_id)
    log.info("Чат %s: новых пунктов %s, всего открыто %s", other, added,
             q1("SELECT COUNT(*) n FROM items WHERE chat_id=? AND status='open'", chat_id)["n"])
    return added


# ─────────── НАПОМИНАНИЯ ───────────
def owner_targets(chat) -> list:
    c = q1("SELECT * FROM conn WHERE id=?", chat["conn_id"]) if chat["conn_id"] else None
    return [c["user_chat_id"]] if c else list(OWNER_IDS)


async def notify_owner(bot: Bot, chat, text: str, markup=None):
    for t in owner_targets(chat):
        try:
            await bot.send_message(t, text, reply_markup=markup)
        except Exception as e:
            log.warning("Не смог написать владелице: %s", e)


MEDIA_LABEL = {"voice": "голосовое", "video_note": "видеокружок", "photo": "фото"}


async def report_media_answer(bot: Bot, item_id: int):
    """Присылает владелице вопрос + ответ голосом/фото (расшифровка + оригинал).
    Если неясно, ответ ли это, — с кнопками."""
    it = q1("SELECT * FROM items WHERE id=?", item_id)
    chat = q1("SELECT * FROM chats WHERE chat_id=?", it["chat_id"])
    ids = json.loads(it["answer_ids"] or "[]")
    ans = [q1("SELECT * FROM msgs WHERE chat_id=? AND msg_id=?", it["chat_id"], i) for i in ids]
    ans = [a for a in ans if a]
    media = [a for a in ans if a["media"]]
    review = it["status"] == "review"
    if not review and (not FORWARD_MEDIA_ANSWERS or not media or all(a["forwarded"] for a in media)):
        return
    head = (f"❓ Не уверен, что {chat['title']} ответила на вопрос:" if review
            else f"💬 {chat['title']} ответила голосом/фото:")
    body = f"{head}\n\nВопрос ({fmt_ts(it['asked_ts'])}): {it['question']}\n\n"
    body += ("Ответ:\n" + "\n".join(f"{fmt_ts(a['ts'])} {a['text']}" for a in ans)) if ans else "Ответа не найдено."
    markup = None
    if review:
        markup = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Ответ принят", callback_data=f"ok:{item_id}"),
            InlineKeyboardButton(text="🔔 Напомнить", callback_data=f"re:{item_id}")],
            [InlineKeyboardButton(text="🗑 Неактуально", callback_data=f"no:{item_id}")]])
    for t in owner_targets(chat):
        for a in media:
            if not a["file_id"]:
                continue
            try:
                send = getattr(bot, {"voice": "send_voice", "video_note": "send_video_note",
                                     "photo": "send_photo"}[a["media"]])
                await send(t, a["file_id"])
            except Exception as e:
                log.warning("Не смог переслать %s: %s", a["media"], e)
        try:
            await bot.send_message(t, body[:4000], reply_markup=markup)
        except Exception as e:
            log.warning("Не смог написать владелице: %s", e)
    for a in media:
        ex("UPDATE msgs SET forwarded=1 WHERE chat_id=? AND msg_id=?", a["chat_id"], a["msg_id"])


async def remind_items(bot: Bot, chat, due) -> bool:
    """Отправляет напоминание по списку вопросов в чат. True — если ушло."""
    cid, t = chat["chat_id"], now()
    if len(due) == 1:
        text = f"{PREFIX}: жду ответа — {due[0]['question']}"
        reply = ReplyParameters(message_id=due[0]["asked_msg_id"], allow_sending_without_reply=True)
    else:
        text = f"{PREFIX}. Жду ответа:\n" + "\n".join(f"{n}. {i['question']}" for n, i in enumerate(due, 1))
        reply = None
    try:
        sent = await bot.send_message(cid, text, business_connection_id=chat["conn_id"], reply_parameters=reply)
        store_msg(cid, sent.message_id, "bot", "bot", text, t)
        for i in due:
            ex("UPDATE items SET reminders=reminders+1, last_remind=? WHERE id=?", t, i["id"])
        return True
    except Exception as e:
        log.warning("Напоминание не отправлено: %s", e)
        for i in due:
            ex("UPDATE items SET last_remind=? WHERE id=?", t, i["id"])
        await notify_owner(bot, chat,
            f"Не получилось отправить напоминание в чат «{chat['title']}».\n"
            f"Обычно так бывает, если в чате не было сообщений больше суток: тогда Telegram "
            f"не даёт боту писать первым. Напишите туда что-нибудь сами — и бот продолжит.\n\n"
            f"Ждут ответа:\n" + "\n".join(f"• {i['question']}" for i in due) + f"\n\n(ошибка: {e})")
        return False


async def send_reminders(bot: Bot, force: bool = False):
    t = now()
    for chat in q("SELECT * FROM chats WHERE conn_id IS NOT NULL"):
        cid = chat["chat_id"]
        last = q1("SELECT MAX(ts) t FROM msgs WHERE chat_id=?", cid)["t"] or 0
        if not force and t - last < QUIET_MIN * 60:
            continue  # в чате прямо сейчас идёт разговор — не встреваем
        if force:
            due = q("SELECT * FROM items WHERE chat_id=? AND status='open' ORDER BY asked_ts", cid)
        else:
            due = q("""SELECT * FROM items WHERE chat_id=? AND status='open' AND reminders<?
                       AND ?-asked_ts>=? AND ?-last_remind>=? ORDER BY asked_ts""",
                    cid, MAX_REMINDERS, t, FIRST_REMIND_AFTER_H * 3600, t, REMIND_EVERY_H * 3600)
        if due:
            await remind_items(bot, chat, due)

        # эскалация: напоминаний было максимум, ответа всё нет
        esc = q("""SELECT * FROM items WHERE chat_id=? AND status='open' AND reminders>=? AND escalated=0
                   AND ?-last_remind>=?""", cid, MAX_REMINDERS, t, REMIND_EVERY_H * 3600)
        if esc:
            await notify_owner(bot, chat,
                f"⚠️ {chat['title']} не ответила после {MAX_REMINDERS} напоминаний:\n"
                + "\n".join(f"{i['id']}. {i['question']} (спросили {fmt_ts(i['asked_ts'])})" for i in esc)
                + "\n\nЗакрыть вопрос: /done номер")
            for i in esc:
                ex("UPDATE items SET escalated=1 WHERE id=?", i["id"])


async def tick(bot: Bot):
    if is_paused():
        return
    h = datetime.now(TZ).hour
    if not (WORK_START <= h < WORK_END):
        return
    if datetime.now(TZ).weekday() in DAYS_OFF:
        return  # выходной — не проверяем и не напоминаем
    for chat in q("SELECT * FROM chats"):
        newest = q1("SELECT MAX(ts) t FROM msgs WHERE chat_id=? AND who!='bot'", chat["chat_id"])["t"] or 0
        pending = q1("SELECT 1 FROM msgs WHERE chat_id=? AND pending=1 AND ts>?", chat["chat_id"], now() - 600)
        if newest > (chat["analyzed_upto"] or 0) and not pending:
            try:
                await analyze_chat(chat["chat_id"], bot=bot)
            except Exception as e:
                log.exception("Ошибка анализа: %s", e)
    await send_reminders(bot)


async def scheduler(bot: Bot):
    while True:
        try:
            await tick(bot)
        except Exception as e:
            log.exception("Ошибка проверки: %s", e)
        await asyncio.sleep(CHECK_EVERY_MIN * 60)


# ─────────── КОМАНДЫ ВЛАДЕЛИЦЫ ───────────
owner_only = F.chat.type == "private"


def status_text() -> str:
    rows = q("""SELECT i.*, c.title FROM items i JOIN chats c ON c.chat_id=i.chat_id
                WHERE i.status='open' ORDER BY i.asked_ts""")
    head = "⏸ Напоминания на паузе.\n\n" if is_paused() else ""
    if datetime.now(TZ).weekday() in DAYS_OFF:
        head += "🌙 Сегодня выходной — напоминаний не будет.\n\n"
    rev = q("""SELECT i.*, c.title FROM items i JOIN chats c ON c.chat_id=i.chat_id
               WHERE i.status='review' ORDER BY i.asked_ts""")
    if rev:
        head += "Ждут вашего решения (ответ голосом/фото):\n" + "\n".join(
            f"{r['id']}. {r['question']}" for r in rev) + "\n\n"
    if not rows:
        return head + "Открытых вопросов нет 👍"
    out = [head + "Ждут ответа:"]
    for r in rows:
        out.append(f"{r['id']}. {r['question']}\n    {r['title']}, спросили {fmt_ts(r['asked_ts'])}, "
                   f"напоминаний: {r['reminders']}")
    out.append("\nЗакрыть вопрос: /done номер")
    return "\n".join(out)


@router.message(owner_only, Command("start", "help"))
async def cmd_help(m: Message):
    if not is_owner(m.from_user.id):
        return await m.answer("Это служебный бот.")
    await m.answer(
        "Я слежу за вашим чатом и напоминаю, если на ваш вопрос долго нет ответа.\n\n"
        "/status — открытые вопросы\n/check — проверить переписку сейчас\n"
        "/remind — отправить напоминания сейчас\n/done 5 — закрыть вопрос №5 (/done all — все)\n"
        "/pause, /resume — выключить / включить напоминания\n\n"
        "Чтобы я изучил старую переписку — пришлите файл result.json (экспорт чата из Telegram Desktop).")


@router.message(owner_only, Command("status"))
async def cmd_status(m: Message):
    if is_owner(m.from_user.id):
        await m.answer(status_text())


@router.message(owner_only, Command("check"))
async def cmd_check(m: Message, bot: Bot):
    if not is_owner(m.from_user.id):
        return
    await m.answer("Проверяю переписку…")
    for chat in q("SELECT * FROM chats"):
        try:
            await analyze_chat(chat["chat_id"], bot=bot)
        except Exception as e:
            await m.answer(f"Ошибка анализа «{chat['title']}»: {e}")
    await m.answer(status_text())


@router.message(owner_only, Command("remind"))
async def cmd_remind(m: Message, bot: Bot):
    if is_owner(m.from_user.id):
        await send_reminders(bot, force=True)
        await m.answer("Напоминания отправлены (если были открытые вопросы).")


@router.message(owner_only, Command("done"))
async def cmd_done(m: Message, command: CommandObject):
    if not is_owner(m.from_user.id):
        return
    arg = (command.args or "").strip().lower()
    if arg == "all":
        ex("UPDATE items SET status='dismissed', closed_ts=? WHERE status IN ('open','review')", now())
        return await m.answer("Все вопросы закрыты.")
    ids = [int(x) for x in re.findall(r"\d+", arg)]
    if not ids:
        return await m.answer("Укажите номер: /done 5")
    for i in ids:
        ex("UPDATE items SET status='dismissed', closed_ts=? WHERE id=? AND status IN ('open','review')", now(), i)
    await m.answer("Готово.\n\n" + status_text())


@router.message(owner_only, Command("pause"))
async def cmd_pause(m: Message):
    if is_owner(m.from_user.id):
        ex("INSERT OR REPLACE INTO kv(k,v) VALUES('paused','1')")
        await m.answer("⏸ Напоминания выключены. Включить: /resume")


@router.message(owner_only, Command("resume"))
async def cmd_resume(m: Message):
    if is_owner(m.from_user.id):
        ex("INSERT OR REPLACE INTO kv(k,v) VALUES('paused','0')")
        await m.answer("▶️ Напоминания включены.")


@router.callback_query(F.data.regexp(r"^(ok|re|no):\d+$"))
async def on_button(cb: CallbackQuery, bot: Bot):
    if not is_owner(cb.from_user.id):
        return await cb.answer()
    action, iid = cb.data.split(":")
    it = q1("SELECT * FROM items WHERE id=?", int(iid))
    if not it or it["status"] != "review":
        await cb.answer("Уже разобрано")
        return await cb.message.edit_reply_markup(reply_markup=None)
    if action == "ok":
        ex("UPDATE items SET status='resolved', closed_ts=? WHERE id=?", now(), it["id"])
        note = "✅ Отмечено: ответ принят."
    elif action == "no":
        ex("UPDATE items SET status='dismissed', closed_ts=? WHERE id=?", now(), it["id"])
        note = "🗑 Вопрос закрыт как неактуальный."
    else:
        ex("UPDATE items SET status='open', closed_ts=NULL WHERE id=?", it["id"])
        chat = q1("SELECT * FROM chats WHERE chat_id=?", it["chat_id"])
        ok = await remind_items(bot, chat, [q1("SELECT * FROM items WHERE id=?", it["id"])])
        note = "🔔 Напоминание отправлено." if ok else "🔔 Вопрос снова открыт, напомню позже."
    await cb.message.edit_text((cb.message.text or "") + "\n\n" + note)
    await cb.answer()


# ─────────── ИМПОРТ СТАРОЙ ПЕРЕПИСКИ (result.json из Telegram Desktop) ───────────
def export_text(t) -> str:
    if isinstance(t, str):
        return t
    if isinstance(t, list):
        return "".join(x if isinstance(x, str) else x.get("text", "") for x in t)
    return ""


def import_export(data: dict, owner_id: int) -> list:
    chats = data["chats"]["list"] if "chats" in data else [data]
    conn = q1("SELECT * FROM conn WHERE owner_id=? AND enabled=1", owner_id)
    result = []
    for ch in chats:
        if ch.get("type") != "personal_chat":
            continue
        cid, title, n = ch["id"], ch.get("name") or str(ch["id"]), 0
        ex("""INSERT INTO chats(chat_id,title,conn_id) VALUES(?,?,?)
              ON CONFLICT(chat_id) DO UPDATE SET conn_id=COALESCE(chats.conn_id, excluded.conn_id)""",
           cid, title, conn["id"] if conn else None)
        for msg in ch.get("messages", []):
            if msg.get("type") != "message":
                continue
            text = export_text(msg.get("text"))
            mt = msg.get("media_type")
            if mt == "voice_message":
                text = ("[голосовое, без расшифровки] " + text).strip()
            elif mt == "video_message":
                text = ("[видеокружок, без расшифровки] " + text).strip()
            elif msg.get("photo"):
                text = ("[фото] " + text).strip()
            elif msg.get("file"):
                text = ("[файл] " + text).strip()
            if not text:
                continue
            ts = int(msg.get("date_unixtime") or
                     datetime.fromisoformat(msg["date"]).replace(tzinfo=TZ).timestamp())
            who = "owner" if msg.get("from_id") == f"user{owner_id}" else "other"
            db.execute("""INSERT OR IGNORE INTO msgs(chat_id,msg_id,who,name,text,ts,forwarded)
                          VALUES(?,?,?,?,?,?,1)""", (cid, msg["id"], who, msg.get("from") or "", text[:1500], ts))
            n += 1
        db.commit()
        result.append((cid, title, n))
    return result


@router.message(owner_only, F.document)
async def on_document(m: Message, bot: Bot):
    if not is_owner(m.from_user.id):
        return
    if not (m.document.file_name or "").lower().endswith(".json"):
        return await m.answer("Пришлите файл result.json из экспорта Telegram Desktop (формат JSON).")
    await m.answer("Читаю историю…")
    f = await bot.download(m.document)
    try:
        data = json.loads(f.read().decode("utf-8"))
        imported = import_export(data, m.from_user.id)
    except Exception as e:
        return await m.answer(f"Не получилось прочитать файл: {e}")
    if not imported:
        return await m.answer("В файле не нашлось личных чатов.")
    for cid, title, n in imported:
        await m.answer(f"«{title}»: загружено сообщений — {n}. Анализирую последние {LOOKBACK_DAYS} дн…")
        try:
            await analyze_chat(cid, delay_first=True)
        except Exception as e:
            await m.answer(f"Ошибка анализа: {e}")
    await m.answer(status_text() + f"\n\nЕсли что-то уже неактуально — закройте через /done. "
                   f"Первые напоминания по этим вопросам уйдут не раньше чем через {REMIND_EVERY_H:g} ч.")


# ─────────── ЗАПУСК ───────────
async def main():
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    asyncio.create_task(scheduler(bot))
    log.info("Бот запущен. Модель: %s", MODEL)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
