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
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command, CommandObject
from aiogram.types import (BusinessConnection, BusinessMessagesDeleted, CallbackQuery,
                           InlineKeyboardButton, InlineKeyboardMarkup, Message, ReplyParameters)
from anthropic import AsyncAnthropic
import aiohttp

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
# перенос в Google Таблицу задач: после стольких напоминаний вопрос уходит в таблицу
SHEET_URL = os.getenv("SHEET_WEBHOOK_URL", "").strip()
SHEET_SECRET = os.getenv("SHEET_SECRET", "").strip()
MOVE_AFTER = int(os.getenv("MOVE_AFTER_REMINDERS", "2"))
WEEKDAY_SHEETS = {0: "🧭 Упр Пн", 1: "🧭 Упр Вт", 3: "🧭 Упр Чт", 4: "🧭 Упр Пт", 5: "🧭 Упр Сб"}
# фокусы на день: анализ владелице вечером, отправка управляющей утром (часы по Москве)
FOCUS_ENABLED = os.getenv("FOCUS_ENABLED", "1") == "1"
FOCUS_PREP_HOUR = int(os.getenv("FOCUS_PREP_HOUR", "22"))
FOCUS_SEND_HOUR = int(os.getenv("FOCUS_SEND_HOUR", "10"))
# как часто анализировать переписку сам (часы); /check — в любой момент
ANALYZE_EVERY_H = float(os.getenv("ANALYZE_EVERY_H", "6"))
# рабочий формат: задачи через бота, чек-лист утром, созвон, вечерний отчёт (часы МСК)
CALL_HOUR = int(os.getenv("CALL_HOUR", "18"))
REPORT_HOUR, REPORT_MIN = 20, 30
NUDGE_ENABLED = os.getenv("NUDGE_ENABLED", "1") == "1"
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
owner_only = F.chat.type == "private"
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
CREATE TABLE IF NOT EXISTS kb_answers (
  id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, q_msg_id INTEGER, question TEXT,
  answer TEXT, source TEXT, status TEXT DEFAULT 'sent', created INTEGER);
CREATE TABLE IF NOT EXISTS tasks2 (
  id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, kind TEXT, text TEXT, due TEXT, done_def TEXT,
  proof INTEGER DEFAULT 0, status TEXT DEFAULT 'draft', created INTEGER, sent_date TEXT, done_ts INTEGER,
  carries INTEGER DEFAULT 0, reason TEXT, sheet_id TEXT, cl_msg_id INTEGER, cl_task_id INTEGER);
CREATE TABLE IF NOT EXISTS ctl_log (
  task_id INTEGER, date TEXT, msg_id INTEGER, item_id INTEGER, done INTEGER DEFAULT 0, PRIMARY KEY (task_id, date));
CREATE TABLE IF NOT EXISTS bz (
  id INTEGER PRIMARY KEY AUTOINCREMENT, section TEXT, title TEXT, text TEXT, src TEXT,
  status TEXT DEFAULT 'ok', created INTEGER);
CREATE TABLE IF NOT EXISTS news (
  id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, summary TEXT, tasks TEXT, src TEXT, status TEXT DEFAULT 'draft',
  created INTEGER, sent_date TEXT, cl_msg_id INTEGER, read_ts INTEGER, added_ts INTEGER, added_by TEXT);
CREATE TABLE IF NOT EXISTS kb_learned (
  id INTEGER PRIMARY KEY AUTOINCREMENT, question TEXT, answer TEXT, created INTEGER);
""")
for table, col, decl in [("msgs", "media", "TEXT"), ("msgs", "file_id", "TEXT"),
                         ("msgs", "pending", "INTEGER DEFAULT 0"), ("msgs", "forwarded", "INTEGER DEFAULT 0"),
                         ("items", "answer_ids", "TEXT"), ("items", "sheet_id", "TEXT"),
                         ("tasks2", "ask_ts", "INTEGER DEFAULT 0"), ("tasks2", "photo", "TEXT"),
                         ("tasks2", "src_msg", "INTEGER")]:
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


def kv_get(k, default=None):
    r = q1("SELECT v FROM kv WHERE k=?", k)
    if not r:
        return default
    try:
        return json.loads(r["v"])
    except Exception:
        return r["v"]


def kv_set(k, v):
    ex("INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)", k, json.dumps(v, ensure_ascii=False))


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


def _decode_audio(path: str):
    """Своё декодирование в 16 кГц моно: встроенное в faster-whisper ломается на новых версиях PyAV."""
    import av
    import numpy as np
    chunks = []
    with av.open(path) as cont:
        st = cont.streams.audio[0]
        res = av.AudioResampler(format="s16", layout="mono", rate=16000)
        for frame in cont.decode(st):
            for f in res.resample(frame):
                chunks.append(f.to_ndarray())
        for f in res.resample(None):
            chunks.append(f.to_ndarray())
    if not chunks:
        return np.zeros(1600, dtype=np.float32)
    return np.concatenate(chunks, axis=1).flatten().astype(np.float32) / 32768.0


def _transcribe_sync(path: str) -> str:
    global _whisper
    if _whisper is None:
        from faster_whisper import WhisperModel
        log.info("Загружаю модель распознавания речи %s (первый раз скачивается)…", WHISPER_MODEL)
        _whisper = WhisperModel(WHISPER_MODEL, device="cpu", compute_type="int8")
    segments, _ = _whisper.transcribe(_decode_audio(path), language="ru", vad_filter=True)
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
    row = q1("SELECT who FROM msgs WHERE chat_id=? AND msg_id=?", chat_id, msg_id)
    if row and row["who"] == "owner" and BZ_TRIGGER.search(text):
        asyncio.create_task(bz_extract(bot, chat_id, msg_id))


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
    if (m.from_user and m.from_user.is_bot and not m.sender_business_bot) or \
            (m.chat.username or "").lower().endswith("bot"):
        if who == "other":  # сообщение от бота-источника новостей (например, Codo)
            asyncio.create_task(news_ingest(bot, m))
        return
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
    if who == "other":
        if NUDGE_ENABLED and m.text:
            asyncio.create_task(nudge_check(bot, m))
    if m.checklist_tasks_done:
        await on_checklist_ticks(bot, m)
    if who == "owner" and BZ_TRIGGER.search(m.text or m.caption or ""):
        asyncio.create_task(bz_extract(bot, m.chat.id, m.message_id))


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
    known = q("SELECT * FROM items WHERE chat_id=? AND status IN ('open','moved')", chat_id)
    done = q("SELECT * FROM items WHERE chat_id=? AND status NOT IN ('open','moved') AND asked_ts>=?",
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
        old = q1("SELECT * FROM items WHERE id=?", iid)
        if old["status"] == "moved" and st == "resolved" and old["sheet_id"]:
            await sheet_call({"action": "status", "id": old["sheet_id"], "status": "Выполнено"})
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
        log.warning("Напоминание через ваш аккаунт не отправлено: %s", e)
        # запасной путь: личный чат бота с сотрудником (работает, если он(а) хоть раз нажал(а) «Старт» у бота)
        owner = owner_name_for(chat)
        if len(due) == 1:
            alt = f"🤖 Напоминание ({owner}): жду ответа — {due[0]['question']}"
        else:
            alt = f"🤖 Напоминание ({owner}). Жду ответа:\n" + "\n".join(
                f"{n}. {i['question']}" for n, i in enumerate(due, 1))
        alt += "\n\nОтветьте, пожалуйста, в нашем обычном чате."
        try:
            await bot.send_message(cid, alt)
            for i in due:
                ex("UPDATE items SET reminders=reminders+1, last_remind=? WHERE id=?", t, i["id"])
            log.info("Напоминание отправлено через чат бота")
            return True
        except Exception as e2:
            log.warning("И через чат бота не получилось: %s", e2)
        for i in due:
            ex("UPDATE items SET last_remind=? WHERE id=?", t, i["id"])
        await notify_owner(bot, chat,
            f"Не получилось отправить напоминание в чат «{chat['title']}».\n"
            f"Обычно так бывает, если в чате не было сообщений больше суток: тогда Telegram "
            f"не даёт боту писать первым. Напишите туда что-нибудь сами — и бот продолжит.\n\n"
            f"Ждут ответа:\n" + "\n".join(f"• {i['question']}" for i in due) + f"\n\n(ошибка: {e})")
        return False


# ─────────── ПЕРЕНОС В GOOGLE ТАБЛИЦУ ───────────
async def sheet_call(payload: dict) -> bool:
    if not SHEET_URL:
        return False
    payload = dict(payload, secret=SHEET_SECRET)
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as s:
            async with s.post(SHEET_URL, json=payload, allow_redirects=True) as r:
                txt = await r.text()
        ok = '"ok":true' in txt.replace(" ", "")
        if not ok:
            log.warning("Таблица ответила: %s", txt[:300])
        return ok
    except Exception as e:
        log.warning("Ошибка связи с таблицей: %s", e)
        return False


async def sheet_json(payload: dict):
    """Запрос к таблице с ответом (для чтения планировщика)."""
    if not SHEET_URL:
        return None
    payload = dict(payload, secret=SHEET_SECRET)
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as s:
            async with s.post(SHEET_URL, json=payload, allow_redirects=True) as r:
                data = json.loads(await r.text())
        return data if data.get("ok") else None
    except Exception as e:
        log.warning("Не удалось прочитать таблицу: %s", e)
        return None


def work_day_sheet():
    """Сегодняшняя (или ближайшая рабочая) вкладка и дата."""
    from datetime import timedelta
    d = datetime.now(TZ)
    for _ in range(7):
        if d.weekday() in WEEKDAY_SHEETS and d.weekday() not in DAYS_OFF:
            return WEEKDAY_SHEETS[d.weekday()], d.strftime("%Y-%m-%d")
        d += timedelta(days=1)
    return "🧭 Упр Пн", datetime.now(TZ).strftime("%Y-%m-%d")


async def move_to_sheet(bot: Bot, chat, items) -> None:
    sheet, date = work_day_sheet()
    moved = []
    for i in items:
        sid = "tg:" + str(uuid.uuid4())
        text = f"{i['question']} (вопрос из Telegram от {fmt_ts(i['asked_ts'])[:5]}, без ответа)"
        if await sheet_call({"action": "add", "id": sid, "date": date, "text": text, "sheet": sheet}):
            ex("UPDATE items SET status='moved', sheet_id=?, closed_ts=? WHERE id=?", sid, now(), i["id"])
            moved.append(i)
    if moved:
        await notify_owner(bot, chat,
            f"📋 Перенесено в таблицу задач (вкладка «{sheet}») — {chat['title']} не ответила "
            f"после {MOVE_AFTER} напоминаний:\n" + "\n".join(f"• {i['question']}" for i in moved))


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
        if due and SHEET_URL and not force:
            to_move = [i for i in due if i["reminders"] >= MOVE_AFTER]
            due = [i for i in due if i["reminders"] < MOVE_AFTER]
            if to_move:
                await move_to_sheet(bot, chat, to_move)
        if due:
            await remind_items(bot, chat, due)

        # эскалация: напоминаний было максимум, ответа всё нет
        esc = q("""SELECT * FROM items WHERE chat_id=? AND status='open' AND reminders>=? AND escalated=0
                   AND ?-last_remind>=?""", cid, MAX_REMINDERS, t, REMIND_EVERY_H * 3600)
        if esc:
            await notify_owner(bot, chat,
                f"⚠️ {chat['title']} не ответила после {MAX_REMINDERS} напоминаний:\n"
                + "\n".join(f"• {i['question']} (спросили {fmt_ts(i['asked_ts'])})" for i in esc)
                + "\n\nСписок с номерами: /status")
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
    t = now()
    scheduled = t - (kv_get("last_analyze", 0) or 0) >= ANALYZE_EVERY_H * 3600 - 300
    if scheduled:
        kv_set("last_analyze", t)
    for chat in q("SELECT * FROM chats"):
        cid = chat["chat_id"]
        newest = q1("SELECT MAX(ts) t FROM msgs WHERE chat_id=? AND who!='bot'", cid)["t"] or 0
        pending = q1("SELECT 1 FROM msgs WHERE chat_id=? AND pending=1 AND ts>?", cid, t - 600)
        # перед напоминанием проверяем свежие ответы, чтобы не напоминать о том, на что уже ответили
        due_soon = q1("""SELECT 1 FROM items WHERE chat_id=? AND status='open' AND reminders<?
                         AND ?-asked_ts>=? AND ?-last_remind>=?""",
                      cid, MAX_REMINDERS, t, FIRST_REMIND_AFTER_H * 3600, t, REMIND_EVERY_H * 3600)
        if newest > (chat["analyzed_upto"] or 0) and not pending and (scheduled or due_soon):
            try:
                await analyze_chat(cid, bot=bot)
            except Exception as e:
                log.exception("Ошибка анализа: %s", e)
                await api_error_notify(bot, chat, e)
    await send_reminders(bot)


async def api_error_notify(bot: Bot, chat, e):
    """Если кончились деньги на балансе Claude API — сообщить владелице (не чаще раза в 12 ч)."""
    txt = str(e).lower()
    if not any(w in txt for w in ("credit balance", "billing", "insufficient", "quota")):
        return
    if now() - (kv_get("api_alert", 0) or 0) < 12 * 3600:
        return
    kv_set("api_alert", now())
    await notify_owner(bot, chat, "⚠️ На балансе Claude API закончились деньги — бот не может анализировать "
                                  "переписку и отвечать по базе знаний. Пополните: platform.claude.com → Billing.")



# ─────────── СБОР БАЗЫ ЗНАНИЙ УПРАВЛЯЮЩЕГО ───────────
# Когда владелица пишет Ольге со словами «база знаний» / «в базу» / «БЗ» / #бз — знание сохраняется отдельно.
BZ_TRIGGER = re.compile(r"баз[аеуы] знани|\bв базу\b|#бз|\bбз\b", re.I)
BZ_SECTIONS = ["Тесто и продукт", "Заказы и поставки", "Персонал и график", "Обучение", "Чистота и санитария",
               "Оборудование и ремонт", "Касса и деньги", "Качество и проверки", "Гости и жалобы",
               "Управление и отчётность", "Другое"]
BZ_SYSTEM = """Ты ведёшь базу знаний управляющего пиццерии Додо Коммунарка. Владелица отметила своё сообщение как знание для базы.
Выдели из него (с учётом контекста переписки) правило, стандарт или инструкцию так, чтобы их понял новый управляющий без контекста.
Убери эмоции, упрёки, имена и разовые детали — оставь суть: что делать, как, когда, почему важно. Ничего не придумывай сверх сказанного.
Если в сообщении несколько разных знаний — верни несколько карточек.
section — одно из: {sections}.
Ответь ТОЛЬКО JSON без ```: {{"cards":[{{"section":"...","title":"до 60 символов","text":"1–8 строк"}}]}}
Если знания для базы нет — {{"cards":[]}}"""


def bz_markup(bid):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✏️ Исправить", callback_data=f"bz:ed:{bid}"),
        InlineKeyboardButton(text="🗑 Удалить", callback_data=f"bz:rm:{bid}")]])


async def bz_extract(bot: Bot, chat_id: int, msg_id: int):
    try:
        await asyncio.sleep(5)
        m = q1("SELECT * FROM msgs WHERE chat_id=? AND msg_id=?", chat_id, msg_id)
        if not m:
            return
        ctx = q("""SELECT * FROM (SELECT * FROM msgs WHERE chat_id=? AND ts<=? AND deleted=0 ORDER BY ts DESC LIMIT 12)
                   ORDER BY ts""", chat_id, m["ts"])
        chat = q1("SELECT * FROM chats WHERE chat_id=?", chat_id)
        conv = "\n".join(f"{'Владелица' if c['who'] == 'owner' else 'Управляющая' if c['who'] == 'other' else 'бот'}: {c['text']}"
                         for c in ctx)
        raw = await ask_claude(max_tokens=2000, system=BZ_SYSTEM.format(sections=", ".join(BZ_SECTIONS)),
                               messages=[{"role": "user", "content": f"Контекст:\n{conv}\n\nОтмеченное сообщение:\n{m['text']}"}])
        for c in parse_json(raw).get("cards", [])[:5]:
            sec = c.get("section") if c.get("section") in BZ_SECTIONS else "Другое"
            cur = db.execute("INSERT INTO bz(section,title,text,src,created) VALUES(?,?,?,?,?)",
                             (sec, str(c.get("title", ""))[:80], str(c.get("text", "")), m["text"][:1000], now()))
            db.commit()
            await notify_owner(bot, chat, f"📚 В базу знаний · {sec}\n«{c.get('title', '')}»\n{c.get('text', '')}",
                               bz_markup(cur.lastrowid))
    except Exception as e:
        log.warning("БЗ: %s", e)


@router.callback_query(F.data.regexp(r"^bz:(ed|rm):\d+$"))
async def on_bz_button(cb: CallbackQuery):
    if not is_owner(cb.from_user.id):
        return await cb.answer()
    _, action, bid = cb.data.split(":")
    if action == "rm":
        ex("UPDATE bz SET status='deleted' WHERE id=?", int(bid))
        await cb.message.edit_text("🗑 Удалено из базы знаний.")
    else:
        kv_set("bz_edit", int(bid))
        await cb.message.answer("Напишите правильный текст карточки одним сообщением — заменю.")
    await cb.answer()


@router.message(owner_only, Command("bz"))
async def cmd_bz(m: Message):
    if not is_owner(m.from_user.id):
        return
    rows = q("SELECT * FROM bz WHERE status='ok' ORDER BY section, id")
    kv_set("bz_next", now())
    if not rows:
        return await m.answer("📚 База знаний пока пуста. Следующее сообщение сюда (в течение 10 минут) я сохраню в базу знаний.\n"
                              "Ещё способы: «в базу знаний» или «#бз» в чате с Ольгой, или сообщение сюда, начинающееся с «БЗ:».")
    by = {}
    for r in rows:
        by.setdefault(r["section"], []).append(r)
    out = [f"📚 База знаний управляющего — карточек: {len(rows)}"]
    for sec, rs in by.items():
        out.append(f"\n{sec} ({len(rs)}):")
        out += [f"• {r['title']}" for r in rs[-8:]]
    out.append("\nВесь текст файлом: /bz_file")
    await m.answer("\n".join(out)[:4000])


@router.message(owner_only, Command("bz_file"))
async def cmd_bz_file(m: Message, bot: Bot):
    if not is_owner(m.from_user.id):
        return
    from aiogram.types import BufferedInputFile
    rows = q("SELECT * FROM bz WHERE status='ok' ORDER BY section, id")
    if not rows:
        return await m.answer("База знаний пока пуста.")
    lines = ["# База знаний управляющего — Додо Коммунарка", ""]
    sec = None
    for r in rows:
        if r["section"] != sec:
            sec = r["section"]
            lines += ["", f"## {sec}", ""]
        d = datetime.fromtimestamp(r["created"], TZ).strftime("%d.%m.%Y")
        lines += [f"### {r['title']}", r["text"], f"_добавлено {d}_", ""]
    await bot.send_document(m.chat.id, BufferedInputFile("\n".join(lines).encode("utf-8"), filename="baza_znaniy_kommunarka.md"),
                            caption=f"📚 База знаний — карточек: {len(rows)}")



# ─────────── НОВОСТИ (Codo и др.) → ЗАДАЧИ УПРАВЛЯЮЩЕЙ ───────────
NEWS_SYSTEM = """Ты помощник владелицы пиццерии Додо Коммунарка. Это сообщение из канала новостей сети Додо.
Реши, важно ли оно для работы управляющей пиццерии: запуск/вывод продукта, изменение стандартов или регламентов,
акции и промо, сроки и дедлайны, обязательные обучения, проверки, изменения в Додо ИС. Рейтинги, вакансии, поздравления,
истории других пиццерий и мотивационные посты — НЕ важны (is_news=false), если в них нет действия для нашей пиццерии.
Сейчас {now} (МСК).
Если важно: title — до 60 символов; summary — суть в 2–5 строках простым языком; tasks — что конкретно сделать управляющей
(до 6 пунктов, каждый до 90 символов, повелительно) со сроком due "YYYY-MM-DD HH:MM" (МСК) — если срок не указан, ставь разумный
срок до даты запуска или +2 дня 20:00.
Ответь ТОЛЬКО JSON без ```: {{"is_news":true|false,"title":"...","summary":"...","tasks":[{{"text":"...","due":"..."}}]}}"""


def news_markup(nid):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Отправить Ольге", callback_data=f"nw:ok:{nid}"),
        InlineKeyboardButton(text="🗑 Не нужно", callback_data=f"nw:rm:{nid}")]])


def news_text(n) -> str:
    tasks = json.loads(n["tasks"] or "[]")
    t = f"📰 {n['title']}\n{n['summary']}"
    if tasks:
        t += "\n\nЗадачи:\n" + "\n".join(f"• {x['text']} — до {fmt_due(x.get('due', ''))}" for x in tasks)
    return t


async def news_ingest(bot: Bot, m: Message, from_owner: bool = False):
    """Новость из чата с ботом сети (через Telegram для бизнеса) или пересланная владелицей."""
    try:
        text = (m.text or m.caption or "").strip()
        if m.photo and len(text) < 200:
            try:
                text += "\n[картинка: " + await describe_photo(bot, m.photo[-1].file_id, text) + "]"
            except Exception:
                pass
        if len(text) < 40:
            if from_owner:
                await m.answer("В пересланном сообщении слишком мало текста — не похоже на новость.")
            return
        raw = await ask_claude(max_tokens=2000, system=NEWS_SYSTEM.format(now=datetime.now(TZ).strftime("%d.%m.%Y %H:%M")),
                               messages=[{"role": "user", "content": text[:6000]}])
        data = parse_json(raw)
        if not data.get("is_news"):
            if from_owner:
                await m.answer("🤷 Не вижу в этом действий для пиццерии. Если нужно — напишите задачи сами, я их разберу.")
            return
        cur = db.execute("INSERT INTO news(title,summary,tasks,src,created) VALUES(?,?,?,?,?)",
                         (str(data.get("title", ""))[:80], str(data.get("summary", "")),
                          json.dumps(data.get("tasks", [])[:6], ensure_ascii=False), text[:3000], now()))
        db.commit()
        n = q1("SELECT * FROM news WHERE id=?", cur.lastrowid)
        chat = main_chat()
        if chat:
            await notify_owner(bot, chat, "📰 Новость сети добавлена в план (утвердите вечером):\n\n" + news_text(n))
    except Exception as e:
        log.warning("Новость: %s", e)


@router.callback_query(F.data.regexp(r"^nw:(ok|rm):\d+$"))
async def on_news_button(cb: CallbackQuery):
    if not is_owner(cb.from_user.id):
        return await cb.answer()
    _, action, nid = cb.data.split(":")
    n = q1("SELECT * FROM news WHERE id=?", int(nid))
    if not n:
        return await cb.answer("Не найдено")
    if action == "ok":
        ex("UPDATE news SET status='queued' WHERE id=?", n["id"])
        await cb.message.edit_text(f"✅ В очереди — уйдёт Ольге в {TASKS_HOUR}:00 МСК.\n\n" + news_text(n))
    else:
        ex("UPDATE news SET status='skip' WHERE id=?", n["id"])
        await cb.message.edit_text("🗑 Пропущено: " + n["title"])
    await cb.answer()


def shift_workday(date_str: str) -> str:
    """Среда → четверг, воскресенье → понедельник (выходные управляющей)."""
    from datetime import timedelta
    d = datetime.strptime(date_str[:10], "%Y-%m-%d")
    while d.weekday() in DAYS_OFF:
        d += timedelta(days=1)
    return d.strftime("%Y-%m-%d")


async def send_news(bot: Bot, chat, today: str) -> int:
    from aiogram.types import InputChecklist, InputChecklistTask
    cid, n_sent = chat["chat_id"], 0
    for n in q("SELECT * FROM news WHERE status='approved' ORDER BY id"):
        tasks = json.loads(n["tasks"] or "[]")
        try:
            await _to_olga(bot, chat, f"📰 Новость сети: {n['title']}\n{n['summary']}")
            items = [InputChecklistTask(id=1, text="Прочитала"),
                     InputChecklistTask(id=2, text="Внесла задачи в чек-лист (планировщик)")]
            tids = []
            for x in tasks:  # срок на выходной управляющей → ближайший рабочий день
                if x.get("due") and len(x["due"]) >= 10:
                    x["due"] = shift_workday(x["due"][:10]) + x["due"][10:]
            for i, x in enumerate(tasks, 3):
                cur = db.execute("""INSERT INTO tasks2(chat_id,kind,text,due,done_def,status,created,sent_date)
                                    VALUES(?,'task',?,?,'','sent',?,?)""",
                                 (cid, str(x.get("text", ""))[:100], x.get("due") or "", now(), today))
                db.commit()
                tids.append(cur.lastrowid)
                items.append(InputChecklistTask(id=i, text=(f"{x.get('text', '')} — до {fmt_due(x.get('due', ''))}")[:100]))
            msg = await bot.send_checklist(business_connection_id=chat["conn_id"], chat_id=cid, checklist=InputChecklist(
                title=f"📰 {n['title']}"[:255], tasks=items[:30], others_can_mark_tasks_as_done=True))
            for i, tid in enumerate(tids, 3):
                ex("UPDATE tasks2 SET cl_msg_id=?, cl_task_id=? WHERE id=?", msg.message_id, i, tid)
            ex("UPDATE news SET status='sent', sent_date=?, cl_msg_id=? WHERE id=?", today, msg.message_id, n["id"])
            kv_set(f"news_tasks:{n['id']}", tids)
            n_sent += 1
        except Exception as e:
            log.warning("Новость не отправлена: %s", e)
    return n_sent


async def news_checklist_tick(bot: Bot, m: Message, cl_id: int, item_id: int) -> bool:
    n = q1("SELECT * FROM news WHERE cl_msg_id=?", cl_id)
    if not n or item_id not in (1, 2):
        return False
    chat = q1("SELECT * FROM chats WHERE chat_id=?", m.chat.id)
    if item_id == 1 and not n["read_ts"]:
        ex("UPDATE news SET read_ts=? WHERE id=?", now(), n["id"])
        await notify_owner(bot, chat, f"👀 {chat['title']} прочитала новость: {n['title']}")
    if item_id == 2 and not n["added_ts"]:
        ex("UPDATE news SET added_ts=?, added_by='manager' WHERE id=?", now(), n["id"])
        await notify_owner(bot, chat, f"📋 {chat['title']} внесла задачи в чек-лист: {n['title']}")
    return True


async def news_deadline(bot: Bot, chat, today: str):
    """20:00 — если «Внесла в чек-лист» не отмечено, бот сам ставит задачи в планировщик."""
    for n in q("SELECT * FROM news WHERE status='sent' AND sent_date=? AND added_ts IS NULL", today):
        tids = kv_get(f"news_tasks:{n['id']}") or []
        date = shift_workday(today)
        added = 0
        for tid in tids:
            t = q1("SELECT * FROM tasks2 WHERE id=?", tid)
            if t and t["status"] != "done" and SHEET_URL and not t["sheet_id"]:
                sid = "tg:" + str(uuid.uuid4())
                if await sheet_call({"action": "add", "id": sid, "date": date, "text": task_line(t)[:200]}):
                    ex("UPDATE tasks2 SET sheet_id=? WHERE id=?", sid, tid)
                    added += 1
        ex("UPDATE news SET added_ts=?, added_by='bot' WHERE id=?", now(), n["id"])
        read = "прочитала" if n["read_ts"] else "НЕ отметила, что прочитала"
        await notify_owner(bot, chat, f"📰 «{n['title']}»: {chat['title']} {read}, задачи в чек-лист сама не внесла — "
                                      f"бот добавил в планировщик: {added}.")


@router.message(owner_only, Command("news"))
async def cmd_news(m: Message):
    """Новости Codo: что в плане, что отправлено, прочитала ли Ольга."""
    if not is_owner(m.from_user.id):
        return
    rows = q("SELECT * FROM news WHERE status!='skip' AND created>=? ORDER BY id DESC LIMIT 10", now() - 14 * 86400)
    if not rows:
        return await m.answer("📰 Новостей за 2 недели нет. Пересылайте посты из Codo сюда — я разберу их в план.")
    st = {"draft": "⏳ в плане, ждёт утверждения", "queued": "⏳ в плане, ждёт утверждения",
          "approved": f"✅ утверждена, уйдёт в {TASKS_HOUR}:00", "sent": "📨 отправлена Ольге"}
    for n in reversed(rows):
        line = f"📰 {n['title']}\n{st.get(n['status'], n['status'])}"
        if n["status"] == "sent":
            line += (" · 👀 прочитала" if n["read_ts"] else " · не отметила «прочитала»")
            if n["added_ts"]:
                line += " · 📋 в чек-листе (" + ("внесла сама" if n["added_by"] == "manager" else "добавил бот") + ")"
        await m.answer(line + "\n\n" + news_text(n).split("\n", 1)[-1][:3000])


@router.message(owner_only, F.forward_origin)
async def on_owner_forward(m: Message, bot: Bot):
    """Пересланное владелицей сообщение = новость на разбор."""
    if not is_owner(m.from_user.id):
        return
    await m.answer("📰 Разбираю новость…")
    await news_ingest(bot, m, from_owner=True)


# ─────────── ФОКУСЫ НА ДЕНЬ ───────────
WD_SHORT = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

# Фокусы по дням недели (утверждены Екатериной). Вечерний анализ может добавить максимум 1 горящий пункт.
FOCUS_BASE = {
    0: ["Коррекция теста до 12:00, фото в группу",
        "Недельная ревизия: посчитать, свести, разобрать, план по недостачам (3 шага)",
        "Пятиминутка с менеджерами: итоги недели и задачи на неделю"],
    1: ["Заказ ВЗ до 12:00",
        "Рейтинг стандартов (РС): разбор ошибок, план, апелляции до 18:00 среды",
        "Аттестации и 2 часа бесед с сотрудниками (обучение стажёров и менеджеров)"],
    3: ["Заказ ВЗ (тесто) до 12:00",
        "График на следующую неделю составлен",
        "МД-аудит и техсостояние: разобрать, ОС, закрыть задачи; к 20:00 отчёт — сломано / сделано / купить (цена) / мастер"],
    4: ["Заказы Зейтц и Деловая Русь (проверить остатки)",
        "Проверка и корректировка графика на след. неделю + курьеры на выходные",
        "Быть на кухне с командой в пик: анализ работы, приоритеты"],
    5: ["Заказ ВЗ до 12:00",
        "Медкнижки, патенты, регистрации, ТД на каждого сотрудника",
        "Задачи на следующую неделю в планер: что сделано, что осталось"],
}

# Навык недели: 1 навык управляющего в неделю — отдельным фокусом каждый день
SKILLS = [
    "Отвечать по схеме «Факт — Цифра — Срок»",
    "Принимать задачу со сроком и закрывать её фактом (фото/цифра)",
    "Начинать день с фокуса №1 и доводить его до конца",
    "Контроль по факту: проверять работу менеджеров по фото и чек-листам",
    "Делегирование менеджерам: кто — что — до когда — как отчитаться",
    "Предупреждать о срыве срока заранее: причина + новый срок",
    "Обратная связь сотруднику: похвала за результат и коррекция по факту",
    "Пятиминутка с менеджерами с итогами и решениями",
]


def week_key(d) -> str:
    from datetime import timedelta
    return (d - timedelta(days=d.weekday())).strftime("%Y-%m-%d")


def current_skill(d) -> str:
    k = week_key(d)
    sk = kv_get(f"skill:{k}")
    if not sk:
        idx = (kv_get("skill_idx", -1) or 0) + 1 if kv_get("skill_idx") is not None else 0
        sk = SKILLS[idx % len(SKILLS)]
        kv_set("skill_idx", idx % len(SKILLS))
        kv_set(f"skill:{k}", sk)
    return sk


FOCUS_SYSTEM = """Ты помощник владелицы пиццерии Додо ({owner}), она управляет удалённо. Управляющая — {other}.
Каждый вечер ты готовишь владелице короткий анализ дня и фокусы управляющей на завтра.

Источники: задачи планировщика на сегодня и на завтра (со статусами), вопросы владелицы без ответа, переписка за последние 2 дня.

Ответь ТОЛЬКО JSON без пояснений и без ```:
{{"analysis":["...","..."],"extra":["..."],"skill_step":"..."}}

analysis — 3–6 коротких пунктов: что сделано сегодня, что зависло или просрочено, где повторяются переносы, на что владелице обратить внимание.
Фокусы на завтра уже заданы владелицей (см. «Обязательные фокусы»). extra — 0 или 1 дополнительный пункт, ТОЛЬКО если есть что-то критичное и горящее (D3, деньги, сорванный срок, проверка), чего нет в обязательных; до 15 слов, в повелительной форме. Если ничего критичного нет — пустой список.
skill_step — одно конкретное упражнение на день по «Навыку недели» (до 15 слов), привязанное к реальным задачам дня."""


def _fmt_tasks(day):
    if not day or not day.get("tasks"):
        return "нет данных"
    out = []
    for t in day["tasks"]:
        extra = f" [{t.get('info')}]" if t.get("info") else ""
        kind = " (чек-лист)" if t.get("kind") == "base" else ""
        out.append(f"- {t.get('status')}: {t.get('text')}{kind}{extra}")
    return "\n".join(out[:120])


def focus_markup(key, chat_id):
    tag = f"{chat_id}:{key}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить утром", callback_data=f"fok:{tag}"),
         InlineKeyboardButton(text="✏️ Изменить", callback_data=f"fed:{tag}")],
        [InlineKeyboardButton(text="❌ Не отправлять", callback_data=f"fno:{tag}")]])


def focus_text_owner(draft):
    lines = [f"🌙 Анализ дня ({draft['title']}):"] + [f"• {a}" for a in draft.get("analysis", [])]
    lines += ["", f"🎯 Фокусы на завтра, {draft['label']}:"]
    lines += [f"{n}. {f}" for n, f in enumerate(draft["focuses"], 1)]
    status = {"draft": "Утвердите — отправлю утром в {h}:00 МСК.", "approved": "✅ Утверждено — отправлю в {h}:00 МСК.",
              "cancelled": "❌ Не отправляю.", "sent": "📨 Отправлено."}[draft["status"]]
    lines += ["", status.format(h=FOCUS_SEND_HOUR, o=draft["title"])]
    return "\n".join(lines)


def focus_text_staff(draft):
    lines = [f"🎯 Фокусы на сегодня, {draft['label']}:"]
    lines += [f"{n}. {f}" for n, f in enumerate(draft["focuses"], 1)]
    if draft.get("sheet"):
        lines += ["", f"Остальные задачи — в таблице на листе «{draft['sheet'].replace('🧭 ', '')}»."]
    return "\n".join(lines)


async def prepare_focus(bot: Bot, chat, key: str, today: str, send: bool = True):
    cid = chat["chat_id"]
    owner, other = owner_name_for(chat), chat["title"]
    d = datetime.strptime(key, "%Y-%m-%d")
    label = f"{WD_SHORT[d.weekday()]} {d.strftime('%d.%m')}"
    plan = await sheet_json({"action": "list", "dates": [today, key]}) or {}
    days = plan.get("days", {})
    items = q("SELECT * FROM items WHERE chat_id=? AND status IN ('open','moved','review') ORDER BY asked_ts", cid)
    q_txt = "\n".join(f"- {i['question']} (спросили {fmt_ts(i['asked_ts'])}, напоминаний {i['reminders']}"
                      f"{', перенесено в таблицу' if i['status'] == 'moved' else ''})" for i in items) or "нет"
    msgs = q("""SELECT * FROM (SELECT * FROM msgs WHERE chat_id=? AND deleted=0 AND who!='bot' AND ts>=?
                ORDER BY ts DESC LIMIT 300) ORDER BY ts""", cid, now() - 2 * 86400)
    chat_txt = "\n".join(f"[{fmt_ts(m['ts'])}] {owner if m['who'] == 'owner' else other}: {m['text']}" for m in msgs) or "нет"
    base = FOCUS_BASE.get(d.weekday(), [])
    skill = current_skill(d)
    user = (f"Сегодня {today}, завтра {key} ({label}).\nНавык недели: {skill}\n\n"
            f"Обязательные фокусы на завтра:\n" + ("\n".join(f"- {b}" for b in base) or "нет") + "\n\n"
            f"Планировщик на СЕГОДНЯ ({(days.get(today) or {}).get('sheet', '')}):\n{_fmt_tasks(days.get(today))}\n\n"
            f"Планировщик на ЗАВТРА ({(days.get(key) or {}).get('sheet', '')}):\n{_fmt_tasks(days.get(key))}\n\n"
            f"Вопросы владелицы без ответа:\n{q_txt}\n\n"
            f"Задачи через бота сегодня: {day_stats_text(cid, today)}\n\nПереписка за 2 дня:\n{chat_txt}")
    raw = await ask_claude(system=FOCUS_SYSTEM.format(owner=owner, other=other),
                           messages=[{"role": "user", "content": user}])
    data = parse_json(raw)
    extra = [str(f).strip() for f in (data.get("extra") or data.get("focuses") or []) if str(f).strip()]
    focuses = base + extra[:1] if base else extra[:3]
    step = str(data.get("skill_step") or "").strip()
    focuses.append(f"🎓 Навык недели: {skill}" + (f" — сегодня: {step}" if step else ""))
    if not focuses:
        raise RuntimeError("Нет фокусов на этот день: " + raw[:200])
    draft = {"chat_id": cid, "key": key, "label": label, "title": other, "status": "draft",
             "analysis": [str(a) for a in data.get("analysis", [])][:6], "focuses": focuses,
             "sheet": (days.get(key) or {}).get("sheet", "")}
    kv_set(f"focus:{cid}:{key}", draft)
    if send:
        await notify_owner(bot, chat, focus_text_owner(draft), focus_markup(key, cid))
    return draft


async def send_focus(bot: Bot, chat, draft) -> bool:
    text = focus_text_staff(draft)
    cid = chat["chat_id"]
    try:
        sent = await bot.send_message(cid, text, business_connection_id=chat["conn_id"])
        store_msg(cid, sent.message_id, "bot", "bot", text, now())
        return True
    except Exception as e:
        log.warning("Фокусы через ваш аккаунт не отправлены: %s", e)
    try:
        await bot.send_message(cid, f"{text}\n\n(от {owner_name_for(chat)})")
        return True
    except Exception as e:
        log.warning("Фокусы через чат бота не отправлены: %s", e)
    await notify_owner(bot, chat, "Не получилось отправить фокусы — Telegram не даёт боту написать первым "
                                  "(в чате не было переписки больше суток). Перешлите сами:\n\n" + text)
    return False


def next_work_day(dt):
    from datetime import timedelta
    n = dt + timedelta(days=1)
    return n if (n.weekday() not in DAYS_OFF and n.weekday() in WEEKDAY_SHEETS) else None


async def focus_tick(bot: Bot):
    return  # заменено вечерним планом на утверждение (plan_review)
    if not FOCUS_ENABLED or is_paused():
        return
    nowm = datetime.now(TZ)
    today = nowm.strftime("%Y-%m-%d")
    for chat in q("SELECT * FROM chats WHERE conn_id IS NOT NULL"):
        cid = chat["chat_id"]
        # вечер: подготовить фокусы на завтра
        tomorrow = next_work_day(nowm)
        if nowm.hour >= FOCUS_PREP_HOUR and tomorrow is not None:
            key = tomorrow.strftime("%Y-%m-%d")
            if not kv_get(f"focus:{cid}:{key}") and not kv_get(f"focus_try:{cid}:{key}"):
                kv_set(f"focus_try:{cid}:{key}", 1)
                try:
                    await prepare_focus(bot, chat, key, today)
                except Exception as e:
                    log.exception("Фокусы не подготовлены: %s", e)
                    await notify_owner(bot, chat, f"Не получилось подготовить фокусы на завтра: {e}")
        # утро: отправить утверждённые
        if nowm.hour >= FOCUS_SEND_HOUR and nowm.weekday() not in DAYS_OFF:
            draft = kv_get(f"focus:{cid}:{today}")
            if draft and draft.get("status") == "approved" and kv_get(f"focus_sent:{cid}:{today}"):
                # утверждено после 10:00 — отправляем фокусы сразу
                await send_focus_list(bot, chat, force=True)
            elif draft and draft.get("status") == "draft" and not draft.get("nagged"):
                draft["nagged"] = 1
                kv_set(f"focus:{cid}:{today}", draft)
                await notify_owner(bot, chat, "Фокусы на сегодня не утверждены — отправить сейчас?",
                                   focus_markup(today, cid))



# ─────────── РАБОЧИЙ ФОРМАТ: ЗАДАЧИ ЧЕРЕЗ БОТА ───────────
INBOX_SYSTEM = """Ты помощник владелицы пиццерии. Она надиктовала поток мыслей для управляющей ({other}).
Преврати его в чёткие пункты. Каждый пункт — либо задача (task), либо вопрос, на который нужен ответ (question).
Сейчас {now} (МСК). Выходные управляющей: {off}.

Правила для задачи:
- text: что сделать, в повелительной форме, конкретно, до 90 символов;
- due: срок в формате "YYYY-MM-DD HH:MM" (МСК). Если срок не назван — ближайший рабочий день управляющей 20:00. "Сегодня" — сегодня, "к вечеру" — 20:00;
- done: как выглядит «готово», до 60 символов (например «фото графика в чат», «скрин из Додо ИС»);
- proof: true, если нужен фото/скрин/файл как доказательство.
Вопрос: text — вопрос до 100 символов, due и done пустые.
Контроль (control): если владелица просит ставить что-то на ежедневный контроль за менеджерами («на контроль», «каждый день до …», «контролировать до …») — text: что проверить у менеджеров, до 90 символов; due: последний день контроля "YYYY-MM-DD 23:59" (если не назван — через 7 дней); done: что считать проверкой (например «фото от менеджера в чат»).
Убери эмоции, повторы и лишнее. Не придумывай задач, которых нет в тексте. Объединяй дубли.
Сначала определи тип сообщения (type):
- "tasks" — поручения/вопросы на конкретные дни;
- "knowledge" — владелица объясняет правило, стандарт, порядок работы, «как надо делать» на будущее (база знаний), без конкретных поручений;
- "both" — есть и правило, и конкретные поручения.
Для "knowledge" items — пустой список.
Ответь ТОЛЬКО JSON без ```: {{"type":"tasks"|"knowledge"|"both","items":[{{"kind":"task"|"question"|"control","text":"...","due":"...","done":"...","proof":true|false}}]}}"""


def main_chat():
    return q1("SELECT * FROM chats WHERE conn_id IS NOT NULL ORDER BY (SELECT COUNT(*) FROM msgs WHERE msgs.chat_id=chats.chat_id) DESC LIMIT 1")


def fmt_due(due: str) -> str:
    if not due:
        return ""
    try:
        d = datetime.strptime(due, "%Y-%m-%d %H:%M")
        today = datetime.now(TZ).date()
        if d.date() == today:
            return f"сегодня {d.strftime('%H:%M')}"
        return f"{WD_SHORT[d.weekday()]} {d.strftime('%d.%m %H:%M')}"
    except Exception:
        return due


def task_line(t) -> str:
    if t["kind"] == "question":
        return f"❓ {t['text']}"
    if t["kind"] == "focus":
        return t["text"]
    if t["kind"] == "control":
        till = t["due"][8:10] + "." + t["due"][5:7] if t["due"] and len(t["due"]) >= 10 else "?"
        return f"👁 {t['text']} — каждый день до {till}" + (f" (готово: {t['done_def']})" if t["done_def"] else "")
    s = f"{t['text']} — до {fmt_due(t['due'])}" if t["due"] else t["text"]
    if t["done_def"]:
        s += f" (готово: {t['done_def']})"
    return s


def draft_markup(tid):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ В очередь", callback_data=f"td:ok:{tid}"),
        InlineKeyboardButton(text="✏️ Изменить", callback_data=f"td:ed:{tid}"),
        InlineKeyboardButton(text="🗑", callback_data=f"td:rm:{tid}")]])


async def inbox_process(bot: Bot, m: Message, text: str, replace_id: int | None = None, photo: str | None = None,
                        force_tasks: bool = False):
    chat = main_chat()
    if not chat:
        return await m.answer("Сначала подключите бота к чату через Telegram для бизнеса.")
    off = ", ".join(WD_SHORT[d] for d in sorted(DAYS_OFF)) or "нет"
    raw = await ask_claude(max_tokens=3000,
                           system=INBOX_SYSTEM.format(other=chat["title"], now=datetime.now(TZ).strftime("%A %d.%m.%Y %H:%M"), off=off),
                           messages=[{"role": "user", "content": text}])
    data = parse_json(raw)
    items = data.get("items", [])
    if not replace_id and not force_tasks and data.get("type") in ("knowledge", "both"):
        pid = now()
        kv_set(f"inbox_pending:{pid}", {"text": text, "items": items, "photo": photo})
        kb = [InlineKeyboardButton(text="📚 В базу знаний", callback_data=f"ib:kb:{pid}")]
        if items:
            kb.append(InlineKeyboardButton(text="📋 В задачи", callback_data=f"ib:tk:{pid}"))
            kb.append(InlineKeyboardButton(text="📚+📋 Оба", callback_data=f"ib:both:{pid}"))
        return await m.answer("Похоже на правило для базы знаний" + (" (и есть поручения)" if items else "") + ". Куда?",
                              reply_markup=InlineKeyboardMarkup(inline_keyboard=[kb]))
    await save_inbox_items(m, chat, items, photo, replace_id)


async def save_inbox_items(m, chat, items, photo=None, replace_id=None):
    if not items:
        return await m.answer("Не нашёл в сообщении задач или вопросов.")
    if replace_id:
        ex("UPDATE tasks2 SET status='cancelled' WHERE id=?", replace_id)
    lines = []
    for i, it in enumerate(items[:15]):
        kind = it.get("kind", "task")
        cur = db.execute("""INSERT INTO tasks2(chat_id,kind,text,due,done_def,proof,status,created,photo,src_msg)
                            VALUES(?,?,?,?,?,?,?,?,?,?)""",
                         (chat["chat_id"], kind, str(it.get("text", ""))[:100],
                          it.get("due") or "", str(it.get("done") or "")[:60], int(bool(it.get("proof"))),
                          "active" if kind == "control" else "queued", now(), photo if i == 0 else None,
                          getattr(m, "message_id", None) if getattr(m, "from_user", None) and is_owner(m.from_user.id) else None))
        db.commit()
        t = q1("SELECT * FROM tasks2 WHERE id=?", cur.lastrowid)
        lines.append(("📌 " if t["kind"] == "task" else "") + task_line(t) + (" 📷" if t["photo"] else ""))
    await m.answer("Добавлено в план (утвердите вечером в 20:00 по Панаме, поправить — /queue):\n" + "\n".join(lines))


@router.callback_query(F.data.regexp(r"^ib:(kb|tk|both):\d+$"))
async def on_inbox_choice(cb: CallbackQuery, bot: Bot):
    if not is_owner(cb.from_user.id):
        return await cb.answer()
    _, action, pid = cb.data.split(":")
    p = kv_get(f"inbox_pending:{pid}")
    if not p:
        return await cb.answer("Уже обработано")
    ex("DELETE FROM kv WHERE k=?", f"inbox_pending:{pid}")
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.answer()
    chat = main_chat()
    if action in ("kb", "both"):
        cid, mid = (chat["chat_id"] if chat else 0), -now()
        store_msg(cid, mid, "owner", "Екатерина", p["text"], now())
        await cb.message.answer("📚 Добавляю в базу знаний…")
        await bz_extract(bot, cid, mid)
    if action in ("tk", "both"):
        await save_inbox_items(cb.message, chat, p["items"], p.get("photo"))


@router.callback_query(F.data.regexp(r"^td:(ok|ed|rm|all):\d+$"))
async def on_draft_button(cb: CallbackQuery):
    if not is_owner(cb.from_user.id):
        return await cb.answer()
    _, action, tid = cb.data.split(":")
    if action == "all":
        n = db.execute("UPDATE tasks2 SET status=CASE kind WHEN 'control' THEN 'active' ELSE 'queued' END "
                       "WHERE status='draft'").rowcount
        db.commit()
        await cb.message.edit_text(f"✅ В очереди: {n}. Уйдут Ольге в {TASKS_HOUR}:00 МСК одним списком.")
        return await cb.answer()
    t = q1("SELECT * FROM tasks2 WHERE id=?", int(tid))
    if not t:
        return await cb.answer("Не найдено")
    if action == "ok":
        ex("UPDATE tasks2 SET status=? WHERE id=?", "active" if t["kind"] == "control" else "queued", t["id"])
        await cb.message.edit_text("✅ В очереди: " + task_line(t))
    elif action == "rm":
        ex("UPDATE tasks2 SET status='cancelled' WHERE id=?", t["id"])
        await cb.message.edit_text("🗑 Удалено: " + task_line(t))
    else:
        kv_set("task_edit", t["id"])
        await cb.message.answer("Напишите, как должно быть (текст, срок, что считать готовым) — переделаю.")
    await cb.answer()


PLAN_HOUR = int(os.getenv("PLAN_HOUR", "4"))   # 04:00 МСК = 20:00 по Панаме


def plan_target():
    """Дата плана: сегодня (если ещё до 10:00 и рабочий день) или следующий рабочий день."""
    from datetime import timedelta
    d = datetime.now(TZ)
    if d.hour >= FOCUS_SEND_HOUR or d.weekday() in DAYS_OFF:
        d += timedelta(days=1)
        while d.weekday() in DAYS_OFF:
            d += timedelta(days=1)
    return d


def plan_ok(date: str) -> bool:
    return (kv_get(f"plan:{date}") or {}).get("status") == "approved"


def plan_text(d) -> str:
    """План Ольги на день: фокусы, поручения, вопросы, новости, переносы — с номерами для /edit и /del."""
    key = d.strftime("%Y-%m-%d")
    chat = main_chat()
    num, out, n = {}, [], 0

    def add(kind, ident, line):
        nonlocal n
        n += 1
        num[n] = [kind, ident]
        out.append(f"{n}. {line}")

    st = (kv_get(f"plan:{key}") or {}).get("status")
    out.append(f"📝 План для Ольги на {WD_SHORT[d.weekday()]} {d.strftime('%d.%m')}"
               + (" — ✅ утверждён" if st == "approved" else " — ждёт утверждения"))
    draft = kv_get(f"focus:{chat['chat_id']}:{key}") if chat else None
    if draft:
        out.append("\n🎯 Фокусы (10:00):")
        for i, f in enumerate(draft["focuses"]):
            add("f", i, f)
    carried_f = q("SELECT * FROM tasks2 WHERE kind='focus' AND status='sent' ORDER BY id")
    for t in carried_f:
        add("t", t["id"], f"🔁 {t['text']} (не сделан, переносится)")
    rows = q("SELECT * FROM tasks2 WHERE status IN ('queued','approved') AND kind IN ('task','question') ORDER BY kind, id")
    carried = q("SELECT * FROM tasks2 WHERE status='sent' AND kind='task' ORDER BY id")
    if rows or carried:
        out.append(f"\n📋 Поручения и вопросы ({TASKS_HOUR}:00):")
        for t in rows:
            add("t", t["id"], task_line(t) + (" 📷" if t["photo"] else ""))
        for t in carried:
            add("t", t["id"], f"🔁 {task_line(t)} [перенос {t['carries']}]")
    news = q("SELECT * FROM news WHERE status IN ('queued','approved') ORDER BY id")
    if news:
        out.append(f"\n📰 Новости Codo ({TASKS_HOUR}:00):")
        for nw in news:
            tasks = json.loads(nw["tasks"] or "[]")
            add("n", nw["id"], nw["title"] + (" — " + "; ".join(x["text"] for x in tasks[:3]) if tasks else ""))
    ctl = q("SELECT * FROM tasks2 WHERE kind='control' AND status='active' ORDER BY id")
    if ctl:
        out.append("\n👁 На контроле:")
        for t in ctl:
            add("t", t["id"], task_line(t))
    kv_set("queue_map", num)
    kv_set("queue_date", key)
    kv_set("plan_shown", now())
    out.append("\n✅ Утвердить: /ok или напишите «утверждаю»\n"
               "✏️ Править можно обычным текстом: «убери 5 и 6, в 3 срок до 15:00»\n"
               "(или командами /edit 3 текст, /del 3)")
    return "\n".join(out)[:4000]


def plan_parts(d) -> list:
    """План по частям: заголовок+фокусы, поручения, новости, контроль, подсказка — отдельными сообщениями."""
    parts = re.split(r"\n(?=\n(?:📋|📰|👁|✅))", plan_text(d))
    return [p.strip() for p in parts if p.strip()]


async def send_plan(send, d, prefix: str = "", photos_bot: Bot | None = None, photos_to: int | None = None):
    """send — корутина-функция отправки текста (m.answer или lambda для bot.send_message)."""
    parts = plan_parts(d)
    if prefix:
        await send(prefix)
    for p in parts:
        await send(p[:4000])
        if p.startswith("📋") and photos_bot and photos_to:
            await plan_photos(photos_bot, photos_to)


async def plan_photos(bot: Bot, chat_id_owner: int):
    num = {int(k): v for k, v in (kv_get("queue_map") or {}).items()}
    for n, (kind, ident) in sorted(num.items()):
        if kind == "t":
            t = q1("SELECT * FROM tasks2 WHERE id=?", ident)
            if t and t["photo"]:
                try:
                    await bot.send_photo(chat_id_owner, t["photo"], caption=f"📷 К пункту {n}: {t['text']}")
                except Exception as e:
                    log.warning("Фото плана: %s", e)


async def plan_review(bot: Bot, chat, d=None):
    """Собирает план на день и присылает владелице на утверждение."""
    d = d or plan_target()
    key = d.strftime("%Y-%m-%d")
    if not kv_get(f"plan:{key}"):
        kv_set(f"plan:{key}", {"status": "draft"})
    if not kv_get(f"focus:{chat['chat_id']}:{key}"):
        from datetime import timedelta
        try:
            await prepare_focus(bot, chat, key, (d - timedelta(days=1)).strftime("%Y-%m-%d"), send=False)
        except Exception as e:
            log.warning("Фокусы для плана: %s", e)
            await api_error_notify(bot, chat, e)
    draft = kv_get(f"focus:{chat['chat_id']}:{key}")
    prefix = ("🌙 Анализ дня:\n" + "\n".join(f"• {a}" for a in draft["analysis"])) if draft and draft.get("analysis") else ""
    for t in owner_targets(chat):
        try:
            await send_plan(lambda x, _t=t: bot.send_message(_t, x), d, prefix, photos_bot=bot, photos_to=t)
        except Exception as e:
            log.warning("План не отправлен: %s", e)


async def plan_nag(bot: Bot, chat, today: str):
    if kv_get(f"plan:{today}") and not plan_ok(today):
        await notify_owner(bot, chat, "⏰ План на сегодня не утверждён — Ольге ничего не ушло.\n"
                                      "/plan — посмотреть, /ok — утвердить и отправить.")


@router.message(owner_only, Command("plan", "queue"))
async def cmd_plan(m: Message, bot: Bot):
    if not is_owner(m.from_user.id):
        return
    chat = main_chat()
    if not chat:
        return await m.answer("Нет подключённого чата.")
    await plan_review(bot, chat)


@router.message(owner_only, Command("focus", "tasks"))
async def cmd_section(m: Message, command: CommandObject, bot: Bot):
    """/focus — только фокусы, /tasks — только поручения и вопросы."""
    if not is_owner(m.from_user.id):
        return
    chat = main_chat()
    d = plan_target()
    key = d.strftime("%Y-%m-%d")
    if command.command == "focus" and not kv_get(f"focus:{chat['chat_id']}:{key}"):
        from datetime import timedelta
        await m.answer("Готовлю фокусы…")
        await prepare_focus(bot, chat, key, (d - timedelta(days=1)).strftime("%Y-%m-%d"), send=False)
    parts = plan_parts(d)
    want = "🎯" if command.command == "focus" else "📋"
    sel = [p for p in parts if ("🎯 Фокусы" in p if want == "🎯" else p.startswith("📋"))]
    await m.answer((sel[0] if sel else ("Фокусов пока нет." if want == "🎯" else "Поручений пока нет."))[:4000]
                   + "\n\nПравить: «убери 2», «в 3 срок до 15:00», «утверждаю».")
    if want == "📋":
        await plan_photos(bot, m.chat.id)


@router.message(owner_only, Command("ok"))
async def cmd_ok(m: Message, bot: Bot):
    if not is_owner(m.from_user.id):
        return
    chat = main_chat()
    key = kv_get("queue_date") or plan_target().strftime("%Y-%m-%d")
    draft = kv_get(f"focus:{chat['chat_id']}:{key}")
    if draft and draft.get("status") in ("draft", "approved"):
        draft["status"] = "approved"
        kv_set(f"focus:{chat['chat_id']}:{key}", draft)
    nt = db.execute("UPDATE tasks2 SET status='approved' WHERE status='queued' AND kind IN ('task','question')").rowcount
    nn = db.execute("UPDATE news SET status='approved' WHERE status='queued'").rowcount
    db.commit()
    kv_set(f"plan:{key}", {"status": "approved", "ts": now()})
    d = datetime.strptime(key, "%Y-%m-%d")
    msg = f"✅ План на {WD_SHORT[d.weekday()]} {d.strftime('%d.%m')} утверждён: фокусы, поручений/вопросов {nt}, новостей {nn}."
    nowm = datetime.now(TZ)
    if key == nowm.strftime("%Y-%m-%d"):  # утвердили уже в течение дня — отправляем то, что пропущено
        if nowm.hour >= FOCUS_SEND_HOUR:
            await send_focus_list(bot, chat, force=True)
        if nowm.hour >= TASKS_HOUR:
            await send_tasks_list(bot, chat, force=True)
        msg += " Отправлено сразу."
    else:
        msg += f" Уйдёт: фокусы в {FOCUS_SEND_HOUR}:00, поручения и новости в {TASKS_HOUR}:00 МСК."
    await m.answer(msg)


PLAN_EDIT_SYSTEM = """Владелица правит план управляющей обычным текстом. Вот план с номерами:
{plan}

Преврати её сообщение в операции. op: "del" (убрать пункт n), "edit" (заменить текст пункта n на text — полностью новый текст пункта,
с учётом её правки), "ok" (утвердить план целиком, если она пишет «утверждаю», «ок», «всё верно, отправляй»).
Если сообщение не про правку плана (новая задача, вопрос) — {{"ops":[]}}.
Ответь ТОЛЬКО JSON без ```: {{"ops":[{{"op":"del"|"edit"|"ok","n":номер,"text":"..."}}]}}"""

PLAN_EDIT_HINT = re.compile(r"\b\d{1,2}\b|утвержда|всё верно|все верно|отправляй|убер|удали|исправ|замени|поменя", re.I)


async def plan_edit_free(bot: Bot, m: Message) -> bool:
    """Правка плана обычным текстом: «убери 5 и 6, в 3 напиши …», «утверждаю»."""
    shown = kv_get("plan_shown") or 0
    if now() - shown > 12 * 3600 or not PLAN_EDIT_HINT.search(m.text or ""):
        return False
    key = kv_get("queue_date")
    plan = plan_text(datetime.strptime(key, "%Y-%m-%d"))
    raw = await ask_claude(max_tokens=1500, system=PLAN_EDIT_SYSTEM.format(plan=plan),
                           messages=[{"role": "user", "content": m.text}])
    ops = parse_json(raw).get("ops", [])
    if not ops:
        return False
    mp = {int(k): v for k, v in (kv_get("queue_map") or {}).items()}
    chat = main_chat()
    done, drop_f = [], []
    approve = False
    for o in ops:
        if o.get("op") == "ok":
            approve = True
            continue
        it = mp.get(int(o.get("n") or 0))
        if not it:
            continue
        kind, ident = it
        if o.get("op") == "edit" and o.get("text"):
            text = str(o["text"])
            if kind == "f":
                draft = kv_get(f"focus:{chat['chat_id']}:{key}")
                draft["focuses"][ident] = text[:100]
                kv_set(f"focus:{chat['chat_id']}:{key}", draft)
            elif kind == "n":
                ex("UPDATE news SET summary=? WHERE id=?", text, ident)
            else:
                ex("UPDATE tasks2 SET text=? WHERE id=?", text[:100], ident)
            done.append(f"✏️ {o.get('n')}: {text[:80]}")
        elif o.get("op") == "del":
            if kind == "f":
                drop_f.append(ident)
            elif kind == "n":
                ex("UPDATE news SET status='skip' WHERE id=?", ident)
            else:
                t = q1("SELECT * FROM tasks2 WHERE id=?", ident)
                ex("UPDATE tasks2 SET status=? WHERE id=?", "finished" if t["kind"] == "control" else "cancelled", ident)
            done.append(f"🗑 {o.get('n')}")
    if drop_f:
        draft = kv_get(f"focus:{chat['chat_id']}:{key}")
        draft["focuses"] = [f for i, f in enumerate(draft["focuses"]) if i not in drop_f]
        kv_set(f"focus:{chat['chat_id']}:{key}", draft)
    if done:
        await send_plan(m.answer, datetime.strptime(key, "%Y-%m-%d"), "Готово:\n" + "\n".join(done))
    if approve:
        await cmd_ok(m, bot)
    return True


@router.message(owner_only, Command("edit"))
async def cmd_edit(m: Message, command: CommandObject):
    if not is_owner(m.from_user.id):
        return
    mt = re.match(r"\s*(\d+)\s+(.+)", command.args or "", re.S)
    if not mt:
        return await m.answer("Формат: /edit 3 новый текст")
    n, text = int(mt.group(1)), mt.group(2).strip()
    it = {int(k): v for k, v in (kv_get("queue_map") or {}).items()}.get(n)
    if not it:
        return await m.answer("Нет такого номера в последнем плане — отправьте /plan.")
    kind, ident = it
    chat = main_chat()
    key = kv_get("queue_date")
    if kind == "f":
        draft = kv_get(f"focus:{chat['chat_id']}:{key}")
        draft["focuses"][ident] = text[:100]
        kv_set(f"focus:{chat['chat_id']}:{key}", draft)
    elif kind == "n":
        ex("UPDATE news SET summary=? WHERE id=?", text, ident)
    else:
        ex("UPDATE tasks2 SET text=? WHERE id=?", text[:100], ident)
    await send_plan(m.answer, datetime.strptime(key, "%Y-%m-%d"), "✏️ Исправлено.")


@router.message(owner_only, Command("del"))
async def cmd_del(m: Message, command: CommandObject):
    if not is_owner(m.from_user.id):
        return
    nums = [int(x) for x in re.findall(r"\d+", command.args or "")]
    if not nums:
        return await m.answer("Укажите номер из плана: /del 3")
    mp = {int(k): v for k, v in (kv_get("queue_map") or {}).items()}
    chat = main_chat()
    key = kv_get("queue_date")
    done, drop_f = [], []
    for n in nums:
        it = mp.get(n)
        if not it:
            continue
        kind, ident = it
        if kind == "f":
            drop_f.append(ident)
        elif kind == "n":
            nw = q1("SELECT * FROM news WHERE id=?", ident)
            ex("UPDATE news SET status='skip' WHERE id=?", ident)
            done.append(f"📰 {nw['title']}")
        else:
            t = q1("SELECT * FROM tasks2 WHERE id=?", ident)
            ex("UPDATE tasks2 SET status=? WHERE id=?", "finished" if t["kind"] == "control" else "cancelled", ident)
            done.append(task_line(t))
    if drop_f:
        draft = kv_get(f"focus:{chat['chat_id']}:{key}")
        done += [f"🎯 {draft['focuses'][i]}" for i in drop_f]
        draft["focuses"] = [f for i, f in enumerate(draft["focuses"]) if i not in drop_f]
        kv_set(f"focus:{chat['chat_id']}:{key}", draft)
    await send_plan(m.answer, datetime.strptime(key, "%Y-%m-%d"),
                    "🗑 Убрано:\n" + "\n".join(f"• {x}" for x in done) if done else "Ничего не убрано.")


@router.message(owner_only, Command("skill"))
async def cmd_skill(m: Message, command: CommandObject):
    if not is_owner(m.from_user.id):
        return
    from datetime import timedelta
    text = (command.args or "").strip()
    d = datetime.now(TZ)
    if d.weekday() == 6:  # в воскресенье ставим на следующую неделю
        d += timedelta(days=1)
    if not text:
        return await m.answer(f"🎓 Навык недели: «{current_skill(d)}». Заменить: /skill текст")
    kv_set(f"skill:{week_key(d)}", text[:120])
    await m.answer(f"🎓 Навык недели с {week_key(d)[8:10]}.{week_key(d)[5:7]}: «{text[:120]}». Будет в фокусах каждый день.")


@router.message(owner_only, Command("send_now"))
async def cmd_send_now(m: Message, bot: Bot):
    if not is_owner(m.from_user.id):
        return
    chat = main_chat()
    n = await send_tasks_list(bot, chat, force=True)
    await m.answer(f"Отправлено задач: {n}." if n else "Нечего отправлять.")


TASKS_HOUR = int(os.getenv("TASKS_HOUR", "12"))        # поручения и вопросы владелицы
ANSWER_BY = os.getenv("ANSWER_BY", "15:00")             # до какого времени отвечать на вопросы


async def _send_checklist(bot: Bot, chat, title: str, tasks, today: str) -> int:
    from aiogram.types import InputChecklist, InputChecklistTask
    cid = chat["chat_id"]
    items = []
    for i, t in enumerate(tasks[:30], 1):
        line = task_line(t)
        if t["status"] == "sent":
            line = "🔁 " + line
        items.append(InputChecklistTask(id=i, text=line[:100]))
    msg = await bot.send_checklist(business_connection_id=chat["conn_id"], chat_id=cid,
                                   checklist=InputChecklist(title=title, tasks=items, others_can_mark_tasks_as_done=True))
    for i, t in enumerate(tasks[:30], 1):
        carries = t["carries"] + (1 if t["status"] == "sent" else 0)
        ex("UPDATE tasks2 SET status='sent', sent_date=?, cl_msg_id=?, cl_task_id=?, carries=? WHERE id=?",
           today, msg.message_id, i, carries, t["id"])
        if t["kind"] == "task" and not t["sheet_id"] and SHEET_URL:
            sid = "tg:" + str(uuid.uuid4())
            if await sheet_call({"action": "add", "id": sid, "date": today, "text": task_line(t)[:200]}):
                ex("UPDATE tasks2 SET sheet_id=? WHERE id=?", sid, t["id"])
    store_msg(cid, msg.message_id, "bot", "bot", title + "\n" + "\n".join(x.text for x in items), now())
    return len(items)


async def send_focus_list(bot: Bot, chat, force=False) -> int:
    """10:00 — фокусы дня отдельным чек-листом."""
    cid = chat["chat_id"]
    d = datetime.now(TZ)
    today = d.strftime("%Y-%m-%d")
    if not plan_ok(today):
        return 0
    if not force and kv_get(f"focus_sent:{cid}:{today}"):
        return 0
    kv_set(f"focus_sent:{cid}:{today}", 1)
    # невыполненные фокусы прошлых дней идут снова (🔁) — пока не будут сделаны
    draft = kv_get(f"focus:{cid}:{today}")
    if draft and draft.get("status") == "approved":
        for f in draft["focuses"]:
            ex("""INSERT INTO tasks2(chat_id,kind,text,due,done_def,status,created)
                  VALUES(?,'focus',?,?,'','queued',?)""", cid, f[:95], today + " 20:00", now())
        draft["status"] = "sent"
        kv_set(f"focus:{cid}:{today}", draft)
    tasks = q("""SELECT * FROM tasks2 WHERE chat_id=? AND kind='focus' AND
                 (status='queued' OR (status='sent' AND sent_date<?)) ORDER BY status='sent' DESC, id""", cid, today)
    if not tasks:
        return 0
    try:
        n = await _send_checklist(bot, chat, f"🎯 Фокусы на {WD_SHORT[d.weekday()]} {d.strftime('%d.%m')} — главное сегодня", tasks, today)
        long = [t["text"] for t in tasks if len(task_line(t)) > 100]
        await bot.send_message(cid, "☝️ Это главное на сегодня — начните с фокуса №1. Отмечайте галочкой, что сделано. "
                                    f"Поручения и вопросы придут в {TASKS_HOUR}:00."
                               + ("\n\nПолностью:\n" + "\n".join(f"• {x}" for x in long) if long else ""),
                               business_connection_id=chat["conn_id"])
        await notify_owner(bot, chat, f"🎯 Фокусы отправлены ({chat['title']}): {n}.")
        return n
    except Exception as e:
        log.warning("Фокусы не отправлены: %s", e)
        await notify_owner(bot, chat, f"Не получилось отправить фокусы: {e}")
        return 0


async def send_tasks_list(bot: Bot, chat, force=False) -> int:
    """12:00 — поручения владелицы (новые + невыполненные) чек-листом и вопросы с ответом до 15:00."""
    cid = chat["chat_id"]
    d = datetime.now(TZ)
    today = d.strftime("%Y-%m-%d")
    if not plan_ok(today):
        return 0
    if not force and kv_get(f"tasks_sent:{cid}:{today}"):
        return 0
    kv_set(f"tasks_sent:{cid}:{today}", 1)
    tasks = q("""SELECT * FROM tasks2 WHERE chat_id=? AND kind='task' AND
                 (status='approved' OR (status='sent' AND sent_date<?)) ORDER BY status='sent' DESC, id""", cid, today)
    questions = q("SELECT * FROM tasks2 WHERE chat_id=? AND kind='question' AND status='approved' ORDER BY id", cid)
    for i, t in enumerate(tasks[:30], 1):  # фото к поручениям — перед списком, с номером пункта
        if t["photo"] and t["status"] == "approved":
            try:
                await bot.send_photo(cid, t["photo"], caption=f"📷 К поручению №{i}: {t['text']}"[:1000],
                                     business_connection_id=chat["conn_id"])
            except Exception as e:
                log.warning("Фото к задаче не отправлено: %s", e)
    sent_n = 0
    if tasks:
        try:
            sent_n += await _send_checklist(bot, chat, f"📋 Поручения на {WD_SHORT[d.weekday()]} {d.strftime('%d.%m')}", tasks, today)
            await bot.send_message(cid, "☝️ Отмечайте галочкой, что сделано. Если не успеваете — напишите заранее "
                                        "причину и новый срок. 🔁 — перенесено со вчера.", business_connection_id=chat["conn_id"])
        except Exception as e:
            log.warning("Поручения не отправлены: %s", e)
            await notify_owner(bot, chat, f"Не получилось отправить поручения: {e}")
    sent_n += await send_control_list(bot, chat, today)
    sent_n += await send_news(bot, chat, today)
    if questions:
        text = f"❓ Вопросы от Екатерины — ответьте, пожалуйста, до {ANSWER_BY}, по номерам:\n" + "\n".join(
            f"{i}. {t['text']}" for i, t in enumerate(questions, 1))
        try:
            msg = await bot.send_message(cid, text, business_connection_id=chat["conn_id"])
            store_msg(cid, msg.message_id, "owner", "Екатерина", text, now())
            ex("UPDATE msgs SET who='owner' WHERE chat_id=? AND msg_id=?", cid, msg.message_id)
            for t in questions:
                ex("UPDATE tasks2 SET status='sent', sent_date=? WHERE id=?", today, t["id"])
                # сразу ставим вопрос на контроль: напоминание в ANSWER_BY, если ответа нет
                ex("""INSERT INTO items(chat_id,question,asked_msg_id,asked_ts,created) VALUES(?,?,?,?,?)""",
                   cid, t["text"], msg.message_id, now(), now())
            sent_n += len(questions)
        except Exception as e:
            log.warning("Вопросы не отправлены: %s", e)
    if sent_n:
        await notify_owner(bot, chat, f"📨 Поручения отправлены ({chat['title']}): задач {len(tasks[:30])}, вопросов {len(questions)}.")
    return sent_n


async def morning_send(bot: Bot, chat, force=False) -> int:
    return await send_focus_list(bot, chat, force) + await send_tasks_list(bot, chat, force)


async def on_checklist_ticks(bot: Bot, m: Message):
    d = m.checklist_tasks_done
    cl_id = d.checklist_message.message_id if d.checklist_message else None
    if not cl_id:
        return
    for tid in d.marked_as_done_task_ids or []:
        if await news_checklist_tick(bot, m, cl_id, tid):
            continue
        if q1("SELECT 1 FROM ctl_log WHERE msg_id=? AND item_id=?", cl_id, tid):
            ex("UPDATE ctl_log SET done=1 WHERE msg_id=? AND item_id=?", cl_id, tid)
            continue
        t = q1("SELECT * FROM tasks2 WHERE chat_id=? AND cl_msg_id=? AND cl_task_id=?", m.chat.id, cl_id, tid)
        if t and t["status"] != "done":
            ex("UPDATE tasks2 SET status='done', done_ts=? WHERE id=?", now(), t["id"])
            if t["sheet_id"]:
                await sheet_call({"action": "status", "id": t["sheet_id"], "status": "Выполнено"})
            if t["proof"] and NUDGE_ENABLED:
                chat = q1("SELECT * FROM chats WHERE chat_id=?", m.chat.id)
                try:
                    await bot.send_message(m.chat.id, f"🤖 Отмечено ✅ «{t['text']}». Пришлите, пожалуйста, "
                                                      f"{t['done_def'] or 'фото или скрин результата'}.",
                                           business_connection_id=chat["conn_id"])
                except Exception:
                    pass
    for tid in d.marked_as_not_done_task_ids or []:
        ex("UPDATE ctl_log SET done=0 WHERE msg_id=? AND item_id=?", cl_id, tid)
        ex("UPDATE tasks2 SET status='sent', done_ts=NULL WHERE chat_id=? AND cl_msg_id=? AND cl_task_id=?",
           m.chat.id, cl_id, tid)


PROMISE = re.compile(r"\b(сделаю|посмотрю|уточню|займусь|отправлю|позвоню|напишу|проверю|закажу|разберусь|доделаю)\b", re.I)
HAS_TIME = re.compile(r"\d{1,2}[:.]\d{2}|\bдо \d|\bк \d|сегодня|завтра|вечер|утр|через|после|\bв (пн|вт|ср|чт|пт|сб|вс)", re.I)
DONE_WORD = re.compile(r"^\s*(готово|сделала|сделано|выполнила|выполнено|отправила)\b", re.I)


async def nudge_check(bot: Bot, m: Message):
    """Дисциплина ответов: обещание без срока → спросить срок; «готово» без факта → попросить фото."""
    try:
        txt = m.text or ""
        chat = q1("SELECT * FROM chats WHERE chat_id=?", m.chat.id)
        k = f"nudge:{m.chat.id}"
        if now() - (kv_get(k, 0) or 0) < 3600:
            return
        reply = None
        if PROMISE.search(txt) and not HAS_TIME.search(txt):
            reply = "🤖 К какому времени? Напишите срок, чтобы задача была принята."
        elif DONE_WORD.search(txt) and len(txt) < 40:
            await asyncio.sleep(180)  # даём время прислать фото следом
            if q1("SELECT 1 FROM msgs WHERE chat_id=? AND who='other' AND media IS NOT NULL AND ts>=?",
                  m.chat.id, int(m.date.timestamp()) - 300):
                return
            reply = "🤖 Отлично! Пришлите, пожалуйста, фото или скрин результата."
        if reply:
            kv_set(k, now())
            sent = await bot.send_message(m.chat.id, reply, business_connection_id=chat["conn_id"],
                                          reply_parameters=ReplyParameters(message_id=m.message_id,
                                                                           allow_sending_without_reply=True))
            store_msg(m.chat.id, sent.message_id, "bot", "bot", reply, now())
    except Exception as e:
        log.warning("nudge: %s", e)


CALL_DAYS = {0, 3}          # созвоны: понедельник и четверг
CHECK_HOUR = int(os.getenv("CHECK_HOUR", "15"))      # сверка статуса


async def send_control_list(bot: Bot, chat, today: str) -> int:
    """Блок 👁 «На контроле за менеджерами» — каждый рабочий день до даты окончания."""
    from aiogram.types import InputChecklist, InputChecklistTask
    cid = chat["chat_id"]
    for t in q("SELECT * FROM tasks2 WHERE chat_id=? AND kind='control' AND status='active' AND substr(due,1,10)<?",
               cid, today):
        ex("UPDATE tasks2 SET status='finished' WHERE id=?", t["id"])
        logs = q("SELECT * FROM ctl_log WHERE task_id=? ORDER BY date", t["id"])
        miss = [l["date"][8:10] + "." + l["date"][5:7] for l in logs if not l["done"]]
        await notify_owner(bot, chat, f"👁 Контроль завершён: {t['text']}\nПроконтролировано {len(logs) - len(miss)} из "
                                      f"{len(logs)} дней" + (f", пропуски: {', '.join(miss)}" if miss else " — без пропусков 👍")
                           + "\nПродлить — напишите боту новый контроль.")
    ctl = q("SELECT * FROM tasks2 WHERE chat_id=? AND kind='control' AND status='active' ORDER BY id", cid)
    if not ctl:
        return 0
    d = datetime.now(TZ)
    items = [InputChecklistTask(id=i, text=task_line(t)[2:][:100]) for i, t in enumerate(ctl[:30], 1)]
    try:
        msg = await bot.send_checklist(business_connection_id=chat["conn_id"], chat_id=cid, checklist=InputChecklist(
            title=f"👁 На контроле за менеджерами · {WD_SHORT[d.weekday()]} {d.strftime('%d.%m')}", tasks=items,
            others_can_mark_tasks_as_done=True))
        for i, t in enumerate(ctl[:30], 1):
            ex("INSERT OR REPLACE INTO ctl_log(task_id,date,msg_id,item_id,done) VALUES(?,?,?,?,0)",
               t["id"], today, msg.message_id, i)
        store_msg(cid, msg.message_id, "bot", "bot", "👁 Контроль: " + "; ".join(x.text for x in items), now())
        return len(items)
    except Exception as e:
        log.warning("Контроль не отправлен: %s", e)
        return 0


def _today_rows(cid, today):
    focus = q("SELECT * FROM tasks2 WHERE chat_id=? AND kind='focus' AND sent_date=? ORDER BY id", cid, today)
    tasks = q("SELECT * FROM tasks2 WHERE chat_id=? AND kind='task' AND status='sent' ORDER BY id", cid)
    ctl = q("""SELECT t.*, l.done ldone FROM ctl_log l JOIN tasks2 t ON t.id=l.task_id
               WHERE t.chat_id=? AND l.date=? ORDER BY t.id""", cid, today)
    openq = q("SELECT * FROM items WHERE chat_id=? AND status='open' ORDER BY asked_ts", cid)
    return focus, tasks, ctl, openq


async def _to_olga(bot: Bot, chat, text: str):
    try:
        sent = await bot.send_message(chat["chat_id"], text, business_connection_id=chat["conn_id"])
        store_msg(chat["chat_id"], sent.message_id, "bot", "bot", text, now())
        return True
    except Exception as e:
        log.warning("Сообщение Ольге не отправлено: %s", e)
        return False


async def status_check(bot: Bot, chat, today):
    """15:00 — сверка по фокусам + напоминание о вопросах."""
    focus, _, _, openq = _today_rows(chat["chat_id"], today)
    lines = []
    if focus:
        lines.append("🔎 Статус по фокусам:")
        lines += [f"{i}. {f['text']} — {'✅' if f['status'] == 'done' else '?'}" for i, f in enumerate(focus, 1)]
        lines.append("Ответьте по номерам: ✅ сделано / ⏳ на каком этапе / ❗ что мешает.")
    asked_today = [x for x in openq if datetime.fromtimestamp(x["asked_ts"], TZ).strftime("%Y-%m-%d") == today]
    if asked_today:
        lines.append(f"\n❓ Жду ответ на вопросы ({len(asked_today)}): " + "; ".join(x["question"] for x in asked_today[:5]))
    if lines and await _to_olga(bot, chat, "\n".join(lines)):
        kv_set(f"check_asked:{chat['chat_id']}:{today}", now())


async def status_followup(bot: Bot, chat, today):
    """16:00 — если на сверку нет ответа, напомнить и сообщить владелице."""
    t0 = kv_get(f"check_asked:{chat['chat_id']}:{today}")
    if not t0:
        return
    if q1("SELECT 1 FROM msgs WHERE chat_id=? AND who='other' AND ts>?", chat["chat_id"], t0):
        return
    await _to_olga(bot, chat, "🔎 Напоминаю: жду статус по фокусам — по номерам ✅ / ⏳ / ❗.")
    await notify_owner(bot, chat, f"🔎 {chat['title']} не ответила на сверку в {CHECK_HOUR}:00.")


async def report_request(bot: Bot, chat, today):
    """17:30 в дни созвона — отчёт к 18:00."""
    await _to_olga(bot, chat, f"📝 К {CALL_HOUR}:00 — отчёт к созвону: по каждому фокусу и поручению что сделано "
                              f"(фото/цифры), что нет и почему, что нужно от Екатерины. Созвон в {CALL_HOUR}:00.")
    kv_set(f"report_asked:{chat['chat_id']}", now())


async def owner_brief(bot: Bot, chat, today):
    """17:50 в дни созвона — шпаргалка владелице."""
    cid = chat["chat_id"]
    focus, tasks, ctl, openq = _today_rows(cid, today)
    t_check = kv_get(f"check_asked:{cid}:{today}") or 0
    t_rep = kv_get(f"report_asked:{cid}", 0) or 0
    replies = q("SELECT ts, text FROM msgs WHERE chat_id=? AND who='other' AND ts>=? ORDER BY ts", cid, t_check or now())
    rep_txt = [r["text"] for r in replies if t_rep and r["ts"] >= t_rep]
    chk_txt = [r["text"] for r in replies if not t_rep or r["ts"] < t_rep]
    nowk = datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
    lines = [f"📞 Через 10 минут созвон ({chat['title']}).",
             "🎯 Фокусы: " + ("; ".join(f"{f['text'][:45]} {'✅' if f['status'] == 'done' else '⏳'}" for f in focus) or "нет"),
             f"🔎 Ответ на сверку {CHECK_HOUR}:00: " + (" / ".join(chk_txt)[:600] if chk_txt else "НЕТ"),
             "📝 Отчёт к созвону: " + (" / ".join(rep_txt)[:800] if rep_txt else "НЕ ПРИСЛАН")]
    over = [t for t in tasks if t["due"] and t["due"] < nowk]
    if over:
        lines.append("⏰ Просрочено: " + "; ".join(f"{t['text']} (перенос {t['carries']})" for t in over[:8]))
    miss = [c for c in ctl if not c["ldone"]]
    if miss:
        lines.append("👁 Контроль не отмечен: " + "; ".join(c["text"] for c in miss[:8]))
    if openq:
        lines.append(f"❓ Вопросов без ответа: {len(openq)}")
    await notify_owner(bot, chat, "\n".join(lines))


async def control_check(bot: Bot, chat, today):
    """18:00 в дни без созвона — проверка контроля и поручений (только Ольге)."""
    _, tasks, ctl, _ = _today_rows(chat["chat_id"], today)
    miss = [c for c in ctl if not c["ldone"]]
    if not tasks and not miss:
        return
    lines = ["🔍 Проверка на 18:00:"]
    if miss:
        lines.append("👁 Контроль менеджеров не отмечен:")
        lines += [f"• {c['text']}" for c in miss]
    if tasks:
        lines.append("📋 Поручения в работе:")
        lines += [f"• {t['text']}" for t in tasks]
    lines.append("Отметьте галочкой выполненное или напишите статус.")
    await _to_olga(bot, chat, "\n".join(lines))


FU_SYSTEM = """Разбери ответ управляющей по списку задач. Сейчас {now} (МСК).
Для каждой задачи по номеру определи:
- status: "done" (сделано), "not_done" (не сделано), "unknown" (про неё ничего не сказано);
- reason: причина, если не сделано (кратко, до 80 символов);
- new_due: новый срок "YYYY-MM-DD HH:MM" (МСК), если назван. «Завтра» без времени — завтра 20:00; «к обеду» — 13:00; «утром» — 11:00. Если срок не назван — пустая строка.
Ответь ТОЛЬКО JSON без ```: {{"items":[{{"n":1,"status":"...","reason":"...","new_due":"..."}}]}}"""


async def ask_followup(bot: Bot, chat, rows, head: str):
    """Спросить по задачам: сделано? если нет — причина и новый срок. Ответ разберём в fu_tick."""
    if not rows:
        return
    text = head + "\n" + "\n".join(f"{i}. {r['text']}" for i, r in enumerate(rows, 1)) + \
        "\n\nПо каждому номеру: ✅ сделано — или причина и новый срок (день и время)."
    if await _to_olga(bot, chat, text):
        kv_set(f"fu_ask:{chat['chat_id']}", {"ts": now(), "ids": [r["id"] for r in rows], "nudged": 0})
        for r in rows:
            ex("UPDATE tasks2 SET ask_ts=? WHERE id=?", now(), r["id"])


async def fu_tick(bot: Bot, chat):
    """Разбор ответа по срокам + повторный вопрос, когда наступил новый срок."""
    cid = chat["chat_id"]
    nowm = datetime.now(TZ)
    pend = kv_get(f"fu_ask:{cid}")
    if pend:
        replies = q("SELECT * FROM msgs WHERE chat_id=? AND who='other' AND ts>? ORDER BY ts", cid, pend["ts"])
        last = replies[-1]["ts"] if replies else 0
        rows = [q1("SELECT * FROM tasks2 WHERE id=?", i) for i in pend["ids"]]
        rows = [r for r in rows if r and r["status"] != "done"]
        if not rows:
            ex("DELETE FROM kv WHERE k=?", f"fu_ask:{cid}")
        elif replies and now() - last >= 300:  # дождались, пока допишет
            ex("DELETE FROM kv WHERE k=?", f"fu_ask:{cid}")
            listing = "\n".join(f"{i}. {r['text']}" for i, r in enumerate(rows, 1))
            raw = await ask_claude(max_tokens=1500, system=FU_SYSTEM.format(now=nowm.strftime("%A %d.%m.%Y %H:%M")),
                                   messages=[{"role": "user", "content": f"Задачи:\n{listing}\n\nОтвет управляющей:\n"
                                              + "\n".join(r["text"] for r in replies)}])
            res = {int(x.get("n", 0)): x for x in parse_json(raw).get("items", [])}
            no_due, report = [], []
            for i, r in enumerate(rows, 1):
                x = res.get(i, {})
                if x.get("status") == "done":
                    ex("UPDATE tasks2 SET status='done', done_ts=? WHERE id=?", now(), r["id"])
                    if r["sheet_id"]:
                        await sheet_call({"action": "status", "id": r["sheet_id"], "status": "Выполнено"})
                    report.append(f"✅ {r['text']}")
                elif x.get("status") == "not_done":
                    nd = (x.get("new_due") or "").strip()
                    ex("UPDATE tasks2 SET reason=?, due=COALESCE(NULLIF(?,''), due) WHERE id=?",
                       (x.get("reason") or "")[:200], nd, r["id"])
                    report.append(f"⏳ {r['text']} — {x.get('reason') or 'без причины'}; новый срок: "
                                  f"{fmt_due(nd) if nd else 'НЕ НАЗВАН'}")
                    if not nd:
                        no_due.append(r)
            if report:
                await notify_owner(bot, chat, f"📝 Ответ по срокам ({chat['title']}):\n" + "\n".join(report))
            if no_due:
                await ask_followup(bot, chat, no_due, "🤖 Не вижу нового срока — к какому дню и времени сделаете?")
        elif not replies and now() - pend["ts"] > 2 * 3600 and not pend.get("nudged"):
            pend["nudged"] = 1
            kv_set(f"fu_ask:{cid}", pend)
            await _to_olga(bot, chat, "🤖 Жду ответ по задачам выше: сделано или причина и новый срок.")
            await notify_owner(bot, chat, f"🔕 {chat['title']} не ответила по срокам задач за 2 часа.")
    # наступил новый срок — спрашиваем снова (только в рабочее время)
    if nowm.weekday() in DAYS_OFF or not (10 <= nowm.hour < 21) or kv_get(f"fu_ask:{cid}"):
        return
    nk = nowm.strftime("%Y-%m-%d %H:%M")
    due_rows = q("""SELECT * FROM tasks2 WHERE chat_id=? AND kind IN ('focus','task') AND status='sent'
                    AND reason IS NOT NULL AND due<>'' AND due<=? AND ask_ts < CAST(strftime('%s', due) AS INTEGER) - 3*3600
                    ORDER BY id""", cid, nk)
    if due_rows:
        await ask_followup(bot, chat, due_rows, "⏰ Наступил срок, который вы назвали:")


async def evening_leftovers(bot: Bot, chat, today):
    """20:30 — только если что-то осталось: вопросы без ответа, контроль, поручения со сроком сегодня."""
    focus, tasks, ctl, openq = _today_rows(chat["chat_id"], today)
    nowk = datetime.now(TZ).strftime("%Y-%m-%d 23:59")
    due = [t for t in tasks if t["due"] and t["due"] <= nowk]
    miss = [c for c in ctl if not c["ldone"]]
    if not (openq or due or miss or q1("SELECT 1 FROM tasks2 WHERE chat_id=? AND kind='focus' AND status='sent'",
                                        chat["chat_id"])):
        return
    open_focus = [f for f in q("SELECT * FROM tasks2 WHERE chat_id=? AND kind='focus' AND status='sent' ORDER BY id",
                                chat["chat_id"])]
    if openq or miss:
        lines = ["🌙 Осталось на сегодня:"]
        lines += [f"❓ {x['question']} — нет ответа" for x in openq[:8]]
        lines += [f"👁 {c['text']} — не отмечено" for c in miss]
        await _to_olga(bot, chat, "\n".join(lines))
    # фокусы и поручения со сроком сегодня — спрашиваем: сделано? нет — причина и новый срок
    await ask_followup(bot, chat, open_focus + [t for t in due if t["id"] not in {f["id"] for f in open_focus}],
                       "🌙 Фокусы и поручения на сегодня — сделано?")
    kv_set(f"report_asked:{chat['chat_id']}", now())


def day_stats_text(cid, date) -> str:
    rows = q("SELECT * FROM tasks2 WHERE chat_id=? AND sent_date=? AND kind!='question'", cid, date)
    if not rows:
        return "задач через бота сегодня не было"
    done = [r for r in rows if r["status"] == "done"]
    left = [r for r in rows if r["status"] == "sent"]
    rep_ts = kv_get(f"report_asked:{cid}", 0) or 0
    report = q("SELECT text FROM msgs WHERE chat_id=? AND who='other' AND ts>=? ORDER BY ts", cid, rep_ts) if rep_ts else []
    s = f"сделано {len(done)} из {len(rows)}"
    if left:
        s += "; не сделано: " + "; ".join(r["text"] for r in left)
    s += "\nОтчёт управляющей: " + (" / ".join(r["text"] for r in report)[:1500] if report else "НЕ ПРИСЛАН")
    return s


async def weekly_stats(bot: Bot, chat):
    from datetime import timedelta
    cid = chat["chat_id"]
    since = (datetime.now(TZ) - timedelta(days=7)).strftime("%Y-%m-%d")
    rows = q("SELECT * FROM tasks2 WHERE chat_id=? AND sent_date>=? AND kind!='question'", cid, since)
    if not rows:
        return
    done = [r for r in rows if r["status"] == "done"]
    ontime = [r for r in done if r["due"] and r["done_ts"] and
              datetime.fromtimestamp(r["done_ts"], TZ).strftime("%Y-%m-%d %H:%M") <= r["due"]]
    carried = [r for r in rows if r["carries"] >= 2 and r["status"] != "done"]
    pct = round(100 * len(done) / len(rows))
    lines = [f"📊 Неделя {chat['title']}: выполнено {len(done)} из {len(rows)} ({pct}%), в срок — {len(ontime)}."]
    if carried:
        lines.append("🔁 Переносится 2+ раза:")
        lines += [f"• {r['text']} (переносов: {r['carries']})" for r in carried[:10]]
    await notify_owner(bot, chat, "\n".join(lines))


async def format_tick(bot: Bot):
    """Расписание дня (МСК). Ср и Вс — выходные Ольги; в Вс вечером — вопрос владелице о контроле на неделю."""
    if is_paused():
        return
    nowm = datetime.now(TZ)
    today = nowm.strftime("%Y-%m-%d")
    hm = (nowm.hour, nowm.minute)
    chats = q("SELECT * FROM chats WHERE conn_id IS NOT NULL")
    if nowm.weekday() == 6 and hm >= (20, 0) and not kv_get(f"sunday_ctl:{today}") and chats:
        kv_set(f"sunday_ctl:{today}", 1)
        act = q("SELECT * FROM tasks2 WHERE kind='control' AND status='active' ORDER BY id")
        await notify_owner(bot, chats[0], "👁 Что ставим на контроль за менеджерами на неделю?\n"
                           "Напишите или наговорите боту, например: «Контроль: фото маркировки ХЦ от менеджера каждый день до 22.11»."
                           + ("\n\nСейчас на контроле:\n" + "\n".join(f"• {task_line(t)}" for t in act) if act else "")
                           + f"\n\n🎓 Навык недели для Ольги: «{current_skill(nowm + __import__('datetime').timedelta(days=1))}»."
                           "\nОставить — ничего не делать; заменить — /skill текст навыка.")
    call_day = nowm.weekday() in CALL_DAYS
    steps = [("plan", PLAN_HOUR <= nowm.hour < 20, lambda c: plan_review(bot, c)),
             ("plan_nag", FOCUS_SEND_HOUR <= nowm.hour < 20, lambda c: plan_nag(bot, c, today)),
             ("sch_focus", FOCUS_SEND_HOUR <= nowm.hour < 20, lambda c: send_focus_list(bot, c)),
             ("sch_tasks", TASKS_HOUR <= nowm.hour < 20, lambda c: send_tasks_list(bot, c)),
             ("check", CHECK_HOUR <= nowm.hour < 20, lambda c: status_check(bot, c, today)),
             ("followup", CHECK_HOUR + 1 <= nowm.hour < 20, lambda c: status_followup(bot, c, today))]
    if call_day:
        steps += [("repreq", hm >= (CALL_HOUR - 1, 30) and nowm.hour < 21, lambda c: report_request(bot, c, today)),
                  ("brief", hm >= (CALL_HOUR - 1, 50) and nowm.hour < 21, lambda c: owner_brief(bot, c, today))]
    else:
        steps += [("ctlcheck", CALL_HOUR <= nowm.hour < 21, lambda c: control_check(bot, c, today))]
    steps += [("newsdl", nowm.hour >= 20 and nowm.hour < 23, lambda c: news_deadline(bot, c, today))]
    steps += [("leftovers", hm >= (REPORT_HOUR, REPORT_MIN) and nowm.hour < 23, lambda c: evening_leftovers(bot, c, today))]
    for chat in chats:
        try:
            await fu_tick(bot, chat)
        except Exception as e:
            log.exception("fu_tick: %s", e)
    if nowm.weekday() in DAYS_OFF:
        return
    for chat in chats:
        cid = chat["chat_id"]
        if nowm.weekday() == 0 and FOCUS_SEND_HOUR <= nowm.hour and not kv_get(f"weekly:{cid}:{today}"):
            kv_set(f"weekly:{cid}:{today}", 1)
            await weekly_stats(bot, chat)
        for key, cond, fn in steps:
            if cond and not kv_get(f"{key}:{cid}:{today}"):
                kv_set(f"{key}:{cid}:{today}", 1)
                try:
                    await fn(chat)
                except Exception as e:
                    log.exception("Шаг %s: %s", key, e)


async def focus_scheduler(bot: Bot):
    while True:
        try:
            await focus_tick(bot)
            await format_tick(bot)
        except Exception as e:
            log.exception("Ошибка фокусов: %s", e)
        await asyncio.sleep(60)


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
    """Список вопросов с номерами 1, 2, 3… — номера действуют для /done до следующего списка."""
    head = "⏸ Напоминания на паузе.\n\n" if is_paused() else ""
    if datetime.now(TZ).weekday() in DAYS_OFF:
        head += "🌙 Сегодня выходной — напоминаний не будет.\n\n"
    base = "SELECT i.*, c.title FROM items i JOIN chats c ON c.chat_id=i.chat_id WHERE i.status=? ORDER BY i.asked_ts"
    rows, rev, mv = q(base, "open"), q(base, "review"), q(base, "moved")
    num, out = {}, [head.rstrip("\n")] if head else []
    n = 0
    if rows:
        out.append("Ждут ответа:")
        for r in rows:
            n += 1; num[n] = r["id"]
            out.append(f"{n}. {r['question']}\n    спросили {fmt_ts(r['asked_ts'])}, напоминаний: {r['reminders']}")
    if rev:
        out.append("\nЖдут вашего решения (ответ голосом/фото):")
        for r in rev:
            n += 1; num[n] = r["id"]
            out.append(f"{n}. {r['question']}")
    if mv:
        out.append("\n📋 Перенесены в таблицу задач:")
        for r in mv:
            n += 1; num[n] = r["id"]
            out.append(f"{n}. {r['question']}")
    kv_set("num_map", num)
    if not n:
        out.append("Открытых вопросов нет 👍")
    else:
        out.append("\nЗакрыть вопрос: /done номер (можно несколько: /done 2 5)")
    return "\n".join(out).strip()


@router.message(owner_only, Command("start", "help"))
async def cmd_help(m: Message):
    if not is_owner(m.from_user.id):
        return await m.answer("Готово ✅ Сюда будут приходить напоминания о рабочих вопросах, "
                              "если не получится написать в обычный чат. Отвечать нужно в обычном чате.")
    await m.answer(
        "Я слежу за вашим чатом и напоминаю, если на ваш вопрос долго нет ответа.\n\n"
        "/status — открытые вопросы\n/check — проверить переписку сейчас\n"
        "/remind — отправить напоминания сейчас\n/done 5 — закрыть вопрос №5 (/done all — все)\n"
        "/pause, /resume — выключить / включить напоминания\n"
        "/export — прислать переписку файлом\n"
        "/plan — план Ольги на утверждение, /ok — утвердить\n"
        "/focus — только фокусы, /tasks — только поручения\n"
        "/news — новости Codo и их статус\n"
        "/queue — что получит Ольга, /edit 3 текст — исправить, /del 2 — убрать\n"
        "/skill текст — навык недели, /send_now — отправить сейчас\n"
        "Фото с подписью сюда = задача с фото; пересланная новость = новость в план\n"
        "/bz — база знаний управляющего, /bz_file — вся база файлом\n"
        "«БЗ: …» сюда или «в базу знаний» в чате с Ольгой — добавить знание\n"
        "Любое сообщение или голосовое сюда = новые задачи для Ольги\n"
        "\n"
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
    nums = [int(x) for x in re.findall(r"\d+", arg)]
    if not nums:
        return await m.answer("Укажите номер из списка /status: /done 5")
    num_map = {int(k): v for k, v in (kv_get("num_map") or {}).items()}
    closed, unknown = [], []
    for n in nums:
        iid = num_map.get(n)
        if not iid:
            unknown.append(n); continue
        it = q1("SELECT * FROM items WHERE id=?", iid)
        if it and it["status"] in ("open", "review", "moved"):
            ex("UPDATE items SET status='dismissed', closed_ts=? WHERE id=?", now(), iid)
            closed.append(f"• {it['question']}")
    msg = ("Закрыто:\n" + "\n".join(closed)) if closed else "Ничего не закрыто."
    if unknown:
        msg += f"\nНет в последнем списке: {', '.join(map(str, unknown))}."
    await m.answer(msg + "\n\n" + status_text())


@router.message(owner_only, Command("export"))
async def cmd_export(m: Message, bot: Bot):
    """Присылает переписку текстовым файлом (для анализа)."""
    if not is_owner(m.from_user.id):
        return
    from aiogram.types import BufferedInputFile
    for chat in q("SELECT * FROM chats"):
        owner, other = owner_name_for(chat), chat["title"]
        rows = q("SELECT * FROM msgs WHERE chat_id=? AND deleted=0 ORDER BY ts", chat["chat_id"])
        if not rows:
            continue
        lines = [f"Переписка: {owner} — {other}, сообщений: {len(rows)}", ""]
        for r in rows:
            who = owner if r["who"] == "owner" else ("БОТ" if r["who"] == "bot" else other)
            lines.append(f"[{datetime.fromtimestamp(r['ts'], TZ).strftime('%d.%m.%Y %H:%M')}] {who}: {r['text']}")
        data = "\n".join(lines).encode("utf-8")
        await bot.send_document(m.chat.id, BufferedInputFile(data, filename=f"perepiska_{other}.txt"),
                                caption=f"Переписка с {other}: {len(rows)} сообщений")


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


@router.callback_query(F.data.regexp(r"^(fok|fed|fno):-?\d+:\d{4}-\d{2}-\d{2}$"))
async def on_focus_button(cb: CallbackQuery, bot: Bot):
    if not is_owner(cb.from_user.id):
        return await cb.answer()
    action, cid, key = cb.data.split(":", 2)
    k = f"focus:{cid}:{key}"
    draft = kv_get(k)
    if not draft or draft.get("status") == "sent":
        await cb.answer("Уже неактуально")
        return await cb.message.edit_reply_markup(reply_markup=None)
    if action == "fed":
        kv_set("focus_edit", k)
        await cb.answer()
        return await cb.message.answer("Пришлите фокусы одним сообщением — каждый с новой строки. Я заменю список.")
    draft["status"] = "approved" if action == "fok" else "cancelled"
    kv_set(k, draft)
    chat = q1("SELECT * FROM chats WHERE chat_id=?", int(cid))
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    # если утро уже наступило — отправляем сразу
    if action == "fok" and key == today and datetime.now(TZ).hour >= FOCUS_SEND_HOUR and chat:
        await send_focus_list(bot, chat, force=True)
        draft = kv_get(k)
    await cb.message.edit_text(focus_text_owner(draft))
    await cb.answer()


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


@router.edited_message(owner_only, F.text | F.caption)
async def on_owner_edit(m: Message, bot: Bot):
    """Владелица отредактировала своё сообщение боту — пересобираем задачи из нового текста."""
    if not is_owner(m.from_user.id):
        return
    old = q("SELECT * FROM tasks2 WHERE src_msg=? AND status IN ('draft','queued','approved','active')", m.message_id)
    sent = q("SELECT * FROM tasks2 WHERE src_msg=? AND status='sent'", m.message_id)
    if not old and not sent:
        return
    for t in old:
        ex("UPDATE tasks2 SET status='cancelled' WHERE id=?", t["id"])
    note = f"\n(уже у Ольги и не изменены: {', '.join(t['text'] for t in sent)})" if sent else ""
    await m.answer("✏️ Сообщение изменено — пересобираю задачи." + note)
    photo = old[0]["photo"] if old and old[0]["photo"] else None
    await inbox_process(bot, m, m.text or m.caption, photo=photo, force_tasks=True)


@router.message(owner_only, F.photo)
async def on_owner_photo(m: Message, bot: Bot):
    if not is_owner(m.from_user.id):
        return
    if m.forward_origin:
        await m.answer("📰 Разбираю новость…")
        return await news_ingest(bot, m, from_owner=True)
    if not (m.caption or "").strip():
        return await m.answer("Подпишите фото — что нужно сделать (подпись к фото), и я поставлю задачу.")
    await m.answer("Разбираю задачу с фото…")
    await inbox_process(bot, m, m.caption, photo=m.photo[-1].file_id)


@router.message(owner_only, F.voice)
async def on_owner_voice(m: Message, bot: Bot):
    if not is_owner(m.from_user.id):
        return
    await m.answer("Расшифровываю…")
    async with _media_lock:
        t = await transcribe(bot, m.voice.file_id)
    if not t:
        return await m.answer("Не удалось расшифровать голосовое.")
    await m.answer(f"🎙 {t[:500]}")
    await inbox_process(bot, m, t)


@router.message(owner_only, F.text)
async def on_owner_text(m: Message, bot: Bot):
    if not is_owner(m.from_user.id) or m.text.startswith("/"):
        return
    be = kv_get("bz_edit")
    if be:
        ex("DELETE FROM kv WHERE k='bz_edit'")
        ex("UPDATE bz SET text=? WHERE id=?", m.text.strip(), int(be))
        return await m.answer("✅ Карточка исправлена.")
    bzn = kv_get("bz_next")
    if bzn and now() - bzn < 600:
        ex("DELETE FROM kv WHERE k='bz_next'")
        m_text = "БЗ: " + m.text
    else:
        m_text = m.text
    if re.match(r"^\s*#?(бз|база знаний|в базу знаний)(\s|[:\-—.,!])", m_text, re.I):
        chat = main_chat()
        cid, mid = (chat["chat_id"] if chat else 0), -now()
        store_msg(cid, mid, "owner", "Екатерина", m_text, now())
        await m.answer("📚 Добавляю в базу знаний…")
        return await bz_extract(bot, cid, mid)
    te = kv_get("task_edit")
    if te:
        ex("DELETE FROM kv WHERE k='task_edit'")
        return await inbox_process(bot, m, m.text, replace_id=int(te))
    k = kv_get("focus_edit")
    if not k and await plan_edit_free(bot, m):
        return
    if not k:
        await m.answer("Разбираю на задачи…")
        return await inbox_process(bot, m, m.text)
    draft = kv_get(k)
    ex("DELETE FROM kv WHERE k='focus_edit'")
    if not draft:
        return await m.answer("Черновик не найден.")
    items = [re.sub(r"^\s*(\d+[.)]|[-•*])\s*", "", ln).strip() for ln in m.text.splitlines()]
    draft["focuses"] = [i for i in items if i][:10]
    draft["status"] = "draft"
    kv_set(k, draft)
    _, cid, key = k.split(":", 2)
    await m.answer(focus_text_owner(draft), reply_markup=focus_markup(key, int(cid)))


# ─────────── ЗАПУСК ───────────
async def main():
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    asyncio.create_task(scheduler(bot))
    asyncio.create_task(focus_scheduler(bot))
    log.info("Бот запущен. Модель: %s", MODEL)
    for c in q("SELECT DISTINCT user_chat_id FROM conn WHERE enabled=1"):
        try:
            await bot.send_message(c["user_chat_id"], "🔄 Бот перезапущен — обновление установлено.")
        except Exception:
            pass
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
