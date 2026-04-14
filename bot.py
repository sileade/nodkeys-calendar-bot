#!/usr/bin/env python3
"""
Nodkeys Calendar & Kindle Bot v3.0
Telegram bot that analyzes forwarded messages using Claude AI
and creates events/tasks/reminders in Apple Calendar (iCloud CalDAV).
Also handles document files — analyzes format via AI, converts with Calibre,
and sends to Kindle via iCloud SMTP.
Features:
- AI-powered message analysis (Claude)
- Auto-detect events, tasks, reminders
- URL detection → "Review" tasks
- Delete events via /delete command or reply
- List today's events via /today
- Kindle: AI format detection, Calibre conversion, SMTP delivery
- Error handling with retries
"""

import os
import re
import sys
import json
import logging
import uuid
import asyncio
import traceback
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from urllib.parse import urlparse

import anthropic
import caldav
from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

# ──────────────────── Configuration ────────────────────
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = int(os.environ.get("TELEGRAM_CHAT_ID", "0"))
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY", "")
CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-20250514")

ICLOUD_USERNAME = os.environ.get("ICLOUD_USERNAME", "")
ICLOUD_PASSWORD = os.environ.get("ICLOUD_PASSWORD", "")
CALDAV_URL = os.environ.get("CALDAV_URL", "https://caldav.icloud.com/")

CALENDAR_MAP = {
    "family": os.environ.get("CALENDAR_FAMILY", "Семейный"),
    "work": os.environ.get("CALENDAR_WORK", "Рабочий"),
    "reminders": os.environ.get("CALENDAR_REMINDERS", "Напоминания ⚠️"),
}

TIMEZONE = ZoneInfo(os.environ.get("TZ", "Europe/Moscow"))

# ──────────────────── Logging ────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("calendar-bot")

# ──────────────────── URL Detection ────────────────────
URL_REGEX = re.compile(
    r'https?://[^\s<>\"\'\)\]]+', re.IGNORECASE
)

def extract_urls(text: str) -> list[str]:
    """Extract all URLs from text."""
    return URL_REGEX.findall(text)

def get_url_domain(url: str) -> str:
    """Get human-readable domain from URL."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.replace("www.", "")
        return domain
    except Exception:
        return url[:50]

# ──────────────────── Claude AI ────────────────────
claude_client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)

SYSTEM_PROMPT = """Ты — умный ассистент-планировщик. Тебе пересылают сообщения из Telegram.
Твоя задача — проанализировать текст и определить:

1. **Тип записи:**
   - `event` — событие с конкретной датой/временем (встреча, визит, мероприятие)
   - `task` — задача с дедлайном (нужно сделать что-то к определённой дате)
   - `reminder` — памятка/напоминание (не забыть, запомнить, обратить внимание)

2. **Календарь:**
   - `work` — рабочие дела, проекты, деловые встречи, IT, техника
   - `family` — семейные дела, личные события, дни рождения, здоровье
   - `reminders` — общие напоминания, заметки, покупки

3. **Детали:**
   - `title` — краткое название (до 50 символов)
   - `description` — полное описание
   - `date` — дата в формате YYYY-MM-DD (если не указана — используй сегодня)
   - `time_start` — время начала HH:MM (если не указано — null)
   - `time_end` — время окончания HH:MM (если не указано — через 1 час от начала, или null)
   - `all_day` — true если событие на весь день
   - `alarm_minutes` — за сколько минут напомнить (по умолчанию: 30 для событий, 60 для задач, 15 для напоминаний)

Текущая дата и время: {current_datetime}
День недели: {weekday}
Часовой пояс: Europe/Moscow

ВАЖНО:
- Если в тексте нет явной даты, попробуй определить из контекста ("завтра", "в пятницу", "через неделю")
- "в пятницу" = ближайшая пятница (если сегодня пятница — следующая)
- "на следующей неделе" = понедельник следующей недели
- Если дата вообще не определяется — поставь сегодняшнюю
- Если время не указано для события — поставь all_day: true
- Для задач без конкретного времени — all_day: true
- Всегда отвечай ТОЛЬКО валидным JSON без markdown-обёртки

Формат ответа (строго JSON):
{{
  "type": "event|task|reminder",
  "calendar": "work|family|reminders",
  "title": "Краткое название",
  "description": "Полное описание",
  "date": "2026-04-15",
  "time_start": "14:00",
  "time_end": "15:00",
  "all_day": false,
  "alarm_minutes": 30,
  "confidence": 0.9,
  "reasoning": "Почему я так решил"
}}"""

WEEKDAYS_RU = {
    0: "Понедельник", 1: "Вторник", 2: "Среда", 3: "Четверг",
    4: "Пятница", 5: "Суббота", 6: "Воскресенье"
}


def analyze_message(text: str) -> dict | None:
    """Send message to Claude for analysis and return structured data."""
    now = datetime.now(TIMEZONE)
    weekday = WEEKDAYS_RU.get(now.weekday(), "")
    prompt = SYSTEM_PROMPT.format(
        current_datetime=now.strftime("%Y-%m-%d %H:%M"),
        weekday=weekday,
    )

    try:
        response = claude_client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1024,
            system=prompt,
            messages=[{"role": "user", "content": text}],
        )
        raw = response.content[0].text.strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1]
            if raw.endswith("```"):
                raw = raw[:-3]
            raw = raw.strip()
        data = json.loads(raw)
        logger.info("Claude analysis: %s", json.dumps(data, ensure_ascii=False))
        return data
    except json.JSONDecodeError as e:
        logger.error("Claude returned invalid JSON: %s — raw: %s", e, raw[:500])
        return None
    except Exception as e:
        logger.error("Claude API error: %s", e)
        return None


# ──────────────────── iCloud CalDAV ────────────────────
_caldav_client = None
_calendars: dict[str, caldav.Calendar] = {}
# Store recent event UIDs for deletion (msg_id -> uid)
_event_store: dict[int, dict] = {}


def get_caldav_client() -> caldav.DAVClient:
    global _caldav_client
    if _caldav_client is None:
        _caldav_client = caldav.DAVClient(
            url=CALDAV_URL, username=ICLOUD_USERNAME, password=ICLOUD_PASSWORD
        )
    return _caldav_client


def reset_caldav_client():
    """Reset CalDAV client on connection errors."""
    global _caldav_client, _calendars
    _caldav_client = None
    _calendars = {}


def get_calendar(name: str) -> caldav.Calendar | None:
    """Get calendar by display name (cached). Handles trailing spaces."""
    if name in _calendars:
        return _calendars[name]
    try:
        client = get_caldav_client()
        principal = client.principal()
        for cal in principal.calendars():
            display = cal.get_display_name()
            _calendars[display] = cal
            _calendars[display.strip()] = cal
            if display == name or display.strip() == name or display.strip() == name.strip():
                return cal
    except Exception as e:
        logger.error("CalDAV error: %s", e)
        reset_caldav_client()
    return None


def create_calendar_event(data: dict) -> str | None:
    """Create a CalDAV event from analyzed data. Returns event UID or None."""
    cal_key = data.get("calendar", "family")
    # Reminders calendar is read-only in iCloud, fallback to family
    if cal_key == "reminders":
        cal_key = "family"
    cal_name = CALENDAR_MAP.get(cal_key, CALENDAR_MAP["family"])
    calendar = get_calendar(cal_name)
    if calendar is None:
        # Fallback: try family calendar
        logger.warning("Calendar '%s' not found, trying family", cal_name)
        cal_name = CALENDAR_MAP["family"]
        calendar = get_calendar(cal_name)
    if calendar is None:
        logger.error("No writable calendar found")
        return None

    uid = str(uuid.uuid4())
    now_str = datetime.now(TIMEZONE).strftime("%Y%m%dT%H%M%S")
    date_str = data.get("date", datetime.now(TIMEZONE).strftime("%Y-%m-%d"))
    title = data.get("title", "Без названия")
    description = data.get("description", "")
    alarm_minutes = data.get("alarm_minutes", 30)
    all_day = data.get("all_day", True)

    if all_day or not data.get("time_start"):
        dtstart = f"DTSTART;VALUE=DATE:{date_str.replace('-', '')}"
        next_day = datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)
        dtend = f"DTEND;VALUE=DATE:{next_day.strftime('%Y%m%d')}"
    else:
        ts = data.get("time_start", "09:00")
        te = data.get("time_end")
        if not te:
            h, m = map(int, ts.split(":"))
            eh = h + 1
            if eh > 23:
                eh = 23
            te = f"{eh:02d}:{m:02d}"
        dtstart = f"DTSTART;TZID=Europe/Moscow:{date_str.replace('-', '')}T{ts.replace(':', '')}00"
        dtend = f"DTEND;TZID=Europe/Moscow:{date_str.replace('-', '')}T{te.replace(':', '')}00"

    type_map = {"event": "\U0001f4c5", "task": "\u2705", "reminder": "\U0001f514"}
    type_emoji = type_map.get(data.get("type", "reminder"), "\U0001f4cc")

    # Escape special chars in description for iCal
    description_escaped = description.replace("\\", "\\\\").replace("\n", "\\n").replace(",", "\\,").replace(";", "\\;")
    title_escaped = title.replace("\\", "\\\\").replace(",", "\\,").replace(";", "\\;")

    vcal = (
        "BEGIN:VCALENDAR\r\n"
        "VERSION:2.0\r\n"
        "PRODID:-//NodkeysBot//CalendarBot//RU\r\n"
        "BEGIN:VEVENT\r\n"
        f"UID:{uid}\r\n"
        f"DTSTAMP:{now_str}\r\n"
        f"{dtstart}\r\n"
        f"{dtend}\r\n"
        f"SUMMARY:{type_emoji} {title_escaped}\r\n"
        f"DESCRIPTION:{description_escaped}\r\n"
        "BEGIN:VALARM\r\n"
        f"TRIGGER:-PT{alarm_minutes}M\r\n"
        "ACTION:DISPLAY\r\n"
        f"DESCRIPTION:Напоминание: {title_escaped}\r\n"
        "END:VALARM\r\n"
        "END:VEVENT\r\n"
        "END:VCALENDAR"
    )

    try:
        calendar.save_event(vcal)
        logger.info("Created event '%s' in '%s' (UID: %s)", title, cal_name, uid)
        return uid
    except Exception as e:
        logger.error("Failed to create event: %s", e)
        reset_caldav_client()
        # Retry once
        try:
            calendar = get_calendar(cal_name)
            if calendar:
                calendar.save_event(vcal)
                logger.info("Created event on retry '%s' in '%s'", title, cal_name)
                return uid
        except Exception as e2:
            logger.error("Retry also failed: %s", e2)
        return None


def delete_event_by_uid(uid: str, cal_key: str = None) -> bool:
    """Delete a calendar event by UID."""
    try:
        client = get_caldav_client()
        principal = client.principal()
        
        calendars_to_search = []
        if cal_key and cal_key in CALENDAR_MAP:
            cal = get_calendar(CALENDAR_MAP[cal_key])
            if cal:
                calendars_to_search = [cal]
        
        if not calendars_to_search:
            calendars_to_search = principal.calendars()
        
        for cal in calendars_to_search:
            try:
                event = cal.event_by_url(f"{cal.url}{uid}.ics")
                event.delete()
                logger.info("Deleted event UID: %s", uid)
                return True
            except Exception:
                pass
            # Try searching
            try:
                for event in cal.events():
                    try:
                        vevent = event.vobject_instance.vevent
                        if str(vevent.uid.value) == uid:
                            event.delete()
                            logger.info("Deleted event UID: %s (found by search)", uid)
                            return True
                    except Exception:
                        continue
            except Exception:
                continue
        
        logger.warning("Event UID %s not found in any calendar", uid)
        return False
    except Exception as e:
        logger.error("Delete error: %s", e)
        reset_caldav_client()
        return False


def search_events_by_title(query: str, days_range: int = 30) -> list[dict]:
    """Search events by title keyword within date range."""
    results = []
    try:
        client = get_caldav_client()
        principal = client.principal()
        now = datetime.now(TIMEZONE)
        start = now - timedelta(days=7)
        end = now + timedelta(days=days_range)
        
        for cal in principal.calendars():
            cal_name = cal.get_display_name().strip()
            try:
                events = cal.search(start=start, end=end, event=True, expand=True)
                for event in events:
                    try:
                        vevent = event.vobject_instance.vevent
                        summary = str(vevent.summary.value) if hasattr(vevent, 'summary') else ""
                        if query.lower() in summary.lower():
                            uid = str(vevent.uid.value) if hasattr(vevent, 'uid') else ""
                            dtstart = vevent.dtstart.value
                            if hasattr(dtstart, 'strftime'):
                                if hasattr(dtstart, 'hour'):
                                    date_str = dtstart.strftime("%d.%m.%Y %H:%M")
                                else:
                                    date_str = dtstart.strftime("%d.%m.%Y")
                            else:
                                date_str = str(dtstart)
                            results.append({
                                "uid": uid,
                                "title": summary,
                                "date": date_str,
                                "calendar": cal_name,
                            })
                    except Exception:
                        continue
            except Exception:
                continue
    except Exception as e:
        logger.error("Search error: %s", e)
        reset_caldav_client()
    return results


def get_today_events() -> list[dict]:
    """Get all events for today."""
    results = []
    try:
        client = get_caldav_client()
        principal = client.principal()
        today_start = datetime.now(TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
        today_end = today_start + timedelta(days=1)
        
        for cal in principal.calendars():
            cal_name = cal.get_display_name().strip()
            try:
                events = cal.search(start=today_start, end=today_end, event=True, expand=True)
                for event in events:
                    try:
                        vevent = event.vobject_instance.vevent
                        summary = str(vevent.summary.value) if hasattr(vevent, 'summary') else "?"
                        uid = str(vevent.uid.value) if hasattr(vevent, 'uid') else ""
                        dtstart = vevent.dtstart.value
                        if hasattr(dtstart, 'hour'):
                            time_str = dtstart.strftime("%H:%M")
                        else:
                            time_str = "весь день"
                        results.append({
                            "uid": uid,
                            "title": summary,
                            "time": time_str,
                            "calendar": cal_name,
                        })
                    except Exception:
                        continue
            except Exception:
                continue
    except Exception as e:
        logger.error("Today events error: %s", e)
        reset_caldav_client()
    return results


def delete_all_test_events() -> int:
    """Delete all events created by the bot (with emoji prefixes)."""
    count = 0
    try:
        client = get_caldav_client()
        principal = client.principal()
        now = datetime.now(TIMEZONE)
        start = now - timedelta(days=30)
        end = now + timedelta(days=365)
        
        for cal in principal.calendars():
            try:
                events = cal.search(start=start, end=end, event=True, expand=False)
                for event in events:
                    try:
                        vevent = event.vobject_instance.vevent
                        summary = str(vevent.summary.value) if hasattr(vevent, 'summary') else ""
                        # Bot-created events have emoji prefixes
                        if any(summary.startswith(e) for e in ["\U0001f4c5", "\u2705", "\U0001f514", "\U0001f4cc", "\U0001f517"]):
                            event.delete()
                            count += 1
                            logger.info("Deleted test event: %s", summary)
                    except Exception:
                        continue
            except Exception:
                continue
    except Exception as e:
        logger.error("Delete all error: %s", e)
        reset_caldav_client()
    return count


# ──────────────────── Telegram Handlers ────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "\U0001f5d3 <b>Nodkeys Calendar Bot v2.0</b>\n\n"
        "Перешлите мне любое сообщение, и я:\n"
        "\u2022 Проанализирую его с помощью AI (Claude)\n"
        "\u2022 Создам событие/задачу/памятку в Apple Calendar\n"
        "\u2022 Ссылки автоматически станут задачами на просмотр\n\n"
        "<b>Команды:</b>\n"
        "/calendars \u2014 список календарей\n"
        "/today \u2014 события на сегодня\n"
        "/delete <i>текст</i> \u2014 найти и удалить запись\n"
        "/cleanup \u2014 удалить все тестовые записи бота\n"
        "/help \u2014 справка",
        parse_mode="HTML",
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "\U0001f4d6 <b>Как пользоваться</b>\n\n"
        "1\ufe0f\u20e3 Перешлите или напишите сообщение\n"
        "2\ufe0f\u20e3 Бот проанализирует текст через Claude AI\n"
        "3\ufe0f\u20e3 Определит тип: событие, задача или памятка\n"
        "4\ufe0f\u20e3 Выберет подходящий календарь\n"
        "5\ufe0f\u20e3 Создаст запись в Apple Calendar\n\n"
        "<b>Примеры:</b>\n"
        "\u2022 \u00abВстреча с клиентом завтра в 15:00\u00bb\n"
        "\u2022 \u00abНе забыть купить подарок маме на ДР 20 апреля\u00bb\n"
        "\u2022 \u00abДедлайн по проекту 1 мая\u00bb\n"
        "\u2022 \u00abhttps://habr.com/article/123\u00bb \u2192 задача на просмотр\n\n"
        "<b>\U0001f5d1 Удаление:</b>\n"
        "\u2022 Ответьте на сообщение бота и напишите \u00abудали\u00bb\n"
        "\u2022 /delete стоматолог \u2014 найдёт и удалит\n"
        "\u2022 /cleanup \u2014 удалит все записи бота\n\n"
        "<b>Календари:</b>\n"
        "\U0001f3e0 Семейный \u2014 личные и семейные дела\n"
        "\U0001f4bc Рабочий \u2014 работа и проекты\n"
        "\u26a0\ufe0f Напоминания \u2014 общие заметки",
        parse_mode="HTML",
    )


def _list_calendars() -> str:
    """List available calendars (blocking)."""
    client = get_caldav_client()
    principal = client.principal()
    cals = principal.calendars()
    text = "\U0001f4c5 <b>Доступные календари:</b>\n\n"
    for cal in cals:
        name = cal.get_display_name().strip()
        text += f"\u2022 {name}\n"
    return text


async def cmd_calendars(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        text = await asyncio.to_thread(_list_calendars)
        await update.message.reply_text(text, parse_mode="HTML")
    except Exception as e:
        reset_caldav_client()
        await update.message.reply_text(f"\u274c Ошибка подключения к iCloud: {e}")


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    events = await asyncio.to_thread(get_today_events)
    today = datetime.now(TIMEZONE).strftime("%d.%m.%Y")
    
    if not events:
        await update.message.reply_text(
            f"\U0001f4c5 <b>События на {today}:</b>\n\nНет событий на сегодня \U0001f389",
            parse_mode="HTML",
        )
        return
    
    text = f"\U0001f4c5 <b>События на {today}:</b>\n\n"
    for i, ev in enumerate(events, 1):
        text += f"{i}. <b>[{ev['calendar']}]</b> {ev['time']} \u2014 {ev['title']}\n"
    
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Search and delete events by keyword."""
    query = " ".join(context.args) if context.args else ""
    
    if not query:
        await update.message.reply_text(
            "\U0001f50d <b>Поиск для удаления</b>\n\n"
            "Использование: /delete <i>ключевое слово</i>\n"
            "Пример: /delete стоматолог",
            parse_mode="HTML",
        )
        return
    
    thinking = await update.message.reply_text(f"\U0001f50d Ищу \u00ab{query}\u00bb...")
    
    results = await asyncio.to_thread(search_events_by_title, query)
    
    if not results:
        await thinking.edit_text(f"\u274c Не найдено записей по запросу \u00ab{query}\u00bb")
        return
    
    if len(results) == 1:
        # Delete immediately
        ev = results[0]
        success = await asyncio.to_thread(delete_event_by_uid, ev["uid"])
        if success:
            await thinking.edit_text(
                f"\U0001f5d1 <b>Удалено:</b>\n"
                f"{ev['title']}\n"
                f"\U0001f4c5 {ev['date']} | {ev['calendar']}",
                parse_mode="HTML",
            )
        else:
            await thinking.edit_text(f"\u274c Не удалось удалить: {ev['title']}")
        return
    
    # Multiple results — show buttons
    text = f"\U0001f50d <b>Найдено {len(results)} записей:</b>\n\n"
    keyboard = []
    for i, ev in enumerate(results[:10]):
        text += f"{i+1}. {ev['title']} ({ev['date']})\n"
        keyboard.append([InlineKeyboardButton(
            f"\U0001f5d1 {i+1}. {ev['title'][:30]}",
            callback_data=f"del:{ev['uid']}"
        )])
    keyboard.append([InlineKeyboardButton("\u274c Отмена", callback_data="del:cancel")])
    
    await thinking.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))


async def cmd_cleanup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete all bot-created events."""
    thinking = await update.message.reply_text("\U0001f9f9 Удаляю все тестовые записи бота...")
    count = await asyncio.to_thread(delete_all_test_events)
    await thinking.edit_text(
        f"\U0001f9f9 <b>Очистка завершена!</b>\n"
        f"Удалено записей: {count}",
        parse_mode="HTML",
    )


async def callback_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle delete button callbacks."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    if data == "del:cancel":
        await query.edit_message_text("\u274c Удаление отменено")
        return
    
    uid = data.replace("del:", "")
    success = await asyncio.to_thread(delete_event_by_uid, uid)
    
    if success:
        await query.edit_message_text("\U0001f5d1 \u2705 Запись удалена!")
    else:
        await query.edit_message_text("\u274c Не удалось удалить запись")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle any text message — analyze and create calendar entry."""
    msg = update.message
    if not msg or not msg.text:
        logger.debug("Skipping: no message or no text")
        return
    
    logger.info("Received message from %s (chat %s): %s",
                msg.from_user.first_name if msg.from_user else '?',
                msg.chat_id,
                msg.text[:80])
    
    # Ignore messages from bots
    if msg.from_user and msg.from_user.is_bot:
        logger.debug("Skipping: message from bot")
        return

    # Only process in the target group or private chat
    chat_id = msg.chat_id
    if CHAT_ID and chat_id != CHAT_ID and msg.chat.type != "private":
        logger.debug("Skipping: wrong chat_id %s (expected %s)", chat_id, CHAT_ID)
        return

    text = msg.text.strip()
    if not text or text.startswith("/"):
        logger.debug("Skipping: empty or command")
        return
    
    logger.info("Processing message: %s", text[:100])
    global _messages_processed
    _messages_processed += 1
    
    # Check if this is a reply to bot's message with "удали" / "delete"
    if msg.reply_to_message and msg.reply_to_message.from_user and msg.reply_to_message.from_user.is_bot:
        lower = text.lower().strip()
        if lower in ("удали", "удалить", "delete", "remove", "отмена", "отмени"):
            reply_id = msg.reply_to_message.message_id
            if reply_id in _event_store:
                ev_data = _event_store[reply_id]
                success = await asyncio.to_thread(delete_event_by_uid, ev_data["uid"], ev_data.get("calendar"))
                if success:
                    await msg.reply_text("\U0001f5d1 \u2705 Запись удалена из календаря!")
                    del _event_store[reply_id]
                else:
                    await msg.reply_text("\u274c Не удалось удалить запись")
            else:
                await msg.reply_text(
                    "\u26a0\ufe0f Не могу найти связанную запись. "
                    "Используйте /delete <ключевое слово> для поиска."
                )
            return

    # ── URL Detection ──
    urls = extract_urls(text)
    if urls and len(text.split()) <= len(urls) * 3 + 5:
        # Message is primarily URLs — create review tasks
        now = datetime.now(TIMEZONE)
        if now.hour < 14:
            task_date = now.strftime("%Y-%m-%d")
            date_label = "сегодня"
        else:
            tomorrow = now + timedelta(days=1)
            task_date = tomorrow.strftime("%Y-%m-%d")
            date_label = "завтра"
        
        results = []
        for url in urls:
            domain = get_url_domain(url)
            data = {
                "type": "task",
                "calendar": "work",
                "title": f"Просмотреть: {domain}",
                "description": f"Ссылка для просмотра: {url}",
                "date": task_date,
                "time_start": None,
                "time_end": None,
                "all_day": True,
                "alarm_minutes": 60,
                "confidence": 1.0,
                "reasoning": f"URL автоматически создан как задача на просмотр ({date_label})",
            }
            uid = await asyncio.to_thread(create_calendar_event, data)
            if uid:
                results.append((url, domain, uid, data))
        
        if results:
            response = f"\U0001f517 <b>Создано {len(results)} задач на просмотр ({date_label}):</b>\n\n"
            for url, domain, uid, data in results:
                response += f"\u2705 <b>{domain}</b>\n<i>{url}</i>\n\n"
                # Store for deletion
                # We'll store after sending the reply
            
            reply_msg = await msg.reply_text(response, parse_mode="HTML", disable_web_page_preview=True)
            
            # Store UIDs for deletion
            for url, domain, uid, data in results:
                _event_store[reply_msg.message_id] = {"uid": uid, "calendar": "work"}
            
            return

    # ── Regular message — Claude analysis ──
    # Build context from forwarded message (v21+ uses forward_origin)
    context_parts = []
    forward_origin = getattr(msg, 'forward_origin', None)
    if forward_origin:
        # Extract info from forward_origin based on type
        origin_type = getattr(forward_origin, 'type', '')
        if hasattr(forward_origin, 'sender_user') and forward_origin.sender_user:
            context_parts.append(f"Переслано от: {forward_origin.sender_user.first_name}")
        if hasattr(forward_origin, 'chat') and forward_origin.chat:
            context_parts.append(f"Переслано из: {forward_origin.chat.title}")
        if hasattr(forward_origin, 'date') and forward_origin.date:
            context_parts.append(f"Дата оригинала: {forward_origin.date.strftime('%Y-%m-%d %H:%M')}")

    full_text = text
    if context_parts:
        full_text = "\n".join(context_parts) + "\n\nТекст сообщения:\n" + text

    thinking_msg = await msg.reply_text("\U0001f914 Анализирую сообщение...")

    # Analyze with Claude (run in thread to avoid blocking event loop)
    logger.info("Calling Claude API for analysis...")
    try:
        data = await asyncio.to_thread(analyze_message, full_text)
    except Exception as e:
        logger.error("analyze_message thread error: %s", e)
        data = None
    logger.info("Claude analysis result: %s", data is not None)
    if data is None:
        await thinking_msg.edit_text(
            "\u274c Не удалось проанализировать сообщение. Попробуйте ещё раз."
        )
        return

    # Create calendar event (run in thread to avoid blocking event loop)
    logger.info("Creating calendar event: %s", data.get('title', '?'))
    try:
        uid = await asyncio.to_thread(create_calendar_event, data)
    except Exception as e:
        logger.error("create_calendar_event thread error: %s", e)
        uid = None

    if uid:
        type_map = {"event": "\U0001f4c5 Событие", "task": "\u2705 Задача", "reminder": "\U0001f514 Напоминание"}
        cal_map = {"work": "\U0001f4bc Рабочий", "family": "\U0001f3e0 Семейный", "reminders": "\u26a0\ufe0f Напоминания"}

        entry_type = type_map.get(data.get("type", ""), data.get("type", ""))
        cal_name = cal_map.get(data.get("calendar", ""), data.get("calendar", ""))
        date_str = data.get("date", "?")
        time_str = ""
        if data.get("time_start") and not data.get("all_day"):
            time_str = f" в {data['time_start']}"
            if data.get("time_end"):
                time_str += f"\u2013{data['time_end']}"

        confidence = data.get("confidence", 0)
        conf_emoji = "\U0001f7e2" if confidence >= 0.8 else "\U0001f7e1" if confidence >= 0.5 else "\U0001f534"

        response = (
            f"\u2705 <b>Создано в Apple Calendar!</b>\n\n"
            f"<b>Тип:</b> {entry_type}\n"
            f"<b>Название:</b> {data.get('title', '?')}\n"
            f"<b>Дата:</b> {date_str}{time_str}\n"
            f"<b>Календарь:</b> {cal_name}\n"
            f"<b>Напоминание:</b> за {data.get('alarm_minutes', 30)} мин\n"
            f"{conf_emoji} Уверенность: {confidence:.0%}\n"
            f"\n\U0001f5d1 <i>Ответьте \u00abудали\u00bb чтобы удалить</i>"
        )
        if data.get("reasoning"):
            response += f"\n\n\U0001f4ad <i>{data['reasoning']}</i>"

        await thinking_msg.edit_text(response, parse_mode="HTML")
        
        # Store UID for deletion
        _event_store[thinking_msg.message_id] = {
            "uid": uid,
            "calendar": data.get("calendar", "reminders"),
            "title": data.get("title", ""),
        }
    else:
        await thinking_msg.edit_text(
            "\u274c Не удалось создать запись в календаре. Проверьте подключение к iCloud."
        )


# ──────────────────── Error Handler ────────────────────
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Log errors and continue."""
    logger.error("Exception while handling an update: %s", context.error)
    logger.error(traceback.format_exc())


# ──────────────────── Health Check Server ────────────────────
_bot_start_time = datetime.now()
_messages_processed = 0

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health' or self.path == '/':
            uptime = (datetime.now() - _bot_start_time).total_seconds()
            try:
                from kindle_handler import get_kindle_stats
                kindle_stats = get_kindle_stats()
            except Exception:
                kindle_stats = {}
            data = {
                "status": "ok",
                "bot": "Nodkeys Calendar & Kindle Bot v3.0",
                "uptime_seconds": int(uptime),
                "messages_processed": _messages_processed,
                **kindle_stats,
            }
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(data).encode())
        elif self.path == '/books':
            try:
                from kindle_handler import get_book_history
                books = get_book_history()
                total = len(books)
                last_book = books[-1]["title"] if books else "No books yet"
                last_sent = books[-1]["sent_at"] if books else "-"
                data = {
                    "total_books": total,
                    "last_book": last_book,
                    "last_sent": last_sent,
                    "books": books,
                }
            except Exception as e:
                data = {
                    "total_books": 0,
                    "last_book": "No books yet",
                    "last_sent": "-",
                    "books": [],
                }
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode())
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        pass  # Suppress health check logs

def start_health_server(port=8085):
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("Health check server started on port %d", port)


# ──────────────────── Main ────────────────────
def main():
    if not BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set!")
        sys.exit(1)
    if not CLAUDE_API_KEY:
        logger.error("CLAUDE_API_KEY not set!")
        sys.exit(1)
    if not ICLOUD_USERNAME or not ICLOUD_PASSWORD:
        logger.error("iCloud credentials not set!")
        sys.exit(1)

    logger.info("Starting Nodkeys Calendar & Kindle Bot v3.0...")
    logger.info("Bot token: ...%s", BOT_TOKEN[-10:])
    logger.info("iCloud user: %s", ICLOUD_USERNAME)
    logger.info("Claude model: %s", CLAUDE_MODEL)

    # Start health check server
    start_health_server(8085)

    # Start iCal proxy server
    from ical_proxy import run_server as run_ical_server
    ical_thread = threading.Thread(target=run_ical_server, args=(8086,), daemon=True)
    ical_thread.start()
    logger.info("iCal proxy started on port 8086")

    app = Application.builder().token(BOT_TOKEN).build()

    # Register handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("calendars", cmd_calendars))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("delete", cmd_delete))
    app.add_handler(CommandHandler("cleanup", cmd_cleanup))
    app.add_handler(CallbackQueryHandler(callback_delete, pattern=r"^del:"))

    # Kindle handlers
    try:
        from kindle_handler import handle_document, callback_kindle
        app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
        app.add_handler(CallbackQueryHandler(callback_kindle, pattern=r"^kindle:"))
        logger.info("Kindle handler registered")
    except ImportError as e:
        logger.warning("Kindle handler not available: %s", e)

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    logger.info("Bot is running! Polling for messages...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=False)


if __name__ == "__main__":
    main()
