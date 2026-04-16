#!/usr/bin/env python3
"""
Nodkeys Calendar & Life Bot v4.1
Telegram bot that analyzes messages using Claude AI and routes them:
- Events/Tasks/Reminders → Apple Calendar (iCloud CalDAV)
- Notes → Apple Notes (iCloud IMAP)
- Diary entries → Apple Notes diary with chronography (one note per day)
- Book search → Flibusta OPDS search + Kindle delivery
- Kindle: AI format detection, Calibre conversion, SMTP delivery
- All through natural language — no commands needed
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
import imaplib
import time as _time
import xml.etree.ElementTree as ET
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, quote
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

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

# ──────────────────── Apple Notes IMAP Config ────────────────────
IMAP_HOST = "imap.mail.me.com"
IMAP_PORT = 993
NOTES_FOLDER = "Notes"

# ──────────────────── Flibusta OPDS Config ────────────────────
FLIBUSTA_BASE_URL = os.environ.get("FLIBUSTA_BASE_URL", "https://flibusta.is")
FLIBUSTA_OPDS_SEARCH = "/opds/opensearch"
FLIBUSTA_TIMEOUT = 15

# MIME type to format mapping
MIME_TO_FORMAT = {
    "application/fb2+zip": "fb2",
    "application/epub+zip": "epub",
    "application/x-mobipocket-ebook": "mobi",
    "application/pdf": "pdf",
    "application/txt+zip": "txt",
    "application/rtf+zip": "rtf",
    "application/html+zip": "html",
}

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


# ══════════════════════════════════════════════════════════
# ██  APPLE NOTES via IMAP
# ══════════════════════════════════════════════════════════

def _get_imap_connection():
    """Create and return an authenticated IMAP connection."""
    conn = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
    conn.login(ICLOUD_USERNAME, ICLOUD_PASSWORD)
    return conn


def create_apple_note(title: str, body_html: str) -> bool:
    """Create a new note in Apple Notes via IMAP.
    
    Args:
        title: Note subject/title
        body_html: HTML body content
    Returns:
        True if successful
    """
    try:
        conn = _get_imap_connection()
        note_id = str(uuid.uuid4())
        now = _time.strftime('%a, %d %b %Y %H:%M:%S %z')

        note_msg = (
            f"Date: {now}\r\n"
            f"From: {ICLOUD_USERNAME}\r\n"
            f"X-Uniform-Type-Identifier: com.apple.mail-note\r\n"
            f"Message-Id: <{note_id}@icloud.com>\r\n"
            f"Content-Type: text/html; charset=utf-8\r\n"
            f"Subject: {title}\r\n"
            f"\r\n"
            f"{body_html}"
        )

        typ, data = conn.append(NOTES_FOLDER, '', imaplib.Time2Internaldate(_time.time()), note_msg.encode('utf-8'))
        conn.logout()

        if typ == 'OK':
            logger.info("Apple Note created: %s", title)
            return True
        else:
            logger.error("Apple Note creation failed: %s %s", typ, data)
            return False
    except Exception as e:
        logger.error("Apple Notes IMAP error: %s", e)
        return False


def create_diary_entry(text: str) -> bool:
    """Add a diary entry to today's note in Apple Notes.
    
    Creates a new note for today or appends to existing one.
    Each entry includes a timestamp (chronography).
    
    Args:
        text: The diary entry text
    Returns:
        True if successful
    """
    now = datetime.now(TIMEZONE)
    today_str = now.strftime("%Y-%m-%d")
    today_display = now.strftime("%d.%m.%Y")
    time_str = now.strftime("%H:%M")
    weekday_names = {
        0: "Понедельник", 1: "Вторник", 2: "Среда", 3: "Четверг",
        4: "Пятница", 5: "Суббота", 6: "Воскресенье"
    }
    weekday = weekday_names.get(now.weekday(), "")
    
    diary_title = f"📔 Дневник — {today_display}, {weekday}"
    
    try:
        conn = _get_imap_connection()
        conn.select(NOTES_FOLDER)
        
        # Search for today's diary note
        typ, data = conn.search(None, 'ALL')
        existing_uid = None
        existing_body = ""
        
        if typ == 'OK' and data[0]:
            msg_ids = data[0].split()
            # Search from newest to oldest
            for msg_id in reversed(msg_ids):
                typ2, msg_data = conn.fetch(msg_id, '(BODY[HEADER.FIELDS (SUBJECT)] BODY[TEXT])')
                if typ2 != 'OK':
                    continue
                # Parse subject
                subject_raw = ""
                body_raw = ""
                for part in msg_data:
                    if isinstance(part, tuple):
                        decoded = part[1].decode('utf-8', errors='replace')
                        if 'HEADER' in part[0].decode('utf-8', errors='replace'):
                            subject_raw = decoded
                        else:
                            body_raw = decoded
                
                # Check if this is today's diary
                if today_str in subject_raw or today_display in subject_raw:
                    if "Дневник" in subject_raw or "📔" in subject_raw:
                        existing_uid = msg_id
                        existing_body = body_raw
                        break
        
        # Build new entry HTML
        new_entry = f'<p><b>🕐 {time_str}</b> — {text.replace(chr(10), "<br>")}</p>'
        
        if existing_uid and existing_body:
            # Append to existing diary note
            # Insert new entry before closing </body> or at the end
            if '</body>' in existing_body.lower():
                idx = existing_body.lower().rfind('</body>')
                updated_body = existing_body[:idx] + '\n<hr>\n' + new_entry + '\n' + existing_body[idx:]
            else:
                updated_body = existing_body + '\n<hr>\n' + new_entry
            
            # Delete old note and create updated one
            conn.store(existing_uid, '+FLAGS', '\\Deleted')
            conn.expunge()
            logger.info("Deleted old diary note, creating updated version")
        else:
            # Create new diary note for today
            updated_body = (
                f"<html><head></head><body>\n"
                f"<h2>📔 Дневник — {today_display}, {weekday}</h2>\n"
                f"<hr>\n"
                f"{new_entry}\n"
                f"</body></html>"
            )
        
        # Create the note
        note_id = str(uuid.uuid4())
        now_rfc = _time.strftime('%a, %d %b %Y %H:%M:%S %z')
        
        note_msg = (
            f"Date: {now_rfc}\r\n"
            f"From: {ICLOUD_USERNAME}\r\n"
            f"X-Uniform-Type-Identifier: com.apple.mail-note\r\n"
            f"Message-Id: <{note_id}@icloud.com>\r\n"
            f"Content-Type: text/html; charset=utf-8\r\n"
            f"Subject: {diary_title}\r\n"
            f"\r\n"
            f"{updated_body}"
        )
        
        typ, data = conn.append(NOTES_FOLDER, '', imaplib.Time2Internaldate(_time.time()), note_msg.encode('utf-8'))
        conn.close()
        conn.logout()
        
        if typ == 'OK':
            logger.info("Diary entry added for %s at %s", today_str, time_str)
            return True
        else:
            logger.error("Diary entry creation failed: %s %s", typ, data)
            return False
            
    except Exception as e:
        logger.error("Diary IMAP error: %s", e)
        return False


# ══════════════════════════════════════════════════════════
# ██  FLIBUSTA BOOK SEARCH via OPDS
# ══════════════════════════════════════════════════════════

def search_flibusta(query: str, limit: int = 10) -> list[dict]:
    """Search books on Flibusta via OPDS catalog.
    
    Args:
        query: Search query string
        limit: Maximum number of results
    Returns:
        List of book dicts with keys: id, title, authors, formats, language, genre
    """
    results = []
    page = 0
    
    while len(results) < limit:
        try:
            url = (
                f"{FLIBUSTA_BASE_URL}{FLIBUSTA_OPDS_SEARCH}"
                f"?searchTerm={quote(query)}&searchType=books&pageNumber={page}"
            )
            logger.info("Flibusta OPDS search: %s", url)
            
            req = Request(url)
            req.add_header('Accept', 'application/atom+xml')
            req.add_header('User-Agent', 'NodkeysBot/4.0')
            
            with urlopen(req, timeout=FLIBUSTA_TIMEOUT) as resp:
                xml_data = resp.read()
            
            root = ET.fromstring(xml_data)
            # OPDS uses Atom namespace
            ns = {
                'atom': 'http://www.w3.org/2005/Atom',
                'dc': 'http://purl.org/dc/terms/',
                'opds': 'http://opds-spec.org/2010/catalog',
            }
            
            entries = root.findall('atom:entry', ns)
            if not entries:
                # Try without namespace (some servers don't use it)
                entries = root.findall('{http://www.w3.org/2005/Atom}entry')
            if not entries:
                entries = root.findall('entry')
            
            if not entries:
                break
            
            for entry in entries:
                if len(results) >= limit:
                    break
                
                book = _parse_opds_entry(entry, ns)
                if book and book.get('id'):
                    results.append(book)
            
            # Check for "next" pagination link
            has_next = False
            for link in root.findall('atom:link', ns) + root.findall('{http://www.w3.org/2005/Atom}link') + root.findall('link'):
                if link.get('rel') == 'next':
                    has_next = True
                    break
            
            if not has_next:
                break
            page += 1
            
        except (URLError, HTTPError) as e:
            logger.error("Flibusta search error: %s", e)
            break
        except ET.ParseError as e:
            logger.error("Flibusta XML parse error: %s", e)
            break
        except Exception as e:
            logger.error("Flibusta unexpected error: %s", e)
            break
    
    logger.info("Flibusta search '%s': found %d results", query, len(results))
    return results


def _parse_opds_entry(entry, ns: dict) -> dict | None:
    """Parse a single OPDS entry into a book dict."""
    book = {"id": 0, "title": "", "authors": [], "formats": [], "language": "", "genre": ""}
    
    # Try multiple namespace approaches
    def find_text(tag):
        for prefix in ['atom:', '{http://www.w3.org/2005/Atom}', '']:
            el = entry.find(f'{prefix}{tag}', ns) if prefix == 'atom:' else entry.find(f'{prefix}{tag}')
            if el is not None and el.text:
                return el.text.strip()
        return ""
    
    book["title"] = find_text("title")
    book["language"] = find_text("language") or find_text("{http://purl.org/dc/terms/}language")
    
    # Authors
    for prefix in ['atom:', '{http://www.w3.org/2005/Atom}', '']:
        if prefix == 'atom:':
            authors = entry.findall(f'{prefix}author', ns)
        else:
            authors = entry.findall(f'{prefix}author')
        for author in authors:
            for np in ['atom:', '{http://www.w3.org/2005/Atom}', '']:
                if np == 'atom:':
                    name_el = author.find(f'{np}name', ns)
                else:
                    name_el = author.find(f'{np}name')
                if name_el is not None and name_el.text:
                    book["authors"].append(name_el.text.strip())
                    break
    
    # Links — extract book ID and formats
    book_id_re = re.compile(r'/b/(\d+)')
    
    for prefix in ['atom:', '{http://www.w3.org/2005/Atom}', '']:
        if prefix == 'atom:':
            links = entry.findall(f'{prefix}link', ns)
        else:
            links = entry.findall(f'{prefix}link')
        
        for link in links:
            href = link.get('href', '')
            rel = link.get('rel', '')
            mime_type = link.get('type', '')
            
            # Extract book ID
            if book["id"] == 0:
                m = book_id_re.search(href)
                if m:
                    book["id"] = int(m.group(1))
            
            # Extract format from acquisition links
            if 'acquisition' in rel or 'open-access' in rel:
                fmt = MIME_TO_FORMAT.get(mime_type)
                if fmt and fmt not in book["formats"]:
                    book["formats"].append(fmt)
            
            # Fallback: extract book ID from alternate link
            if rel == 'alternate' and book["id"] == 0:
                m = book_id_re.search(href)
                if m:
                    book["id"] = int(m.group(1))
    
    # Genre from categories
    for prefix in ['atom:', '{http://www.w3.org/2005/Atom}', '']:
        if prefix == 'atom:':
            cats = entry.findall(f'{prefix}category', ns)
        else:
            cats = entry.findall(f'{prefix}category')
        for cat in cats:
            label = cat.get('label', '')
            if label:
                book["genre"] = label
                break
    
    if not book["id"]:
        return None
    
    return book


def download_book(book_id: int, fmt: str = "epub") -> tuple[bytes | None, str]:
    """Download a book from Flibusta.
    
    Args:
        book_id: Flibusta book ID
        fmt: Format to download (epub, fb2, mobi, etc.)
    Returns:
        Tuple of (file_bytes, filename) or (None, error_message)
    """
    try:
        url = f"{FLIBUSTA_BASE_URL}/b/{book_id}/{fmt}"
        logger.info("Downloading book: %s", url)
        
        req = Request(url)
        req.add_header('User-Agent', 'NodkeysBot/4.0')
        
        with urlopen(req, timeout=60) as resp:
            data = resp.read()
            
            # Try to get filename from Content-Disposition
            cd = resp.headers.get('Content-Disposition', '')
            filename = ""
            if cd:
                fn_match = re.search(r'filename\*?=(?:UTF-8\'\'|"?)([^";]+)"?', cd)
                if fn_match:
                    from urllib.parse import unquote
                    filename = unquote(fn_match.group(1))
            
            if not filename:
                ext = fmt
                if fmt in ('fb2', 'html', 'txt', 'rtf'):
                    ext = f"{fmt}.zip"
                filename = f"book_{book_id}.{ext}"
            
            logger.info("Downloaded %d bytes: %s", len(data), filename)
            return data, filename
    
    except Exception as e:
        logger.error("Book download error: %s", e)
        return None, str(e)


# ══════════════════════════════════════════════════════════
# ██  CLAUDE AI
# ══════════════════════════════════════════════════════════

claude_client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)

SYSTEM_PROMPT = """Ты — умный ассистент-планировщик. Тебе пересылают сообщения из Telegram.
Твоя задача — проанализировать текст и определить, содержит ли он информацию для создания записи.

## Шаг 1: Определи тип действия

Возможные типы:
- `event` — событие с конкретной датой/временем (встреча, визит, мероприятие, звонок)
- `task` — задача с дедлайном (нужно сделать что-то к определённой дате, посмотреть ссылку, купить)
- `reminder` — памятка/напоминание с конкретным содержанием (не забыть взять, позвонить, проверить)
- `note` — заметка, мысль, идея, информация для запоминания (без привязки к дате/времени)
- `diary` — личная запись, мысль о жизни, рефлексия, наблюдение, что-то о себе, дневниковая запись
- `book_search` — пользователь хочет найти книгу, скачать книгу, отправить книгу на Kindle

Запись НЕ нужна если:
- Сообщение слишком абстрактное ("ок", "да", "понял")
- Нет конкретного действия, информации или мысли
- Это просто реакция или эмоция без содержания

Если запись НЕ нужна — верни:
{{
  "action": "skip",
  "reasoning": "Объяснение почему запись не создана"
}}

## Шаг 2: Если запись нужна — определи детали

### Для event/task/reminder (календарные записи):

1. **Календарь:**
   - `work` — рабочие дела, проекты, деловые встречи, IT, техника, программирование
   - `family` — семейные дела, личные события, дни рождения, здоровье, друзья
   - `reminders` — покупки, бытовые дела, общие напоминания

2. **Детали:**
   - `title` — краткое название (до 50 символов), отражающее суть
   - `description` — подробное описание: что именно нужно сделать, контекст, ссылки
   - `date` — дата в формате YYYY-MM-DD
   - `time_start` — время начала HH:MM (null если не указано)
   - `time_end` — время окончания HH:MM (null если не указано)
   - `all_day` — true если событие на весь день или время не указано
   - `alarm_minutes` — за сколько минут напомнить (15 для напоминаний, 30 для событий, 60 для задач)

### Для note (заметка в Apple Notes):
   - `title` — краткий заголовок заметки
   - `content` — полный текст заметки

### Для diary (дневниковая запись):
   - `content` — текст дневниковой записи (будет добавлен в дневник дня с таймстампом)

### Для book_search (поиск книги):
   - `query` — поисковый запрос: название книги и/или автор (извлеки из сообщения)
   - `title` — краткое описание что ищем (для отображения пользователю)

Текущая дата и время: {current_datetime}
День недели: {weekday}
Часовой пояс: Europe/Moscow

## Правила определения даты:
- "завтра" = следующий день
- "послезавтра" = через 2 дня
- "в пятницу" = ближайшая пятница (если сегодня пятница — следующая)
- "на следующей неделе" = понедельник следующей недели
- "через неделю" = +7 дней
- Если дата не указана но есть конкретное действие — ставь сегодня
- Если время не указано для события — ставь all_day: true

## Правила определения типа:
- Если есть дата/время и конкретное действие → event/task/reminder
- Если это информация для запоминания, идея, ссылка с пометкой "запомни/заметь" → note
- Если это личная мысль, наблюдение, рефлексия, запись о жизни → diary
- Если пользователь хочет найти/скачать/прочитать книгу → book_search
- "Запиши заметку", "сохрани", "запомни что..." → note
- "Сегодня я понял что...", "Интересное наблюдение...", личные мысли → diary
- "Найди книгу...", "Скачай...", "Хочу почитать...", "Отправь на Kindle..." → book_search

## Правила определения уверенности (confidence):
- 0.95-1.0 — чёткая дата, время, конкретное действие
- 0.8-0.95 — конкретное действие, дата определяется из контекста
- 0.6-0.8 — действие понятно, но дата/время неточные
- 0.4-0.6 — смысл размытый, но можно интерпретировать
- Ниже 0.4 — лучше вернуть "skip"

Формат ответа (строго JSON):

Для event/task/reminder:
{{
  "action": "create",
  "type": "event|task|reminder",
  "calendar": "work|family|reminders",
  "title": "Краткое название",
  "description": "Подробное описание с контекстом",
  "date": "2026-04-15",
  "time_start": "14:00",
  "time_end": "15:00",
  "all_day": false,
  "alarm_minutes": 30,
  "confidence": 0.9,
  "reasoning": "Анализ"
}}

Для note:
{{
  "action": "create",
  "type": "note",
  "title": "Заголовок заметки",
  "content": "Полный текст заметки",
  "confidence": 0.9,
  "reasoning": "Анализ"
}}

Для diary:
{{
  "action": "create",
  "type": "diary",
  "content": "Текст дневниковой записи",
  "confidence": 0.9,
  "reasoning": "Анализ"
}}

Для book_search:
{{
  "action": "create",
  "type": "book_search",
  "query": "Мастер и Маргарита Булгаков",
  "title": "Поиск: Мастер и Маргарита",
  "confidence": 0.9,
  "reasoning": "Анализ"
}}

ВАЖНО: Всегда отвечай ТОЛЬКО валидным JSON без markdown-обёртки."""


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

    for attempt in range(3):
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
            logger.info("Claude analysis (attempt %d): %s", attempt + 1, json.dumps(data, ensure_ascii=False))
            return data
        except json.JSONDecodeError as e:
            logger.error("Claude returned invalid JSON (attempt %d): %s — raw: %s", attempt + 1, e, raw[:500])
            if attempt < 2:
                continue
            return None
        except Exception as e:
            err_str = str(e).lower()
            logger.error("Claude error (attempt %d): %s", attempt + 1, e)
            if "credit balance" in err_str or "rate" in err_str:
                return None
            if attempt < 2:
                _time.sleep(2 ** attempt)
                continue
            return None
    return None


# ══════════════════════════════════════════════════════════
# ██  iCloud CalDAV
# ══════════════════════════════════════════════════════════

_caldav_client = None
_calendars: dict[str, caldav.Calendar] = {}
# Store recent event UIDs for deletion (msg_id -> uid)
_event_store: dict[int, dict] = {}
_EVENT_STORE_FILE = os.path.join(os.path.dirname(__file__), "data", "event_store.json")

def _save_event_store():
    """Persist event store to disk."""
    try:
        os.makedirs(os.path.dirname(_EVENT_STORE_FILE), exist_ok=True)
        data = {str(k): v for k, v in _event_store.items()}
        with open(_EVENT_STORE_FILE, "w") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception as e:
        logger.error("Failed to save event store: %s", e)

def _load_event_store():
    """Load event store from disk."""
    global _event_store
    try:
        if os.path.exists(_EVENT_STORE_FILE):
            with open(_EVENT_STORE_FILE, "r") as f:
                data = json.load(f)
            _event_store = {int(k): v for k, v in data.items()}
            logger.info("Loaded %d events from store", len(_event_store))
    except Exception as e:
        logger.error("Failed to load event store: %s", e)

_load_event_store()


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
    if cal_key == "reminders":
        cal_key = "family"
    cal_name = CALENDAR_MAP.get(cal_key, CALENDAR_MAP["family"])
    calendar = get_calendar(cal_name)
    if calendar is None:
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

    type_map = {"event": "📅", "task": "✅", "reminder": "🔔"}
    type_emoji = type_map.get(data.get("type", "reminder"), "📌")

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


def get_week_events() -> list[dict]:
    """Get all events for the current week (Mon-Sun)."""
    results = []
    try:
        client = get_caldav_client()
        principal = client.principal()
        now = datetime.now(TIMEZONE)
        monday = now - timedelta(days=now.weekday())
        week_start = monday.replace(hour=0, minute=0, second=0, microsecond=0)
        week_end = week_start + timedelta(days=7)
        
        for cal in principal.calendars():
            cal_name = cal.get_display_name().strip()
            try:
                events = cal.search(start=week_start, end=week_end, event=True, expand=True)
                for event in events:
                    try:
                        vevent = event.vobject_instance.vevent
                        summary = str(vevent.summary.value) if hasattr(vevent, 'summary') else "?"
                        uid = str(vevent.uid.value) if hasattr(vevent, 'uid') else ""
                        dtstart = vevent.dtstart.value
                        if hasattr(dtstart, 'hour'):
                            time_str = dtstart.strftime("%H:%M")
                            date_str = dtstart.strftime("%a %d.%m")
                            sort_key = dtstart.isoformat()
                        else:
                            time_str = "весь день"
                            date_str = dtstart.strftime("%a %d.%m")
                            sort_key = dtstart.isoformat()
                        results.append({
                            "uid": uid,
                            "title": summary,
                            "time": time_str,
                            "date": date_str,
                            "calendar": cal_name,
                            "sort_key": sort_key,
                        })
                    except Exception:
                        continue
            except Exception:
                continue
    except Exception as e:
        logger.error("Week events error: %s", e)
        reset_caldav_client()
    results.sort(key=lambda x: x.get("sort_key", ""))
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
                        if any(summary.startswith(e) for e in ["📅", "✅", "🔔", "📌", "🔗"]):
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


# ══════════════════════════════════════════════════════════
# ██  TELEGRAM HANDLERS
# ══════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🗓 <b>Nodkeys Calendar Bot v4.0</b>\n\n"
        "Перешлите мне любое сообщение, и я:\n"
        "• Проанализирую его с помощью AI (Claude)\n"
        "• Создам событие/задачу/памятку в Apple Calendar\n"
        "• Сохраню заметку в Apple Notes\n"
        "• Добавлю запись в дневник\n"
        "• Ссылки автоматически станут задачами на просмотр\n\n"
        "<b>Команды:</b>\n"
        "/calendars — список календарей\n"
        "/today — события на сегодня\n"
        "/delete <i>текст</i> — найти и удалить запись\n"
        "/book <i>название</i> — найти книгу\n"
        "/cleanup — удалить все тестовые записи бота\n"
        "/help — справка",
        parse_mode="HTML",
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 <b>Как пользоваться</b>\n\n"
        "1️⃣ Перешлите или напишите сообщение\n"
        "2️⃣ Бот проанализирует текст через Claude AI\n"
        "3️⃣ Определит тип: событие, задача, памятка, заметка или дневник\n"
        "4️⃣ Выберет подходящее действие\n"
        "5️⃣ Создаст запись\n\n"
        "<b>Типы записей:</b>\n"
        "📅 <b>Событие</b> — встреча, визит, мероприятие → Apple Calendar\n"
        "✅ <b>Задача</b> — дело с дедлайном → Apple Calendar\n"
        "🔔 <b>Напоминание</b> — не забыть что-то → Apple Calendar\n"
        "📝 <b>Заметка</b> — идея, информация → Apple Notes\n"
        "📔 <b>Дневник</b> — личная мысль, наблюдение → Apple Notes (дневник дня)\n"
        "📚 <b>Книга</b> — поиск и скачивание книг → Флибуста + Kindle\n\n"
        "<b>Примеры:</b>\n"
        "• «Встреча с клиентом завтра в 15:00» → 📅 событие\n"
        "• «Запомни: пароль от WiFi — 12345» → 📝 заметка\n"
        "• «Сегодня понял что нужно больше отдыхать» → 📔 дневник\n"
        "• «https://habr.com/article/123» → ✅ задача на просмотр\n\n"
        "<b>📚 Поиск книг:</b>\n"
        "• «Найди книгу Мастер и Маргарита» → 📚 поиск + Kindle\n"
        "• «Хочу почитать Стивена Кинга» → 📚 поиск\n\n"
        "<b>🗑 Удаление:</b>\n"
        "• Ответьте на сообщение бота и напишите «удали»\n"
        "• /delete стоматолог — найдёт и удалит\n"
        "• /cleanup — удалит все записи бота",
        parse_mode="HTML",
    )


def _list_calendars() -> str:
    """List available calendars (blocking)."""
    client = get_caldav_client()
    principal = client.principal()
    cals = principal.calendars()
    text = "📅 <b>Доступные календари:</b>\n\n"
    for cal in cals:
        name = cal.get_display_name().strip()
        text += f"• {name}\n"
    return text


async def cmd_calendars(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        text = await asyncio.to_thread(_list_calendars)
        await update.message.reply_text(text, parse_mode="HTML")
    except Exception as e:
        reset_caldav_client()
        await update.message.reply_text(f"❌ Ошибка подключения к iCloud: {e}")


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    events = await asyncio.to_thread(get_today_events)
    today = datetime.now(TIMEZONE).strftime("%d.%m.%Y")
    
    if not events:
        await update.message.reply_text(
            f"📅 <b>События на {today}:</b>\n\nНет событий на сегодня 🎉",
            parse_mode="HTML",
        )
        return
    
    text = f"📅 <b>События на {today}:</b>\n\n"
    for i, ev in enumerate(events, 1):
        text += f"{i}. <b>[{ev['calendar']}]</b> {ev['time']} — {ev['title']}\n"
    
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Search and delete events by keyword."""
    query = " ".join(context.args) if context.args else ""
    
    if not query:
        await update.message.reply_text(
            "🔍 <b>Поиск для удаления</b>\n\n"
            "Использование: /delete <i>ключевое слово</i>\n"
            "Пример: /delete стоматолог",
            parse_mode="HTML",
        )
        return
    
    thinking = await update.message.reply_text(f"🔍 Ищу «{query}»...")
    
    results = await asyncio.to_thread(search_events_by_title, query)
    
    if not results:
        await thinking.edit_text(f"❌ Не найдено записей по запросу «{query}»")
        return
    
    if len(results) == 1:
        ev = results[0]
        success = await asyncio.to_thread(delete_event_by_uid, ev["uid"])
        if success:
            await thinking.edit_text(
                f"🗑 <b>Удалено:</b>\n"
                f"{ev['title']}\n"
                f"📅 {ev['date']} | {ev['calendar']}",
                parse_mode="HTML",
            )
        else:
            await thinking.edit_text(f"❌ Не удалось удалить: {ev['title']}")
        return
    
    text = f"🔍 <b>Найдено {len(results)} записей:</b>\n\n"
    keyboard = []
    for i, ev in enumerate(results[:10]):
        text += f"{i+1}. {ev['title']} ({ev['date']})\n"
        keyboard.append([InlineKeyboardButton(
            f"🗑 {i+1}. {ev['title'][:30]}",
            callback_data=f"del:{ev['uid']}"
        )])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="del:cancel")])
    
    await thinking.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))


async def cmd_cleanup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete all bot-created events."""
    thinking = await update.message.reply_text("🧹 Удаляю все тестовые записи бота...")
    count = await asyncio.to_thread(delete_all_test_events)
    await thinking.edit_text(
        f"🧹 <b>Очистка завершена!</b>\n"
        f"Удалено записей: {count}",
        parse_mode="HTML",
    )


async def callback_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle delete button callbacks."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    if data == "del:cancel":
        await query.edit_message_text("❌ Удаление отменено")
        return
    
    uid = data.replace("del:", "")
    success = await asyncio.to_thread(delete_event_by_uid, uid)
    
    if success:
        await query.edit_message_text("🗑 ✅ Запись удалена!")
    else:
        await query.edit_message_text("❌ Не удалось удалить запись")


# ──────────────────── Book Search Command ────────────────────

async def cmd_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Search for books on Flibusta and offer to send to Kindle."""
    query = " ".join(context.args) if context.args else ""
    
    if not query:
        await update.message.reply_text(
            "📚 <b>Поиск книг</b>\n\n"
            "Использование: /book <i>название или автор</i>\n"
            "Пример: /book Мастер и Маргарита\n"
            "Пример: /book Стивен Кинг",
            parse_mode="HTML",
        )
        return
    
    thinking = await update.message.reply_text(f"🔍 Ищу «{query}» на Флибусте...")
    
    try:
        results = await asyncio.to_thread(search_flibusta, query, 8)
    except Exception as e:
        logger.error("Book search thread error: %s", e)
        results = []
    
    if not results:
        await thinking.edit_text(
            f"❌ По запросу «{query}» ничего не найдено.\n\n"
            f"💡 Попробуйте другой запрос или проверьте написание.",
        )
        return
    
    # Store results in context for callback
    context.user_data["book_search_results"] = results
    
    text = f"📚 <b>Найдено {len(results)} книг:</b>\n\n"
    keyboard = []
    
    for i, book in enumerate(results):
        authors_str = ", ".join(book["authors"][:2]) if book["authors"] else "Неизвестный автор"
        formats_str = ", ".join(book["formats"][:4]).upper() if book["formats"] else "?"
        lang = f" [{book['language']}]" if book.get("language") else ""
        
        text += f"{i+1}. <b>{book['title'][:60]}</b>\n"
        text += f"   ✍️ {authors_str}{lang}\n"
        text += f"   📄 {formats_str}\n\n"
        
        # Prefer epub, then fb2, then mobi
        preferred_fmt = "epub"
        if "epub" in book["formats"]:
            preferred_fmt = "epub"
        elif "fb2" in book["formats"]:
            preferred_fmt = "fb2"
        elif "mobi" in book["formats"]:
            preferred_fmt = "mobi"
        elif book["formats"]:
            preferred_fmt = book["formats"][0]
        
        keyboard.append([InlineKeyboardButton(
            f"📖 {i+1}. {book['title'][:30]}",
            callback_data=f"book:{i}:{preferred_fmt}"
        )])
    
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="book:cancel")])
    
    await thinking.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def callback_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle book selection callback — download and send to Kindle."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    if data == "book:cancel":
        await query.edit_message_text("❌ Поиск отменён")
        context.user_data.pop("book_search_results", None)
        return
    
    # Parse callback: book:{index}:{format}
    parts = data.split(":")
    if len(parts) < 3:
        await query.edit_message_text("❌ Ошибка данных")
        return
    
    try:
        book_idx = int(parts[1])
        book_fmt = parts[2]
    except (ValueError, IndexError):
        await query.edit_message_text("❌ Ошибка данных")
        return
    
    results = context.user_data.get("book_search_results", [])
    if book_idx >= len(results):
        await query.edit_message_text("❌ Книга не найдена в результатах")
        return
    
    book = results[book_idx]
    authors_str = ", ".join(book["authors"][:2]) if book["authors"] else "Неизвестный автор"
    
    await query.edit_message_text(
        f"⏳ Скачиваю книгу...\n\n"
        f"📖 <b>{book['title']}</b>\n"
        f"✍️ {authors_str}\n"
        f"📄 Формат: {book_fmt.upper()}",
        parse_mode="HTML",
    )
    
    # Download the book
    try:
        file_data, filename = await asyncio.to_thread(download_book, book["id"], book_fmt)
    except Exception as e:
        logger.error("Book download thread error: %s", e)
        file_data = None
        filename = str(e)
    
    if file_data is None:
        await query.edit_message_text(
            f"❌ Не удалось скачать книгу.\n\n"
            f"Ошибка: {filename[:200]}",
        )
        return
    
    # Save to temp file
    import tempfile
    tmp_dir = "/tmp/kindle_files"
    os.makedirs(tmp_dir, exist_ok=True)
    tmp_path = os.path.join(tmp_dir, filename)
    
    with open(tmp_path, 'wb') as f:
        f.write(file_data)
    
    logger.info("Book saved to: %s (%d bytes)", tmp_path, len(file_data))
    
    # Check if we need to unzip (fb2, txt, rtf, html come as zip)
    actual_path = tmp_path
    if filename.endswith('.zip'):
        try:
            import zipfile
            with zipfile.ZipFile(tmp_path, 'r') as zf:
                names = zf.namelist()
                if names:
                    # Extract the first file
                    extracted = zf.extract(names[0], tmp_dir)
                    actual_path = extracted
                    filename = os.path.basename(extracted)
                    logger.info("Extracted from zip: %s", actual_path)
        except Exception as e:
            logger.error("Unzip error: %s", e)
            # Continue with the zip file itself
    
    # Determine if conversion is needed
    from pathlib import Path
    ext = Path(actual_path).suffix.lower()
    
    kindle_native = {".epub", ".pdf", ".txt", ".doc", ".docx", ".rtf", ".htm", ".html"}
    calibre_supported = {".fb2", ".mobi", ".azw", ".azw3", ".djvu", ".cbz", ".cbr", ".chm", ".lit", ".odt"}
    
    needs_conversion = ext not in kindle_native
    convert_to = "epub" if needs_conversion else None
    
    if needs_conversion and ext not in calibre_supported:
        # Can't convert, try sending as-is
        needs_conversion = False
    
    send_path = actual_path
    converted = False
    
    if needs_conversion and convert_to:
        await query.edit_message_text(
            f"🔄 Конвертирую {ext.upper()} → EPUB...\n\n"
            f"📖 <b>{book['title']}</b>",
            parse_mode="HTML",
        )
        
        try:
            from kindle_handler import convert_with_calibre
            converted_path = convert_with_calibre(actual_path, convert_to)
            if converted_path and os.path.exists(converted_path):
                send_path = converted_path
                converted = True
            else:
                logger.warning("Conversion failed, trying to send original")
        except Exception as e:
            logger.error("Conversion error: %s", e)
    
    # Send to Kindle
    await query.edit_message_text(
        f"📧 Отправляю на Kindle...\n\n"
        f"📖 <b>{book['title']}</b>\n"
        f"✍️ {authors_str}",
        parse_mode="HTML",
    )
    
    try:
        from kindle_handler import send_email_to_kindle, add_book_to_history, store_book_file, KINDLE_EMAIL
        
        subject = f"{book['title']} - {authors_str}"
        success, error_msg = send_email_to_kindle(send_path, KINDLE_EMAIL, subject)
        
        if success:
            # Store book permanently
            book_id_num = len(add_book_to_history.__defaults__ or []) + 1
            try:
                from kindle_handler import get_books_history
                book_id_num = len(get_books_history()) + 1
            except Exception:
                book_id_num = 1
            
            stored_file = store_book_file(send_path, book_id_num)
            
            add_book_to_history(
                filename=os.path.basename(send_path),
                title=book['title'],
                author=authors_str,
                format_from=book_fmt.upper(),
                format_to="EPUB" if converted else book_fmt.upper(),
                kindle_email=KINDLE_EMAIL,
                converted=converted,
                file_size=os.path.getsize(send_path),
                stored_file=stored_file,
            )
            
            result_text = (
                f"✅ <b>Книга отправлена на Kindle!</b>\n\n"
                f"📖 <b>{book['title']}</b>\n"
                f"✍️ {authors_str}\n"
                f"📄 Формат: {Path(send_path).suffix.upper()}\n"
            )
            if converted:
                result_text += f"🔄 Конвертировано: {book_fmt.upper()} → EPUB\n"
            result_text += f"\n📬 <i>Книга появится на Kindle через 1-5 минут</i>"
            
            await query.edit_message_text(result_text, parse_mode="HTML")
        else:
            await query.edit_message_text(
                f"❌ <b>Ошибка отправки на Kindle</b>\n\n{error_msg}",
                parse_mode="HTML",
            )
    except ImportError:
        # Kindle handler not available — send as Telegram document
        await query.edit_message_text(
            f"📖 <b>{book['title']}</b>\n\n"
            f"⚠️ Kindle handler недоступен. Отправляю файл в чат.",
            parse_mode="HTML",
        )
        try:
            with open(send_path, 'rb') as f:
                await context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=f,
                    filename=os.path.basename(send_path),
                    caption=f"📖 {book['title']} — {authors_str}",
                )
        except Exception as e:
            logger.error("Send document error: %s", e)
    except Exception as e:
        logger.error("Kindle send error: %s", e)
        await query.edit_message_text(
            f"❌ <b>Ошибка:</b> {str(e)[:200]}",
            parse_mode="HTML",
        )
    
    # Cleanup temp files
    try:
        for f in os.listdir(tmp_dir):
            fp = os.path.join(tmp_dir, f)
            if os.path.isfile(fp):
                os.remove(fp)
    except Exception:
        pass
    
    context.user_data.pop("book_search_results", None)


# ──────────────────── Main Message Handler ────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle any text message — analyze and create calendar entry, note, or diary."""
    msg = update.message
    if not msg or not msg.text:
        logger.debug("Skipping: no message or no text")
        return
    
    logger.info("Received message from %s (chat %s): %s",
                msg.from_user.first_name if msg.from_user else '?',
                msg.chat_id,
                msg.text[:80])
    
    if msg.from_user and msg.from_user.is_bot:
        logger.debug("Skipping: message from bot")
        return

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
                    await msg.reply_text("🗑 ✅ Запись удалена из календаря!")
                    del _event_store[reply_id]
                    _save_event_store()
                else:
                    await msg.reply_text("❌ Не удалось удалить запись")
            else:
                await msg.reply_text(
                    "⚠️ Не могу найти связанную запись. "
                    "Используйте /delete <ключевое слово> для поиска."
                )
            return

    # ── URL Detection ──
    urls = extract_urls(text)
    if urls and len(text.split()) <= len(urls) * 3 + 5:
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
            response = f"🔗 <b>Создано {len(results)} задач на просмотр ({date_label}):</b>\n\n"
            for url, domain, uid, data in results:
                response += f"✅ <b>{domain}</b>\n<i>{url}</i>\n\n"
            
            reply_msg = await msg.reply_text(response, parse_mode="HTML", disable_web_page_preview=True)
            
            for url, domain, uid, data in results:
                _event_store[reply_msg.message_id] = {"uid": uid, "calendar": "work"}
                _save_event_store()
            
            return

    # ── Regular message — Claude analysis ──
    context_parts = []
    forward_origin = getattr(msg, 'forward_origin', None)
    if forward_origin:
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

    thinking_msg = await msg.reply_text("🤔 Анализирую сообщение...")

    logger.info("Calling Claude API for analysis...")
    try:
        data = await asyncio.to_thread(analyze_message, full_text)
    except Exception as e:
        logger.error("analyze_message thread error: %s", e)
        data = None
    logger.info("Claude analysis result: %s", data is not None)
    if data is None:
        await thinking_msg.edit_text(
            "❌ Не удалось проанализировать сообщение. Попробуйте ещё раз."
        )
        return

    # Check if Claude decided to skip
    if data.get("action") == "skip":
        reasoning = data.get("reasoning", "Сообщение слишком абстрактное")
        await thinking_msg.edit_text(
            f"🤔 <b>Не создаю запись</b>\n\n"
            f"💭 <i>{reasoning}</i>\n\n"
            f"💡 Попробуйте уточнить: что именно нужно сделать, когда и где.",
            parse_mode="HTML"
        )
        return
    
    # Check confidence threshold
    confidence_val = data.get("confidence", 0)
    if confidence_val < 0.4:
        reasoning = data.get("reasoning", "Недостаточно информации")
        await thinking_msg.edit_text(
            f"⚠️ <b>Низкая уверенность ({confidence_val:.0%})</b>\n\n"
            f"💭 <i>{reasoning}</i>\n\n"
            f"💡 Уточните сообщение: добавьте дату, время или конкретное действие.",
            parse_mode="HTML"
        )
        return

    entry_type = data.get("type", "")
    
    # ── Route by type ──
    
    if entry_type == "note":
        # Create Apple Note
        title = data.get("title", "Заметка")
        content = data.get("content", data.get("description", text))
        
        html_body = (
            f"<html><head></head><body>\n"
            f"<div>{content.replace(chr(10), '<br>')}</div>\n"
            f"</body></html>"
        )
        
        success = await asyncio.to_thread(create_apple_note, title, html_body)
        
        if success:
            response = (
                f"📝 <b>Заметка сохранена в Apple Notes!</b>\n\n"
                f"<b>Заголовок:</b> {title}\n"
                f"<b>Содержание:</b> {content[:200]}{'...' if len(content) > 200 else ''}\n"
                f"\n🟢 Уверенность: {confidence_val:.0%}"
            )
            if data.get("reasoning"):
                response += f"\n\n💭 <i>{data['reasoning']}</i>"
            await thinking_msg.edit_text(response, parse_mode="HTML")
        else:
            await thinking_msg.edit_text(
                "❌ Не удалось создать заметку в Apple Notes. Проверьте подключение."
            )
        return
    
    elif entry_type == "diary":
        # Create diary entry
        content = data.get("content", text)
        
        success = await asyncio.to_thread(create_diary_entry, content)
        
        now = datetime.now(TIMEZONE)
        time_str = now.strftime("%H:%M")
        
        if success:
            response = (
                f"\U0001f4d4 <b>\u0417\u0430\u043f\u0438\u0441\u044c \u0432 \u0434\u043d\u0435\u0432\u043d\u0438\u043a\u0435!</b>\n\n"
                f"\U0001f550 <b>{time_str}</b>\n"
                f"{content[:300]}{'...' if len(content) > 300 else ''}\n"
                f"\n\U0001f7e2 \u0423\u0432\u0435\u0440\u0435\u043d\u043d\u043e\u0441\u0442\u044c: {confidence_val:.0%}"
            )
            if data.get("reasoning"):
                response += f"\n\n\U0001f4ad <i>{data['reasoning']}</i>"
            await thinking_msg.edit_text(response, parse_mode="HTML")
        else:
            await thinking_msg.edit_text(
                "\u274c \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0434\u043e\u0431\u0430\u0432\u0438\u0442\u044c \u0437\u0430\u043f\u0438\u0441\u044c \u0432 \u0434\u043d\u0435\u0432\u043d\u0438\u043a. \u041f\u0440\u043e\u0432\u0435\u0440\u044c\u0442\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u0435."
            )
        return
    
    elif entry_type == "book_search":
        # Search for books on Flibusta
        search_query = data.get("query", text)
        search_title = data.get("title", f"\u041f\u043e\u0438\u0441\u043a: {search_query}")
        
        await thinking_msg.edit_text(f"\U0001f50d \u0418\u0449\u0443 \u00ab{search_query}\u00bb \u043d\u0430 \u0424\u043b\u0438\u0431\u0443\u0441\u0442\u0435...")
        
        try:
            results = await asyncio.to_thread(search_flibusta, search_query, 8)
        except Exception as e:
            logger.error("Book search thread error: %s", e)
            results = []
        
        if not results:
            await thinking_msg.edit_text(
                f"\u274c \u041f\u043e \u0437\u0430\u043f\u0440\u043e\u0441\u0443 \u00ab{search_query}\u00bb \u043d\u0438\u0447\u0435\u0433\u043e \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u043e.\n\n"
                f"\U0001f4a1 \u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439\u0442\u0435 \u0434\u0440\u0443\u0433\u043e\u0439 \u0437\u0430\u043f\u0440\u043e\u0441 \u0438\u043b\u0438 \u043f\u0440\u043e\u0432\u0435\u0440\u044c\u0442\u0435 \u043d\u0430\u043f\u0438\u0441\u0430\u043d\u0438\u0435.",
            )
            return
        
        # Store results in context for callback
        context.user_data["book_search_results"] = results
        
        text_resp = f"\U0001f4da <b>\u041d\u0430\u0439\u0434\u0435\u043d\u043e {len(results)} \u043a\u043d\u0438\u0433:</b>\n\n"
        keyboard = []
        
        for i, book in enumerate(results):
            authors_str = ", ".join(book["authors"][:2]) if book["authors"] else "\u041d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u044b\u0439 \u0430\u0432\u0442\u043e\u0440"
            formats_str = ", ".join(book["formats"][:4]).upper() if book["formats"] else "?"
            lang = f" [{book['language']}]" if book.get("language") else ""
            
            text_resp += f"{i+1}. <b>{book['title'][:60]}</b>\n"
            text_resp += f"   \u270d\ufe0f {authors_str}{lang}\n"
            text_resp += f"   \U0001f4c4 {formats_str}\n\n"
            
            # Prefer epub, then fb2, then mobi
            preferred_fmt = "epub"
            if "epub" in book["formats"]:
                preferred_fmt = "epub"
            elif "fb2" in book["formats"]:
                preferred_fmt = "fb2"
            elif "mobi" in book["formats"]:
                preferred_fmt = "mobi"
            elif book["formats"]:
                preferred_fmt = book["formats"][0]
            
            keyboard.append([InlineKeyboardButton(
                f"\U0001f4d6 {i+1}. {book['title'][:30]}",
                callback_data=f"book:{i}:{preferred_fmt}"
            )])
        
        keyboard.append([InlineKeyboardButton("\u274c \u041e\u0442\u043c\u0435\u043d\u0430", callback_data="book:cancel")])
        
        await thinking_msg.edit_text(
            text_resp,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return
    
    else:
        # Calendar event (event/task/reminder)
        logger.info("Creating calendar event: %s", data.get('title', '?'))
        try:
            uid = await asyncio.to_thread(create_calendar_event, data)
        except Exception as e:
            logger.error("create_calendar_event thread error: %s", e)
            uid = None

        if uid:
            type_map = {"event": "📅 Событие", "task": "✅ Задача", "reminder": "🔔 Напоминание"}
            cal_map = {"work": "💼 Рабочий", "family": "🏠 Семейный", "reminders": "⚠️ Напоминания"}

            entry_type_label = type_map.get(data.get("type", ""), data.get("type", ""))
            cal_name = cal_map.get(data.get("calendar", ""), data.get("calendar", ""))
            date_str = data.get("date", "?")
            time_str = ""
            if data.get("time_start") and not data.get("all_day"):
                time_str = f" в {data['time_start']}"
                if data.get("time_end"):
                    time_str += f"–{data['time_end']}"

            confidence = data.get("confidence", 0)
            conf_emoji = "🟢" if confidence >= 0.8 else "🟡" if confidence >= 0.5 else "🔴"

            response = (
                f"✅ <b>Создано в Apple Calendar!</b>\n\n"
                f"<b>Тип:</b> {entry_type_label}\n"
                f"<b>Название:</b> {data.get('title', '?')}\n"
                f"<b>Дата:</b> {date_str}{time_str}\n"
                f"<b>Календарь:</b> {cal_name}\n"
                f"<b>Напоминание:</b> за {data.get('alarm_minutes', 30)} мин\n"
                f"{conf_emoji} Уверенность: {confidence:.0%}\n"
                f"\n🗑 <i>Ответьте «удали» чтобы удалить</i>"
            )
            if data.get("reasoning"):
                response += f"\n\n💭 <i>{data['reasoning']}</i>"

            await thinking_msg.edit_text(response, parse_mode="HTML")
            
            _event_store[thinking_msg.message_id] = {
                "uid": uid,
                "calendar": data.get("calendar", "reminders"),
                "title": data.get("title", ""),
            }
            _save_event_store()
        else:
            await thinking_msg.edit_text(
                "❌ Не удалось создать запись в календаре. Проверьте подключение к iCloud."
            )


# ──────────────────── Error Handler ────────────────────
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Log errors and continue."""
    logger.error("Exception while handling an update: %s", context.error)
    logger.error(traceback.format_exc())


# ══════════════════════════════════════════════════════════
# ██  HEALTH CHECK SERVER
# ══════════════════════════════════════════════════════════

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
                "bot": "Nodkeys Calendar & Life Bot v4.1",
                "uptime_seconds": int(uptime),
                "messages_processed": _messages_processed,
                **kindle_stats,
            }
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(data).encode())
        elif self.path == '/weekly':
            try:
                events = get_week_events()
                html = '''<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #1a1c2e; color: #e0e0e0; margin: 0; padding: 8px; font-size: 13px; }
.event { display: flex; padding: 4px 8px; border-bottom: 1px solid #2a2d42; align-items: center; }
.event:last-child { border-bottom: none; }
.date { color: #4ade80; font-weight: 600; min-width: 75px; font-size: 12px; }
.time { color: #94a3b8; min-width: 50px; font-size: 12px; }
.title { flex: 1; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.cal { color: #64748b; font-size: 11px; margin-left: 8px; }
.empty { text-align: center; padding: 20px; color: #64748b; }
h3 { margin: 4px 8px 8px; color: #4ade80; font-size: 14px; border-bottom: 1px solid #4ade80; padding-bottom: 4px; }
</style></head><body>
<h3>📅 This Week</h3>\n'''
                if events:
                    for ev in events:
                        title = ev['title'][:40]
                        html += f'<div class="event"><span class="date">{ev["date"]}</span><span class="time">{ev["time"]}</span><span class="title">{title}</span><span class="cal">{ev["calendar"]}</span></div>\n'
                else:
                    html += '<div class="empty">No events this week</div>\n'
                html += '</body></html>'
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(html.encode())
            except Exception as e:
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.end_headers()
                self.wfile.write(f'<html><body style="background:#1a1c2e;color:#e0e0e0;font-family:sans-serif;padding:20px">Error: {e}</body></html>'.encode())
        elif self.path == '/repos':
            try:
                import urllib.request
                req = urllib.request.Request(
                    'https://api.github.com/users/sileade/repos?sort=updated&per_page=30',
                    headers={'User-Agent': 'Nodkeys-Bot'}
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    repos = json.loads(resp.read().decode())
                html = '''<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #1a1c2e; color: #e0e0e0; margin: 0; padding: 8px; font-size: 13px; }
.repo { display: flex; padding: 4px 8px; border-bottom: 1px solid #2a2d42; align-items: center; }
.repo:last-child { border-bottom: none; }
.repo a { color: #4ade80; text-decoration: none; font-weight: 600; flex: 1; }
.repo a:hover { text-decoration: underline; }
.desc { color: #94a3b8; font-size: 12px; margin-left: 8px; max-width: 200px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.meta { color: #64748b; font-size: 11px; min-width: 80px; text-align: right; }
.lang { font-size: 11px; padding: 1px 6px; border-radius: 8px; background: #2a2d42; margin-left: 8px; }
h3 { margin: 4px 8px 8px; color: #4ade80; font-size: 14px; border-bottom: 1px solid #4ade80; padding-bottom: 4px; }
</style></head><body>
<h3>📦 Repositories</h3>\n'''
                for repo in repos:
                    name = repo['name']
                    url = repo['html_url']
                    desc = (repo.get('description') or '')[:50]
                    lang = repo.get('language') or ''
                    updated = repo['updated_at'][:10]
                    stars = repo.get('stargazers_count', 0)
                    star_str = f'⭐{stars} ' if stars > 0 else ''
                    lang_html = f'<span class="lang">{lang}</span>' if lang else ''
                    html += f'<div class="repo"><a href="{url}" target="_blank">{name}</a>{lang_html}<span class="desc">{desc}</span><span class="meta">{star_str}{updated}</span></div>\n'
                html += '</body></html>'
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(html.encode())
            except Exception as e:
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.end_headers()
                self.wfile.write(f'<html><body style="background:#1a1c2e;color:#e0e0e0;font-family:sans-serif;padding:20px">Error: {e}</body></html>'.encode())
        elif self.path == '/kindle':
            try:
                from kindle_handler import get_book_history
                books = get_book_history()
                html = '''<!DOCTYPE html>
<html><head><meta charset="utf-8">
<meta http-equiv="refresh" content="60">
<style>
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #1a1c2e; color: #e0e0e0; margin: 0; padding: 8px; font-size: 13px; }
.book { display: flex; padding: 5px 8px; border-bottom: 1px solid #2a2d42; align-items: center; }
.book:hover { background: #2a2d42; }
.book:last-child { border-bottom: none; }
.num { color: #64748b; min-width: 25px; font-size: 12px; }
.title { color: #4ade80; font-weight: 600; flex: 1; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.author { color: #94a3b8; font-size: 12px; margin-left: 8px; max-width: 120px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.format { font-size: 11px; padding: 1px 6px; border-radius: 8px; background: #2a2d42; margin-left: 8px; text-transform: uppercase; }
.date { color: #64748b; font-size: 11px; min-width: 80px; text-align: right; margin-left: 8px; }
.dl { color: #4ade80; text-decoration: none; font-size: 14px; margin-left: 8px; padding: 2px 6px; border-radius: 4px; }
.dl:hover { background: #4ade80; color: #1a1c2e; }
.empty { text-align: center; padding: 20px; color: #64748b; }
h3 { margin: 4px 8px 8px; color: #4ade80; font-size: 14px; border-bottom: 1px solid #4ade80; padding-bottom: 4px; }
</style></head><body>
<h3>📚 Kindle Library</h3>\n'''
                if books:
                    for i, book in enumerate(reversed(books[-20:]), 1):
                        title = book.get('title', book.get('filename', '?'))[:40]
                        author = book.get('author', 'Unknown')[:20]
                        fmt = book.get('format_to', book.get('format_from', '?'))
                        sent = book.get('sent_at', '')[:10]
                        stored = book.get('stored_file', '')
                        dl_html = f'<a class="dl" href="/download/{stored}" title="Download">⬇</a>' if stored else ''
                        html += f'<div class="book"><span class="num">{i}</span><span class="title">{title}</span><span class="author">{author}</span><span class="format">{fmt}</span><span class="date">{sent}</span>{dl_html}</div>\n'
                else:
                    html += '<div class="empty">No books sent yet</div>\n'
                html += '</body></html>'
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(html.encode())
            except Exception as e:
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.end_headers()
                self.wfile.write(f'<html><body style="background:#1a1c2e;color:#e0e0e0;font-family:sans-serif;padding:20px">Error: {e}</body></html>'.encode())
        elif self.path.startswith('/download/'):
            try:
                filename = self.path[len('/download/'):]
                from kindle_handler import BOOKS_STORAGE_DIR
                file_path = os.path.join(BOOKS_STORAGE_DIR, filename)
                if os.path.exists(file_path) and os.path.isfile(file_path):
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/octet-stream')
                    self.send_header('Content-Disposition', f'attachment; filename="{filename}"')
                    self.send_header('Content-Length', str(os.path.getsize(file_path)))
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    with open(file_path, 'rb') as f:
                        self.wfile.write(f.read())
                else:
                    self.send_response(404)
                    self.send_header('Content-Type', 'text/html; charset=utf-8')
                    self.end_headers()
                    self.wfile.write(b'<html><body style="background:#1a1c2e;color:#e0e0e0;font-family:sans-serif;padding:20px">File not found</body></html>')
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.end_headers()
                self.wfile.write(f'<html><body style="background:#1a1c2e;color:#e0e0e0;font-family:sans-serif;padding:20px">Error: {e}</body></html>'.encode())
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


# ══════════════════════════════════════════════════════════
# ██  MAIN
# ══════════════════════════════════════════════════════════

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

    logger.info("Starting Nodkeys Calendar & Life Bot v4.1...")
    logger.info("Bot token: ...%s", BOT_TOKEN[-10:])
    logger.info("iCloud user: %s", ICLOUD_USERNAME)
    logger.info("Claude model: %s", CLAUDE_MODEL)
    logger.info("Flibusta base URL: %s", FLIBUSTA_BASE_URL)

    # Start health check server
    start_health_server(8085)

    # Start iCal proxy server
    try:
        from ical_proxy import run_server as run_ical_server
        ical_thread = threading.Thread(target=run_ical_server, args=(8086,), daemon=True)
        ical_thread.start()
        logger.info("iCal proxy started on port 8086")
    except ImportError:
        logger.warning("iCal proxy not available")

    app = Application.builder().token(BOT_TOKEN).build()

    # Register handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("calendars", cmd_calendars))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("delete", cmd_delete))
    app.add_handler(CommandHandler("cleanup", cmd_cleanup))
    app.add_handler(CallbackQueryHandler(callback_delete, pattern=r"^del:"))
    app.add_handler(CallbackQueryHandler(callback_book, pattern=r"^book:"))

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
