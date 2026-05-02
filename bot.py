#!/usr/bin/env python3
"""
Nodkeys Calendar & Life Bot v8.2
Telegram bot that analyzes messages using Claude AI and routes them:
- Events/Tasks/Reminders → Apple Calendar (iCloud CalDAV)
- Notes → Apple Notes (iCloud IMAP)
- Diary entries → Apple Notes diary with chronography (one note per day)
- Book search → Flibusta OPDS + Anna's Archive + Jackett + AI Rethink
- Kindle: AI format detection, Calibre conversion, SMTP delivery
- X-Ray: AI-powered book analysis (characters, themes, timeline)
- Kindle Clippings: parse My Clippings.txt → key takeaways
- URL→Kindle: send web articles to Kindle as clean EPUB
- All through natural language — no commands needed
"""

VERSION = "8.0"

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
import socketserver
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, quote
from urllib.request import Request, urlopen
# URLError/HTTPError handled inline via urllib

import anthropic
import caldav
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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
CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6")

# SOCKS5 proxy for Telegram API (e.g., ss-proxy:1080 in Docker network)
TELEGRAM_PROXY_URL = os.environ.get("TELEGRAM_PROXY_URL", "")  # socks5://ss-proxy:1080
CLAUDE_PROXY_URL = os.environ.get("CLAUDE_PROXY_URL", "")  # socks5://ss-proxy:1080
# Custom Telegram API base URL (to bypass DNS-level blocks)
TELEGRAM_BASE_URL = os.environ.get("TELEGRAM_BASE_URL", "")  # https://149.154.167.220/bot

ICLOUD_USERNAME = os.environ.get("ICLOUD_USERNAME", "")
ICLOUD_PASSWORD = os.environ.get("ICLOUD_PASSWORD", "")
CALDAV_URL = os.environ.get("CALDAV_URL", "https://caldav.icloud.com/")

CALENDAR_MAP = {
    "family": os.environ.get("CALENDAR_FAMILY", "Семейный"),
    "work": os.environ.get("CALENDAR_WORK", "Рабочий"),
    "reminders": os.environ.get("CALENDAR_REMINDERS", "Напоминания ⚠️"),
}
# Apple Reminders lists (CalDAV VTODO collections) for smart routing
REMINDER_LISTS = {
    "personal": "👤 Личное",
    "home": "🏠 Дом",
    "work": "💼 Работа",
    "longterm": "🎯 Долгосрочные",
}
REMINDER_LIST_DEFAULT = "personal"
REMINDERS_FILE = "/app/data/reminders.json"

TIMEZONE = ZoneInfo(os.environ.get("TZ", "Europe/Moscow"))

# ──────────────────── Multi-Chat & Per-User Routing ────────────────────
# Comma-separated list of allowed chat IDs (personal + group chats)
_allowed_ids_raw = os.environ.get("ALLOWED_CHAT_IDS", "")
ALLOWED_CHAT_IDS: set[int] = set()
if CHAT_ID:
    ALLOWED_CHAT_IDS.add(CHAT_ID)
for _cid in _allowed_ids_raw.split(","):
    _cid = _cid.strip()
    if _cid:
        try:
            ALLOWED_CHAT_IDS.add(int(_cid))
        except ValueError:
            pass

# Per-user routing in group chats
# Format: "username_or_id:display_name:calendar_rule|..."
# calendar_rule: "family" (always family), "work" (always work), "auto" (Claude decides)
# Example: "vera123:Вера:family|seleadi:Ilea:auto"
_group_users_raw = os.environ.get("GROUP_USERS", "")
GROUP_USERS: dict[str, dict] = {}  # key = username (lowercase) or str(user_id)
for _entry in _group_users_raw.split("|"):
    _entry = _entry.strip()
    if not _entry:
        continue
    _parts = _entry.split(":")
    if len(_parts) >= 3:
        _key = _parts[0].strip().lower().lstrip("@")
        GROUP_USERS[_key] = {
            "display_name": _parts[1].strip(),
            "calendar_rule": _parts[2].strip().lower(),  # family / work / auto
        }
    elif len(_parts) == 2:
        _key = _parts[0].strip().lower().lstrip("@")
        GROUP_USERS[_key] = {
            "display_name": _parts[0].strip(),
            "calendar_rule": _parts[1].strip().lower(),
        }


def get_user_routing(user) -> dict | None:
    """Get per-user routing config from GROUP_USERS.
    
    Logic: if user is explicitly listed → use their rule.
    If GROUP_USERS is configured but user is NOT listed → default to 'family'.
    If GROUP_USERS is empty → return None (no routing override).
    
    Args:
        user: telegram.User object (msg.from_user)
    Returns:
        dict with 'display_name' and 'calendar_rule', or None if not configured.
    """
    if not user or not GROUP_USERS:
        return None
    # Match by username (lowercase, without @)
    if user.username:
        key = user.username.lower()
        if key in GROUP_USERS:
            return GROUP_USERS[key]
    # Match by user_id
    uid_key = str(user.id)
    if uid_key in GROUP_USERS:
        return GROUP_USERS[uid_key]
    # Match by first_name (fallback)
    if user.first_name:
        name_key = user.first_name.lower()
        if name_key in GROUP_USERS:
            return GROUP_USERS[name_key]
    # User NOT in GROUP_USERS → default to family calendar
    display = user.first_name or user.username or str(user.id)
    logger.info("User '%s' not in GROUP_USERS → defaulting to family calendar", display)
    return {"display_name": display, "calendar_rule": "family"}


def apply_calendar_override(data: dict, user_routing: dict | None) -> dict:
    """Override calendar in Claude's response based on per-user routing rules.
    
    Args:
        data: Claude analysis result dict
        user_routing: result from get_user_routing(), or None
    Returns:
        Modified data dict with calendar potentially overridden.
    """
    if not user_routing:
        return data
    rule = user_routing.get("calendar_rule", "auto")
    if rule == "auto":
        return data  # Claude decides
    # Force calendar for this user
    entry_type = data.get("type", "")
    if entry_type in ("event", "task", "reminder"):
        original_cal = data.get("calendar", "family")
        data["calendar"] = rule
        if original_cal != rule:
            logger.info("Calendar override: %s → %s (user rule: %s)",
                       original_cal, rule, user_routing.get("display_name", "?"))
    return data

# ──────────────────── Apple Notes IMAP Config ────────────────────
IMAP_HOST = "imap.mail.me.com"
IMAP_PORT = 993
NOTES_FOLDER = "Notes"

# ──────────────────── Flibusta OPDS Config ────────────────────
FLIBUSTA_BASE_URL = os.environ.get("FLIBUSTA_BASE_URL", "https://flibusta.is")
FLIBUSTA_OPDS_SEARCH = "/opds/opensearch"
FLIBUSTA_TIMEOUT = 15
FLIBUSTA_PROXY_URL = os.environ.get("FLIBUSTA_PROXY_URL", "socks5://ss-proxy:1080")

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

# ──────────────────── Flibusta HTTP Client ────────────────────
import httpx as _flib_httpx
if FLIBUSTA_PROXY_URL:
    _flib_client = _flib_httpx.Client(
        proxy=FLIBUSTA_PROXY_URL,
        timeout=FLIBUSTA_TIMEOUT,
        verify=False,
        follow_redirects=True,
        headers={"User-Agent": f"NodkeysBot/{VERSION}", "Accept": "application/atom+xml"},
    )
    logger_init_msg = f"Flibusta using proxy: {FLIBUSTA_PROXY_URL}"
else:
    _flib_client = _flib_httpx.Client(
        timeout=FLIBUSTA_TIMEOUT,
        verify=False,
        follow_redirects=True,
        headers={"User-Agent": f"NodkeysBot/{VERSION}", "Accept": "application/atom+xml"},
    )
    logger_init_msg = "Flibusta direct (no proxy)"

# ──────────────────── Logging ────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("calendar-bot")

logger.info(logger_init_msg)

# ──────────────────── URL Detection ────────────────────
URL_REGEX = re.compile(
    r'https?://[^\s<>\"\'\'\)\]]+', re.IGNORECASE
)

def extract_urls(text: str) -> list[str]:
    """Extract all URLs from text, stripping trailing punctuation."""
    urls = URL_REGEX.findall(text)
    cleaned = []
    for url in urls:
        # Strip trailing punctuation that's not part of URL
        url = url.rstrip('.,;:!?)')
        if url:
            cleaned.append(url)
    return cleaned

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
    """Search books on Flibusta via HTML web search + OPDS catalog.
    
    Strategy:
    1. Try HTML web search first (finds more books, including classics)
    2. If HTML fails, try OPDS search
    3. If both fail and query has multiple words, retry with shorter query
    
    Args:
        query: Search query string
        limit: Maximum number of results
    Returns:
        List of book dicts with keys: id, title, authors, formats, language, genre
    """
    # First try HTML web search (more comprehensive, finds classics like Tolstoy)
    results = _search_flibusta_web(query, limit)
    
    # If web search returned nothing, try OPDS
    if not results:
        logger.info("Web search returned 0 results for '%s', trying OPDS", query)
        results = _search_flibusta_opds(query, limit)
    
    # If still nothing and query has multiple words, retry with shorter query
    if not results and ' ' in query:
        short_query = ' '.join(query.split()[:-1])
        if short_query:
            logger.info("Flibusta retry with shorter query: '%s'", short_query)
            return search_flibusta(short_query, limit)
    
    return results


def _search_flibusta_opds(query: str, limit: int = 10) -> list[dict]:
    """Search books on Flibusta via OPDS catalog."""
    results = []
    page = 0
    
    while len(results) < limit:
        try:
            url = (
                f"{FLIBUSTA_BASE_URL}{FLIBUSTA_OPDS_SEARCH}"
                f"?searchTerm={quote(query)}&searchType=books&pageNumber={page}"
            )
            logger.info("Flibusta OPDS search: %s", url)
            
            resp = _flib_client.get(url)
            resp.raise_for_status()
            xml_data = resp.content
            
            root = ET.fromstring(xml_data)
            ns = {
                'atom': 'http://www.w3.org/2005/Atom',
                'dc': 'http://purl.org/dc/terms/',
                'opds': 'http://opds-spec.org/2010/catalog',
            }
            
            entries = root.findall('atom:entry', ns)
            if not entries:
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
            
            has_next = False
            for link in root.findall('atom:link', ns) + root.findall('{http://www.w3.org/2005/Atom}link') + root.findall('link'):
                if link.get('rel') == 'next':
                    has_next = True
                    break
            
            if not has_next:
                break
            page += 1
            
        except _flib_httpx.HTTPStatusError as e:
            logger.error("Flibusta HTTP error: %s", e)
            break
        except ET.ParseError as e:
            logger.error("Flibusta XML parse error: %s", e)
            break
        except Exception as e:
            logger.error("Flibusta unexpected error: %s", e)
            break
    
    logger.info("Flibusta OPDS search '%s': found %d results", query, len(results))
    return results


def _search_flibusta_web(query: str, limit: int = 10) -> list[dict]:
    """Search books on Flibusta via HTML web search (more comprehensive than OPDS)."""
    results = []
    try:
        url = f"{FLIBUSTA_BASE_URL}/booksearch?ask={quote(query)}&t=0"
        logger.info("Flibusta web search: %s", url)
        resp = _flib_client.get(url)
        if resp.status_code != 200:
            logger.warning("Flibusta web search returned %d", resp.status_code)
            return results
        
        html = resp.text
        
        # Check for "nothing found" response first
        if 'Ничего не найдено' in html:
            logger.info("Flibusta web search '%s': nothing found (explicit)", query)
            return results
        
        # Find the main content area (exclude sidebar)
        # The main content is inside <div id="main"> or before <div id="sidebar">
        sidebar_idx = html.find('id="sidebar')
        if sidebar_idx > 0:
            main_html = html[:sidebar_idx]
        else:
            main_html = html
        
        # Find the books section in main content only
        books_idx = -1
        for marker in ['Найденные книги', 'Найденных книг', 'найденные книги']:
            books_idx = main_html.find(marker)
            if books_idx >= 0:
                break
        
        if books_idx < 0:
            # Try to find <ul> with /b/ links in main content area
            ul_idx = main_html.find('<ul>')
            if ul_idx >= 0 and '/b/' in main_html[ul_idx:]:
                books_idx = ul_idx
                logger.info("Flibusta web search: found book list in main content")
            else:
                logger.info("Flibusta web search: no books section found for '%s'", query)
                return results
        
        books_html = main_html[books_idx:]
        
        # Parse book entries: <li><a href="/b/BOOK_ID">Title</a> - <a href="/a/AUTHOR_ID">Author</a></li>
        # Titles may contain <b> tags for search term highlighting
        li_pattern = re.compile(r'<li>(.*?)</li>', re.DOTALL)
        book_id_pattern = re.compile(r'href="/b/(\d+)"')
        author_pattern = re.compile(r'href="/a/\d+">([^<]+)</a>')
        
        for li_match in li_pattern.finditer(books_html):
            if len(results) >= limit:
                break
            
            li_html = li_match.group(1)
            
            # Extract book ID
            bid_match = book_id_pattern.search(li_html)
            if not bid_match:
                continue
            book_id = int(bid_match.group(1))
            
            # Extract title: text of the first <a href="/b/..."> link (strip HTML tags)
            title_pattern = re.compile(r'<a href="/b/\d+"[^>]*>(.*?)</a>', re.DOTALL)
            title_match = title_pattern.search(li_html)
            if not title_match:
                continue
            title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()
            
            # Extract authors
            authors = author_pattern.findall(li_html)
            
            # Standard formats for Flibusta books
            formats = ["epub", "fb2", "mobi"]
            
            results.append({
                "id": book_id,
                "title": title,
                "authors": authors,
                "formats": formats,
                "language": "ru",
                "genre": "",
            })
        
        logger.info("Flibusta web search '%s': found %d results", query, len(results))
    except Exception as e:
        logger.error("Flibusta web search error: %s", e)
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


# ──────────────────── ANNA'S ARCHIVE FALLBACK ────────────────────
ANNAS_ARCHIVE_URL = "https://annas-archive.org"

def search_annas_archive(query: str, limit: int = 10) -> list[dict]:
    """Резервный поиск через Anna's Archive."""
    results = []
    try:
        url = f"{ANNAS_ARCHIVE_URL}/search?q={quote(query)}&content=book_fiction&content=book_nonfiction&lang=ru&lang=en"
        resp = _flib_client.get(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html",
        })
        if resp.status_code != 200:
            logger.warning("Anna's Archive returned %d", resp.status_code)
            return results

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.select("a[href*='/md5/']")[:limit]:
            title_el = item.select_one("h3")
            author_el = item.select_one("div.italic")
            if title_el:
                results.append({
                    "title": title_el.get_text(strip=True),
                    "author": author_el.get_text(strip=True) if author_el else "Unknown",
                    "url": ANNAS_ARCHIVE_URL + item["href"],
                    "source": "annas_archive",
                })
    except Exception as e:
        logger.error("Anna's Archive search error: %s", e)
    return results


# ──────────────────── JACKETT BOOK SEARCH ────────────────────
JACKETT_URL = "http://jackett:9117"
JACKETT_API_KEY = os.environ.get("JACKETT_API_KEY", "zudqeyrvgh880mqr2a9i68o5cb2r0gie")

def search_jackett_books(query: str, limit: int = 5) -> list[dict]:
    """Поиск книг через Jackett (RuTracker, RuTor и др.)."""
    results = []
    try:
        import httpx
        resp = httpx.get(
            f"{JACKETT_URL}/api/v2.0/indexers/all/results",
            params={"apikey": JACKETT_API_KEY, "Query": query, "Category[]": "7000"},
            timeout=15,
        )
        if resp.status_code == 200:
            for item in resp.json().get("Results", [])[:limit]:
                results.append({
                    "title": item.get("Title", ""),
                    "author": "",
                    "url": item.get("Details", ""),
                    "magnet": item.get("MagnetUri", ""),
                    "size": item.get("Size", 0),
                    "source": "jackett",
                })
    except Exception as e:
        logger.error("Jackett search error: %s", e)
    return results


def download_book(book_id: int, fmt: str = "epub") -> tuple[bytes | None, str]:
    """Download a book from Flibusta (using httpx + proxy).
    
    Args:
        book_id: Flibusta book ID
        fmt: Format to download (epub, fb2, mobi, etc.)
    Returns:
        Tuple of (file_bytes, filename) or (None, error_message)
    """
    try:
        url = f"{FLIBUSTA_BASE_URL}/b/{book_id}/{fmt}"
        logger.info("Downloading book: %s", url)
        
        # Use a longer timeout for downloads
        download_client = _flib_httpx.Client(
            proxy=FLIBUSTA_PROXY_URL if FLIBUSTA_PROXY_URL else None,
            timeout=60,
            verify=False,
            follow_redirects=True,
            headers={"User-Agent": f"NodkeysBot/{VERSION}"},
        )
        
        try:
            resp = download_client.get(url)
            resp.raise_for_status()
            data = resp.content
            
            # Try to get filename from Content-Disposition
            cd = resp.headers.get('Content-Disposition', '')
            filename = ""
            if cd:
                fn_match = re.search(r'filename\*?=(?:UTF-8\'\'\'|"?)([^";]+)"?', cd)
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
        finally:
            download_client.close()
    
    except Exception as e:
        logger.error("Book download error: %s", e)
        return None, str(e)


# ══════════════════════════════════════════════════════════
# ██  CLAUDE AI
# ══════════════════════════════════════════════════════════

# Use SOCKS5 proxy for Claude API if configured
if CLAUDE_PROXY_URL:
    import httpx as _httpx
    _claude_http_client = _httpx.Client(proxy=CLAUDE_PROXY_URL)
    claude_client = anthropic.Anthropic(api_key=CLAUDE_API_KEY, http_client=_claude_http_client)
    logger.info("Claude API using proxy: %s", CLAUDE_PROXY_URL)
else:
    claude_client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)

def _ai_rethink_book_query(query: str, author: str | None = None) -> list[str]:
    """Ask Claude for alternative book names when search fails.
    Returns a list of alternative search queries."""
    author_hint = f" автора {author}" if author else ""
    rethink_prompt = f"""Книга \"{query}\"{author_hint} не найдена на Flibusta по прямому поиску.

Подумай: какие АЛЬТЕРНАТИВНЫЕ НАЗВАНИЯ может иметь эта книга?
Учти:
1. Разные варианты перевода названия на русский
2. Оригинальное название на языке автора
3. Сокращённые или народные названия
4. Фамилию автора (полную, на русском)
5. Если пользователь мог ошибиться в названии — предложи правильное

Примеры:
- "Странник среди звёзд" Джека Лондона → ["Межзвёздный скиталец", "Межзвёздный скиталец Лондон", "The Star Rover", "Звёздный скиталец"]
- "Над пропастью во ржи" → ["Ловец во ржи", "The Catcher in the Rye", "Сэлинджер"]
- "Маленький принц" → ["Le Petit Prince", "Маленький принц Экзюпери"]
- "451 по Фаренгейту" → ["451 градус по Фаренгейту", "Fahrenheit 451", "Брэдбери 451"]

Ответь ТОЛЬКО JSON-массивом строк (до 6 вариантов), от наиболее вероятного к менее.
Пример: ["Межзвёздный скиталец", "Межзвёздный скиталец Лондон", "The Star Rover"]"""

    try:
        response = claude_client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=256,
            messages=[{"role": "user", "content": rethink_prompt}],
        )
        raw = response.content[0].text.strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1]
            if raw.endswith("```"):
                raw = raw[:-3].strip()
        result = json.loads(raw)
        if isinstance(result, list):
            return [str(q) for q in result[:6]]
    except Exception as e:
        logger.error("AI rethink error: %s", e)
    return []


SYSTEM_PROMPT = """Ты — умный персональный ассистент-планировщик (Telegram-бот). Ты помогаешь управлять календарём, задачами, привычками, заметками и книгами.

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
- Если время не указано для события — all_day: true

## Правила выбора инструмента:
- Конкретное действие с датой/временем → create_calendar_event
- Повторяющееся действие (лекарства, курс, ежедневно) → create_recurring_tasks
- Перенести/изменить/отменить событие → edit_event
- Информация для запоминания, идея, список → create_note
- Личная мысль, рефлексия, наблюдение → create_diary_entry
- Найти/скачать/прочитать книгу → search_book
- X-Ray анализ книги → xray_book
- Отправить URL на Kindle → url_to_kindle
- YouTube ссылка + "перескажи/о чём" → youtube_summary
- "Выпил воду", "сделал зарядку" (отметка привычки) → mark_habit_done
- "Хочу отслеживать...", "добавь привычку" → add_habit
- "Что на неделе/сегодня", "планы" → show_schedule
- "Как привычки", "статистика" → show_habits
- "Найди время для...", "когда свободен" → find_free_slot
- "Запомни что...", личные факты → save_memory
- Сообщение слишком размытое для действия → ask_clarification
- Просроченные задачи, "разгреби" → reschedule_overdue
- Приветствие, благодарность, общий вопрос → chat_response

## Правила для привычек:
- habit_name должен быть КОРОТКИМ (1-3 слова)
- "Выпил 2 стакана воды утром" → habit_name: "Пить воду"
- "Сделал зарядку 30 минут" → habit_name: "Зарядка"

## Правила для книг:
- ВСЕГДА извлекай ПОЛНОЕ название книги
- Если автор очевиден (классика) — ОБЯЗАТЕЛЬНО укажи
- search_queries от точного к общему, включая альтернативные названия/переводы

## Правила для recurring_tasks:
- "неделю", "каждый день", "N раз в день", "курс" → create_recurring_tasks
- times = массив времён (3 раза в день → ["09:00", "14:00", "21:00"])

## Категории событий:
- work — рабочие дела, проекты, IT, клиенты
- home — семья, дом, дети, быт
- personal — здоровье, хобби, друзья, покупки
- longterm — долгосрочные цели, учёба, планы

## Календари:
- work — рабочие события
- family — личные/семейные события
- reminders — быстрые напоминания

ВАЖНО: Всегда вызывай ОДИН инструмент. Если сообщение не требует действия — используй chat_response."""


WEEKDAYS_RU = {
    0: "Понедельник", 1: "Вторник", 2: "Среда", 3: "Четверг",
    4: "Пятница", 5: "Суббота", 6: "Воскресенье"
}


# ══════════════════════════════════════════════════════════
# ██  CLAUDE TOOL CALLING DEFINITIONS
# ══════════════════════════════════════════════════════════

CLAUDE_TOOLS = [
    {
        "name": "create_calendar_event",
        "description": "Create a calendar event, task, or reminder in Apple Calendar. Use for any action with a date/time: meetings, tasks, reminders, appointments.",
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "Short event title (max 50 chars)"},
                "description": {"type": "string", "description": "Detailed description with context"},
                "event_type": {"type": "string", "enum": ["event", "task", "reminder"], "description": "event=meetings/appointments, task=things to do, reminder=quick reminders"},
                "calendar": {"type": "string", "enum": ["work", "family", "reminders"], "description": "work=job/projects, family=personal/home, reminders=quick reminders"},
                "category": {"type": "string", "enum": ["work", "home", "personal", "longterm"], "description": "Category tag for grouping"},
                "date": {"type": "string", "description": "Date in YYYY-MM-DD format"},
                "time_start": {"type": "string", "description": "Start time HH:MM or null if all-day"},
                "time_end": {"type": "string", "description": "End time HH:MM or null"},
                "all_day": {"type": "boolean", "description": "True if no specific time"},
                "alarm_minutes": {"type": "integer", "description": "Reminder before event in minutes (15/30/60)"}
            },
            "required": ["title", "event_type", "calendar", "category", "date", "all_day"]
        }
    },
    {
        "name": "create_recurring_tasks",
        "description": "Create a series of repeating tasks/reminders over a period (e.g. take medicine 3x/day for a week, daily exercise for a month).",
        "input_schema": {
            "type": "object",
            "properties": {
                "summary": {"type": "string", "description": "Brief description of the recurring series"},
                "title": {"type": "string", "description": "Task title for each occurrence"},
                "description": {"type": "string", "description": "Details about the task"},
                "calendar": {"type": "string", "enum": ["work", "family", "reminders"]},
                "start_date": {"type": "string", "description": "Start date YYYY-MM-DD"},
                "end_date": {"type": "string", "description": "End date YYYY-MM-DD"},
                "times": {"type": "array", "items": {"type": "string"}, "description": "Array of times per day, e.g. ['09:00', '14:00', '21:00']"},
                "alarm_minutes": {"type": "integer", "description": "Reminder before each occurrence"}
            },
            "required": ["summary", "title", "start_date", "end_date", "times"]
        }
    },
    {
        "name": "edit_event",
        "description": "Edit, reschedule, or delete an existing calendar event. Use when user wants to move/change/cancel something.",
        "input_schema": {
            "type": "object",
            "properties": {
                "search_query": {"type": "string", "description": "Keywords to find the event"},
                "new_date": {"type": "string", "description": "New date YYYY-MM-DD or null if unchanged"},
                "new_time": {"type": "string", "description": "New time HH:MM or null if unchanged"},
                "new_title": {"type": "string", "description": "New title or null if unchanged"},
                "delete": {"type": "boolean", "description": "True to delete the event entirely"}
            },
            "required": ["search_query"]
        }
    },
    {
        "name": "create_note",
        "description": "Create a note in Apple Notes. Use for saving information, ideas, links, lists.",
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "Note title"},
                "content": {"type": "string", "description": "Full note content (can include markdown)"}
            },
            "required": ["title", "content"]
        }
    },
    {
        "name": "create_diary_entry",
        "description": "Add a diary/journal entry. Use for personal thoughts, reflections, observations about life.",
        "input_schema": {
            "type": "object",
            "properties": {
                "content": {"type": "string", "description": "The diary entry text"}
            },
            "required": ["content"]
        }
    },
    {
        "name": "search_book",
        "description": "Search for a book on Flibusta and optionally send to Kindle. Use when user wants to find/download/read a book.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Full book title as stated by user"},
                "author": {"type": "string", "description": "Book author if known"},
                "search_queries": {"type": "array", "items": {"type": "string"}, "description": "2-3 search variants from exact to general"},
                "send_to_kindle": {"type": "boolean", "description": "True if user wants to send to Kindle"}
            },
            "required": ["query", "search_queries"]
        }
    },
    {
        "name": "xray_book",
        "description": "Generate X-Ray analysis of a book (characters, themes, plot). Use when user asks for book analysis.",
        "input_schema": {
            "type": "object",
            "properties": {
                "book_title": {"type": "string", "description": "Book title"},
                "author": {"type": "string", "description": "Author if known"},
                "progress_percent": {"type": "integer", "description": "Reading progress 0-100, null if not specified"}
            },
            "required": ["book_title"]
        }
    },
    {
        "name": "url_to_kindle",
        "description": "Send a web article/URL to Kindle for reading. Use when user wants to read a link on their e-reader.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "The URL to send"},
                "title": {"type": "string", "description": "Article title if known"}
            },
            "required": ["url"]
        }
    },
    {
        "name": "youtube_summary",
        "description": "Summarize a YouTube video. Use when user sends a YouTube link and asks for summary/retelling.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "YouTube video URL"},
                "question": {"type": "string", "description": "Specific question about the video, or 'summary' for general"}
            },
            "required": ["url"]
        }
    },
    {
        "name": "mark_habit_done",
        "description": "Mark a habit as completed for today. Use when user reports doing something regularly tracked (exercised, drank water, read, meditated).",
        "input_schema": {
            "type": "object",
            "properties": {
                "habit_name": {"type": "string", "description": "Short habit name (1-3 words), e.g. 'Зарядка', 'Пить воду'"}
            },
            "required": ["habit_name"]
        }
    },
    {
        "name": "add_habit",
        "description": "Add a new habit to track. Use when user wants to start tracking something daily.",
        "input_schema": {
            "type": "object",
            "properties": {
                "habit_name": {"type": "string", "description": "Short habit name (1-3 words)"}
            },
            "required": ["habit_name"]
        }
    },
    {
        "name": "show_schedule",
        "description": "Show calendar events for today or the week. Use when user asks about their plans/schedule.",
        "input_schema": {
            "type": "object",
            "properties": {
                "period": {"type": "string", "enum": ["today", "week"], "description": "Show today or full week"}
            },
            "required": ["period"]
        }
    },
    {
        "name": "show_habits",
        "description": "Show habit tracker status and statistics. Use when user asks about their habits/progress.",
        "input_schema": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "find_free_slot",
        "description": "Find free time slots in the calendar. Use when user asks 'when am I free', 'find time for X'.",
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "What the slot is for"},
                "duration_hours": {"type": "number", "description": "How many hours needed"},
                "preferred_time": {"type": "string", "enum": ["morning", "afternoon", "evening", "any"], "description": "Preferred time of day"}
            },
            "required": ["title", "duration_hours"]
        }
    },
    {
        "name": "save_memory",
        "description": "Save a fact about the user for future reference. Use when user says 'remember that...', shares personal info, preferences.",
        "input_schema": {
            "type": "object",
            "properties": {
                "fact": {"type": "string", "description": "The fact to remember about the user"}
            },
            "required": ["fact"]
        }
    },
    {
        "name": "ask_clarification",
        "description": "Ask the user a clarifying question when the message is too vague to act on. Use sparingly - only when truly ambiguous.",
        "input_schema": {
            "type": "object",
            "properties": {
                "question": {"type": "string", "description": "The clarifying question to ask"},
                "context": {"type": "string", "description": "What was understood so far"}
            },
            "required": ["question"]
        }
    },
    {
        "name": "reschedule_overdue",
        "description": "Show and offer to reschedule overdue tasks. Use when user mentions overdue tasks or asks to clean up.",
        "input_schema": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "chat_response",
        "description": "Send a conversational response when no action is needed. Use for greetings, thanks, general questions, or when the message is not actionable.",
        "input_schema": {
            "type": "object",
            "properties": {
                "message": {"type": "string", "description": "The response message to send to user"}
            },
            "required": ["message"]
        }
    },

    {
        "name": "server_status",
        "description": "Check server infrastructure status: running containers, CPU/RAM usage, disk space. Use when user asks about server health, what is running, system status.",
        "input_schema": {
            "type": "object",
            "properties": {
                "check_type": {"type": "string", "enum": ["containers", "resources", "disk", "all"], "description": "What to check"}
            },
            "required": ["check_type"]
        }
    },
    {
        "name": "manage_container",
        "description": "Restart, stop, or check logs of a Docker container. Use when user asks to restart a service, check logs, or manage containers.",
        "input_schema": {
            "type": "object",
            "properties": {
                "container": {"type": "string", "description": "Container name (e.g. plex-server, sonarr, radarr, jellyfin)"},
                "action": {"type": "string", "enum": ["restart", "stop", "start", "logs"], "description": "Action to perform"}
            },
            "required": ["container", "action"]
        }
    },
    {
        "name": "execute_server_command",
        "description": "Execute a shell command on the server. Use for advanced server management tasks. Only safe read commands unless user explicitly asks for changes.",
        "input_schema": {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "Shell command to execute"},
                "reason": {"type": "string", "description": "Why this command is needed"}
            },
            "required": ["command", "reason"]
        }
    },
    {
        "name": "search_knowledge",
        "description": "Search the bot knowledge base built from all past interactions. Use when user asks a question that might have been discussed before, or asks to recall past conversations.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"},
                "category": {"type": "string", "enum": ["all", "tasks", "notes", "conversations", "server", "finance"], "description": "Category to search in"}
            },
            "required": ["query"]
        }
    },
    {
        "name": "add_expense",
        "description": "Record an expense/payment. Use when user mentions spending money, paying bills, or when a receipt is recognized.",
        "input_schema": {
            "type": "object",
            "properties": {
                "amount": {"type": "number", "description": "Amount in rubles"},
                "category": {"type": "string", "enum": ["food", "transport", "housing", "utilities", "entertainment", "health", "clothing", "subscriptions", "education", "other"], "description": "Expense category"},
                "description": {"type": "string", "description": "What was purchased/paid for"},
                "date": {"type": "string", "description": "Date of expense (YYYY-MM-DD), defaults to today"}
            },
            "required": ["amount", "category", "description"]
        }
    },
    {
        "name": "show_expenses",
        "description": "Show expense statistics and history. Use when user asks about spending, budget, financial summary.",
        "input_schema": {
            "type": "object",
            "properties": {
                "period": {"type": "string", "enum": ["today", "week", "month", "year"], "description": "Time period for report"},
                "category": {"type": "string", "description": "Filter by category (optional)"}
            },
            "required": ["period"]
        }
    },
    {
        "name": "web_search",
        "description": "Search the internet for information. Use when user asks a factual question, needs current info, prices, news, or anything requiring web lookup.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"},
                "search_type": {"type": "string", "enum": ["general", "news", "prices"], "description": "Type of search"}
            },
            "required": ["query"]
        }
    },
    {
        "name": "multi_step_plan",
        "description": "Create and execute a multi-step plan for complex tasks. Use when user asks to plan a trip, organize an event, or any task requiring multiple actions.",
        "input_schema": {
            "type": "object",
            "properties": {
                "goal": {"type": "string", "description": "The overall goal"},
                "steps": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of steps to accomplish the goal"
                },
                "create_events": {"type": "boolean", "description": "Whether to create calendar events for the steps"}
            },
            "required": ["goal", "steps"]
        }
    },

]

# ══════════════════════════════════════════════════════════
# ██  PHOTO / DOCUMENT RECOGNITION (Claude Vision)
# ══════════════════════════════════════════════════════════

PHOTO_ANALYSIS_PROMPT = """Ты — умный ассистент. Тебе отправили фотографию документа.
Проанализируй изображение и извлеки ВСЮ полезную информацию.

Текущая дата: {current_datetime}
День недели: {weekday}
Часовой пояс: Europe/Moscow

## Типы документов и что извлекать:

### Медицинские документы (рецепты, назначения, выписки):
- Названия лекарств, дозировки, частота приёма, длительность курса
- Даты приёма врача, следующего визита
- Диагнозы, рекомендации

### Билеты и бронирования:
- Дата, время, место
- Номер рейса/поезда/места
- Название мероприятия

### Расписания, графики:
- Все даты и события
- Время начала/окончания

### Чеки, счета, платёжки, коммунальные квитанции:
- Сумма к оплате, дата выставления, период
- Дедлайн оплаты (если указан — создай напоминание!)
- Поставщик услуги (электричество, газ, вода, интернет и т.д.)
- Номер лицевого счёта, показания счётчиков
- Если это коммунальная платёжка — создай задачу "Оплатить [услуга] [сумма]₽" с дедлайном

### Другие документы:
- Ключевая информация для запоминания
- Даты, сроки, контакты

## Формат ответа (строго JSON):

Если найдены повторяющиеся задачи (лекарства, курс лечения и т.д.):
{{
  "action": "create_series",
  "type": "recurring_tasks",
  "document_type": "medical|ticket|schedule|receipt|other",
  "summary": "Краткое описание документа",
  "tasks": [
    {{
      "title": "Краткое название задачи",
      "description": "Подробное описание",
      "calendar": "family",
      "type": "reminder",
      "start_date": "2026-04-20",
      "end_date": "2026-04-27",
      "times": ["09:00", "21:00"],
      "alarm_minutes": 15,
      "repeat_daily": true
    }}
  ],
  "one_time_events": [
    {{
      "title": "Визит к врачу",
      "description": "Повторный приём",
      "calendar": "family",
      "type": "event",
      "date": "2026-04-27",
      "time_start": "10:00",
      "time_end": "10:30",
      "all_day": false,
      "alarm_minutes": 60
    }}
  ],
  "confidence": 0.9,
  "reasoning": "Анализ документа"
}}

Если найдено одиночное событие/задача:
{{
  "action": "create",
  "type": "event|task|reminder",
  "document_type": "medical|ticket|schedule|receipt|other",
  "summary": "Краткое описание документа",
  "calendar": "family",
  "title": "Название",
  "description": "Описание",
  "date": "2026-04-20",
  "time_start": "14:00",
  "time_end": "15:00",
  "all_day": false,
  "alarm_minutes": 30,
  "confidence": 0.9,
  "reasoning": "Анализ"
}}

Если документ содержит только информацию для заметки:
{{
  "action": "create",
  "type": "note",
  "document_type": "medical|ticket|schedule|receipt|other",
  "summary": "Краткое описание документа",
  "title": "Заголовок заметки",
  "content": "Извлечённая информация",
  "confidence": 0.9,
  "reasoning": "Анализ"
}}

ВАЖНО:
- Для лекарств: ОБЯЗАТЕЛЬНО создавай повторяющиеся задачи на КАЖДЫЙ день курса
- Для "2 раза в день" — указывай times: ["09:00", "21:00"]
- Для "3 раза в день" — times: ["08:00", "14:00", "20:00"]
- Для "утром натощак" — times: ["07:30"]
- alarm_minutes для лекарств = 5 (чтобы не пропустить)
- Всегда ставь calendar: "family" для медицинских документов
- Если длительность курса не указана, ставь 7 дней по умолчанию
- Всегда отвечай ТОЛЬКО валидным JSON без markdown-обёртки."""


def analyze_photo_with_claude(image_data: bytes, media_type: str = "image/jpeg",
                               caption: str = "", sender_context: str = "") -> dict | None:
    """Analyze a photo/document using Claude Vision API.
    
    Args:
        image_data: Raw image bytes
        media_type: MIME type (image/jpeg, image/png, etc.)
        caption: Optional caption text from the user
        sender_context: Context about the sender for calendar routing
    
    Returns:
        Parsed JSON dict with analysis results, or None on error.
    """
    import base64
    now = datetime.now(TIMEZONE)
    weekday = WEEKDAYS_RU.get(now.weekday(), "")
    
    prompt = PHOTO_ANALYSIS_PROMPT.format(
        current_datetime=now.strftime("%Y-%m-%d %H:%M"),
        weekday=weekday,
    )
    if sender_context:
        prompt += f"\n\n## Контекст отправителя\n{sender_context}"
    
    image_b64 = base64.b64encode(image_data).decode("utf-8")
    
    user_content = []
    user_content.append({
        "type": "image",
        "source": {
            "type": "base64",
            "media_type": media_type,
            "data": image_b64,
        }
    })
    if caption:
        user_content.append({
            "type": "text",
            "text": f"Подпись пользователя: {caption}"
        })
    else:
        user_content.append({
            "type": "text",
            "text": "Проанализируй этот документ и создай соответствующие записи."
        })
    
    for attempt in range(3):
        try:
            response = claude_client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=4096,
                system=prompt,
                messages=[{"role": "user", "content": user_content}],
            )
            raw = response.content[0].text.strip()
            # Strip markdown code fences if present
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1]
                if raw.endswith("```"):
                    raw = raw[:-3]
                raw = raw.strip()
            data = json.loads(raw)
            logger.info("Photo analysis (attempt %d): %s",
                       attempt + 1, json.dumps(data, ensure_ascii=False)[:500])
            return data
        except json.JSONDecodeError as e:
            logger.error("Photo analysis invalid JSON (attempt %d): %s — raw: %s",
                        attempt + 1, e, raw[:500] if 'raw' in dir() else "N/A")
            if attempt < 2:
                continue
            return None
        except Exception as e:
            logger.error("Photo analysis error (attempt %d): %s", attempt + 1, e)
            if attempt < 2:
                _time.sleep(2 ** attempt)
                continue
            return None
    return None


def create_recurring_tasks(tasks_data: list[dict], calendar_override: str | None = None) -> list[str]:
    """Create a series of recurring calendar events from task definitions.
    
    Args:
        tasks_data: List of task dicts with start_date, end_date, times, etc.
        calendar_override: Force all tasks to this calendar (e.g. "family")
    
    Returns:
        List of created event UIDs.
    """
    created_uids = []
    
    for task in tasks_data:
        title = task.get("title", "Напоминание")
        description = task.get("description", "")
        cal_key = calendar_override or task.get("calendar", "family")
        alarm_minutes = task.get("alarm_minutes", 5)
        times = task.get("times", ["09:00"])
        start_date_str = task.get("start_date", datetime.now(TIMEZONE).strftime("%Y-%m-%d"))
        end_date_str = task.get("end_date")
        
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        
        if end_date_str:
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        else:
            end_date = start_date + timedelta(days=6)  # Default 7 days
        
        current_date = start_date
        day_count = 0
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            
            for time_str in times:
                event_data = {
                    "calendar": cal_key,
                    "title": title,
                    "description": description,
                    "date": date_str,
                    "time_start": time_str,
                    "all_day": False,
                    "alarm_minutes": alarm_minutes,
                    "type": "reminder",
                }
                
                # Calculate time_end (15 min after start)
                h, m = map(int, time_str.split(":"))
                end_m = m + 15
                end_h = h
                if end_m >= 60:
                    end_m -= 60
                    end_h += 1
                if end_h > 23:
                    end_h = 23
                event_data["time_end"] = f"{end_h:02d}:{end_m:02d}"
                
                uid = create_calendar_event(event_data)
                if uid:
                    created_uids.append(uid)
            
            current_date += timedelta(days=1)
            day_count += 1
            
            # Safety limit: max 60 days
            if day_count > 60:
                logger.warning("Recurring task limit reached (60 days)")
                break
    
    return created_uids




def analyze_message(text: str, sender_context: str = "") -> dict | None:
    """Send message to Claude with Tool Calling and return structured tool use data.
    
    Args:
        text: Message text to analyze.
        sender_context: Optional context about the message sender for calendar routing.
    Returns:
        dict with 'tool_name' and 'tool_input' keys, or None on failure.
    """
    now = datetime.now(TIMEZONE)
    weekday = WEEKDAYS_RU.get(now.weekday(), "")
    prompt = SYSTEM_PROMPT.format(
        current_datetime=now.strftime("%Y-%m-%d %H:%M"),
        weekday=weekday,
    )
    if sender_context:
        prompt += f"\n\n## Контекст отправителя\n{sender_context}"
    
    # Add knowledge context (recent interactions)
    try:
        recent_kb = _load_knowledge()[-10:]
        if recent_kb:
            prompt += "\n\nПоследние взаимодействия (контекст):\n"
            for kb_entry in recent_kb:
                prompt += f"- [{kb_entry.get('timestamp','')}] {kb_entry.get('user_message','')[:60]} -> {kb_entry.get('tool_used','')}: {kb_entry.get('result','')[:60]}\n"
    except Exception:
        pass
    
    # Add memory context
    memory_ctx = _get_memory_context()
    if memory_ctx:
        prompt += memory_ctx
    
    for attempt in range(3):
        try:
            response = claude_client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=1024,
                system=prompt,
                tools=CLAUDE_TOOLS,
                messages=[{"role": "user", "content": text}],
            )
            
            # Extract tool use from response
            for block in response.content:
                if block.type == "tool_use":
                    tool_name = block.name
                    tool_input = block.input
                    logger.info("Claude tool call (attempt %d): %s(%s)", 
                              attempt + 1, tool_name, 
                              json.dumps(tool_input, ensure_ascii=False)[:200])
                    return {"tool_name": tool_name, "tool_input": tool_input}
            
            # If Claude responded with text only (no tool call), treat as chat
            for block in response.content:
                if hasattr(block, "text") and block.text:
                    logger.info("Claude text response (no tool): %s", block.text[:100])
                    return {"tool_name": "chat_response", "tool_input": {"message": block.text}}
            
            logger.warning("Claude returned empty response (attempt %d)", attempt + 1)
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
            url=CALDAV_URL, username=ICLOUD_USERNAME, password=ICLOUD_PASSWORD,
            timeout=30
        )
    return _caldav_client


def reset_caldav_client():
    """Reset CalDAV client on connection errors."""
    global _caldav_client, _calendars
    _caldav_client = None
    _calendars = {}


def _caldav_retry(func, *args, max_retries=2, **kwargs):
    """Execute a CalDAV operation with automatic retry on connection errors."""
    for attempt in range(max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            err_msg = str(e).lower()
            is_conn_err = any(s in err_msg for s in [
                "remote end closed", "connection reset", "broken pipe",
                "timed out", "connection refused", "eof occurred",
            ])
            if is_conn_err and attempt < max_retries:
                logger.warning("CalDAV retry (%d/%d): %s",
                             attempt + 1, max_retries + 1, e)
                reset_caldav_client()
                _time.sleep(1 * (attempt + 1))
                continue
            raise


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
    """Get all events for today (with CalDAV retry)."""
    results = []
    try:
        principal = _caldav_retry(lambda: get_caldav_client().principal())
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


# Cache for week events to avoid hammering Apple CalDAV
_week_events_cache: list[dict] = []
_week_events_cache_time: float = 0
_WEEK_EVENTS_CACHE_TTL = 300  # 5 minutes

def get_week_events() -> list[dict]:
    """Get all events for the current week (Mon-Sun) with CalDAV retry and caching."""
    global _week_events_cache, _week_events_cache_time
    import time as _time
    if _week_events_cache and (_time.time() - _week_events_cache_time) < _WEEK_EVENTS_CACHE_TTL:
        return _week_events_cache
    results = []
    try:
        principal = _caldav_retry(lambda: get_caldav_client().principal())
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
    if results:
        _week_events_cache = results
        _week_events_cache_time = _time.time()
    return results



def _load_reminders() -> list:
    """Load reminders from JSON file."""
    try:
        with open(REMINDERS_FILE, "r") as f:
            return json.loads(f.read())
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def _save_reminders(reminders: list):
    """Save reminders to JSON file."""
    import os as _os
    _os.makedirs(_os.path.dirname(REMINDERS_FILE), exist_ok=True)
    with open(REMINDERS_FILE, "w") as f:
        f.write(json.dumps(reminders, ensure_ascii=False, indent=2))

def create_reminder(title: str, due_date: str = None, due_time: str = None, notes: str = "", list_key: str = None) -> str | None:
    """Create a reminder stored locally in JSON.
    
    Args:
        title: Reminder title
        due_date: Optional due date in YYYY-MM-DD format
        due_time: Optional due time in HH:MM format
        notes: Optional description/notes
        list_key: Key from REMINDER_LISTS (personal/home/work/longterm)
    
    Returns:
        UID of created reminder or None on failure
    """
    try:
        uid = str(uuid.uuid4())
        now = datetime.now(TIMEZONE)
        
        if not list_key or list_key not in REMINDER_LISTS:
            list_key = REMINDER_LIST_DEFAULT
        
        reminder = {
            "uid": uid,
            "title": title,
            "due_date": due_date,
            "due_time": due_time,
            "notes": notes,
            "list_key": list_key,
            "created": now.isoformat(),
            "completed": False,
            "notified": False,
            "notified_15m": False,
        }
        
        reminders = _load_reminders()
        reminders.append(reminder)
        _save_reminders(reminders)
        
        logger.info("Created reminder '%s' (UID: %s, list: %s)", title, uid, list_key)
        return uid
    except Exception as e:
        logger.error("Failed to create reminder: %s", e)
        return None

def get_pending_reminders() -> list[dict]:
    """Get all pending (incomplete) reminders from local storage."""
    reminders = _load_reminders()
    results = []
    for r in reminders:
        if not r.get("completed", False):
            due_str = ""
            if r.get("due_date"):
                due_str = r["due_date"][8:10] + "." + r["due_date"][5:7]
                if r.get("due_time"):
                    due_str += " " + r["due_time"]
            results.append({
                "uid": r["uid"],
                "title": r["title"],
                "due": due_str,
                "list_key": r.get("list_key", "personal"),
            })
    return results

def complete_reminder(uid: str) -> bool:
    """Mark a reminder as completed by UID."""
    try:
        reminders = _load_reminders()
        for r in reminders:
            if r["uid"] == uid or r["uid"].startswith(uid):
                r["completed"] = True
                r["completed_at"] = datetime.now(TIMEZONE).isoformat()
                _save_reminders(reminders)
                logger.info("Completed reminder: %s", r["title"])
                return True
        return False
    except Exception as e:
        logger.error("Complete reminder error: %s", e)
        return False

def delete_all_test_events() -> int:
    """Delete all events created by the bot (with emoji prefixes)."""
    count = 0
    try:
        principal = _caldav_retry(lambda: get_caldav_client().principal())
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


async def cmd_remind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /remind command - create calendar events as tasks with category tags.
    
    Creates VEVENT in iCloud Calendar via CalDAV with:
    - 🔔 marker + category tag in title
    - AI-determined category: [Работа], [Дом], [Личное], [Долгосрочные]
    - Alarm/reminder set
    - Also stored locally for /remind list view
    
    Usage:
        /remind - show pending reminders with complete buttons
        /remind <text> - create reminder (AI parses date/time/category)
    """
    if update.effective_chat.id not in ALLOWED_CHAT_IDS:
        return
    
    text = " ".join(context.args) if context.args else ""
    
    # Category tag mapping
    TAG_MAP = {
        "personal": "[Личное]",
        "home": "[Дом]",
        "work": "[Работа]",
        "longterm": "[Долгосрочные]",
    }
    
    if not text:
        # List pending reminders from local storage
        reminders = await asyncio.to_thread(get_pending_reminders)
        if not reminders:
            await update.message.reply_text("✅ Нет активных напоминаний!")
            return
        
        # Group by category
        by_cat = {}
        for r in reminders:
            cat = r.get("list_key", "personal")
            by_cat.setdefault(cat, []).append(r)
        
        msg = "🔔 **Активные напоминания:**\n\n"
        buttons = []
        idx = 0
        for cat_key in ["personal", "home", "work", "longterm"]:
            items = by_cat.get(cat_key, [])
            if not items:
                continue
            cat_label = REMINDER_LISTS.get(cat_key, "📋")
            msg += f"**{cat_label}:**\n"
            for r in items[:10]:
                idx += 1
                due = f" ⏰ {r['due']}" if r['due'] else ""
                msg += f"  {idx}. {r['title']}{due}\n"
                buttons.append([InlineKeyboardButton(
                    f"✅ {idx}. {r['title'][:25]}",
                    callback_data=f"rdone:{r['uid'][:8]}"
                )])
            msg += "\n"
        
        keyboard = InlineKeyboardMarkup(buttons[:10])
        await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=keyboard)
        return
    
    # Parse the reminder text with AI
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    weekday = datetime.now(TIMEZONE).strftime("%A")
    
    prompt = f"""Разбери текст напоминания. Сегодня {today} ({weekday}).
Текст: "{text}"
Ответь ТОЛЬКО JSON:
{{"title": "краткое название задачи", "due_date": "YYYY-MM-DD или null (если не указано - используй сегодня)", "due_time": "HH:MM или null", "notes": "доп. заметки или пустая строка", "list": "personal|home|work|longterm"}}

Категории:
- personal: личные дела (покупки, здоровье, хобби, звонки)
- home: семейные дела (дом, дети, семья, ремонт, уборка, готовка)
- work: рабочие задачи (проекты, клиенты, встречи, отчёты, код)
- longterm: долгосрочные цели (учёба, планы на год, мечты)

Если дата не указана явно, ставь сегодняшнюю дату {today}.
Если время не указано, ставь null."""

    try:
        import httpx
        proxy_url = os.environ.get("CLAUDE_PROXY_URL")
        client_kwargs = {"timeout": 15.0}
        if proxy_url:
            client_kwargs["proxy"] = proxy_url
        with httpx.Client(**client_kwargs) as client:
            response = client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": os.environ.get("CLAUDE_API_KEY", ""),
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6"),
                    "max_tokens": 200,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
        result_text = response.json()["content"][0]["text"]
        json_match = re.search(r'\{[^}]+\}', result_text)
        if json_match:
            data = json.loads(json_match.group())
        else:
            data = {"title": text, "due_date": today, "due_time": None, "notes": "", "list": "personal"}
    except Exception as e:
        logger.warning("AI parse failed for remind, using raw text: %s", e)
        data = {"title": text, "due_date": today, "due_time": None, "notes": "", "list": "personal"}
    
    # Determine category
    list_key = data.get("list", REMINDER_LIST_DEFAULT)
    if list_key not in REMINDER_LISTS:
        list_key = REMINDER_LIST_DEFAULT
    
    tag = TAG_MAP.get(list_key, "[Личное]")
    title = data.get("title", text)
    due_date = data.get("due_date") or today
    due_time = data.get("due_time")
    notes = data.get("notes", "")
    
    # Create CalDAV event in family calendar (syncs to all Apple devices)
    event_data = {
        "title": f"{tag} {title}",
        "date": due_date,
        "time_start": due_time,
        "time_end": None,
        "all_day": not bool(due_time),
        "description": notes if notes else f"Задача создана через /remind\nКатегория: {tag}",
        "calendar": "family",
        "type": "task",
        "alarm_minutes": 15 if due_time else 540,  # 15 min before if timed, 9:00 AM if all-day
    }
    
    uid = await asyncio.to_thread(create_calendar_event, event_data)
    
    # Also save locally for /remind list view
    if uid:
        await asyncio.to_thread(
            create_reminder,
            title,
            due_date,
            due_time,
            notes,
            list_key
        )
    
    if uid:
        due_info = f"\n📅 {due_date}"
        if due_time:
            due_info += f" {due_time}"
        
        list_label = REMINDER_LISTS.get(list_key, "📋")
        
        await update.message.reply_text(
            f"🔔 Задача создана в iCloud Calendar!\n\n"
            f"**{tag} {title}**{due_info}\n"
            f"📋 Категория: {list_label}\n"
            f"📲 Синхронизируется с iPhone/Mac",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text("❌ Не удалось создать задачу. Проверьте CalDAV подключение.")

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("CMD /start from user %s (chat %s)", update.effective_user.username, update.effective_chat.id)
    await update.message.reply_text(
        f"🗓 <b>Nodkeys Calendar Bot v{VERSION}</b>\n\n"
        "Перешлите мне любое сообщение, и я:\n"
        "• Проанализирую его с помощью AI (Claude)\n"
        "• Создам событие/задачу/памятку в Apple Calendar\n"
        "• Сохраню заметку в Apple Notes\n"
        "• Добавлю запись в дневник\n"
        "• Найду и отправлю книги на Kindle\n"
        "• Сделаю X-Ray анализ книги\n"
        "• Отправлю статью на Kindle\n"
        "• 🎤 Распознаю голосовые сообщения\n"
        "• 📊 Отслеживаю привычки\n"
        "• ☀️ Утренний дайджест (08:00)\n"
        "• 📋 Еженедельное ревью (Вс 20:00)\n\n"
        "<b>Команды:</b>\n"
        "/calendars — список календарей\n"
        "/today — события на сегодня\n"
        "/week — события на неделю\n"
        "/habits — трекер привычек\n"
        "/remind — напоминания\n"
        "/delete <i>текст</i> — найти и удалить запись\n"
        "/book <i>название</i> — найти книгу\n"
        "/xray <i>название</i> — X-Ray анализ книги\n"
        "/cleanup — удалить все тестовые записи бота\n"
        "/help — справка",
        parse_mode="HTML",
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("CMD /help from user %s (chat %s)", update.effective_user.username, update.effective_chat.id)
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
        "📚 <b>Поиск книг:</b>\n"
        "• «Найди книгу Мастер и Маргарита» → 📚 поиск + Kindle\n"
        "• «Хочу почитать Стивена Кинга» → 📚 поиск\n\n"
        "🔬 <b>X-Ray анализ:</b>\n"
        "• «Сделай x-ray по Войне и миру» → персонажи, темы, таймлайн\n"
        "• /xray Мастер и Маргарита → структурированный анализ\n\n"
        "🌐 <b>URL → Kindle:</b>\n"
        "• «Отправь на киндл https://habr.com/article»\n"
        "• «На читалку https://medium.com/post»\n\n"
        "📸 <b>Распознавание документов:</b>\n"
        "• Фото рецепта → серия напоминаний о лекарствах\n"
        "• Фото билета → событие в календаре\n"
        "• Фото расписания → несколько событий\n\n"
        "📎 <b>Kindle Clippings:</b>\n"
        "• Отправьте файл My Clippings.txt → ключевые цитаты и выводы\n\n"
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
    logger.info("CMD /calendars from user %s", update.effective_user.username)
    try:
        text = await asyncio.to_thread(_list_calendars)
        await update.message.reply_text(text, parse_mode="HTML")
    except Exception as e:
        reset_caldav_client()
        await update.message.reply_text(f"❌ Ошибка подключения к iCloud: {e}")


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("CMD /today from user %s", update.effective_user.username)
    events = await asyncio.to_thread(get_today_events)
    today = datetime.now(TIMEZONE).strftime("%d.%m.%Y")
    
    if not events:
        await update.message.reply_text(
            f"📅 <b>События на {today}:</b>\n\nНет событий на сегодня 🎉",
            parse_mode="HTML",
        )
        return
    
    text = f"📅 <b>События на {today}:</b>\n\n"
    buttons = []
    for i, ev in enumerate(events, 1):
        text += f"{i}. <b>[{ev['calendar']}]</b> {ev['time']} — {ev['title']}\n"
        if ev.get("uid"):
            buttons.append([InlineKeyboardButton(
                f"✅ Выполнено: {ev['title'][:25]}",
                callback_data=f"done:{ev['uid'][:20]}"
            )])
    
    kb = InlineKeyboardMarkup(buttons[:5]) if buttons else None
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb)



async def cmd_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show events for the current week."""
    logger.info("CMD /week from user %s", update.effective_user.username)
    events = await asyncio.to_thread(get_week_events)
    now = datetime.now(TIMEZONE)
    monday = now - timedelta(days=now.weekday())
    sunday = monday + timedelta(days=6)
    period = f"{monday.strftime('%d.%m')} — {sunday.strftime('%d.%m.%Y')}"
    
    if not events:
        await update.message.reply_text(
            f"📅 <b>Неделя {period}:</b>\n\nНет событий на этой неделе 🎉",
            parse_mode="HTML",
        )
        return
    
    # Group by day
    by_day = {}
    for ev in events:
        by_day.setdefault(ev["date"], []).append(ev)
    
    text = f"📅 <b>Неделя {period}:</b>\n\n"
    for day, day_events in by_day.items():
        text += f"<b>📌 {day}:</b>\n"
        for ev in day_events:
            text += f"  • {ev['time']} — {ev['title']}\n"
        text += "\n"
    
    if len(text) > 4000:
        text = text[:3950] + "\n\n<i>...и ещё события</i>"
    
    await update.message.reply_text(text, parse_mode="HTML")

async def cmd_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Search and delete events by keyword."""
    logger.info("CMD /delete from user %s, args=%s", update.effective_user.username, context.args)
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
    logger.info("CMD /cleanup from user %s", update.effective_user.username)
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


# ──────────────────── X-Ray Book Analysis ────────────────────

XRAY_PROMPT = """You are a literary analyst. Generate an X-Ray analysis for the book "{book_title}"{author_part}.

{progress_instruction}

Provide the analysis in Russian, using this EXACT structure:

🔬 <b>X-Ray: {book_title}</b>{author_line}

<b>📚 О книге:</b>
(Краткое описание в 2-3 предложениях: жанр, год написания, контекст)

<b>👥 Персонажи:</b>
• <b>Имя</b> — краткое описание и роль
(до 10 ключевых персонажей)

<b>🎯 Главные темы:</b>
• Тема — краткое описание
(до 5 тем)

<b>📍 Ключевые локации:</b>
• Место — значение в сюжете
(до 5 локаций)

<b>📅 Таймлайн:</b>
(Краткая хронология событий без спойлеров)

<b>💡 Интересные факты:</b>
• 2-3 факта о книге или авторе

IMPORTANT:
- Use HTML formatting (<b>, <i>, \u2022)
- Keep it concise but informative
- Answer in Russian
- DO NOT use markdown, only HTML tags
- Maximum 4000 characters total"""


def generate_xray_analysis(book_title: str, author: str = "", progress_percent: int | None = None) -> str:
    """Generate X-Ray analysis for a book using Claude."""
    author_part = f" by {author}" if author else ""
    author_line = f"\n✍️ {author}" if author else ""
    
    if progress_percent is not None and progress_percent < 100:
        progress_instruction = (
            f"The reader is at {progress_percent}% of the book. "
            f"DO NOT reveal any plot points, twists, or character developments "
            f"that happen after the {progress_percent}% mark. "
            f"Mark the analysis as spoiler-free up to {progress_percent}%."
        )
    else:
        progress_instruction = "The reader has finished the book. You may include full analysis."
    
    prompt = XRAY_PROMPT.format(
        book_title=book_title,
        author_part=author_part,
        author_line=author_line,
        progress_instruction=progress_instruction,
    )
    
    try:
        response = claude_client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        result = response.content[0].text.strip()
        # Ensure it starts with the emoji header
        if not result.startswith("🔬"):
            result = f"🔬 <b>X-Ray: {book_title}</b>{author_line}\n\n" + result
        return result
    except Exception as e:
        logger.error("X-Ray Claude error: %s", e)
        raise


async def cmd_xray(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate X-Ray analysis for a book."""
    logger.info("CMD /xray from user %s, args=%s", update.effective_user.username, context.args)
    query = " ".join(context.args) if context.args else ""
    
    if not query:
        await update.message.reply_text(
            "🔬 <b>X-Ray анализ книги</b>\n\n"
            "Использование: /xray <i>название книги</i>\n"
            "Пример: /xray Мастер и Маргарита\n\n"
            "📝 Персонажи, темы, локации, таймлайн\n"
            "🚫 Без спойлеров (можно указать % прочтения)",
            parse_mode="HTML",
        )
        return
    
    thinking = await update.message.reply_text(f"🔬 Генерирую X-Ray анализ «{query}»...")
    
    try:
        result = await asyncio.to_thread(generate_xray_analysis, query)
        await thinking.edit_text(result, parse_mode="HTML")
    except Exception as e:
        logger.error("X-Ray command error: %s", e)
        await thinking.edit_text(f"❌ Ошибка генерации X-Ray: {str(e)[:200]}")


# ──────────────────── URL → Kindle (EPUB) ────────────────────

def url_to_epub(url: str, title_hint: str = "") -> tuple[str | None, str]:
    """Download a web article, clean it, and convert to EPUB.
    Returns (epub_path, title) or (None, error_message)."""
    from bs4 import BeautifulSoup
    
    tmp_dir = "/tmp/kindle_files"
    os.makedirs(tmp_dir, exist_ok=True)
    
    try:
        # Download the page
        headers = {
            "User-Agent": f"NodkeysBot/{VERSION}",
            "Accept": "text/html,application/xhtml+xml",
        }
        req = Request(url, headers=headers)
        with urlopen(req, timeout=30) as resp:
            raw_html = resp.read()
            # Detect encoding
            content_type = resp.headers.get("Content-Type", "")
            charset = "utf-8"
            if "charset=" in content_type:
                charset = content_type.split("charset=")[-1].strip()
            html_text = raw_html.decode(charset, errors="replace")
    except Exception as e:
        logger.error("URL fetch error: %s", e)
        return None, f"Ошибка загрузки: {e}"
    
    # Parse and clean HTML
    soup = BeautifulSoup(html_text, "html.parser")
    
    # Extract title
    page_title = title_hint
    if not page_title:
        if soup.title and soup.title.string:
            page_title = soup.title.string.strip()
        else:
            page_title = get_url_domain(url)
    
    # Remove unwanted elements
    for tag in soup.find_all(["script", "style", "nav", "footer", "header",
                               "aside", "iframe", "noscript", "form",
                               "button", "input", "select", "textarea"]):
        tag.decompose()
    
    # Remove ad-related elements
    for tag in soup.find_all(attrs={"class": re.compile(
        r"(ad|ads|advert|banner|popup|modal|cookie|consent|sidebar|widget|share|social|comment)",
        re.IGNORECASE
    )}):
        tag.decompose()
    
    # Try to find the main article content
    article = (
        soup.find("article") or
        soup.find(attrs={"class": re.compile(r"(article|post|content|entry|story)", re.IGNORECASE)}) or
        soup.find("main") or
        soup.body
    )
    
    if not article:
        return None, "Не удалось извлечь контент страницы"
    
    # Build clean HTML for EPUB
    clean_html = f"""<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta charset="utf-8"/>
<title>{page_title}</title>
<style>
body {{ font-family: serif; line-height: 1.6; padding: 1em; }}
h1, h2, h3 {{ margin-top: 1.5em; }}
img {{ max-width: 100%; height: auto; }}
blockquote {{ border-left: 3px solid #ccc; padding-left: 1em; margin-left: 0; color: #555; }}
pre, code {{ font-family: monospace; font-size: 0.9em; background: #f5f5f5; padding: 0.2em; }}
</style>
</head>
<body>
<h1>{page_title}</h1>
<p><small>Источник: {url}</small></p>
<hr/>
{article}
</body>
</html>"""
    
    # Save as HTML first
    safe_name = re.sub(r'[^\w\s\-]', '', page_title)[:60].strip() or "article"
    html_path = os.path.join(tmp_dir, f"{safe_name}.html")
    epub_path = os.path.join(tmp_dir, f"{safe_name}.epub")
    
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(clean_html)
    
    # Convert to EPUB using Calibre
    try:
        from kindle_handler import convert_with_calibre
        converted = convert_with_calibre(html_path, "epub")
        if converted and os.path.exists(converted):
            return converted, page_title
    except ImportError:
        pass
    except Exception as e:
        logger.warning("Calibre conversion failed: %s, trying direct EPUB", e)
    
    # Fallback: try ebook-convert directly
    try:
        import subprocess
        result = subprocess.run(
            ["ebook-convert", html_path, epub_path,
             "--title", page_title,
             "--authors", get_url_domain(url),
             "--language", "ru"],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode == 0 and os.path.exists(epub_path):
            return epub_path, page_title
        logger.error("ebook-convert failed: %s", result.stderr[:200])
    except FileNotFoundError:
        logger.warning("ebook-convert not found, sending HTML directly")
    except Exception as e:
        logger.error("ebook-convert error: %s", e)
    
    # Last resort: send HTML file directly (Kindle supports HTML)
    if os.path.exists(html_path):
        return html_path, page_title
    
    return None, "Не удалось создать EPUB"


async def callback_urlkindle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle URL→Kindle device selection callback."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    if not data.startswith("urlkindle:"):
        return
    
    kindle_email = data[10:]  # Remove "urlkindle:" prefix
    
    if kindle_email == "cancel":
        await query.edit_message_text("❌ Отправка отменена.")
        context.user_data.pop("url_kindle_pending", None)
        return
    
    pending = context.user_data.get("url_kindle_pending")
    if not pending:
        await query.edit_message_text("⚠️ Данные не найдены. Попробуйте заново.")
        return
    
    epub_path = pending["epub_path"]
    article_title = pending["title"]
    article_url = pending["url"]
    
    # Find device name
    device_name = kindle_email
    try:
        from kindle_handler import get_kindle_devices
        for name, email in get_kindle_devices():
            if email == kindle_email:
                device_name = name
                break
    except ImportError:
        pass
    
    await query.edit_message_text(
        f"📧 Отправлю на {device_name}...\n\n"
        f"📖 <b>{article_title}</b>",
        parse_mode="HTML",
    )
    
    try:
        from kindle_handler import send_email_to_kindle, add_book_to_history, store_book_file, get_books_history
        
        subject = article_title
        success, error_msg = send_email_to_kindle(epub_path, kindle_email, subject)
        
        if success:
            # Store in history
            try:
                book_id_num = len(get_books_history()) + 1
            except Exception:
                book_id_num = 1
            
            stored_file = store_book_file(epub_path, book_id_num)
            
            from pathlib import Path as _P
            add_book_to_history(
                filename=os.path.basename(epub_path),
                title=article_title,
                author=get_url_domain(article_url),
                format_from="HTML",
                format_to=_P(epub_path).suffix.upper().lstrip("."),
                kindle_email=kindle_email,
                converted=True,
                file_size=os.path.getsize(epub_path),
                stored_file=stored_file,
            )
            
            await query.edit_message_text(
                f"✅ <b>Статья отправлена на {device_name}!</b>\n\n"
                f"📖 <b>{article_title}</b>\n"
                f"🔗 <code>{article_url[:60]}</code>\n"
                f"📤 Формат: {_P(epub_path).suffix.upper()}\n\n"
                f"📬 <i>Статья появится на {device_name} через 1-5 минут</i>",
                parse_mode="HTML",
            )
        else:
            await query.edit_message_text(
                f"❌ <b>Ошибка отправки</b>\n\n{error_msg}",
                parse_mode="HTML",
            )
    except ImportError:
        await query.edit_message_text("⚠️ Kindle handler недоступен.")
    except Exception as e:
        logger.error("URL Kindle send error: %s", e)
        await query.edit_message_text(f"❌ <b>Ошибка:</b> {str(e)[:200]}", parse_mode="HTML")
    finally:
        context.user_data.pop("url_kindle_pending", None)
        # Cleanup temp files
        try:
            tmp_dir = "/tmp/kindle_files"
            for f in os.listdir(tmp_dir):
                fp = os.path.join(tmp_dir, f)
                if os.path.isfile(fp):
                    os.remove(fp)
        except Exception:
            pass


# ──────────────────── Kindle Clippings Parser ────────────────────

def parse_kindle_clippings(text: str) -> dict[str, list[str]]:
    """Parse Kindle 'My Clippings.txt' file into a dict of {book_title: [highlights]}."""
    books = {}
    entries = text.split("==========")
    
    for entry in entries:
        entry = entry.strip()
        if not entry:
            continue
        
        lines = entry.split("\n")
        if len(lines) < 3:
            continue
        
        # First line is the book title (and author)
        book_title = lines[0].strip()
        # Remove BOM if present
        book_title = book_title.lstrip("\ufeff")
        
        # The highlight text is after the metadata line and empty line
        highlight_lines = []
        found_empty = False
        for line in lines[1:]:
            if not found_empty and line.strip() == "":
                found_empty = True
                continue
            if found_empty and line.strip():
                highlight_lines.append(line.strip())
        
        highlight = " ".join(highlight_lines).strip()
        if highlight and book_title:
            if book_title not in books:
                books[book_title] = []
            # Avoid duplicates
            if highlight not in books[book_title]:
                books[book_title].append(highlight)
    
    return books


def summarize_clippings_with_ai(books: dict[str, list[str]]) -> str:
    """Use Claude to generate key takeaways from Kindle clippings."""
    # Build a compact representation
    parts = []
    total_highlights = 0
    for title, highlights in sorted(books.items(), key=lambda x: -len(x[1])):
        parts.append(f"\n### {title} ({len(highlights)} цитат)")
        for h in highlights[:20]:  # Limit per book to avoid token overflow
            parts.append(f"- {h[:300]}")
        total_highlights += len(highlights)
    
    clippings_text = "\n".join(parts)
    
    prompt = f"""Analyze these Kindle highlights/clippings from the user's reading.
Total: {total_highlights} highlights from {len(books)} books.

{clippings_text}

Generate a structured analysis in Russian using HTML formatting:

1. For each book with 3+ highlights, provide:
   - Book title
   - Key Takeaways (2-3 main insights from the highlights)
   - Best Quote (the most impactful highlight)

2. At the end, provide:
   - Overall reading themes/patterns
   - Action Items (practical things to do based on the highlights)

Use <b>, <i>, \u2022 for formatting. Keep it concise. Max 4000 chars.
Start with: 📚 <b>Kindle Clippings Анализ</b>"""
    
    try:
        response = claude_client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()
    except Exception as e:
        logger.error("Clippings AI error: %s", e)
        # Fallback: just list the books and counts
        result = "📚 <b>Kindle Clippings</b>\n\n"
        for title, highlights in sorted(books.items(), key=lambda x: -len(x[1])):
            result += f"\u2022 <b>{title}</b> — {len(highlights)} цитат\n"
        return result


# ──────────────────── Book Search Command ────────────────────

async def cmd_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Search for books on Flibusta and offer to send to Kindle."""
    logger.info("CMD /book from user %s, args=%s", update.effective_user.username, context.args)
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
        logger.info("Flibusta returned 0 results, trying Anna's Archive")
        try:
            await thinking.edit_text(f"🔍 Ищу «{query}» в Anna's Archive...")
            results = await asyncio.to_thread(search_annas_archive, query)
        except Exception as e:
            logger.error("Anna's Archive search error: %s", e)
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
    
    # Show device selection
    try:
        from kindle_handler import get_kindle_devices
        devices = get_kindle_devices()
    except ImportError:
        devices = [("Kindle", os.getenv("KINDLE_EMAIL", ""))]
    
    # Store book info for the device selection callback
    context.user_data["book_kindle_pending"] = {
        "send_path": send_path,
        "book_title": book['title'],
        "authors_str": authors_str,
        "book_fmt": book_fmt,
        "converted": converted,
    }
    
    # Always show device selection buttons
    keyboard = []
    for name, email in devices:
        keyboard.append([InlineKeyboardButton(
            f"📱 {name}",
            callback_data=f"bookdev:{email}"
        )])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="bookdev:cancel")])
    
    fmt_str = book_fmt.upper()
    if converted:
        fmt_str += " → EPUB"
    
    await query.edit_message_text(
        f"📖 <b>{book['title']}</b>\n"
        f"✍️ {authors_str}\n"
        f"📄 Формат: {fmt_str}\n\n"
        f"📱 <b>Отправить на:</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def callback_bookdev(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Kindle device selection for book search results."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    if not data.startswith("bookdev:"):
        return
    
    kindle_email = data[8:]  # Remove "bookdev:" prefix
    
    if kindle_email == "cancel":
        await query.edit_message_text("❌ Отправка отменена.")
        context.user_data.pop("book_kindle_pending", None)
        context.user_data.pop("book_search_results", None)
        return
    
    await _send_book_to_kindle_device(query, context, kindle_email)


async def _send_book_to_kindle_device(query, context, kindle_email: str):
    """Send the pending book to the specified Kindle device."""
    pending = context.user_data.get("book_kindle_pending")
    if not pending:
        await query.edit_message_text("⚠️ Данные книги не найдены. Попробуйте заново.")
        return
    
    send_path = pending["send_path"]
    book_title = pending["book_title"]
    authors_str = pending["authors_str"]
    book_fmt = pending["book_fmt"]
    converted = pending["converted"]
    
    # Find device name
    device_name = kindle_email
    try:
        from kindle_handler import get_kindle_devices
        for name, email in get_kindle_devices():
            if email == kindle_email:
                device_name = name
                break
    except ImportError:
        pass
    
    await query.edit_message_text(
        f"📧 Отправляю на {device_name}...\n\n"
        f"📖 <b>{book_title}</b>\n"
        f"✍️ {authors_str}",
        parse_mode="HTML",
    )
    
    try:
        from kindle_handler import send_email_to_kindle, add_book_to_history, store_book_file
        
        subject = f"{book_title} - {authors_str}"
        success, error_msg = send_email_to_kindle(send_path, kindle_email, subject)
        
        if success:
            # Store book permanently
            try:
                from kindle_handler import get_books_history
                book_id_num = len(get_books_history()) + 1
            except Exception:
                book_id_num = 1
            
            stored_file = store_book_file(send_path, book_id_num)
            
            add_book_to_history(
                filename=os.path.basename(send_path),
                title=book_title,
                author=authors_str,
                format_from=book_fmt.upper(),
                format_to="EPUB" if converted else book_fmt.upper(),
                kindle_email=kindle_email,
                converted=converted,
                file_size=os.path.getsize(send_path),
                stored_file=stored_file,
            )
            
            from pathlib import Path as _Path
            result_text = (
                f"✅ <b>Книга отправлена на {device_name}!</b>\n\n"
                f"📖 <b>{book_title}</b>\n"
                f"✍️ {authors_str}\n"
                f"📄 Формат: {_Path(send_path).suffix.upper()}\n"
            )
            if converted:
                result_text += f"🔄 Конвертировано: {book_fmt.upper()} → EPUB\n"
            result_text += f"\n📬 <i>Книга появится на {device_name} через 1-5 минут</i>"
            
            await query.edit_message_text(result_text, parse_mode="HTML")
        else:
            await query.edit_message_text(
                f"❌ <b>Ошибка отправки на {device_name}</b>\n\n{error_msg}",
                parse_mode="HTML",
            )
    except ImportError:
        await query.edit_message_text(
            f"📖 <b>{book_title}</b>\n\n"
            f"⚠️ Kindle handler недоступен.",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error("Kindle send error: %s", e)
        await query.edit_message_text(
            f"❌ <b>Ошибка:</b> {str(e)[:200]}",
            parse_mode="HTML",
        )
    
    # Cleanup temp files
    tmp_dir = "/tmp/kindle_files"
    try:
        for f in os.listdir(tmp_dir):
            fp = os.path.join(tmp_dir, f)
            if os.path.isfile(fp):
                os.remove(fp)
    except Exception:
        pass
    
    context.user_data.pop("book_kindle_pending", None)
    context.user_data.pop("book_search_results", None)


# ──────────────────── Main Message Handler ────────────────────


# ══════════════════════════════════════════════════════════
# ██  VOICE MESSAGE HANDLER (Transcription via Claude)
# ══════════════════════════════════════════════════════════

async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle voice messages — transcribe and process as text."""
    msg = update.message
    if not msg or not (msg.voice or msg.audio):
        return
    
    chat_id = msg.chat_id
    if ALLOWED_CHAT_IDS and chat_id not in ALLOWED_CHAT_IDS:
        return
    
    voice = msg.voice or msg.audio
    duration = voice.duration if voice.duration else 0
    
    if duration > 300:  # 5 min max
        await msg.reply_text("⚠️ Голосовое сообщение слишком длинное (макс. 5 минут)")
        return
    
    thinking = await msg.reply_text("🎤 Распознаю голосовое сообщение...")
    
    try:
        # Download voice file
        tg_file = await context.bot.get_file(voice.file_id)
        voice_bytes = await tg_file.download_as_bytearray()
        
        # Use Claude to transcribe (send as audio)
        import base64
        audio_b64 = base64.b64encode(bytes(voice_bytes)).decode()
        
        # Determine media type
        mime_type = "audio/ogg"  # Telegram voice messages are OGG/Opus
        if msg.audio:
            mime_type = msg.audio.mime_type or "audio/mpeg"
        
        proxy_url = os.environ.get("CLAUDE_PROXY_URL")
        client_kwargs = {}
        if proxy_url:
            import httpx as _httpx
            client_kwargs["http_client"] = _httpx.Client(proxy=proxy_url)
        
        client = anthropic.Anthropic(api_key=CLAUDE_API_KEY, **client_kwargs)
        
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=2000,
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Транскрибируй это голосовое сообщение. Верни ТОЛЬКО текст сообщения, без пояснений."
                    },
                    {
                        "type": "document",
                        "source": {
                            "type": "base64",
                            "media_type": mime_type,
                            "data": audio_b64,
                        }
                    }
                ]
            }]
        )
        
        transcribed_text = response.content[0].text.strip()
        
        if not transcribed_text:
            await thinking.edit_text("❌ Не удалось распознать речь")
            return
        
        await thinking.edit_text(
            f"🎤 <b>Распознано:</b>\n<i>{transcribed_text[:500]}</i>\n\n🤔 Анализирую...",
            parse_mode="HTML"
        )
        
        # Now process as regular text message
        # Get user routing
        user_routing = get_user_routing(msg.from_user)
        sender_context = ""
        if user_routing:
            sender_name = user_routing.get("display_name", "")
            cal_rule = user_routing.get("calendar_rule", "auto")
            if cal_rule == "auto":
                sender_context = f"Сообщение от пользователя: {sender_name}. Определи календарь на основе содержания."
            else:
                sender_context = f"Сообщение от пользователя: {sender_name}. Календарь: {cal_rule}."
        
        data = await asyncio.to_thread(analyze_message, transcribed_text, sender_context)
        
        if data is None:
            await thinking.edit_text(
                f"🎤 <b>Распознано:</b>\n<i>{transcribed_text[:300]}</i>\n\n❌ Не удалось проанализировать"
            , parse_mode="HTML")
            return
        
        # Voice: show transcription, then dispatch via Tool Calling
        tool_name = data.get("tool_name", "")
        tool_input = data.get("tool_input", {})
        
        if tool_name == "chat_response":
            msg_text = tool_input.get("message", "Не удалось обработать")
            await thinking.edit_text(
                f"🎤 <b>Распознано:</b>\n<i>{transcribed_text[:300]}</i>\n\n"
                f"💬 {msg_text}",
                parse_mode="HTML"
            )
            return
        
        if tool_name == "ask_clarification":
            question = tool_input.get("question", "Уточните запрос")
            await thinking.edit_text(
                f"🎤 <b>Распознано:</b>\n<i>{transcribed_text[:300]}</i>\n\n"
                f"❓ {question}",
                parse_mode="HTML"
            )
            return
        
        # ── Voice: dispatch all other tools via unified handler ──
        if tool_name == "save_memory":
            fact = tool_input.get("fact", "")
            if fact:
                await asyncio.to_thread(_add_memory_fact, fact)
                await thinking.edit_text(
                    f"🎤🧠 <b>Распознано и запомнил!</b>\n\n"
                    f"<i>{transcribed_text[:200]}</i>\n\n"
                    f"💾 {fact}",
                    parse_mode="HTML"
                )
            return
        
        if tool_name == "create_note":
            title = tool_input.get("title", "Заметка")
            note_content = tool_input.get("content", transcribed_text)
            html_body = f"<html><head></head><body><div>{note_content.replace(chr(10), '<br>')}</div></body></html>"
            success = await asyncio.to_thread(create_apple_note, title, html_body)
            if success:
                await thinking.edit_text(
                    f"🎤📝 <b>Голосовое → Заметка!</b>\n\n"
                    f"<b>Заголовок:</b> {title}\n"
                    f"<b>Текст:</b> {note_content[:200]}",
                    parse_mode="HTML"
                )
            else:
                await thinking.edit_text("❌ Не удалось создать заметку")
            return
        
        if tool_name == "create_diary_entry":
            diary_content = tool_input.get("content", transcribed_text)
            success = await asyncio.to_thread(create_diary_entry, diary_content)
            if success:
                await thinking.edit_text(
                    f"🎤📔 <b>Голосовое → Дневник!</b>\n\n{diary_content[:300]}",
                    parse_mode="HTML"
                )
            else:
                await thinking.edit_text("❌ Не удалось добавить в дневник")
            return
        
        if tool_name == "create_calendar_event":
            # Map tool_input to existing event creation format
            user_routing_v = get_user_routing(msg.from_user)
            data_mapped = {
                "action": "create",
                "type": tool_input.get("event_type", "event"),
                "calendar": tool_input.get("calendar", "family"),
                "category": tool_input.get("category", "personal"),
                "title": tool_input.get("title", ""),
                "description": tool_input.get("description", ""),
                "date": tool_input.get("date", datetime.now(TIMEZONE).strftime("%Y-%m-%d")),
                "time_start": tool_input.get("time_start"),
                "time_end": tool_input.get("time_end"),
                "all_day": tool_input.get("all_day", True),
                "alarm_minutes": tool_input.get("alarm_minutes", 30),
                "confidence": 0.95,
            }
            data_mapped = apply_calendar_override(data_mapped, user_routing_v)
            
            uid = await asyncio.to_thread(create_calendar_event, data_mapped)
            if uid:
                type_map = {"event": "📅 Событие", "task": "✅ Задача", "reminder": "🔔 Напоминание"}
                cal_map = {"work": "💼 Рабочий", "family": "🏠 Семейный", "reminders": "⚠️ Напоминания"}
                evt_type = data_mapped.get("type", "event")
                response_text = (
                    f"🎤 <b>Голосовое → Календарь!</b>\n\n"
                    f"<b>Текст:</b> <i>{transcribed_text[:200]}</i>\n\n"
                    f"<b>Тип:</b> {type_map.get(evt_type, evt_type)}\n"
                    f"<b>Название:</b> {data_mapped.get('title', '?')}\n"
                    f"<b>Дата:</b> {data_mapped.get('date', '?')}\n"
                    f"<b>Календарь:</b> {cal_map.get(data_mapped.get('calendar', ''), '')}\n"
                    f"\n🗑 <i>Ответьте «удали» чтобы удалить</i>"
                )
                await thinking.edit_text(response_text, parse_mode="HTML")
                _event_store[thinking.message_id] = {"uid": uid, "calendar": data_mapped.get("calendar", "reminders")}
                _save_event_store()
            else:
                await thinking.edit_text("❌ Не удалось создать запись в календаре")
            return
        
        if tool_name == "mark_habit_done":
            habit_name = tool_input.get("habit_name", "")
            await thinking.edit_text(
                f"🎤 <b>Распознано:</b> <i>{transcribed_text[:200]}</i>\n\n✅ Отмечаю привычку...",
                parse_mode="HTML"
            )
            await _handle_habit_done(habit_name, thinking, chat_id)
            return
        
        if tool_name == "web_search":
            query = tool_input.get("query", "")
            await thinking.edit_text(f"🎤🔍 <b>Ищу:</b> {query}...", parse_mode="HTML")
            try:
                results = await asyncio.to_thread(_web_search, query)
                response = f"🎤 <b>Распознано:</b> <i>{transcribed_text[:150]}</i>\n\n🌐 <b>Результаты:</b>\n\n{results}"
                await thinking.edit_text(response[:4000], parse_mode="HTML")
            except Exception as e:
                await thinking.edit_text(f"❌ Ошибка поиска: {str(e)[:100]}")
            return
        
        # ── Fallback: show transcription ──
        await thinking.edit_text(
            f"🎤 <b>Распознано:</b>\n<i>{transcribed_text[:500]}</i>",
            parse_mode="HTML"
        )
    
    except Exception as e:
        logger.error("Voice handler error: %s", e)
        await thinking.edit_text(f"❌ Ошибка обработки голосового: {str(e)[:100]}")




# ══════════════════════════════════════════════════════════
# ██  Predictive Planning - Smart Slot Finder
# ══════════════════════════════════════════════════════════
def _find_free_slots(duration_hours: float = 1.0, preferred_time: str = "any", days_ahead: int = 7) -> list[dict]:
    """Find free time slots in the calendar for the next N days."""
    from datetime import date as _date
    
    slots = []
    try:
        principal = _caldav_retry(lambda: get_caldav_client().principal())
        now = datetime.now(TIMEZONE)
        
        # Define working hours
        work_start = 9
        work_end = 21
        
        for day_offset in range(days_ahead):
            day = now + timedelta(days=day_offset)
            day_start = day.replace(hour=work_start, minute=0, second=0, microsecond=0)
            day_end = day.replace(hour=work_end, minute=0, second=0, microsecond=0)
            
            if day_offset == 0 and now.hour >= work_start:
                # Today: start from next hour
                day_start = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            
            if day_start >= day_end:
                continue
            
            # Get all events for this day
            day_events = []
            for cal in principal.calendars():
                try:
                    events = cal.search(start=day_start, end=day_end, event=True, expand=True)
                    for event in events:
                        try:
                            vevent = event.vobject_instance.vevent
                            dtstart = vevent.dtstart.value
                            dtend = vevent.dtend.value if hasattr(vevent, 'dtend') else dtstart + timedelta(hours=1)
                            if hasattr(dtstart, 'hour'):
                                day_events.append((dtstart, dtend))
                        except Exception:
                            continue
                except Exception:
                    continue
            
            # Sort events by start time
            day_events.sort(key=lambda x: x[0])
            
            # Find gaps
            current = day_start
            for ev_start, ev_end in day_events:
                if hasattr(ev_start, 'astimezone'):
                    ev_start = ev_start.astimezone(TIMEZONE)
                if hasattr(ev_end, 'astimezone'):
                    ev_end = ev_end.astimezone(TIMEZONE)
                
                gap_hours = (ev_start - current).total_seconds() / 3600
                if gap_hours >= duration_hours:
                    slot_start = current
                    # Apply preferred_time filter
                    if preferred_time == "morning" and slot_start.hour >= 13:
                        pass
                    elif preferred_time == "afternoon" and slot_start.hour < 12:
                        pass
                    elif preferred_time == "evening" and slot_start.hour < 17:
                        pass
                    else:
                        slots.append({
                            "date": slot_start.strftime("%a %d.%m"),
                            "start": slot_start.strftime("%H:%M"),
                            "end": (slot_start + timedelta(hours=duration_hours)).strftime("%H:%M"),
                            "day_name": slot_start.strftime("%A"),
                            "iso_date": slot_start.strftime("%Y-%m-%d"),
                        })
                current = max(current, ev_end)
            
            # Check gap after last event
            if current < day_end:
                gap_hours = (day_end - current).total_seconds() / 3600
                if gap_hours >= duration_hours:
                    slot_start = current
                    if preferred_time == "morning" and slot_start.hour >= 13:
                        pass
                    elif preferred_time == "afternoon" and slot_start.hour < 12:
                        pass
                    elif preferred_time == "evening" and slot_start.hour < 17:
                        pass
                    else:
                        slots.append({
                            "date": slot_start.strftime("%a %d.%m"),
                            "start": slot_start.strftime("%H:%M"),
                            "end": (slot_start + timedelta(hours=duration_hours)).strftime("%H:%M"),
                            "day_name": slot_start.strftime("%A"),
                            "iso_date": slot_start.strftime("%Y-%m-%d"),
                        })
        
    except Exception as e:
        logger.error("Find free slots error: %s", e)
        reset_caldav_client()
    
    return slots[:10]  # Return max 10 slots


def _check_day_overload(target_date: str = None) -> dict:
    """Check if a specific day is overloaded with events."""
    try:
        principal = _caldav_retry(lambda: get_caldav_client().principal())
        now = datetime.now(TIMEZONE)
        
        if target_date:
            day = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=TIMEZONE)
        else:
            day = now
        
        day_start = day.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        
        event_count = 0
        total_hours = 0
        
        for cal in principal.calendars():
            try:
                events = cal.search(start=day_start, end=day_end, event=True, expand=True)
                for event in events:
                    try:
                        vevent = event.vobject_instance.vevent
                        dtstart = vevent.dtstart.value
                        dtend = vevent.dtend.value if hasattr(vevent, 'dtend') else dtstart + timedelta(hours=1)
                        if hasattr(dtstart, 'hour'):
                            event_count += 1
                            duration = (dtend - dtstart).total_seconds() / 3600
                            total_hours += duration
                    except Exception:
                        continue
            except Exception:
                continue
        
        is_overloaded = event_count >= 5 or total_hours >= 6
        return {
            "event_count": event_count,
            "total_hours": round(total_hours, 1),
            "is_overloaded": is_overloaded,
            "date": day.strftime("%a %d.%m"),
        }
    except Exception as e:
        logger.error("Day overload check error: %s", e)
        return {"event_count": 0, "total_hours": 0, "is_overloaded": False}


# ══════════════════════════════════════════════════════════
# ██  Contextual Triggers & Workflows
# ══════════════════════════════════════════════════════════
WORKFLOWS_FILE = os.path.join(os.path.dirname(__file__), "data", "workflows.json")

def _load_workflows() -> list:
    try:
        with open(WORKFLOWS_FILE, "r") as f:
            return json.loads(f.read())
    except (FileNotFoundError, json.JSONDecodeError):
        # Default workflows
        return [
            {
                "trigger": "командировка",
                "suggestions": ["Собрать чемодан", "Заказать такси в аэропорт", "Проверить документы", "Зарядить повербанк"]
            },
            {
                "trigger": "отпуск",
                "suggestions": ["Оплатить все счета заранее", "Настроить автоответ на почте", "Передать дела коллегам", "Скачать карты офлайн"]
            },
            {
                "trigger": "день рождения",
                "suggestions": ["Купить подарок", "Заказать торт", "Забронировать ресторан"]
            },
            {
                "trigger": "переезд",
                "suggestions": ["Заказать грузчиков", "Упаковать вещи", "Переоформить адрес", "Подключить интернет"]
            },
            {
                "trigger": "собеседование",
                "suggestions": ["Подготовить резюме", "Изучить компанию", "Выбрать одежду", "Проверить маршрут"]
            },
        ]

def _save_workflows(data: list):
    os.makedirs(os.path.dirname(WORKFLOWS_FILE), exist_ok=True)
    with open(WORKFLOWS_FILE, "w") as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=2))

def _check_workflow_triggers(title: str) -> list[str]:
    """Check if event title matches any workflow triggers."""
    workflows = _load_workflows()
    title_lower = title.lower()
    suggestions = []
    for wf in workflows:
        trigger = wf.get("trigger", "").lower()
        if trigger and trigger in title_lower:
            suggestions.extend(wf.get("suggestions", []))
    return suggestions


# ══════════════════════════════════════════════════════════
# ██  Financial Triggers (salary day, bill reminders)
# ══════════════════════════════════════════════════════════
FINANCIAL_TRIGGERS_FILE = os.path.join(os.path.dirname(__file__), "data", "financial_triggers.json")

def _load_financial_triggers() -> list:
    try:
        with open(FINANCIAL_TRIGGERS_FILE, "r") as f:
            return json.loads(f.read())
    except (FileNotFoundError, json.JSONDecodeError):
        return [
            {"day": 1, "name": "Оплата аренды", "reminder": "Не забудь оплатить аренду!"},
            {"day": 10, "name": "Коммунальные платежи", "reminder": "Проверь и оплати коммунальные счета"},
            {"day": 25, "name": "Зарплата", "reminder": "Зарплата пришла! Проверь бюджет и отложи сбережения"},
        ]

def _save_financial_triggers(data: list):
    os.makedirs(os.path.dirname(FINANCIAL_TRIGGERS_FILE), exist_ok=True)
    with open(FINANCIAL_TRIGGERS_FILE, "w") as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=2))


# ══════════════════════════════════════════════════════════
# ██  Pattern Analysis
# ══════════════════════════════════════════════════════════
PATTERNS_FILE = os.path.join(os.path.dirname(__file__), "data", "patterns.json")

def _load_patterns() -> dict:
    try:
        with open(PATTERNS_FILE, "r") as f:
            return json.loads(f.read())
    except (FileNotFoundError, json.JSONDecodeError):
        return {"task_completions": [], "task_failures": [], "active_hours": {}, "last_contacts": {}}

def _save_patterns(data: dict):
    os.makedirs(os.path.dirname(PATTERNS_FILE), exist_ok=True)
    with open(PATTERNS_FILE, "w") as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=2))

def _record_task_activity(task_title: str, completed: bool, hour: int = None):
    """Record task completion/failure for pattern analysis."""
    patterns = _load_patterns()
    now = datetime.now(TIMEZONE)
    entry = {
        "title": task_title,
        "date": now.strftime("%Y-%m-%d"),
        "hour": hour or now.hour,
        "weekday": now.weekday(),
    }
    if completed:
        patterns.setdefault("task_completions", []).append(entry)
    else:
        patterns.setdefault("task_failures", []).append(entry)
    
    # Track active hours
    hour_key = str(now.hour)
    active_hours = patterns.setdefault("active_hours", {})
    active_hours[hour_key] = active_hours.get(hour_key, 0) + 1
    
    # Keep last 200 entries
    patterns["task_completions"] = patterns["task_completions"][-200:]
    patterns["task_failures"] = patterns["task_failures"][-200:]
    _save_patterns(patterns)

def _record_contact(name: str):
    """Record last contact with a person for forgotten-reminder detection."""
    patterns = _load_patterns()
    patterns.setdefault("last_contacts", {})[name.lower()] = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    _save_patterns(patterns)

def _get_productivity_insights() -> str:
    """Analyze patterns and return insights."""
    patterns = _load_patterns()
    insights = []
    
    # Analyze active hours
    active_hours = patterns.get("active_hours", {})
    if active_hours:
        sorted_hours = sorted(active_hours.items(), key=lambda x: x[1], reverse=True)
        peak_hours = [h for h, _ in sorted_hours[:3]]
        if peak_hours:
            insights.append(f"Пиковая активность: {', '.join(h + ':00' for h in peak_hours)}")
    
    # Analyze task failures by time
    failures = patterns.get("task_failures", [])
    if len(failures) >= 5:
        evening_failures = sum(1 for f in failures if f.get("hour", 0) >= 18)
        morning_failures = sum(1 for f in failures if f.get("hour", 12) < 12)
        if evening_failures > morning_failures * 2:
            insights.append("Вечерние задачи срываются чаще. Попробуй ставить важное на утро.")
        elif morning_failures > evening_failures * 2:
            insights.append("Утренние задачи срываются чаще. Возможно, стоит начинать позже.")
    
    # Analyze forgotten contacts
    last_contacts = patterns.get("last_contacts", {})
    now = datetime.now(TIMEZONE)
    forgotten = []
    for name, last_date_str in last_contacts.items():
        try:
            last_date = datetime.strptime(last_date_str, "%Y-%m-%d")
            days_ago = (now - last_date.replace(tzinfo=TIMEZONE)).days
            if days_ago > 30:
                forgotten.append((name.title(), days_ago))
        except Exception:
            continue
    
    if forgotten:
        forgotten.sort(key=lambda x: x[1], reverse=True)
        names = [f"{n} ({d} дн.)" for n, d in forgotten[:3]]
        insights.append(f"Давно не общался: {', '.join(names)}")
    
    return "\n".join(insights) if insights else ""


# ══════════════════════════════════════════════════════════
# ██  Autonomous Agents - Meeting Prep
# ══════════════════════════════════════════════════════════
def _prepare_meeting_brief(event_title: str) -> str:
    """Use Claude to generate a brief preparation note for an upcoming meeting."""
    try:
        # Get memory context for relevant facts
        mem = _load_memory()
        facts = mem.get("facts", [])
        relevant_facts = [f for f in facts if any(
            word.lower() in f.lower() 
            for word in event_title.lower().split() 
            if len(word) > 3
        )]
        
        context = ""
        if relevant_facts:
            context = "\nИзвестные факты: " + "; ".join(relevant_facts[:5])
        
        response = claude_client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=500,
            system="Ты - ассистент для подготовки ко встречам. Пиши кратко на русском.",
            messages=[{
                "role": "user",
                "content": f"Подготовь краткую шпаргалку для встречи/события: \"{event_title}\".{context}\n\nНапиши 3-5 пунктов: что подготовить, о чем помнить, возможные вопросы. Будь конкретен и полезен."
            }]
        )
        return response.content[0].text.strip()
    except Exception as e:
        logger.error("Meeting prep error: %s", e)
        return ""

# ══════════════════════════════════════════════════════════
# ██  Weather Forecast
# ══════════════════════════════════════════════════════════
def _get_weather_forecast() -> str:
    """Get today's weather forecast using wttr.in API."""
    import urllib.request
    try:
        req = urllib.request.Request(
            "https://wttr.in/Moscow?format=%C+%t+%w&lang=ru",
            headers={"User-Agent": "curl/7.68.0"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            weather = resp.read().decode("utf-8").strip()
        return weather if weather and "Unknown" not in weather else ""
    except Exception as e:
        logger.warning("Weather API error: %s", e)
        return ""

# ══════════════════════════════════════════════════════════
# ██  Geo-Reminders (Location-based)
# ══════════════════════════════════════════════════════════
GEO_REMINDERS_FILE = os.path.join(os.path.dirname(__file__), "data", "geo_reminders.json")

def _load_geo_reminders() -> list:
    try:
        with open(GEO_REMINDERS_FILE, "r") as f:
            return json.loads(f.read())
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def _save_geo_reminders(data: list):
    os.makedirs(os.path.dirname(GEO_REMINDERS_FILE), exist_ok=True)
    with open(GEO_REMINDERS_FILE, "w") as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=2))

async def handle_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle location messages — check geo-reminders."""
    msg = update.message
    if not msg or not msg.location:
        return
    
    chat_id = msg.chat_id
    if ALLOWED_CHAT_IDS and chat_id not in ALLOWED_CHAT_IDS:
        return
    
    lat = msg.location.latitude
    lon = msg.location.longitude
    
    geo_reminders = await asyncio.to_thread(_load_geo_reminders)
    if not geo_reminders:
        return
    
    # Check which reminders are nearby (within 500m)
    import math
    triggered = []
    remaining = []
    
    for reminder in geo_reminders:
        r_lat = reminder.get("lat", 0)
        r_lon = reminder.get("lon", 0)
        
        # Haversine distance approximation
        dlat = math.radians(lat - r_lat)
        dlon = math.radians(lon - r_lon)
        a = math.sin(dlat/2)**2 + math.cos(math.radians(r_lat)) * math.cos(math.radians(lat)) * math.sin(dlon/2)**2
        distance = 6371000 * 2 * math.asin(math.sqrt(a))
        
        if distance <= reminder.get("radius", 500):
            triggered.append(reminder)
        else:
            remaining.append(reminder)
    
    if triggered:
        response = "📍 <b>Напоминания для этой локации:</b>\n\n"
        for r in triggered:
            response += f"• {r.get('text', '?')}\n"
        
        await msg.reply_text(response, parse_mode="HTML")
        
        # Remove triggered reminders
        await asyncio.to_thread(_save_geo_reminders, remaining)

# ══════════════════════════════════════════════════════════
# ██  YouTube Summarization
# ══════════════════════════════════════════════════════════
def _summarize_youtube(url: str, question: str = "summary") -> str:
    """Summarize a YouTube video using transcript/subtitles + Claude."""
    import urllib.request
    import urllib.parse
    import re as _re
    
    # Extract video ID
    video_id = None
    if "youtu.be/" in url:
        video_id = url.split("youtu.be/")[1].split("?")[0].split("&")[0]
    elif "v=" in url:
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        video_id = params.get("v", [None])[0]
    
    if not video_id:
        return ""
    
    # Try to get transcript via youtube-transcript-api style
    # Use a simple approach: fetch the page and extract captions
    transcript_text = ""
    
    # Method 1: Try fetching auto-generated captions via timedtext API
    try:
        # First get the video page to find caption tracks
        req = urllib.request.Request(
            f"https://www.youtube.com/watch?v={video_id}",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            page_html = resp.read().decode("utf-8", errors="ignore")
        
        # Extract title
        title_match = _re.search(r'"title":"([^"]+)"', page_html)
        video_title = title_match.group(1) if title_match else "Unknown"
        
        # Find captions URL in playerCaptionsTracklistRenderer
        caption_match = _re.search(r'"captionTracks":\[(.+?)\]', page_html)
        if caption_match:
            tracks_json = "[" + caption_match.group(1) + "]"
            import json as _json
            tracks = _json.loads(tracks_json)
            
            # Prefer Russian, then English, then any
            caption_url = None
            for lang_pref in ["ru", "en", "a."]:
                for track in tracks:
                    if _re.search(lang_pref, track.get("languageCode", "")):
                        caption_url = track.get("baseUrl", "")
                        break
                if caption_url:
                    break
            if not caption_url and tracks:
                caption_url = tracks[0].get("baseUrl", "")
            
            if caption_url:
                # Fetch captions XML
                cap_req = urllib.request.Request(caption_url + "&fmt=srv3",
                    headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(cap_req, timeout=15) as cap_resp:
                    cap_xml = cap_resp.read().decode("utf-8", errors="ignore")
                
                # Extract text from XML
                texts = _re.findall(r'<text[^>]*>(.*?)</text>', cap_xml, _re.DOTALL)
                transcript_text = " ".join(
                    t.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&#39;", "'").replace("&quot;", '"')
                    for t in texts
                )
    except Exception as e:
        logger.warning("YouTube transcript fetch error: %s", e)
    
    if not transcript_text:
        # Fallback: just use the title and description
        return ""
    
    # Truncate transcript if too long (Claude context limit)
    if len(transcript_text) > 15000:
        transcript_text = transcript_text[:15000] + "... [обрезано]"
    
    # Summarize with Claude
    try:
        response = claude_client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=2000,
            system="Ты — ассистент для создания кратких содержаний видео. Пиши на русском языке.",
            messages=[{
                "role": "user",
                "content": f"Вот транскрипт видео \"{video_title}\":\n\n{transcript_text}\n\nСоздай структурированное краткое содержание:\n1. Главная тема (1-2 предложения)\n2. Ключевые тезисы (3-7 пунктов)\n3. Выводы/рекомендации (если есть)\n\nПиши кратко и по делу."
            }]
        )
        return f"📹 <b>{video_title}</b>\n\n" + response.content[0].text.strip()
    except Exception as e:
        logger.error("Claude summarization error: %s", e)
        return ""


# ══════════════════════════════════════════════════════════
# ██  TOOL DISPATCH HELPERS
# ══════════════════════════════════════════════════════════

async def _handle_habit_done(habit_name: str, thinking_msg, chat_id: int):
    """Handle marking a habit as done."""
    habits = _load_habits(chat_id)
    if not habits:
        # Auto-create the habit
        habits[habit_name] = {"created": datetime.now(TIMEZONE).strftime("%Y-%m-%d"), "history": []}
        _save_habits(chat_id, habits)
    
    # Fuzzy match
    matched_key = None
    name_lower = habit_name.lower()
    for key in habits:
        if name_lower in key.lower() or key.lower() in name_lower:
            matched_key = key
            break
    
    if not matched_key:
        # Auto-create
        matched_key = habit_name
        habits[matched_key] = {"created": datetime.now(TIMEZONE).strftime("%Y-%m-%d"), "history": []}
    
    today_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    history = habits[matched_key].get("history", [])
    
    if today_str in history:
        await thinking_msg.edit_text(
            f"\u2705 <b>{matched_key}</b> уже отмечена сегодня!",
            parse_mode="HTML"
        )
        return
    
    history.append(today_str)
    habits[matched_key]["history"] = history
    _save_habits(chat_id, habits)
    
    # Calculate streak
    streak = 0
    check_date = datetime.now(TIMEZONE).date()
    while check_date.strftime("%Y-%m-%d") in history:
        streak += 1
        check_date -= timedelta(days=1)
    
    streak_text = f"\U0001f525 Серия: {streak} дн." if streak > 1 else ""
    await thinking_msg.edit_text(
        f"\u2705 <b>{matched_key}</b> — выполнено!\n{streak_text}",
        parse_mode="HTML"
    )

async def _handle_habit_add(habit_name: str, thinking_msg, chat_id: int):
    """Handle adding a new habit."""
    habits = _load_habits(chat_id)
    if habit_name in habits:
        await thinking_msg.edit_text(
            f"\U0001f4ca <b>{habit_name}</b> уже отслеживается!",
            parse_mode="HTML"
        )
        return
    
    habits[habit_name] = {"created": datetime.now(TIMEZONE).strftime("%Y-%m-%d"), "history": []}
    _save_habits(chat_id, habits)
    await thinking_msg.edit_text(
        f"\u2795 <b>Привычка добавлена:</b> {habit_name}\n\n"
        f"Отмечайте выполнение просто написав что сделали!",
        parse_mode="HTML"
    )

async def _handle_show_week(thinking_msg):
    """Handle showing week schedule."""
    try:
        events = await asyncio.to_thread(get_week_events)
        if not events:
            await thinking_msg.edit_text("\U0001f4c5 <b>На этой неделе пусто!</b>", parse_mode="HTML")
            return
        
        now = datetime.now(TIMEZONE)
        response = f"\U0001f4c5 <b>Неделя {now.strftime('%d.%m')}:</b>\n\n"
        
        # Group by date
        by_date = {}
        for ev in events:
            d = ev.get("date", "?")
            by_date.setdefault(d, []).append(ev)
        
        for date_str in sorted(by_date.keys()):
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                day_name = WEEKDAYS_RU.get(dt.weekday(), "")
                display = f"{dt.strftime('%d.%m')} {day_name}"
            except Exception:
                display = date_str
            response += f"<b>{display}:</b>\n"
            for ev in by_date[date_str]:
                time_str = ev.get("time", "")
                title = ev.get("title", ev.get("summary", "?"))
                if time_str:
                    response += f"  {time_str} — {title}\n"
                else:
                    response += f"  \u2022 {title}\n"
            response += "\n"
        
        if len(response) > 4000:
            response = response[:3997] + "..."
        await thinking_msg.edit_text(response, parse_mode="HTML")
    except Exception as e:
        logger.error("Show week error: %s", e)
        await thinking_msg.edit_text(f"\u274c Ошибка: {str(e)[:100]}")

async def _handle_show_today(thinking_msg):
    """Handle showing today's schedule."""
    try:
        events = await asyncio.to_thread(get_today_events)
        if not events:
            await thinking_msg.edit_text("\U0001f4c5 <b>Сегодня пусто!</b>\n\nНет запланированных событий.", parse_mode="HTML")
            return
        
        now = datetime.now(TIMEZONE)
        day_name = WEEKDAYS_RU.get(now.weekday(), "")
        response = f"\U0001f4c5 <b>Сегодня ({now.strftime('%d.%m')}, {day_name}):</b>\n\n"
        
        for ev in events:
            time_str = ev.get("time", "")
            title = ev.get("title", ev.get("summary", "?"))
            if time_str:
                response += f"  {time_str} — {title}\n"
            else:
                response += f"  \u2022 {title}\n"
        
        await thinking_msg.edit_text(response, parse_mode="HTML")
    except Exception as e:
        logger.error("Show today error: %s", e)
        await thinking_msg.edit_text(f"\u274c Ошибка: {str(e)[:100]}")

async def _handle_show_habits(thinking_msg, chat_id: int):
    """Handle showing habits status."""
    habits = _load_habits(chat_id)
    if not habits:
        await thinking_msg.edit_text(
            "\U0001f4ca <b>Нет отслеживаемых привычек</b>\n\n"
            "Напишите \"хочу отслеживать [привычку]\" чтобы начать!",
            parse_mode="HTML"
        )
        return
    
    today_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    response = "\U0001f4ca <b>Трекер привычек:</b>\n\n"
    
    for name, data in habits.items():
        history = data.get("history", [])
        done_today = today_str in history
        
        # Calculate streak
        streak = 0
        check_date = datetime.now(TIMEZONE).date()
        while check_date.strftime("%Y-%m-%d") in history:
            streak += 1
            check_date -= timedelta(days=1)
        
        # Last 7 days visualization
        week_viz = ""
        for i in range(6, -1, -1):
            d = (datetime.now(TIMEZONE) - timedelta(days=i)).strftime("%Y-%m-%d")
            week_viz += "\u2705" if d in history else "\u2b1c"
        
        status = "\u2705" if done_today else "\u2b1c"
        streak_text = f" \U0001f525{streak}" if streak > 1 else ""
        response += f"{status} <b>{name}</b>{streak_text}\n   {week_viz}\n"
    
    response += "\n<i>Отмечайте привычки просто написав что сделали!</i>"
    await thinking_msg.edit_text(response, parse_mode="HTML")

async def _handle_book_search(update, context, book_data, thinking_msg):
    """Delegate to existing book search logic."""
    # This calls into the existing book search code
    query = book_data.get("query", "")
    author = book_data.get("author")
    search_queries = book_data.get("search_queries", [query])
    send_to_kindle = book_data.get("send_to_kindle", False)
    
    # Use the existing search_flibusta function
    results = None
    for sq in search_queries:
        results = await asyncio.to_thread(search_flibusta, sq)
        if results:
            break
    
    if not results:
        # Try AI rethink
        alt_queries = await asyncio.to_thread(_ai_rethink_book_query, query, author)
        for aq in alt_queries:
            results = await asyncio.to_thread(search_flibusta, aq)
            if results:
                break
    
    if not results:
        await thinking_msg.edit_text(
            f"\U0001f4da <b>Книга не найдена:</b> {query}\n\n"
            f"Попробуйте другое название или автора.",
            parse_mode="HTML"
        )
        return
    
    # Show results with inline keyboard
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    response = f"\U0001f4da <b>Найдено ({len(results)}):</b>\n\n"
    buttons = []
    for i, book in enumerate(results[:5], 1):
        title = book.get("title", "?")
        book_author = book.get("author", "?")
        response += f"{i}. <b>{title}</b> — {book_author}\n"
        btn_data = f"book_{book.get('id', i)}"
        if send_to_kindle:
            btn_data = f"kindle_{book.get('id', i)}"
        buttons.append([InlineKeyboardButton(f"{i}. {title[:30]}", callback_data=btn_data)])
    
    markup = InlineKeyboardMarkup(buttons) if buttons else None
    await thinking_msg.edit_text(response, parse_mode="HTML", reply_markup=markup)

async def _handle_xray(update, context, xray_data, thinking_msg):
    """Delegate to existing X-Ray logic."""
    book_title = xray_data.get("book_title", "")
    author = xray_data.get("author", "")
    progress = xray_data.get("progress_percent")
    
    author_str = f" ({author})" if author else ""
    progress_str = f" [{progress}%]" if progress else ""
    
    xray_prompt = f"Сделай подробный X-Ray анализ книги \"{book_title}\"{author_str}{progress_str}.\n"
    xray_prompt += "Включи: главные персонажи, темы, ключевые события, связи между персонажами."
    if progress and progress < 100:
        xray_prompt += f"\nВАЖНО: читатель на {progress}% — НЕ спойлери то, что после этого момента!"
    
    try:
        response = claude_client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=2048,
            messages=[{"role": "user", "content": xray_prompt}],
        )
        xray_text = response.content[0].text.strip()
        if len(xray_text) > 4000:
            xray_text = xray_text[:3997] + "..."
        await thinking_msg.edit_text(
            f"\U0001f52c <b>X-Ray: {book_title}{author_str}</b>\n\n{xray_text}",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error("X-Ray error: %s", e)
        await thinking_msg.edit_text(f"\u274c Ошибка X-Ray: {str(e)[:100]}")

async def _handle_url_to_kindle(update, context, kindle_data, thinking_msg):
    """Delegate to existing URL-to-Kindle logic."""
    url = kindle_data.get("url", "")
    title = kindle_data.get("title", "")
    
    try:
        success = await asyncio.to_thread(send_url_to_kindle, url)
        if success:
            display = title or url[:50]
            await thinking_msg.edit_text(
                f"\U0001f4e4 <b>Отправлено на Kindle!</b>\n\n\U0001f4d6 {display}",
                parse_mode="HTML"
            )
        else:
            await thinking_msg.edit_text("\u274c Не удалось отправить на Kindle")
    except Exception as e:
        logger.error("URL to Kindle error: %s", e)
        await thinking_msg.edit_text(f"\u274c Ошибка: {str(e)[:100]}")

async def _handle_edit_event(update, context, edit_data, thinking_msg):
    """Delegate to existing edit event logic."""
    search_query = edit_data.get("search_query", "")
    new_date = edit_data.get("new_date")
    new_time = edit_data.get("new_time")
    new_title = edit_data.get("new_title")
    delete = edit_data.get("delete", False)
    
    await thinking_msg.edit_text(f"\U0001f50d <b>Ищу событие:</b> {search_query}...", parse_mode="HTML")
    
    try:
        # Search in calendar
        events = await asyncio.to_thread(search_events, search_query)
        if not events:
            await thinking_msg.edit_text(
                f"\u274c <b>Не найдено:</b> {search_query}\n\nПопробуйте другие ключевые слова.",
                parse_mode="HTML"
            )
            return
        
        event = events[0]  # Take first match
        
        if delete:
            uid = event.get("uid", "")
            cal_key = event.get("calendar", "")
            ok = await asyncio.to_thread(delete_event_by_uid, uid, cal_key)
            if ok:
                await thinking_msg.edit_text(
                    f"\U0001f5d1 <b>Удалено:</b> {event.get('title', search_query)}",
                    parse_mode="HTML"
                )
            else:
                await thinking_msg.edit_text("\u274c Не удалось удалить событие")
            return
        
        # Reschedule
        uid = event.get("uid", "")
        cal_key = event.get("calendar", "")
        ok = await asyncio.to_thread(
            reschedule_event, uid, cal_key, new_date, new_time, new_title
        )
        if ok:
            changes = []
            if new_date:
                changes.append(f"\U0001f4c5 {new_date}")
            if new_time:
                changes.append(f"\u23f0 {new_time}")
            if new_title:
                changes.append(f"\u270f\ufe0f {new_title}")
            await thinking_msg.edit_text(
                f"\u2705 <b>Изменено:</b> {event.get('title', search_query)}\n\n" +
                "\n".join(changes),
                parse_mode="HTML"
            )
        else:
            await thinking_msg.edit_text("\u274c Не удалось изменить событие")
    except Exception as e:
        logger.error("Edit event error: %s", e)
        await thinking_msg.edit_text(f"\u274c Ошибка: {str(e)[:100]}")

async def _handle_recurring_tasks(update, context, recurring_data, thinking_msg):
    """Delegate to existing recurring tasks logic."""
    summary = recurring_data.get("summary", "")
    tasks = recurring_data.get("tasks", [])
    
    if not tasks:
        await thinking_msg.edit_text("\u274c Нет задач для создания")
        return
    
    total_created = 0
    total_failed = 0
    
    for task_def in tasks:
        title = task_def.get("title", summary)
        start_date = task_def.get("start_date", "")
        end_date = task_def.get("end_date", "")
        times = task_def.get("times", ["09:00"])
        calendar = task_def.get("calendar", "reminders")
        alarm_minutes = task_def.get("alarm_minutes", 0)
        
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except (ValueError, TypeError):
            continue
        
        current = start_dt
        while current <= end_dt:
            date_str = current.strftime("%Y-%m-%d")
            for t in times:
                try:
                    ok = await asyncio.to_thread(
                        create_calendar_event,
                        title=title,
                        date=date_str,
                        time_start=t,
                        time_end=None,
                        all_day=False,
                        calendar=calendar,
                        alarm_minutes=alarm_minutes,
                        description=summary,
                    )
                    if ok:
                        total_created += 1
                    else:
                        total_failed += 1
                except Exception:
                    total_failed += 1
            current += timedelta(days=1)
    
    days_count = 0
    if tasks:
        try:
            s = datetime.strptime(tasks[0].get("start_date", ""), "%Y-%m-%d")
            e = datetime.strptime(tasks[0].get("end_date", ""), "%Y-%m-%d")
            days_count = (e - s).days + 1
        except Exception:
            pass
    
    fail_note = f" (ошибок: {total_failed})" if total_failed else ""
    await thinking_msg.edit_text(
        f"\U0001f501 <b>Серия создана!</b>\n\n"
        f"\U0001f4cb {summary}\n"
        f"\U0001f4c5 {days_count} дней\n"
        f"\u2705 Создано {total_created} напоминаний{fail_note}",
        parse_mode="HTML"
    )
    
    # Store for deletion
    # (simplified - not storing UIDs for recurring in this version)

async def _create_calendar_event_from_data(update, context, data: dict, thinking_msg):
    """Create a calendar event from structured data dict."""
    title = data.get("title", "")
    description = data.get("description", "")
    date = data.get("date", "")
    time_start = data.get("time_start")
    time_end = data.get("time_end")
    all_day = data.get("all_day", True)
    calendar_key = data.get("calendar", "family")
    alarm_minutes = data.get("alarm_minutes", 30)
    category = data.get("category", "personal")
    event_type = data.get("type", "event")
    
    # Add category emoji to title
    cat_emojis = {"work": "\U0001f4bc", "home": "\U0001f3e0", "personal": "\U0001f464", "longterm": "\U0001f3af"}
    cat_names = {"work": "Работа", "home": "Дом", "personal": "Личное", "longterm": "Долгосрочное"}
    emoji = cat_emojis.get(category, "\U0001f4cc")
    cat_display = cat_names.get(category, category)
    
    full_title = f"{emoji} [{cat_display}] {title}"
    
    try:
        uid = await asyncio.to_thread(
            create_calendar_event,
            title=full_title,
            date=date,
            time_start=time_start,
            time_end=time_end,
            all_day=all_day,
            calendar=calendar_key,
            alarm_minutes=alarm_minutes,
            description=description,
        )
        
        if uid:
            # Build confirmation message
            type_labels = {"event": "\U0001f4c5 Событие", "task": "\u2705 Задача", "reminder": "\U0001f514 Напоминание"}
            type_label = type_labels.get(event_type, "\U0001f4cc Запись")
            
            msg_text = f"{type_label} <b>создано!</b>\n\n"
            msg_text += f"<b>{title}</b>\n"
            if date:
                try:
                    dt = datetime.strptime(date, "%Y-%m-%d")
                    day_name = WEEKDAYS_RU.get(dt.weekday(), "")
                    msg_text += f"\U0001f4c5 {dt.strftime('%d.%m.%Y')} ({day_name})\n"
                except Exception:
                    msg_text += f"\U0001f4c5 {date}\n"
            if time_start:
                time_display = time_start
                if time_end:
                    time_display += f"\u2013{time_end}"
                msg_text += f"\u23f0 {time_display}\n"
            if description:
                msg_text += f"\n<i>{description[:150]}</i>\n"
            msg_text += f"\n\U0001f4c1 {cat_display} | \U0001f4d3 {calendar_key}"
            
            reply = await thinking_msg.edit_text(msg_text, parse_mode="HTML")
            
            # Store for deletion
            _event_store[reply.message_id] = {
                "uid": uid if isinstance(uid, str) else "",
                "calendar": calendar_key,
                "title": title,
            }
            _save_event_store()
        else:
            await thinking_msg.edit_text("\u274c Не удалось создать событие в календаре")
    except Exception as e:
        logger.error("Create event error: %s", e)
        await thinking_msg.edit_text(f"\u274c Ошибка: {str(e)[:100]}")

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
    # Authorization: allow if chat_id is in allowed list, or user is in GROUP_USERS
    user_routing = get_user_routing(msg.from_user)
    is_allowed_chat = (not ALLOWED_CHAT_IDS) or (chat_id in ALLOWED_CHAT_IDS)
    is_allowed_user = user_routing is not None
    is_private = msg.chat.type == "private"
    
    if not is_allowed_chat and not is_allowed_user and not is_private:
        logger.debug("Skipping: unauthorized chat_id %s, user %s",
                    chat_id, msg.from_user.username if msg.from_user else '?')
        return
    
    # Log user routing info for group chats
    if user_routing:
        logger.info("User routing: %s → calendar_rule=%s",
                   user_routing.get('display_name', '?'),
                   user_routing.get('calendar_rule', 'auto'))

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
                # Support both single UID and multi-UID (recurring_tasks) entries
                uids = ev_data.get("uids", [])
                if not uids and ev_data.get("uid"):
                    uids = [ev_data["uid"]]
                cal_key = ev_data.get("calendar")
                deleted = 0
                failed = 0
                for uid in uids:
                    ok = await asyncio.to_thread(delete_event_by_uid, uid, cal_key)
                    if ok:
                        deleted += 1
                    else:
                        failed += 1
                if deleted > 0:
                    if len(uids) == 1:
                        await msg.reply_text("🗑 ✅ Запись удалена из календаря!")
                    else:
                        fail_note = f" (не удалось: {failed})" if failed else ""
                        await msg.reply_text(f"🗑 ✅ Удалено {deleted} из {len(uids)} записей!{fail_note}")
                    del _event_store[reply_id]
                    _save_event_store()
                else:
                    await msg.reply_text("❌ Не удалось удалить записи")
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
        # Determine calendar for URL tasks based on user routing
        url_calendar = "work"
        if user_routing and user_routing.get("calendar_rule") != "auto":
            url_calendar = user_routing["calendar_rule"]
        
        for url in urls:
            domain = get_url_domain(url)
            data = {
                "type": "task",
                "calendar": url_calendar,
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
            
            # Also save to Read-It-Later queue
            read_later = await asyncio.to_thread(_load_read_later)
            for url, domain, uid, data in results:
                read_later.append({
                    "url": url,
                    "title": domain,
                    "added": datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M"),
                })
            # Keep max 50 items
            if len(read_later) > 50:
                read_later = read_later[-50:]
            await asyncio.to_thread(_save_read_later, read_later)
            
            response += f"📚 <i>Также добавлено в список чтения (всего: {len(read_later)})</i>"
            
            reply_msg = await msg.reply_text(response, parse_mode="HTML", disable_web_page_preview=True)
            
            for url, domain, uid, data in results:
                _event_store[reply_msg.message_id] = {"uid": uid, "calendar": url_calendar}
                _save_event_store()
            
            return

    # ── Regular message — Claude analysis ──
    context_parts = []
    forward_origin = getattr(msg, 'forward_origin', None)
    if forward_origin:
        if hasattr(forward_origin, 'sender_user') and forward_origin.sender_user:
            context_parts.append(f"Переслано от: {forward_origin.sender_user.first_name}")
        if hasattr(forward_origin, 'chat') and forward_origin.chat:
            context_parts.append(f"Переслано из: {forward_origin.chat.title}")
        if hasattr(forward_origin, 'date') and forward_origin.date:
            context_parts.append(f"Дата оригинала: {forward_origin.date.strftime('%Y-%m-%d %H:%M')}")

    full_text = text
    if context_parts:
        full_text = "\n".join(context_parts) + "\n\nТекст сообщения:\n" + text

    # Build sender context for Claude (helps with calendar routing in group chats)
    sender_context = ""
    if user_routing:
        sender_name = user_routing.get("display_name", "")
        cal_rule = user_routing.get("calendar_rule", "auto")
        if cal_rule == "auto":
            sender_context = (
                f"Сообщение от пользователя: {sender_name}. "
                f"Определи календарь на основе содержания: work для рабочих дел, family для семейных."
            )
        else:
            sender_context = (
                f"Сообщение от пользователя: {sender_name}. "
                f"Все календарные записи этого пользователя идут в календарь: {cal_rule}."
            )
    elif msg.from_user and msg.chat.type != "private":
        sender_context = f"Сообщение от пользователя: {msg.from_user.first_name or '?'}."

    thinking_msg = await msg.reply_text("🤔 Анализирую сообщение...")

    logger.info("Calling Claude API for analysis...")
    try:
        data = await asyncio.to_thread(analyze_message, full_text, sender_context)
    except Exception as e:
        logger.error("analyze_message thread error: %s", e)
        data = None
    logger.info("Claude analysis result: %s", data is not None)
    if data is None:
        await thinking_msg.edit_text(
            "❌ Не удалось проанализировать сообщение. Попробуйте ещё раз."
        )
        return

    # (skip check removed — Tool Calling handles this via ask_clarification tool)
    
    # (confidence check removed — Tool Calling handles uncertainty via ask_clarification tool)
    
    # Streaming-like: show what was understood (Tool Calling version)
    tool_name_preview = data.get("tool_name", "")
    tool_input_preview = data.get("tool_input", {})
    tool_type_labels = {
        "create_calendar_event": "📅 Событие",
        "create_note": "📝 Заметка",
        "create_diary_entry": "📖 Дневник",
        "search_book": "📚 Поиск книги",
        "mark_habit_done": "✅ Привычка",
        "add_habit": "➕ Привычка",
        "save_memory": "🧠 Память",
        "server_status": "🖥 Сервер",
        "manage_container": "🐳 Контейнер",
        "web_search": "🔍 Поиск",
        "add_expense": "💰 Расход",
        "expense_report": "📊 Отчёт",
    }
    if tool_name_preview in tool_type_labels:
        title_val = tool_input_preview.get("title", tool_input_preview.get("query", "..."))
        progress_text = f"{tool_type_labels[tool_name_preview]}: <b>{title_val}</b>"
        date_val = tool_input_preview.get("date", "")
        if date_val:
            progress_text += f"\n📅 {date_val}"
            if tool_name_preview == "create_calendar_event":
                try:
                    overload = await asyncio.to_thread(_check_day_overload, date_val)
                    if overload.get("is_overloaded"):
                        ec = overload["event_count"]
                        th = overload["total_hours"]
                        progress_text += "\n\u26a0\ufe0f <i>\u0414\u0435\u043d\u044c \u0437\u0430\u0433\u0440\u0443\u0436\u0435\u043d: " + str(ec) + " \u0441\u043e\u0431\u044b\u0442\u0438\u0439 (" + str(th) + "\u0447)</i>"
                except Exception:
                    pass
        time_val = tool_input_preview.get("time_start", "")
        if time_val:
            progress_text += f" в {time_val}"
        try:
            await thinking_msg.edit_text(f"⏳ {progress_text}\n\n<i>Создаю...</i>", parse_mode="HTML")
        except Exception:
            pass

    original_text = update.message.text or ''
    
    # ═══ Check pending dialog state ═══
    if chat_id in _dialog_state:
        state = _dialog_state[chat_id]
        if _time.time() < state.get("expires", 0):
            # Append context to the message for Claude
            pending_context = state.get("context", "")
            if pending_context:
                # Combine pending context with new message for better analysis
                text = f"[Контекст предыдущего вопроса: {pending_context}] {text}"
            del _dialog_state[chat_id]
        else:
            # Expired - remove stale state
            del _dialog_state[chat_id]
    
    # ═══ Tool Calling Dispatch ═══
    tool_name = data.get("tool_name", "")
    tool_input = data.get("tool_input", {})
    
    logger.info("Dispatching tool: %s", tool_name)
    
    # ── chat_response ──
    if tool_name == "chat_response":
        response_msg = tool_input.get("message", "")
        if response_msg:
            try:
                await thinking_msg.edit_text(response_msg, parse_mode="HTML")
            except Exception:
                # HTML parse error - send without formatting
                import html as _html_mod
                clean_msg = _html_mod.unescape(response_msg)
                # Remove any broken HTML tags
                clean_msg = re.sub(r'<[^>]*$', '', clean_msg)
                try:
                    await thinking_msg.edit_text(clean_msg)
                except Exception:
                    await thinking_msg.edit_text(response_msg[:4000])
        else:
            await thinking_msg.edit_text("👍")
        return
    
    # ── save_memory ──
    if tool_name == "save_memory":
        fact = tool_input.get("fact", "")
        if fact:
            await asyncio.to_thread(_add_memory_fact, fact)
            await thinking_msg.edit_text(
                f"\U0001f9e0 <b>Запомнил!</b>\n\n"
                f"\U0001f4be <i>{fact}</i>\n\n"
                f"Я буду учитывать это в будущем.",
                parse_mode="HTML"
            )
            try:
                _add_knowledge_entry(original_text, f"memory:{fact[:50]}", "save_memory", fact[:200], "general")
            except Exception:
                pass
        else:
            await thinking_msg.edit_text("\u274c Не удалось извлечь факт для запоминания.")
        return
    
    # ── ask_clarification ──
    if tool_name == "ask_clarification":
        question = tool_input.get("question", "Уточните, пожалуйста.")
        context_info = tool_input.get("context", "")
        _dialog_state[chat_id] = {
            "pending_action": "clarify",
            "context": context_info,
            "expires": _time.time() + 300,
        }
        await thinking_msg.edit_text(
            f"\u2753 <b>Нужно уточнение</b>\n\n{question}",
            parse_mode="HTML"
        )
        return
    
    # ── find_free_slot ──
    if tool_name == "find_free_slot":
        title = tool_input.get("title", "задачу")
        duration = tool_input.get("duration_hours", 1.0)
        preferred = tool_input.get("preferred_time", "any")
        
        await thinking_msg.edit_text("\U0001f50d <b>Ищу свободные слоты...</b>", parse_mode="HTML")
        try:
            slots = await asyncio.to_thread(_find_free_slots, duration, preferred)
            if slots:
                response = f"\U0001f4c5 <b>Свободные слоты для \"{title}\" ({duration}ч):</b>\n\n"
                for i, slot in enumerate(slots[:5], 1):
                    response += f"  {i}. <b>{slot['date']}</b> {slot['start']}\u2013{slot['end']}\n"
                overload_warnings = []
                checked_dates = set()
                for slot in slots[:5]:
                    iso_date = slot.get("iso_date", "")
                    if iso_date and iso_date not in checked_dates:
                        checked_dates.add(iso_date)
                        overload = await asyncio.to_thread(_check_day_overload, iso_date)
                        if overload.get("is_overloaded"):
                            overload_warnings.append(f"\u26a0\ufe0f {slot['date']}: уже {overload['event_count']} событий ({overload['total_hours']}ч)")
                if overload_warnings:
                    response += "\n" + "\n".join(overload_warnings) + "\n"
                response += "\n\U0001f4a1 <i>Напишите дату и время чтобы создать событие</i>"
                await thinking_msg.edit_text(response, parse_mode="HTML")
            else:
                await thinking_msg.edit_text(
                    f"\U0001f630 <b>Не нашёл свободных слотов</b>\n\nНа ближайшую неделю нет окна в {duration}ч.",
                    parse_mode="HTML"
                )
        except Exception as e:
            logger.error("Find slot error: %s", e)
            await thinking_msg.edit_text(f"\u274c Ошибка: {str(e)[:100]}")
        return
    
    # ── reschedule_overdue ──
    if tool_name == "reschedule_overdue":
        await thinking_msg.edit_text("\U0001f504 <b>Проверяю просроченные задачи...</b>", parse_mode="HTML")
        try:
            reminders = await asyncio.to_thread(get_pending_reminders)
            today_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
            overdue = [r for r in reminders if r.get("due_date") and r["due_date"] < today_str and not r.get("completed")]
            if not overdue:
                await thinking_msg.edit_text("\u2705 <b>Нет просроченных задач!</b>", parse_mode="HTML")
                return
            response = f"\U0001f4cb <b>Просроченные задачи ({len(overdue)}):</b>\n\n"
            for r in overdue[:10]:
                days_overdue = (datetime.strptime(today_str, "%Y-%m-%d") - datetime.strptime(r["due_date"], "%Y-%m-%d")).days
                response += f"  \u2022 {r['title']} <i>(просрочено {days_overdue} дн.)</i>\n"
            response += "\n\U0001f4a1 <i>Напишите \"перенеси все на сегодня\" для переноса</i>"
            await thinking_msg.edit_text(response, parse_mode="HTML")
        except Exception as e:
            logger.error("Reschedule overdue error: %s", e)
            await thinking_msg.edit_text(f"\u274c Ошибка: {str(e)[:100]}")
        return
    
    # ── youtube_summary ──
    if tool_name == "youtube_summary":
        url = tool_input.get("url", "")
        question = tool_input.get("question", "summary")
        await thinking_msg.edit_text("\U0001f3ac <b>Анализирую видео...</b>", parse_mode="HTML")
        try:
            summary = await asyncio.to_thread(_summarize_youtube, url, question)
            if summary:
                # Truncate if too long for Telegram
                if len(summary) > 4000:
                    summary = summary[:3997] + "..."
                await thinking_msg.edit_text(
                    f"\U0001f3ac <b>YouTube Summary</b>\n\n{summary}",
                    parse_mode="HTML"
                )
            else:
                await thinking_msg.edit_text("\u274c Не удалось получить субтитры или содержание видео.")
        except Exception as e:
            logger.error("YouTube summary error: %s", e)
            await thinking_msg.edit_text(f"\u274c Ошибка: {str(e)[:100]}")
        return
    
    # ── create_note ──
    if tool_name == "create_note":
        title = tool_input.get("title", "Заметка")
        note_content = tool_input.get("content", "")
        body_html = f"<h1>{title}</h1><p>{note_content.replace(chr(10), '<br>')}</p>"
        success = await asyncio.to_thread(create_apple_note, title, body_html)
        if success:
            await thinking_msg.edit_text(
                f"\U0001f4dd <b>Заметка сохранена!</b>\n\n"
                f"\U0001f4cc <b>{title}</b>\n"
                f"<i>{note_content[:200]}</i>",
                parse_mode="HTML"
            )
        else:
            await thinking_msg.edit_text("\u274c Не удалось сохранить заметку в Apple Notes")
        return
    
    # ── create_diary_entry ──
    if tool_name == "create_diary_entry":
        diary_content = tool_input.get("content", "")
        success = await asyncio.to_thread(create_diary_entry, diary_content)
        if success:
            now_time = datetime.now(TIMEZONE).strftime("%H:%M")
            await thinking_msg.edit_text(
                f"\U0001f4d4 <b>Записано в дневник</b> ({now_time})\n\n"
                f"<i>{diary_content[:300]}</i>",
                parse_mode="HTML"
            )
        else:
            await thinking_msg.edit_text("\u274c Не удалось записать в дневник")
        return
    
    # ── search_book ──
    if tool_name == "search_book":
        query = tool_input.get("query", "")
        author = tool_input.get("author")
        search_queries = tool_input.get("search_queries", [query])
        send_to_kindle = tool_input.get("send_to_kindle", False)
        
        await thinking_msg.edit_text(f"\U0001f4da <b>Ищу книгу:</b> {query}...", parse_mode="HTML")
        
        # Reuse existing book search logic
        book_data = {
            "type": "book_search",
            "query": query,
            "author": author,
            "search_queries": search_queries,
            "send_to_kindle": send_to_kindle,
            "title": f"Поиск: {query}",
        }
        # Call the existing book search handler
        await _handle_book_search(update, context, book_data, thinking_msg)
        try:
            _add_knowledge_entry(original_text, f"book_search:{query}", "search_book", f"Search: {query}", "general")
        except Exception:
            pass
        return
    
    # ── xray_book ──
    if tool_name == "xray_book":
        book_title = tool_input.get("book_title", "")
        author = tool_input.get("author")
        progress = tool_input.get("progress_percent")
        
        await thinking_msg.edit_text(f"\U0001f52c <b>X-Ray анализ:</b> {book_title}...", parse_mode="HTML")
        
        xray_data = {
            "type": "xray",
            "book_title": book_title,
            "author": author,
            "progress_percent": progress,
        }
        await _handle_xray(update, context, xray_data, thinking_msg)
        return
    
    # ── url_to_kindle ──
    if tool_name == "url_to_kindle":
        url = tool_input.get("url", "")
        title = tool_input.get("title", "")
        
        await thinking_msg.edit_text(f"\U0001f4e4 <b>Отправляю на Kindle:</b> {url[:50]}...", parse_mode="HTML")
        
        kindle_data = {
            "type": "url_to_kindle",
            "url": url,
            "title": title,
        }
        await _handle_url_to_kindle(update, context, kindle_data, thinking_msg)
        return
    
    # ── mark_habit_done ──
    if tool_name == "mark_habit_done":
        habit_name = tool_input.get("habit_name", "")
        await _handle_habit_done(habit_name, thinking_msg, chat_id)
        try:
            _add_knowledge_entry(original_text, f"habit_done:{habit_name}", "mark_habit_done", habit_name, "general")
        except Exception:
            pass
        return
    
    # ── add_habit ──
    if tool_name == "add_habit":
        habit_name = tool_input.get("habit_name", "")
        await _handle_habit_add(habit_name, thinking_msg, chat_id)
        return
    
    # ── show_schedule ──
    if tool_name == "show_schedule":
        period = tool_input.get("period", "today")
        if period == "week":
            await _handle_show_week(thinking_msg)
        else:
            await _handle_show_today(thinking_msg)
        return
    
    # ── show_habits ──
    if tool_name == "show_habits":
        await _handle_show_habits(thinking_msg, chat_id)
        return
    # ── server_status ──
    if tool_name == "server_status":
        check_type = tool_input.get("check_type", "all")
        await thinking_msg.edit_text("\U0001f50d <b>Проверяю сервер...</b>", parse_mode="HTML")
        try:
            status = await asyncio.to_thread(_get_server_status, check_type)
            await thinking_msg.edit_text(status, parse_mode="HTML")
            _add_knowledge_entry(original_text, f"server_status:{check_type}", "server_status", status[:200], "server")
        except Exception as e:
            logger.error("Server status error: %s", e)
            await thinking_msg.edit_text(f"\u274c Ошибка: {str(e)[:200]}")
        return
    
    # ── manage_container ──
    if tool_name == "manage_container":
        container = tool_input.get("container", "")
        action = tool_input.get("action", "")
        await thinking_msg.edit_text(f"\U0001f504 <b>{action.title()} {container}...</b>", parse_mode="HTML")
        try:
            result = await asyncio.to_thread(_manage_container, container, action)
            if action == "logs":
                await thinking_msg.edit_text(
                    f"\U0001f4cb <b>Логи {container}:</b>\n<pre>{result[:3500]}</pre>",
                    parse_mode="HTML"
                )
            else:
                await thinking_msg.edit_text(
                    f"\u2705 <b>{action.title()} {container}:</b>\n<pre>{result[:1000]}</pre>",
                    parse_mode="HTML"
                )
            _add_knowledge_entry(original_text, f"manage:{container}:{action}", "manage_container", result[:200], "server")
        except Exception as e:
            logger.error("Container manage error: %s", e)
            await thinking_msg.edit_text(f"\u274c Ошибка: {str(e)[:200]}")
        return
    
    # ── execute_server_command ──
    if tool_name == "execute_server_command":
        command = tool_input.get("command", "")
        reason = tool_input.get("reason", "")
        
        # Safety: block dangerous commands
        dangerous = ["rm -rf /", "mkfs", "dd if=", ":(){ :|:& };:", "> /dev/sd"]
        if any(d in command for d in dangerous):
            await thinking_msg.edit_text("\U0001f6ab <b>Команда заблокирована</b>\nЭта команда потенциально опасна.", parse_mode="HTML")
            return
        
        await thinking_msg.edit_text(f"\U0001f4bb <b>Выполняю:</b> <code>{command[:100]}</code>\n<i>{reason}</i>", parse_mode="HTML")
        try:
            result = await asyncio.to_thread(_run_server_command, command)
            await thinking_msg.edit_text(
                f"\U0001f4bb <b>Результат:</b>\n<pre>{result[:3500]}</pre>",
                parse_mode="HTML"
            )
            _add_knowledge_entry(original_text, f"ssh:{command[:100]}", "execute_server_command", result[:200], "server")
        except Exception as e:
            logger.error("Server command error: %s", e)
            await thinking_msg.edit_text(f"\u274c Ошибка: {str(e)[:200]}")
        return
    
    # ── search_knowledge ──
    if tool_name == "search_knowledge":
        query = tool_input.get("query", "")
        category = tool_input.get("category", "all")
        await thinking_msg.edit_text(f"\U0001f50e <b>Ищу в базе знаний:</b> {query}...", parse_mode="HTML")
        try:
            results = await asyncio.to_thread(_search_knowledge, query, category)
            if results:
                response = f"\U0001f4da <b>Найдено в базе знаний ({len(results)} записей):</b>\n\n"
                for i, entry in enumerate(results[:7], 1):
                    ts = entry.get("timestamp", "")
                    msg = entry.get("user_message", "")[:80]
                    result_text = entry.get("result", "")[:100]
                    tool = entry.get("tool_used", "")
                    response += f"<b>{i}. [{ts}]</b> {msg}\n"
                    if result_text:
                        response += f"   \u2192 <i>{result_text}</i>\n"
                    response += "\n"
                await thinking_msg.edit_text(response, parse_mode="HTML")
            else:
                await thinking_msg.edit_text(
                    f"\U0001f50e <b>Ничего не найдено</b>\n\nПо запросу \"{query}\" нет записей в базе знаний.",
                    parse_mode="HTML"
                )
        except Exception as e:
            logger.error("Knowledge search error: %s", e)
            await thinking_msg.edit_text(f"\u274c Ошибка: {str(e)[:200]}")
        return
    
    # ── add_expense ──
    if tool_name == "add_expense":
        amount = tool_input.get("amount", 0)
        category = tool_input.get("category", "other")
        description = tool_input.get("description", "")
        date = tool_input.get("date")
        
        try:
            entry = await asyncio.to_thread(_add_expense, amount, category, description, date)
            cat_label = EXPENSE_CATEGORIES.get(category, category)
            await thinking_msg.edit_text(
                f"\U0001f4b8 <b>Расход записан!</b>\n\n"
                f"{cat_label}: <b>{description}</b>\n"
                f"\U0001f4b0 Сумма: <b>{amount:,.0f} \u20bd</b>\n"
                f"\U0001f4c5 Дата: {entry['date']}",
                parse_mode="HTML"
            )
            _add_knowledge_entry(original_text, f"expense:{amount}:{category}", "add_expense", f"{description}: {amount}RUB", "finance")
        except Exception as e:
            logger.error("Add expense error: %s", e)
            await thinking_msg.edit_text(f"\u274c Ошибка: {str(e)[:200]}")
        return
    
    # ── show_expenses ──
    if tool_name == "show_expenses":
        period = tool_input.get("period", "month")
        category = tool_input.get("category")
        
        await thinking_msg.edit_text("\U0001f4ca <b>Считаю расходы...</b>", parse_mode="HTML")
        try:
            report = await asyncio.to_thread(_get_expense_report, period, category)
            
            response = f"\U0001f4ca <b>Расходы {report['period']}:</b>\n\n"
            response += f"\U0001f4b0 <b>Итого: {report['total']:,.0f} \u20bd</b> ({report['count']} операций)\n\n"
            
            if report["by_category"]:
                response += "<b>По категориям:</b>\n"
                sorted_cats = sorted(report["by_category"].items(), key=lambda x: x[1], reverse=True)
                for cat, amount in sorted_cats:
                    cat_label = EXPENSE_CATEGORIES.get(cat, cat)
                    pct = (amount / report["total"] * 100) if report["total"] > 0 else 0
                    bar = "\u2588" * max(1, int(pct / 5))
                    response += f"  {cat_label}: <b>{amount:,.0f} \u20bd</b> ({pct:.0f}%) {bar}\n"
            
            if report["recent"]:
                response += "\n<b>Последние:</b>\n"
                for e in report["recent"][-5:]:
                    cat_label = EXPENSE_CATEGORIES.get(e["category"], "")
                    response += f"  {e['date']} {cat_label} {e['description']}: <b>{e['amount']:,.0f} \u20bd</b>\n"
            
            await thinking_msg.edit_text(response, parse_mode="HTML")
        except Exception as e:
            logger.error("Show expenses error: %s", e)
            await thinking_msg.edit_text(f"\u274c Ошибка: {str(e)[:200]}")
        return
    
    # ── web_search ──
    if tool_name == "web_search":
        query = tool_input.get("query", "")
        search_type = tool_input.get("search_type", "general")
        
        await thinking_msg.edit_text(f"\U0001f310 <b>Ищу:</b> {query}...", parse_mode="HTML")
        try:
            results = await asyncio.to_thread(_web_search, query, search_type)
            response = f"\U0001f310 <b>Результаты поиска: \"{query}\"</b>\n\n{results}"
            await thinking_msg.edit_text(response[:4000], parse_mode="HTML")
            _add_knowledge_entry(original_text, f"search:{query}", "web_search", results[:200], "general")
        except Exception as e:
            logger.error("Web search error: %s", e)
            await thinking_msg.edit_text(f"\u274c Ошибка поиска: {str(e)[:200]}")
        return
    
    # ── multi_step_plan ──
    if tool_name == "multi_step_plan":
        goal = tool_input.get("goal", "")
        steps = tool_input.get("steps", [])
        create_events = tool_input.get("create_events", False)
        
        response = f"\U0001f4cb <b>План: {goal}</b>\n\n"
        for i, step in enumerate(steps, 1):
            response += f"  {i}. {step}\n"
        
        if create_events:
            response += "\n\U0001f4c5 <i>Напишите для каждого шага дату и время, чтобы я создал события в календаре.</i>"
        else:
            response += "\n\U0001f4a1 <i>Хотите создать события в календаре для этих шагов?</i>"
        
        await thinking_msg.edit_text(response, parse_mode="HTML")
        _add_knowledge_entry(original_text, f"plan:{goal}", "multi_step_plan", f"{len(steps)} steps", "tasks")
        return

    
    # ── edit_event ──
    if tool_name == "edit_event":
        search_query = tool_input.get("search_query", "")
        new_date = tool_input.get("new_date")
        new_time = tool_input.get("new_time")
        new_title = tool_input.get("new_title")
        delete = tool_input.get("delete", False)
        
        edit_data = {
            "type": "edit_event",
            "search_query": search_query,
            "new_date": new_date,
            "new_time": new_time,
            "new_title": new_title,
            "delete": delete,
        }
        await _handle_edit_event(update, context, edit_data, thinking_msg)
        return
    
    # ── create_recurring_tasks ──
    if tool_name == "create_recurring_tasks":
        summary = tool_input.get("summary", "")
        title = tool_input.get("title", summary)
        description = tool_input.get("description", "")
        calendar = tool_input.get("calendar", "reminders")
        start_date = tool_input.get("start_date", datetime.now(TIMEZONE).strftime("%Y-%m-%d"))
        end_date = tool_input.get("end_date", "")
        times = tool_input.get("times", ["09:00"])
        alarm_minutes = tool_input.get("alarm_minutes", 0)
        
        recurring_data = {
            "type": "recurring_tasks",
            "summary": summary,
            "tasks": [{
                "title": title,
                "description": description,
                "calendar": calendar,
                "type": "reminder",
                "start_date": start_date,
                "end_date": end_date,
                "times": times,
                "alarm_minutes": alarm_minutes,
                "repeat_daily": True,
            }],
            "one_time_events": [],
        }
        await _handle_recurring_tasks(update, context, recurring_data, thinking_msg)
        return
    
    # ── create_calendar_event (default action) ──
    if tool_name == "create_calendar_event":
        # Map tool_input to the existing event creation format
        data_mapped = {
            "action": "create",
            "type": tool_input.get("event_type", "event"),
            "calendar": tool_input.get("calendar", "family"),
            "category": tool_input.get("category", "personal"),
            "title": tool_input.get("title", ""),
            "description": tool_input.get("description", ""),
            "date": tool_input.get("date", datetime.now(TIMEZONE).strftime("%Y-%m-%d")),
            "time_start": tool_input.get("time_start"),
            "time_end": tool_input.get("time_end"),
            "all_day": tool_input.get("all_day", True),
            "alarm_minutes": tool_input.get("alarm_minutes", 30),
            "confidence": 0.95,
        }
        
        # Apply user routing override
        data_mapped = apply_calendar_override(data_mapped, user_routing)
        
        # Show progress
        progress_text = f"\u2705 {data_mapped['type'].capitalize()}: {data_mapped['title']}"
        if data_mapped.get("date"):
            progress_text += f" / \U0001f4c5 {data_mapped['date']}"
        if data_mapped.get("time_start"):
            progress_text += f" в {data_mapped['time_start']}"
        try:
            await thinking_msg.edit_text(f"\u23f3 {progress_text}\n\n<i>Создаю...</i>", parse_mode="HTML")
        except Exception:
            pass
        
        # Create the event using existing logic
        await _create_calendar_event_from_data(update, context, data_mapped, thinking_msg)
        try:
            _add_knowledge_entry(
                original_text,
                f"created:{data_mapped.get('type','event')}:{data_mapped.get('title','')}",
                "create_calendar_event",
                f"{data_mapped.get('title','')} on {data_mapped.get('date','')}",
                data_mapped.get("category", "general")
            )
        except Exception:
            pass
        return
    
    # ── Unknown tool ──
    logger.warning("Unknown tool_name: %s", tool_name)
    await thinking_msg.edit_text(f"\u2753 Не знаю как обработать: {tool_name}")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Log errors and continue."""
    logger.error("Exception while handling an update: %s", context.error)
    logger.error(traceback.format_exc())



# ══════════════════════════════════════════════════════════
# ██  HABIT TRACKING SYSTEM
# ══════════════════════════════════════════════════════════
HABITS_FILE = "/app/data/habits.json"

def _load_habits() -> dict:
    """Load habits data: {habits: [...], log: {date: {habit_id: true}}}"""
    try:
        with open(HABITS_FILE, "r") as f:
            return json.loads(f.read())
    except (FileNotFoundError, json.JSONDecodeError):
        return {"habits": [], "log": {}}

def _save_habits(data: dict):
    with open(HABITS_FILE, "w") as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=2))

async def cmd_habits(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /habits command — manage habit tracking."""
    if ALLOWED_CHAT_IDS and update.effective_chat.id not in ALLOWED_CHAT_IDS:
        return
    
    args = context.args if context.args else []
    text = " ".join(args)
    
    habits_data = await asyncio.to_thread(_load_habits)
    habits = habits_data.get("habits", [])
    log = habits_data.get("log", {})
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    
    if not text:
        # Show today's habits with check buttons
        if not habits:
            await update.message.reply_text(
                "📊 <b>Трекер привычек</b>\n\n"
                "Нет привычек. Добавьте:\n"
                "/habits add <i>название</i>\n\n"
                "Примеры:\n"
                "• /habits add Зарядка\n"
                "• /habits add Чтение 30 мин\n"
                "• /habits add Пить воду",
                parse_mode="HTML"
            )
            return
        
        today_log = log.get(today, {})
        text_msg = f"📊 <b>Привычки на {today}:</b>\n\n"
        buttons = []
        for h in habits:
            done = today_log.get(h["id"], False)
            emoji = "✅" if done else "⬜"
            text_msg += f"{emoji} {h['name']}\n"
            if not done:
                buttons.append([InlineKeyboardButton(
                    f"✅ {h['name']}",
                    callback_data=f"habit:{h['id']}"
                )])
        
        # Stats: streak
        total_done = sum(1 for h in habits if today_log.get(h["id"], False))
        text_msg += f"\n📈 Выполнено: {total_done}/{len(habits)}"
        
        # Calculate streak
        streak = 0
        check_date = datetime.now(TIMEZONE) - timedelta(days=1)
        while True:
            d = check_date.strftime("%Y-%m-%d")
            day_log = log.get(d, {})
            if day_log and all(day_log.get(h["id"], False) for h in habits):
                streak += 1
                check_date -= timedelta(days=1)
            else:
                break
        if streak > 0:
            text_msg += f"\n🔥 Серия: {streak} дн."
        
        text_msg += "\n\n/habits stats — статистика за неделю"
        
        kb = InlineKeyboardMarkup(buttons) if buttons else None
        await update.message.reply_text(text_msg, parse_mode="HTML", reply_markup=kb)
        return
    
    if text.startswith("add "):
        habit_name = text[4:].strip()
        if not habit_name:
            await update.message.reply_text("❌ Укажите название привычки")
            return
        habit_id = str(uuid.uuid4())[:8]
        habits_data.setdefault("habits", []).append({"id": habit_id, "name": habit_name, "created": today})
        await asyncio.to_thread(_save_habits, habits_data)
        await update.message.reply_text(
            f"✅ Привычка добавлена: <b>{habit_name}</b>\n\n"
            f"Отмечайте выполнение через /habits",
            parse_mode="HTML"
        )
        return
    
    if text.startswith("remove ") or text.startswith("del "):
        parts = text.split(" ", 1)
        name_to_remove = parts[1].strip().lower() if len(parts) > 1 else ""
        removed = False
        for i, h in enumerate(habits_data.get("habits", [])):
            if name_to_remove in h["name"].lower():
                habits_data["habits"].pop(i)
                await asyncio.to_thread(_save_habits, habits_data)
                await update.message.reply_text(f"🗑 Привычка удалена: <b>{h['name']}</b>", parse_mode="HTML")
                removed = True
                break
        if not removed:
            await update.message.reply_text("❌ Привычка не найдена")
        return
    
    if text == "stats":
        # Weekly stats
        if not habits:
            await update.message.reply_text("Нет привычек для статистики")
            return
        
        text_msg = "📊 <b>Статистика за 7 дней:</b>\n\n"
        days = []
        for i in range(6, -1, -1):
            d = (datetime.now(TIMEZONE) - timedelta(days=i)).strftime("%Y-%m-%d")
            days.append(d)
        
        # Header
        text_msg += "<code>"
        text_msg += "Привычка".ljust(15)
        for d in days:
            text_msg += datetime.strptime(d, "%Y-%m-%d").strftime("%a")[0:2] + " "
        text_msg += "\n"
        text_msg += "-" * (15 + len(days) * 3) + "\n"
        
        for h in habits:
            row = h["name"][:14].ljust(15)
            for d in days:
                done = log.get(d, {}).get(h["id"], False)
                row += ("✓  " if done else "·  ")
            text_msg += row + "\n"
        text_msg += "</code>"
        
        await update.message.reply_text(text_msg, parse_mode="HTML")
        return
    
    await update.message.reply_text(
        "📊 <b>Команды привычек:</b>\n"
        "/habits — показать на сегодня\n"
        "/habits add <i>название</i> — добавить\n"
        "/habits remove <i>название</i> — удалить\n"
        "/habits stats — статистика за неделю",
        parse_mode="HTML"
    )

# ══════════════════════════════════════════════════════════
# ██  HEALTH CHECK SERVER
# ══════════════════════════════════════════════════════════

_bot_start_time = datetime.now()
_messages_processed = 0

# ══════════════════════════════════════════════════════════
# ██  Long-term Memory
# ══════════════════════════════════════════════════════════
MEMORY_FILE = os.path.join(os.path.dirname(__file__), "data", "memory.json")

def _load_memory() -> dict:
    """Load long-term memory: {facts: [...], preferences: {...}}"""
    try:
        with open(MEMORY_FILE, "r") as f:
            return json.loads(f.read())
    except (FileNotFoundError, json.JSONDecodeError):
        return {"facts": [], "preferences": {}}

def _save_memory(data: dict):
    os.makedirs(os.path.dirname(MEMORY_FILE), exist_ok=True)
    with open(MEMORY_FILE, "w") as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=2))

def _get_memory_context() -> str:
    """Get memory facts as context string for Claude."""
    mem = _load_memory()
    facts = mem.get("facts", [])
    if not facts:
        return ""
    facts_str = "\n".join(f"- {f}" for f in facts[-30:])  # Last 30 facts
    return f"\n\n## Известные факты о пользователе\n{facts_str}"

def _add_memory_fact(fact: str):
    """Add a new fact to long-term memory."""
    mem = _load_memory()
    facts = mem.get("facts", [])
    # Avoid duplicates (fuzzy)
    fact_lower = fact.lower().strip()
    for existing in facts:
        if existing.lower().strip() == fact_lower:
            return
    facts.append(fact)
    # Keep max 100 facts
    if len(facts) > 100:
        facts = facts[-100:]
    mem["facts"] = facts
    _save_memory(mem)

# ══════════════════════════════════════════════════════════
# ██  Multi-step Dialog State
# ══════════════════════════════════════════════════════════
_dialog_state: dict[int, dict] = {}  # chat_id -> {pending_action, context, expires}

# ══════════════════════════════════════════════════════════
# ██  Read-It-Later Queue
# ══════════════════════════════════════════════════════════
READ_LATER_FILE = os.path.join(os.path.dirname(__file__), "data", "read_later.json")

def _load_read_later() -> list:
    try:
        with open(READ_LATER_FILE, "r") as f:
            return json.loads(f.read())
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def _save_read_later(data: list):
    os.makedirs(os.path.dirname(READ_LATER_FILE), exist_ok=True)
    with open(READ_LATER_FILE, "w") as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=2))



# ══════════════════════════════════════════════════════════
# ██  Knowledge Base (auto-learning from interactions)
# ══════════════════════════════════════════════════════════
KNOWLEDGE_BASE_FILE = os.path.join(os.path.dirname(__file__), "data", "knowledge_base.json")

def _load_knowledge() -> list:
    """Load knowledge base entries."""
    try:
        with open(KNOWLEDGE_BASE_FILE, "r") as f:
            return json.loads(f.read())
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def _save_knowledge(data: list):
    """Save knowledge base."""
    os.makedirs(os.path.dirname(KNOWLEDGE_BASE_FILE), exist_ok=True)
    # Keep max 500 entries
    if len(data) > 500:
        data = data[-500:]
    with open(KNOWLEDGE_BASE_FILE, "w") as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=2))

def _add_knowledge_entry(user_message: str, action_key: str, tool_used: str, result: str, category: str = "general"):
    """Add an entry to the knowledge base."""
    try:
        kb = _load_knowledge()
        entry = {
            "timestamp": datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M"),
            "user_message": user_message[:200],
            "action_key": action_key,
            "tool_used": tool_used,
            "result": result[:300],
            "category": category,
        }
        kb.append(entry)
        _save_knowledge(kb)
    except Exception as e:
        logger.error("Knowledge base write error: %s", e)

def _search_knowledge(query: str, category: str = "all") -> list:
    """Search knowledge base by query string and optional category."""
    kb = _load_knowledge()
    query_lower = query.lower()
    results = []
    for entry in reversed(kb):  # newest first
        if category != "all" and entry.get("category", "") != category:
            continue
        # Search in user_message, action_key, result
        searchable = f"{entry.get('user_message', '')} {entry.get('action_key', '')} {entry.get('result', '')}".lower()
        if query_lower in searchable:
            results.append(entry)
        if len(results) >= 20:
            break
    return results


# ══════════════════════════════════════════════════════════
# ██  Expense Tracker
# ══════════════════════════════════════════════════════════
EXPENSES_FILE = os.path.join(os.path.dirname(__file__), "data", "expenses.json")

EXPENSE_CATEGORIES = {
    "food": "🍔 Еда",
    "transport": "🚗 Транспорт",
    "housing": "🏠 Жильё",
    "utilities": "💡 Коммуналка",
    "health": "🏥 Здоровье",
    "entertainment": "🎬 Развлечения",
    "clothing": "👕 Одежда",
    "education": "📚 Образование",
    "subscriptions": "📱 Подписки",
    "gifts": "🎁 Подарки",
    "travel": "✈️ Путешествия",
    "other": "📦 Прочее",
}

def _load_expenses() -> list:
    """Load expense records."""
    try:
        with open(EXPENSES_FILE, "r") as f:
            return json.loads(f.read())
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def _save_expenses(data: list):
    """Save expense records."""
    os.makedirs(os.path.dirname(EXPENSES_FILE), exist_ok=True)
    with open(EXPENSES_FILE, "w") as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=2))

def _add_expense(amount: float, category: str, description: str, date: str = None) -> dict:
    """Add a new expense entry."""
    expenses = _load_expenses()
    if not date:
        date = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    entry = {
        "id": str(uuid.uuid4())[:8],
        "date": date,
        "amount": float(amount),
        "category": category,
        "description": description,
        "created": datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M"),
    }
    expenses.append(entry)
    _save_expenses(expenses)
    return entry

def _get_expense_report(period: str = "month", category: str = None) -> dict:
    """Generate expense report for a given period."""
    expenses = _load_expenses()
    now = datetime.now(TIMEZONE)
    
    # Filter by period
    if period == "today":
        start = now.replace(hour=0, minute=0, second=0)
        period_label = f"за сегодня ({now.strftime('%d.%m.%Y')})"
    elif period == "week":
        start = now - timedelta(days=7)
        period_label = f"за неделю ({start.strftime('%d.%m')} - {now.strftime('%d.%m.%Y')})"
    elif period == "month":
        start = now.replace(day=1, hour=0, minute=0, second=0)
        period_label = f"за {now.strftime('%B %Y')}"
    elif period == "year":
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0)
        period_label = f"за {now.year} год"
    else:
        start = now - timedelta(days=30)
        period_label = "за последние 30 дней"
    
    start_str = start.strftime("%Y-%m-%d")
    filtered = [e for e in expenses if e.get("date", "") >= start_str]
    
    # Filter by category
    if category and category != "all":
        filtered = [e for e in filtered if e.get("category", "") == category]
    
    # Calculate stats
    total = sum(e.get("amount", 0) for e in filtered)
    by_category = {}
    for e in filtered:
        cat = e.get("category", "other")
        by_category[cat] = by_category.get(cat, 0) + e.get("amount", 0)
    
    return {
        "period": period_label,
        "total": total,
        "count": len(filtered),
        "by_category": by_category,
        "recent": sorted(filtered, key=lambda x: x.get("date", ""))[-10:],
    }


# ══════════════════════════════════════════════════════════
# ██  Server Management (Docker)
# ══════════════════════════════════════════════════════════
import subprocess

def _get_server_status(check_type: str = "all") -> str:
    """Get server/container status."""
    try:
        if check_type == "containers" or check_type == "all":
            result = subprocess.run(
                ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}\t{{.Image}}"],
                capture_output=True, text=True, timeout=15
            )
            lines = result.stdout.strip().split("\n")
            
            healthy = []
            unhealthy = []
            for line in lines:
                if not line.strip():
                    continue
                parts = line.split("\t")
                name = parts[0] if len(parts) > 0 else "?"
                status = parts[1] if len(parts) > 1 else "?"
                if "(healthy)" in status or "Up" in status:
                    healthy.append(f"  ✅ {name}: {status}")
                else:
                    unhealthy.append(f"  ❌ {name}: {status}")
            
            response = f"🖥️ <b>Статус контейнеров ({len(lines)} шт.):</b>\n\n"
            if unhealthy:
                response += "<b>⚠️ Проблемные:</b>\n" + "\n".join(unhealthy) + "\n\n"
            response += "<b>Работают:</b>\n" + "\n".join(healthy[:30])
            
            if check_type == "all":
                # Add disk and memory info
                disk = subprocess.run(["df", "-h", "/"], capture_output=True, text=True, timeout=5)
                mem = subprocess.run(["free", "-h"], capture_output=True, text=True, timeout=5)
                response += f"\n\n<b>💾 Диск:</b>\n<pre>{disk.stdout.strip()}</pre>"
                response += f"\n\n<b>🧠 Память:</b>\n<pre>{mem.stdout.strip()}</pre>"
            
            return response
        
        elif check_type == "disk":
            disk = subprocess.run(["df", "-h"], capture_output=True, text=True, timeout=5)
            return f"💾 <b>Дисковое пространство:</b>\n<pre>{disk.stdout.strip()}</pre>"
        
        elif check_type == "memory":
            mem = subprocess.run(["free", "-h"], capture_output=True, text=True, timeout=5)
            return f"🧠 <b>Оперативная память:</b>\n<pre>{mem.stdout.strip()}</pre>"
        
        elif check_type == "cpu":
            uptime = subprocess.run(["uptime"], capture_output=True, text=True, timeout=5)
            return f"⚡ <b>CPU Load:</b>\n<pre>{uptime.stdout.strip()}</pre>"
        
        else:
            return f"❓ Неизвестный тип проверки: {check_type}"
    
    except subprocess.TimeoutExpired:
        return "⏰ Таймаут выполнения команды"
    except Exception as e:
        return f"❌ Ошибка: {str(e)}"

def _manage_container(container: str, action: str) -> str:
    """Manage a Docker container (restart, stop, start, logs)."""
    allowed_actions = {"restart", "stop", "start", "logs", "inspect"}
    if action not in allowed_actions:
        return f"❌ Действие '{action}' не разрешено. Доступны: {', '.join(allowed_actions)}"
    
    try:
        if action == "logs":
            result = subprocess.run(
                ["docker", "logs", "--tail", "50", container],
                capture_output=True, text=True, timeout=15
            )
            output = result.stdout or result.stderr
            return output[-3500:] if len(output) > 3500 else output
        
        elif action == "inspect":
            result = subprocess.run(
                ["docker", "inspect", "--format",
                 "Name: {{.Name}}\nImage: {{.Config.Image}}\nStatus: {{.State.Status}}\nStarted: {{.State.StartedAt}}\nRestarts: {{.RestartCount}}",
                 container],
                capture_output=True, text=True, timeout=10
            )
            return result.stdout.strip() or result.stderr.strip()
        
        else:
            result = subprocess.run(
                ["docker", action, container],
                capture_output=True, text=True, timeout=30
            )
            output = result.stdout.strip() or result.stderr.strip()
            return output or f"✅ {action} выполнен для {container}"
    
    except subprocess.TimeoutExpired:
        return f"⏰ Таймаут при выполнении {action} для {container}"
    except Exception as e:
        return f"❌ Ошибка: {str(e)}"

def _run_server_command(command: str) -> str:
    """Execute a shell command on the server (inside Docker host via socket)."""
    try:
        # Run command directly (we have access to host via docker socket)
        result = subprocess.run(
            command, shell=True,
            capture_output=True, text=True, timeout=30
        )
        output = result.stdout.strip()
        if result.returncode != 0:
            output += f"\n[stderr] {result.stderr.strip()}"
        return output[-3500:] if len(output) > 3500 else output
    except subprocess.TimeoutExpired:
        return "⏰ Таймаут выполнения команды (30с)"
    except Exception as e:
        return f"❌ Ошибка: {str(e)}"


# ══════════════════════════════════════════════════════════
# ██  Web Search (DuckDuckGo)
# ══════════════════════════════════════════════════════════

def _web_search(query: str, search_type: str = "general") -> str:
    """Search the web using DuckDuckGo HTML."""
    import urllib.request
    import urllib.parse
    from html.parser import HTMLParser
    
    try:
        encoded_query = urllib.parse.quote_plus(query)
        url = f"https://html.duckduckgo.com/html/?q={encoded_query}"
        
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
        })
        
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        
        # Simple extraction of results
        results = []
        # Find result snippets between <a class="result__snippet"> tags
        import re as _re
        snippets = _re.findall(r'class="result__snippet"[^>]*>(.*?)</a>', html, _re.DOTALL)
        titles = _re.findall(r'class="result__a"[^>]*>(.*?)</a>', html, _re.DOTALL)
        urls = _re.findall(r'class="result__url"[^>]*>(.*?)</a>', html, _re.DOTALL)
        
        for i in range(min(5, len(snippets))):
            title = _re.sub(r'<[^>]+>', '', titles[i]).strip() if i < len(titles) else ""
            snippet = _re.sub(r'<[^>]+>', '', snippets[i]).strip()
            link = _re.sub(r'<[^>]+>', '', urls[i]).strip() if i < len(urls) else ""
            results.append(f"<b>{i+1}. {title}</b>\n{snippet}\n<i>{link}</i>")
        
        if results:
            return "\n\n".join(results)
        else:
            return f"Не удалось найти результаты по запросу: {query}"
    
    except Exception as e:
        return f"Ошибка поиска: {str(e)}"


class SilentHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
    """Многопоточный HTTP-сервер с подавлением BrokenPipeError."""
    daemon_threads = True
    allow_reuse_address = True

    def handle_error(self, request, client_address):
        """Подавляем BrokenPipeError вместо вывода traceback."""
        import sys
        exc_type = sys.exc_info()[0]
        if exc_type is BrokenPipeError:
            pass  # клиент отключился — это нормально
        else:
            super().handle_error(request, client_address)


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
      try:
        if self.path == '/health' or self.path == '/':
            uptime = (datetime.now() - _bot_start_time).total_seconds()
            try:
                from kindle_handler import get_kindle_stats
                kindle_stats = get_kindle_stats()
            except Exception:
                kindle_stats = {}
            data = {
                "status": "ok",
                "bot": f"Nodkeys Calendar & Life Bot v{VERSION}",
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
                # Use cached repos if available and fresh (< 10 min)
                import time as _time
                _cache = getattr(HealthHandler, '_repos_cache', None)
                _cache_time = getattr(HealthHandler, '_repos_cache_time', 0)
                if _cache and (_time.time() - _cache_time) < 600:
                    repos = _cache
                else:
                    headers = {'User-Agent': 'Nodkeys-Bot'}
                    gh_token = os.getenv('GITHUB_TOKEN', '')
                    if gh_token:
                        headers['Authorization'] = f'token {gh_token}'
                    req = urllib.request.Request(
                        'https://api.github.com/users/sileade/repos?sort=updated&per_page=30',
                        headers=headers
                    )
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        repos = json.loads(resp.read().decode())
                    HealthHandler._repos_cache = repos
                    HealthHandler._repos_cache_time = _time.time()
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
                from kindle_handler import get_books_history
                books = get_books_history()
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
                filename = os.path.basename(self.path[len('/download/'):])
                # Prevent path traversal
                if '..' in filename or '/' in filename:
                    raise ValueError('Invalid filename')
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
                from kindle_handler import get_books_history
                books = get_books_history()
                total = len(books)
                last_book = books[-1]["title"] if books else "No books yet"
                last_sent = books[-1]["sent_at"] if books else "-"
                data = {
                    "total_books": total,
                    "last_book": last_book,
                    "last_sent": last_sent,
                    "books": books,
                }
            except Exception:
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
      except BrokenPipeError:
          pass  # клиент отключился
      except Exception as e:
          logger.warning("Health handler error: %s", e)
    
    def log_message(self, format, *args):
        pass  # Suppress health check logs

def start_health_server(port=8085):
    server = SilentHTTPServer(('0.0.0.0', port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("Health check server started on port %d", port)


# ══════════════════════════════════════════════════════════
# ██  MAIN
# ══════════════════════════════════════════════════════════


# ──────────────────── PHOTO / DOCUMENT HANDLER ────────────────────

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle photos sent to the bot — analyze documents via Claude Vision."""
    if not update.message or not update.message.photo:
        return
    
    # Authorization check
    chat_id = update.message.chat_id
    user_id = update.message.from_user.id if update.message.from_user else None
    
    if ALLOWED_CHAT_IDS:
        if chat_id not in ALLOWED_CHAT_IDS and user_id not in ALLOWED_CHAT_IDS:
            if chat_id != CHAT_ID:
                return
    elif chat_id != CHAT_ID:
        return
    
    # Get the highest resolution photo
    photo = update.message.photo[-1]
    caption = update.message.caption or ""
    
    # Determine sender context for calendar routing
    sender_context = ""
    user = update.message.from_user
    if user:
        routing = get_user_routing(user)
        if routing:
            sender_context = (
                f"Отправитель: {routing['display_name']} "
                f"(calendar_rule={routing['calendar_rule']})"
            )
        else:
            sender_context = f"Отправитель: {user.first_name or user.username or 'Unknown'}"
    
    thinking = await update.message.reply_text(
        "📸 <b>Анализирую фото документа...</b>\n"
        "🔍 Распознаю текст и извлекаю данные...",
        parse_mode="HTML",
    )
    
    try:
        # Download photo
        tg_file = await context.bot.get_file(photo.file_id)
        image_bytes = await tg_file.download_as_bytearray()
        
        # Determine media type
        media_type = "image/jpeg"  # Telegram photos are always JPEG
        
        # Analyze with Claude Vision
        data = await asyncio.to_thread(
            analyze_photo_with_claude,
            bytes(image_bytes), media_type, caption, sender_context
        )
        
        if not data:
            await thinking.edit_text(
                "❌ Не удалось проанализировать фото.\n"
                "Попробуйте отправить более чёткое изображение.",
            )
            return
        
        action = data.get("action", "skip")
        doc_type = data.get("document_type", "other")
        summary = data.get("summary", "")
        
        doc_type_emoji = {
            "medical": "🏥", "ticket": "🎫", "schedule": "📋",
            "receipt": "🧾", "other": "📄",
        }
        doc_emoji = doc_type_emoji.get(doc_type, "📄")
        
        # Apply calendar override for non-@seleadi users
        calendar_override = None
        if user:
            routing = get_user_routing(user)
            if routing and routing["calendar_rule"] != "auto":
                calendar_override = routing["calendar_rule"]
        
        if action == "create_series":
            # Recurring tasks (medications, courses, etc.)
            tasks = data.get("tasks", [])
            one_time = data.get("one_time_events", [])
            
            await thinking.edit_text(
                f"{doc_emoji} <b>Документ распознан!</b>\n\n"
                f"📋 {summary}\n\n"
                f"⏳ Создаю {len(tasks)} серий напоминаний"
                f"{' и ' + str(len(one_time)) + ' событий' if one_time else ''}...",
                parse_mode="HTML",
            )
            
            # Create recurring tasks
            recurring_uids = await asyncio.to_thread(
                create_recurring_tasks, tasks, calendar_override
            )
            
            # Create one-time events
            onetime_uids = []
            for evt in one_time:
                if calendar_override:
                    evt["calendar"] = calendar_override
                uid = await asyncio.to_thread(create_calendar_event, evt)
                if uid:
                    onetime_uids.append(uid)
            
            # Build response
            total_created = len(recurring_uids) + len(onetime_uids)
            
            response = f"{doc_emoji} <b>Документ обработан!</b>\n\n"
            response += f"📋 <b>{summary}</b>\n\n"
            
            if recurring_uids:
                response += f"🔔 <b>Создано напоминаний:</b> {len(recurring_uids)}\n"
                for task in tasks:
                    times_str = ", ".join(task.get("times", []))
                    response += (
                        f"  • {task.get('title', '?')} "
                        f"({task.get('start_date', '?')} — {task.get('end_date', '?')}, "
                        f"{times_str})\n"
                    )
            
            if onetime_uids:
                response += f"\n📅 <b>Создано событий:</b> {len(onetime_uids)}\n"
                for evt in one_time:
                    time_info = ""
                    if evt.get("time_start"):
                        time_info = f" в {evt['time_start']}"
                    response += f"  • {evt.get('title', '?')} — {evt.get('date', '?')}{time_info}\n"
            
            if not total_created:
                response += "\n⚠️ Не удалось создать записи в календаре."
            
            confidence = data.get("confidence", 0)
            conf_emoji = "🟢" if confidence >= 0.8 else "🟡" if confidence >= 0.5 else "🔴"
            response += f"\n{conf_emoji} Уверенность: {confidence:.0%}"
            
            await thinking.edit_text(response, parse_mode="HTML")
            
            # Store all UIDs for reply-based deletion
            all_photo_uids = list(recurring_uids) + list(onetime_uids)
            if all_photo_uids:
                _event_store[thinking.message_id] = {
                    "uids": all_photo_uids,
                    "calendar": calendar_override or "family",
                    "title": summary,
                }
                _save_event_store()
        
        elif action == "create":
            entry_type = data.get("type", "note")
            
            if calendar_override and entry_type in ("event", "task", "reminder"):
                data["calendar"] = calendar_override
            
            if entry_type in ("event", "task", "reminder"):
                uid = await asyncio.to_thread(create_calendar_event, data)
                
                if uid:
                    type_map = {"event": "📅 Событие", "task": "✅ Задача", "reminder": "🔔 Напоминание"}
                    cal_map = {"work": "💼 Рабочий", "family": "🏠 Семейный", "reminders": "⚠️ Напоминания"}
                    
                    entry_label = type_map.get(entry_type, entry_type)
                    cal_name = cal_map.get(data.get("calendar", ""), data.get("calendar", ""))
                    date_str = data.get("date", "?")
                    time_str = ""
                    if data.get("time_start") and not data.get("all_day"):
                        time_str = f" в {data['time_start']}"
                    
                    response = (
                        f"{doc_emoji} <b>Документ → {entry_label}</b>\n\n"
                        f"📋 {summary}\n\n"
                        f"📌 <b>{data.get('title', '?')}</b>\n"
                        f"📅 {date_str}{time_str}\n"
                        f"📂 {cal_name}\n"
                    )
                    if data.get("description"):
                        response += f"📝 {data['description'][:200]}\n"
                    
                    confidence = data.get("confidence", 0)
                    conf_emoji = "🟢" if confidence >= 0.8 else "🟡" if confidence >= 0.5 else "🔴"
                    response += f"\n{conf_emoji} Уверенность: {confidence:.0%}"
                    
                    await thinking.edit_text(response, parse_mode="HTML")
                    
                    # Store UID for reply-based deletion
                    _event_store[thinking.message_id] = {
                        "uid": uid,
                        "calendar": data.get("calendar", "family"),
                        "title": data.get("title", ""),
                    }
                    _save_event_store()
                else:
                    await thinking.edit_text(
                        f"{doc_emoji} <b>Документ распознан</b>\n\n"
                        f"📋 {summary}\n\n"
                        f"❌ Не удалось создать запись в календаре."
                    )
            
            elif entry_type == "note":
                # Save as Apple Note
                note_title = data.get("title", "Документ")
                note_content = data.get("content", summary)
                
                try:
                    create_apple_note(note_title, f"<p>{note_content}</p>")
                    response = (
                        f"{doc_emoji} <b>Документ → 📝 Заметка</b>\n\n"
                        f"📋 {summary}\n\n"
                        f"📝 <b>{note_title}</b>\n"
                        f"{note_content[:300]}"
                    )
                    await thinking.edit_text(response, parse_mode="HTML")
                except Exception as e:
                    logger.error("Save note from photo error: %s", e)
                    await thinking.edit_text(
                        f"{doc_emoji} <b>Документ распознан</b>\n\n"
                        f"📋 {summary}\n\n"
                        f"📝 <b>{note_title}</b>\n"
                        f"{note_content[:300]}\n\n"
                        f"⚠️ Не удалось сохранить в Apple Notes: {str(e)[:100]}"
                    )
            else:
                await thinking.edit_text(
                    f"{doc_emoji} <b>Документ распознан</b>\n\n"
                    f"📋 {summary}\n\n"
                    f"{data.get('reasoning', '')}"
                )
        
        elif action == "skip":
            await thinking.edit_text(
                f"📸 <b>Фото проанализировано</b>\n\n"
                f"{data.get('reasoning', 'Не удалось извлечь полезную информацию.')}"
            )
        
        else:
            await thinking.edit_text(
                f"📸 <b>Фото проанализировано</b>\n\n"
                f"📋 {summary}\n\n"
                f"ℹ️ {data.get('reasoning', 'Неизвестный тип действия.')}"
            )
    
    except Exception as e:
        logger.error("Photo handler error: %s", e)
        await thinking.edit_text(
            f"❌ Ошибка обработки фото: {str(e)[:200]}"
        )


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

    logger.info("Starting Nodkeys Calendar & Life Bot v%s...", VERSION)
    logger.info("Bot token: ...%s", BOT_TOKEN[-10:])
    logger.info("iCloud user: %s", ICLOUD_USERNAME)
    logger.info("Claude model: %s", CLAUDE_MODEL)
    logger.info("Flibusta base URL: %s", FLIBUSTA_BASE_URL)
    logger.info("Allowed chat IDs: %s", ALLOWED_CHAT_IDS or "any")
    if GROUP_USERS:
        for ukey, udata in GROUP_USERS.items():
            logger.info("Group user: %s → %s (calendar: %s)",
                       ukey, udata['display_name'], udata['calendar_rule'])

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

    # Build application with optional SOCKS5 proxy and custom base URL for Telegram API
    builder = Application.builder().token(BOT_TOKEN)
    
    if TELEGRAM_PROXY_URL:
        from telegram.request import HTTPXRequest
        logger.info("Using Telegram proxy: %s", TELEGRAM_PROXY_URL)
        proxy_request = HTTPXRequest(proxy=TELEGRAM_PROXY_URL)
        builder = builder.request(proxy_request).get_updates_request(HTTPXRequest(proxy=TELEGRAM_PROXY_URL))
    
    if TELEGRAM_BASE_URL:
        logger.info("Using custom Telegram base URL: %s", TELEGRAM_BASE_URL)
        builder = builder.base_url(TELEGRAM_BASE_URL)
    
    app = builder.build()

    # Register handlers
    app.add_handler(CommandHandler("remind", cmd_remind))
    # Reminder completion callback
    from telegram.ext import CallbackQueryHandler
    async def reminder_done_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        if data.startswith("rdone:"):
            uid_prefix = data[6:]
            reminders = _load_reminders()
            found = False
            for r in reminders:
                if r["uid"].startswith(uid_prefix) and not r.get("completed"):
                    r["completed"] = True
                    r["completed_at"] = datetime.now(TIMEZONE).isoformat()
                    found = True
                    _save_reminders(reminders)
                    await query.edit_message_text(
                        query.message.text + f"\n\n✅ Выполнено: **{r['title']}**",
                        parse_mode="Markdown"
                    )
                    break
            if not found:
                await query.answer("Напоминание не найдено", show_alert=True)
    app.add_handler(CallbackQueryHandler(reminder_done_callback, pattern="^rdone:"))
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("calendars", cmd_calendars))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("week", cmd_week))
    app.add_handler(CommandHandler("habits", cmd_habits))
    app.add_handler(CommandHandler("delete", cmd_delete))
    app.add_handler(CommandHandler("cleanup", cmd_cleanup))
    app.add_handler(CommandHandler("book", cmd_book))
    app.add_handler(CommandHandler("xray", cmd_xray))
    app.add_handler(CallbackQueryHandler(callback_delete, pattern=r"^del:"))
    app.add_handler(CallbackQueryHandler(callback_book, pattern=r"^book:"))
    app.add_handler(CallbackQueryHandler(callback_bookdev, pattern=r"^bookdev:"))
    app.add_handler(CallbackQueryHandler(callback_urlkindle, pattern=r"^urlkindle:"))

    # Kindle handlers
    try:
        from kindle_handler import handle_document, callback_kindle
        app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
        app.add_handler(CallbackQueryHandler(callback_kindle, pattern=r"^kindle:"))
        logger.info("Kindle handler registered")
    except ImportError as e:
        logger.warning("Kindle handler not available: %s", e)

    # Photo/document recognition handler
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    # Voice message handler
    app.add_handler(MessageHandler(filters.VOICE | filters.AUDIO, handle_voice))
    # Habit completion callback
    async def _habit_done_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        habit_id = query.data.replace("habit:", "")
        habits_data = _load_habits()
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        habits_data.setdefault("log", {}).setdefault(today, {})[habit_id] = True
        _save_habits(habits_data)
        # Find habit name
        habit_name = next((h["name"] for h in habits_data.get("habits", []) if h["id"] == habit_id), "?")
        await query.edit_message_text(
            query.message.text + f"\n\n✅ Отмечено: <b>{habit_name}</b>",
            parse_mode="HTML"
        )
    app.add_handler(CallbackQueryHandler(_habit_done_callback, pattern="^habit:"))
    # Event done callback (from /today buttons)
    async def _event_done_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer("✅ Отмечено как выполненное")
        uid_prefix = query.data.replace("done:", "")
        # Try to delete the event
        success = await asyncio.to_thread(delete_event_by_uid, uid_prefix)
        if success:
            await query.edit_message_text(
                query.message.text + f"\n\n✅ <b>Выполнено и удалено из календаря</b>",
                parse_mode="HTML"
            )
        else:
            await query.edit_message_text(
                query.message.text + f"\n\n✅ <b>Отмечено</b>",
                parse_mode="HTML"
            )
    app.add_handler(CallbackQueryHandler(_event_done_callback, pattern="^done:"))
    
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    logger.info("Bot is running! Polling for messages...")
    
    # --- Reminder notification scheduler ---
    async def _reminder_notification_loop(app_instance):
        """Background task that checks reminders and sends Telegram notifications."""
        bot = app_instance.bot
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                now = datetime.now(TIMEZONE)
                reminders = _load_reminders()
                changed = False
                
                for r in reminders:
                    if r.get("completed"):
                        continue
                    if not r.get("due_date"):
                        continue
                    
                    # Parse due datetime
                    try:
                        if r.get("due_time"):
                            due_dt = datetime.strptime(f"{r['due_date']} {r['due_time']}", "%Y-%m-%d %H:%M")
                        else:
                            due_dt = datetime.strptime(r["due_date"], "%Y-%m-%d").replace(hour=9, minute=0)
                        due_dt = due_dt.replace(tzinfo=TIMEZONE)
                    except (ValueError, TypeError):
                        continue
                    
                    # 15-minute warning
                    if not r.get("notified_15m"):
                        diff = (due_dt - now).total_seconds()
                        if 0 < diff <= 900:
                            list_label = REMINDER_LISTS.get(r.get("list_key", "personal"), "📋")
                            try:
                                await bot.send_message(
                                    chat_id=CHAT_ID,
                                    text=f"⏰ **Через 15 минут:**\n\n🔔 {r['title']}\n📋 {list_label}\n📅 {r.get('due_time', '')}",
                                    parse_mode="Markdown"
                                )
                            except Exception as e:
                                logger.error("Reminder notify error: %s", e)
                            r["notified_15m"] = True
                            changed = True
                    
                    # Due time notification
                    if not r.get("notified"):
                        diff = (now - due_dt).total_seconds()
                        if 0 <= diff <= 120:
                            list_label = REMINDER_LISTS.get(r.get("list_key", "personal"), "📋")
                            try:
                                await bot.send_message(
                                    chat_id=CHAT_ID,
                                    text=f"🔔🔔🔔 **НАПОМИНАНИЕ:**\n\n{r['title']}\n📋 {list_label}\n\nОтметить: /remind",
                                    parse_mode="Markdown"
                                )
                            except Exception as e:
                                logger.error("Reminder notify error: %s", e)
                            r["notified"] = True
                            changed = True
                    
                    # Overdue notification (once, 1 hour after)
                    if r.get("notified") and not r.get("overdue_notified"):
                        diff = (now - due_dt).total_seconds()
                        if diff > 3600:
                            list_label = REMINDER_LISTS.get(r.get("list_key", "personal"), "📋")
                            try:
                                await bot.send_message(
                                    chat_id=CHAT_ID,
                                    text=f"⚠️ **Просрочено:**\n\n🔔 {r['title']}\n📋 {list_label}\n📅 Было: {r.get('due_date', '')} {r.get('due_time', '')}\n\nОтметить: /remind",
                                    parse_mode="Markdown"
                                )
                            except Exception as e:
                                logger.error("Overdue notify error: %s", e)
                            r["overdue_notified"] = True
                            changed = True
                
                if changed:
                    _save_reminders(reminders)
                    
            except Exception as e:
                logger.error("Reminder scheduler error: %s", e)
                await asyncio.sleep(60)
    

    # --- Morning Briefing & Weekly Review scheduler ---
    async def _briefing_loop(app_instance):
        """Background task: morning briefing (08:00) and weekly review (Sun 20:00)."""
        bot = app_instance.bot
        _last_briefing_date = ""
        _last_review_week = ""
        
        # Wait 60s after startup for CalDAV client to initialize
        await asyncio.sleep(60)
        
        while True:
            try:
                await asyncio.sleep(30)
                now = datetime.now(TIMEZONE)
                today_str = now.strftime("%Y-%m-%d")
                week_str = now.strftime("%Y-W%W")
                
                # ── Morning Briefing at 08:00 ──
                if now.hour == 8 and now.minute < 2 and today_str != _last_briefing_date:
                    _last_briefing_date = today_str
                    try:
                        events = await asyncio.to_thread(get_today_events)
                        reminders = await asyncio.to_thread(get_pending_reminders)
                        
                        # Filter today's reminders
                        today_reminders = [r for r in reminders 
                                          if r.get("due_date") == today_str and not r.get("completed")]
                        
                        weekday_names = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
                        weekday = weekday_names[now.weekday()]
                        
                        msg = f"☀️ <b>Доброе утро! {weekday}, {now.strftime('%d.%m.%Y')}</b>\n\n"
                        
                        if events:
                            msg += f"📅 <b>События на сегодня ({len(events)}):</b>\n"
                            for ev in events[:8]:
                                msg += f"  • {ev['time']} — {ev['title']}\n"
                            if len(events) > 8:
                                msg += f"  <i>...и ещё {len(events)-8}</i>\n"
                            msg += "\n"
                        else:
                            msg += "📅 Нет событий на сегодня — свободный день! 🎉\n\n"
                        
                        if today_reminders:
                            msg += f"🔔 <b>Напоминания ({len(today_reminders)}):</b>\n"
                            for r in today_reminders[:5]:
                                time_str = f" ⏰ {r['due_time']}" if r.get('due_time') else ""
                                msg += f"  • {r['title']}{time_str}\n"
                            msg += "\n"
                        
                        # Overdue tasks (smart proactive)
                        try:
                            all_reminders = await asyncio.to_thread(get_pending_reminders)
                            overdue = [r for r in all_reminders if r.get("due_date") and r["due_date"] < today_str and not r.get("completed")]
                            if overdue:
                                msg += f"⚠️ <b>Просроченные задачи ({len(overdue)}):</b>\n"
                                for r in overdue[:5]:
                                    msg += f"  • {r['title']} (с {r.get('due_date', '?')})"
                                    msg += "\n"
                                msg += "\n💡 <i>Напишите \"перенеси\" чтобы перенести их на сегодня</i>\n\n"
                        except Exception as oe:
                            logger.warning("Overdue check in briefing failed: %s", oe)
                        
                        # Weather forecast
                        try:
                            weather = await asyncio.to_thread(_get_weather_forecast)
                            if weather:
                                msg += f"🌤 <b>Погода:</b> {weather}\n\n"
                        except Exception as we:
                            logger.warning("Weather in briefing failed: %s", we)
                        
                        # Habits
                        habits_data = _load_habits()
                        habits = habits_data.get("habits", [])
                        if habits:
                            msg += f"📊 <b>Привычки на сегодня ({len(habits)}):</b>\n"
                            for h in habits[:5]:
                                msg += f"  ⬜ {h['name']}\n"
                            msg += "\n"
                        
                        # Read-it-later reminder on weekends
                        if now.weekday() >= 5:  # Saturday or Sunday
                            read_later = _load_read_later()
                            if read_later:
                                msg += f"📚 <b>Отложенное чтение ({len(read_later)}):</b>\n"
                                for item in read_later[:3]:
                                    msg += f"  • {item.get('title', item.get('url', '?')[:40])}\n"
                                msg += "\n"
                        
                        # Financial triggers
                        try:
                            fin_triggers = _load_financial_triggers()
                            today_day = now.day
                            for ft in fin_triggers:
                                if ft.get("day") == today_day:
                                    msg += f"💰 <b>{ft['name']}:</b> {ft['reminder']}\n\n"
                        except Exception:
                            pass
                        
                        # Pattern insights (weekly on Monday)
                        if now.weekday() == 0:
                            try:
                                insights = await asyncio.to_thread(_get_productivity_insights)
                                if insights:
                                    msg += f"🧠 <b>Инсайты:</b>\n{insights}\n\n"
                            except Exception:
                                pass
                        
                        # Meeting prep for first event of the day
                        if events:
                            first_event = events[0] if events else None
                            if first_event and first_event.get("time") != "весь день":
                                try:
                                    brief = await asyncio.to_thread(_prepare_meeting_brief, first_event["title"])
                                    if brief and len(brief) > 20:
                                        msg += f"📋 <b>К событию \"{first_event["title"]}\":</b>\n<i>{brief[:300]}</i>\n\n"
                                except Exception:
                                    pass
                        
                        msg += "💪 Продуктивного дня!"
                        
                        await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML")
                        logger.info("Morning briefing sent")
                    except Exception as e:
                        logger.error("Morning briefing error: %s", e)
                
                # ── Weekly Review on Sunday at 20:00 ──
                if now.weekday() == 6 and now.hour == 20 and now.minute < 2 and week_str != _last_review_week:
                    _last_review_week = week_str
                    try:
                        events = await asyncio.to_thread(get_week_events)
                        habits_data = _load_habits()
                        habits = habits_data.get("habits", [])
                        log = habits_data.get("log", {})
                        
                        msg = "📋 <b>Еженедельное ревью</b>\n"
                        msg += f"<i>Неделя {now.strftime('%d.%m.%Y')}</i>\n\n"
                        
                        # Events summary
                        msg += f"📅 <b>Событий за неделю:</b> {len(events)}\n"
                        
                        # Habits summary
                        if habits:
                            days_in_week = 7
                            total_possible = len(habits) * days_in_week
                            total_done = 0
                            for i in range(7):
                                d = (now - timedelta(days=i)).strftime("%Y-%m-%d")
                                day_log = log.get(d, {})
                                total_done += sum(1 for h in habits if day_log.get(h["id"], False))
                            
                            pct = (total_done / total_possible * 100) if total_possible > 0 else 0
                            msg += f"\n📊 <b>Привычки:</b> {total_done}/{total_possible} ({pct:.0f}%)\n"
                            
                            for h in habits:
                                done_count = sum(1 for i in range(7) 
                                               if log.get((now - timedelta(days=i)).strftime("%Y-%m-%d"), {}).get(h["id"], False))
                                bar = "█" * done_count + "░" * (7 - done_count)
                                msg += f"  {bar} {h['name']} ({done_count}/7)\n"
                        
                        # Reminders summary
                        reminders = await asyncio.to_thread(get_pending_reminders)
                        overdue = [r for r in reminders if r.get("due_date") and r["due_date"] < today_str and not r.get("completed")]
                        if overdue:
                            msg += f"\n⚠️ <b>Просроченных задач:</b> {len(overdue)}\n"
                            for r in overdue[:3]:
                                msg += f"  • {r['title']} (с {r['due_date']})\n"
                        
                        msg += "\n💡 <i>Спланируйте следующую неделю!</i>"
                        
                        await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML")
                        logger.info("Weekly review sent")
                    except Exception as e:
                        logger.error("Weekly review error: %s", e)
                        
            except Exception as e:
                logger.error("Briefing scheduler error: %s", e)
                await asyncio.sleep(60)
    
    # Start reminder scheduler
    async def _proactive_loop(app_instance):
        """Proactive loop: runs every 2 hours to check upcoming events and send prep."""
        bot = app_instance.bot
        CHAT_ID = list(ALLOWED_CHAT_IDS)[0] if ALLOWED_CHAT_IDS else None
        if not CHAT_ID:
            return
        
        _last_prep_event = ""
        
        while True:
            try:
                await asyncio.sleep(7200)  # Every 2 hours
                now = datetime.now(TIMEZONE)
                
                # Only during waking hours
                if now.hour < 8 or now.hour > 22:
                    continue
                
                # Check for events in the next 2 hours that need prep
                try:
                    events = await asyncio.to_thread(get_today_events)
                    for ev in events:
                        if ev.get("time") == "весь день":
                            continue
                        try:
                            ev_time = datetime.strptime(ev["time"], "%H:%M").replace(
                                year=now.year, month=now.month, day=now.day, tzinfo=TIMEZONE
                            )
                            time_until = (ev_time - now).total_seconds() / 60
                            
                            # If event is 30-120 min away and we haven't prepped it
                            if 30 <= time_until <= 120 and ev["title"] != _last_prep_event:
                                _last_prep_event = ev["title"]
                                brief = await asyncio.to_thread(_prepare_meeting_brief, ev["title"])
                                if brief and len(brief) > 30:
                                    prep_msg = (
                                        f"📋 <b>Подготовка к \"{ev['title']}\" через {int(time_until)} мин:</b>\n\n"
                                        f"{brief[:500]}"
                                    )
                                    await bot.send_message(chat_id=CHAT_ID, text=prep_msg, parse_mode="HTML")
                                break
                        except Exception:
                            continue
                except Exception as e:
                    logger.debug("Proactive event check error: %s", e)
                
            except Exception as e:
                logger.error("Proactive loop error: %s", e)
                await asyncio.sleep(300)
    
    async def _post_init(application):
        asyncio.create_task(_reminder_notification_loop(application))
        asyncio.create_task(_briefing_loop(application))
        asyncio.create_task(_proactive_loop(application))
    # Register location handler for geo-reminders
    app.add_handler(MessageHandler(filters.LOCATION, handle_location))
    
    app.post_init = _post_init
    
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=False)


if __name__ == "__main__":
    main()
