#!/usr/bin/env python3
"""
Nodkeys Calendar & Life Bot v5.0
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

VERSION = "5.2"

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
- `xray` — пользователь хочет получить X-Ray анализ книги (персонажи, темы, таймлайн)
- `url_to_kindle` — пользователь хочет отправить веб-статью/URL на Kindle
- `recurring_tasks` — повторяющиеся задачи: курс лекарств, ежедневные действия на период, регулярные напоминания

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
   - `query` — ПОЛНОЕ название книги как указал пользователь (не сокращай, не обрезай!)
   - `author` — автор книги, если указан или очевиден из контекста (null если не указан)
   - `search_queries` — массив из 2-3 поисковых запросов для поиска, от точного к общему:
     1. Полное название + автор (если известен)
     2. Только полное название книги
     3. Ключевые слова из названия (если название длинное)
   - `title` — краткое описание что ищем (для отображения пользователю)
   - `send_to_kindle` — true если пользователь явно просит отправить на Kindle/читалку

### Для xray (X-Ray анализ книги):
   - `book_title` — название книги для анализа
   - `author` — автор книги (null если не указан)
   - `progress_percent` — процент прочтения (null если не указан, 100 если прочитал)

### Для url_to_kindle (URL → Kindle):
   - `url` — ссылка на статью/страницу
   - `title` — название статьи (если понятно из контекста)

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
- "Сделай x-ray", "Анализ персонажей", "О чём книга", "Темы книги" → xray
- "Отправь статью на киндл", "На киндл [URL]", "Почитать на читалке [URL]" → url_to_kindle
- Если есть ПОВТОРЯЮЩЕЕСЯ действие на период (лекарства, курс, ежедневно N раз в день, на неделю/месяц) → recurring_tasks
- "Пить таблетки 3 раза в день 7 дней", "Принимать витамины неделю", "Курс лечения 10 дней" → recurring_tasks
- "Ежедневно делать зарядку месяц", "Каждый день пить воду" → recurring_tasks

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
  "query": "Странник среди звёзд",
  "author": "Хайнлайн",
  "search_queries": ["Странник среди звёзд Хайнлайн", "Странник среди звёзд", "странник звёзд"],
  "send_to_kindle": true,
  "title": "Поиск: Странник среди звёзд",
  "confidence": 0.95,
  "reasoning": "Пользователь просит отправить на Kindle книгу 'Странник среди звёзд'. Автор — Роберт Хайнлайн (классика НФ)."
}}

Для xray:
{{
  "action": "create",
  "type": "xray",
  "book_title": "Мастер и Маргарита",
  "author": "Булгаков",
  "progress_percent": 50,
  "confidence": 0.95,
  "reasoning": "Пользователь просит X-Ray анализ книги"
}}

Для url_to_kindle:
{{
  "action": "create",
  "type": "url_to_kindle",
  "url": "https://habr.com/ru/articles/123456/",
  "title": "Статья про Python",
  "confidence": 0.95,
  "reasoning": "Пользователь просит отправить статью на Kindle"
}}

Для recurring_tasks (повторяющиеся задачи):
{{
  "action": "create_series",
  "type": "recurring_tasks",
  "summary": "Краткое описание",
  "tasks": [
    {{
      "title": "Название задачи",
      "description": "Подробности",
      "calendar": "family",
      "type": "reminder",
      "start_date": "2026-04-20",
      "end_date": "2026-04-27",
      "times": ["10:00", "14:00", "18:00"],
      "alarm_minutes": 0,
      "repeat_daily": true
    }}
  ],
  "one_time_events": [],
  "confidence": 0.9,
  "reasoning": "Анализ"
}}

Примеры анализа recurring_tasks:
- "Пить таблетки глицина 3 раза в день неделю" → type: "recurring_tasks", tasks: [{{title: "Пить глицин", times: ["09:00", "14:00", "21:00"], start_date: сегодня, end_date: +7 дней}}]
- "Принимать витамины утром и вечером 10 дней" → type: "recurring_tasks", tasks: [{{title: "Витамины", times: ["08:00", "20:00"], start_date: сегодня, end_date: +10 дней}}]
- "Ежедневно зарядка в 7 утра месяц" → type: "recurring_tasks", tasks: [{{title: "Зарядка", times: ["07:00"], start_date: сегодня, end_date: +30 дней}}]

ВАЖНО для recurring_tasks:
- Если есть слова "неделю", "каждый день", "ежедневно", "N раз в день", "курс", "на протяжении" — это RECURRING_TASKS, не обычный reminder!
- start_date = сегодня (если не указано иное)
- end_date = start_date + период (неделя = +7, месяц = +30)
- times = массив времён приёма (3 раза в день → ["09:00", "14:00", "21:00"], 2 раза → ["09:00", "21:00"])
- Если указано конкретное время ("в 10 часов") — используй его как первый приём, остальные распредели равномерно

Примеры анализа xray:
- "Сделай x-ray по Войне и миру" → type: "xray", book_title: "Война и мир", author: "Толстой"
- "Анализ персонажей 1984" → type: "xray", book_title: "1984", author: "Оруэлл"
- "Читаю Мастера и Маргариту, на 50%, сделай xray" → type: "xray", progress_percent: 50

Примеры анализа url_to_kindle:
- "Отправь эту статью на киндл https://habr.com/article/123" → type: "url_to_kindle", url: "https://habr.com/article/123"
- "На киндл https://example.com/long-read" → type: "url_to_kindle"
- "Хочу почитать на читалке https://medium.com/article" → type: "url_to_kindle"

Примеры анализа book_search:
- "Отправь на киндл войну и мир" → query: "Война и мир", author: "Толстой", search_queries: ["Война и мир Толстой", "Война и мир"]
- "Скачай книгу странник среди звезд" → query: "Странник среди звёзд", author: "Хайнлайн", search_queries: ["Странник среди звёзд", "Stranger star"]
- "Найди 1984" → query: "1984", author: "Оруэлл", search_queries: ["1984 Оруэлл", "1984"]
- "Хочу почитать что-нибудь Стругацких" → query: "Стругацкие", author: "Стругацкие", search_queries: ["Стругацкие", "Аркадий Борис Стругацкие"]

ВАЖНО для book_search:
- ВСЕГДА извлекай ПОЛНОЕ название книги, не обрезай слова
- Если автор очевиден из контекста (классика, известные книги) — ОБЯЗАТЕЛЬНО укажи его
- search_queries должны идти от точного к общему
- Используй свои знания о литературе для определения автора
- Если книга может иметь АЛЬТЕРНАТИВНЫЕ НАЗВАНИЯ (переводы, варианты) — ОБЯЗАТЕЛЬНО добавь их в search_queries
  Примеры: "Странник среди звёзд" Джека Лондона → на русском также "Межзвёздный скиталец", оригинал "The Star Rover"
  "Над пропастью во ржи" → "The Catcher in the Rye"
  "Маленький принц" → "Le Petit Prince"

ВАЖНО: Всегда отвечай ТОЛЬКО валидным JSON без markdown-обёртки."""


WEEKDAYS_RU = {
    0: "Понедельник", 1: "Вторник", 2: "Среда", 3: "Четверг",
    4: "Пятница", 5: "Суббота", 6: "Воскресенье"
}

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

### Чеки, счета:
- Сумма, дата, назначение платежа
- Дедлайны оплаты

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
    """Send message to Claude for analysis and return structured data.
    
    Args:
        text: Message text to analyze.
        sender_context: Optional context about the message sender for calendar routing.
    """
    now = datetime.now(TIMEZONE)
    weekday = WEEKDAYS_RU.get(now.weekday(), "")
    prompt = SYSTEM_PROMPT.format(
        current_datetime=now.strftime("%Y-%m-%d %H:%M"),
        weekday=weekday,
    )
    if sender_context:
        prompt += f"\n\n## Контекст отправителя\n{sender_context}"

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


def get_week_events() -> list[dict]:
    """Get all events for the current week (Mon-Sun) with CalDAV retry."""
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
    return results


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
        "• Отправлю статью на Kindle\n\n"
        "<b>Команды:</b>\n"
        "/calendars — список календарей\n"
        "/today — события на сегодня\n"
        "/week — события на неделю\n"
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
    for i, ev in enumerate(events, 1):
        text += f"{i}. <b>[{ev['calendar']}]</b> {ev['time']} — {ev['title']}\n"
    
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
    
    # ── Apply per-user calendar override ──
    data = apply_calendar_override(data, user_routing)
    
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
        # Search for books — use search_queries from Claude for multi-query search
        search_query = data.get("query", text)
        search_author = data.get("author")  # May be None
        search_queries = data.get("search_queries", [search_query])
        send_to_kindle = data.get("send_to_kindle", False)
        
        logger.info("Book search: query='%s', author='%s', queries=%s, kindle=%s",
                    search_query, search_author, search_queries, send_to_kindle)
        
        # Try each search query until we get results
        all_results = []
        for sq in search_queries:
            await thinking_msg.edit_text(f"\U0001f50d \u0418\u0449\u0443 \u00ab{sq}\u00bb \u043d\u0430 \u0424\u043b\u0438\u0431\u0443\u0441\u0442\u0435...")
            try:
                results = await asyncio.to_thread(search_flibusta, sq, 10)
                if results:
                    all_results.extend(results)
                    logger.info("Flibusta found %d results for '%s'", len(results), sq)
                    break  # Got results, stop trying
            except Exception as e:
                logger.error("Book search error for '%s': %s", sq, e)
        
        # Fallback to Anna's Archive / Jackett if nothing found
        if not all_results:
            for sq in search_queries[:2]:
                try:
                    await thinking_msg.edit_text(f"\U0001f50d \u0418\u0449\u0443 \u00ab{sq}\u00bb \u0432 Anna's Archive...")
                    results = await asyncio.to_thread(search_annas_archive, sq)
                    if results:
                        all_results.extend(results)
                        break
                except Exception as e:
                    logger.error("Anna's Archive error: %s", e)
        
        if not all_results:
            for sq in search_queries[:2]:
                try:
                    await thinking_msg.edit_text(f"\U0001f50d \u0418\u0449\u0443 \u00ab{sq}\u00bb \u0447\u0435\u0440\u0435\u0437 Jackett...")
                    results = await asyncio.to_thread(search_jackett_books, sq)
                    if results:
                        all_results.extend(results)
                        break
                except Exception as e:
                    logger.error("Jackett error: %s", e)
        
        # AI RETHINK: if no results or results seem irrelevant, ask Claude for alternative names
        needs_rethink = False
        if not all_results:
            needs_rethink = True
        elif search_author:
            # Check if any result matches the expected author
            author_lower = search_author.lower()
            has_author_match = any(
                author_lower in " ".join(b.get("authors", [])).lower()
                for b in all_results
            )
            if not has_author_match:
                needs_rethink = True
                logger.info("No results match author '%s', triggering AI rethink", search_author)
        
        if needs_rethink:
            await thinking_msg.edit_text(f"\U0001f9e0 \u0418\u0449\u0443 \u0430\u043b\u044c\u0442\u0435\u0440\u043d\u0430\u0442\u0438\u0432\u043d\u044b\u0435 \u043d\u0430\u0437\u0432\u0430\u043d\u0438\u044f \u00ab{search_query}\u00bb...")
            try:
                alt_queries = await asyncio.to_thread(_ai_rethink_book_query, search_query, search_author)
                if alt_queries:
                    logger.info("AI rethink suggested: %s", alt_queries)
                    for aq in alt_queries:
                        await thinking_msg.edit_text(f"\U0001f50d \u0418\u0449\u0443 \u00ab{aq}\u00bb \u043d\u0430 \u0424\u043b\u0438\u0431\u0443\u0441\u0442\u0435...")
                        try:
                            results = await asyncio.to_thread(search_flibusta, aq, 10)
                            if results:
                                all_results.extend(results)
                                logger.info("AI rethink: found %d results for '%s'", len(results), aq)
                                break
                        except Exception as e:
                            logger.error("AI rethink search error: %s", e)
            except Exception as e:
                logger.error("AI rethink error: %s", e)
        
        if not all_results:
            await thinking_msg.edit_text(
                f"\u274c \u041f\u043e \u0437\u0430\u043f\u0440\u043e\u0441\u0443 \u00ab{search_query}\u00bb \u043d\u0438\u0447\u0435\u0433\u043e \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u043e.\n\n"
                f"\U0001f4a1 \u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439\u0442\u0435 \u0434\u0440\u0443\u0433\u043e\u0439 \u0437\u0430\u043f\u0440\u043e\u0441 \u0438\u043b\u0438 \u043f\u0440\u043e\u0432\u0435\u0440\u044c\u0442\u0435 \u043d\u0430\u043f\u0438\u0441\u0430\u043d\u0438\u0435.",
            )
            return
        
        # Deduplicate by book ID
        seen_ids = set()
        unique_results = []
        for book in all_results:
            bid = book.get("id", "")
            if bid and bid not in seen_ids:
                seen_ids.add(bid)
                unique_results.append(book)
            elif not bid:
                unique_results.append(book)
        
        # Rank results by relevance to query and author
        def _relevance_score(book):
            score = 0
            title_lower = book.get("title", "").lower()
            query_lower = search_query.lower()
            authors_lower = " ".join(book.get("authors", [])).lower()
            
            # Exact title match
            if query_lower == title_lower:
                score += 100
            # Title contains full query
            elif query_lower in title_lower:
                score += 50
            # All query words in title
            query_words = query_lower.split()
            matched_words = sum(1 for w in query_words if w in title_lower)
            score += matched_words * 10
            
            # Author match (if Claude identified the author)
            if search_author:
                author_lower = search_author.lower()
                if author_lower in authors_lower:
                    score += 40
                # Partial author match
                for aw in author_lower.split():
                    if aw in authors_lower:
                        score += 15
            
            # Prefer Russian language
            if book.get("language") == "ru":
                score += 5
            
            # Prefer books with more formats (more likely to be popular)
            score += min(len(book.get("formats", [])), 3)
            
            return score
        
        unique_results.sort(key=_relevance_score, reverse=True)
        
        # Limit to top 8
        results = unique_results[:8]
        
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
    
    elif entry_type == "xray":
        # X-Ray book analysis
        book_title = data.get("book_title", "")
        xray_author = data.get("author", "")
        progress = data.get("progress_percent")
        
        if not book_title:
            await thinking_msg.edit_text(
                "❌ Не удалось определить название книги.\n"
                "💡 Попробуйте: /xray Мастер и Маргарита"
            )
            return
        
        await thinking_msg.edit_text(
            f"🔬 Генерирую X-Ray анализ...\n\n"
            f"📖 <b>{book_title}</b>"
            + (f"\n✍️ {xray_author}" if xray_author else ""),
            parse_mode="HTML",
        )
        
        try:
            xray_result = await asyncio.to_thread(
                generate_xray_analysis, book_title, xray_author, progress
            )
            await thinking_msg.edit_text(xray_result, parse_mode="HTML")
        except Exception as e:
            logger.error("X-Ray generation error: %s", e)
            await thinking_msg.edit_text(f"❌ Ошибка генерации X-Ray: {str(e)[:200]}")
        return
    
    elif entry_type == "url_to_kindle":
        # URL → Kindle
        article_url = data.get("url", "")
        article_title = data.get("title", "")
        
        if not article_url:
            # Try to extract URL from original text
            found_urls = extract_urls(text)
            if found_urls:
                article_url = found_urls[0]
            else:
                await thinking_msg.edit_text(
                    "❌ Не найдена ссылка для отправки на Kindle.\n"
                    "💡 Отправьте ссылку с пометкой «на киндл»"
                )
                return
        
        await thinking_msg.edit_text(
            f"🌐 Скачиваю статью...\n\n"
            f"🔗 <code>{article_url[:80]}</code>",
            parse_mode="HTML",
        )
        
        try:
            epub_path, final_title = await asyncio.to_thread(
                url_to_epub, article_url, article_title
            )
        except Exception as e:
            logger.error("URL to EPUB error: %s", e)
            await thinking_msg.edit_text(
                f"❌ Не удалось скачать статью.\n\n{str(e)[:200]}"
            )
            return
        
        if not epub_path:
            await thinking_msg.edit_text(
                "❌ Не удалось сконвертировать статью в EPUB."
            )
            return
        
        # Show device selection
        try:
            from kindle_handler import get_kindle_devices
            devices = get_kindle_devices()
        except ImportError:
            devices = [("Kindle", os.getenv("KINDLE_EMAIL", ""))]
        
        context.user_data["url_kindle_pending"] = {
            "epub_path": epub_path,
            "title": final_title,
            "url": article_url,
        }
        
        keyboard = []
        for name, email in devices:
            keyboard.append([InlineKeyboardButton(
                f"📱 {name}",
                callback_data=f"urlkindle:{email}"
            )])
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="urlkindle:cancel")])
        
        await thinking_msg.edit_text(
            f"🌐 <b>Статья готова к отправке!</b>\n\n"
            f"📖 <b>{final_title}</b>\n"
            f"🔗 <code>{article_url[:60]}</code>\n\n"
            f"📱 <b>Отправить на:</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return
    
    elif entry_type == "recurring_tasks":
        # Create series of recurring calendar events
        logger.info("Creating recurring tasks: %s", data.get('summary', '?'))
        tasks = data.get("tasks", [])
        one_time = data.get("one_time_events", [])
        
        if not tasks and not one_time:
            await thinking_msg.edit_text("❌ Не удалось извлечь задачи из сообщения.")
            return
        
        # Apply calendar override for user
        cal_override = None
        if user_routing and user_routing.get("calendar_rule") not in (None, "auto"):
            cal_override = user_routing["calendar_rule"]
        if cal_override:
            for t in tasks:
                t["calendar"] = cal_override
            for e in one_time:
                e["calendar"] = cal_override
        
        try:
            recurring_uids = await asyncio.to_thread(
                create_recurring_tasks, tasks, cal_override
            )
        except Exception as exc:
            logger.error("create_recurring_tasks error: %s", exc)
            recurring_uids = []
        
        # Create one-time events
        one_time_uids = []
        for evt in one_time:
            try:
                uid = await asyncio.to_thread(create_calendar_event, evt)
                if uid:
                    one_time_uids.append(uid)
            except Exception as exc:
                logger.error("one-time event error: %s", exc)
        
        total = len(recurring_uids) + len(one_time_uids)
        summary = data.get("summary", "Задачи")
        reasoning = data.get("reasoning", "")
        
        if total > 0:
            # Build details
            lines = [f"✅ <b>Создано {total} записей в Apple Calendar!</b>\n"]
            lines.append(f"📝 <b>{summary}</b>\n")
            
            for t in tasks:
                times_str = ", ".join(t.get("times", []))
                lines.append(
                    f"🔁 <b>{t.get('title', '?')}</b>\n"
                    f"   📅 {t.get('start_date', '?')} — {t.get('end_date', '?')}\n"
                    f"   ⏰ {times_str}\n"
                    f"   🗓 Календарь: {t.get('calendar', '?')}"
                )
            
            for evt in one_time:
                lines.append(
                    f"📌 <b>{evt.get('title', '?')}</b>\n"
                    f"   📅 {evt.get('date', '?')} {evt.get('time_start', '')}"
                )
            
            lines.append(f"\n💬 {reasoning[:300]}")
            
            await thinking_msg.edit_text(
                "\n".join(lines),
                parse_mode="HTML",
            )
        else:
            await thinking_msg.edit_text(
                f"❌ Не удалось создать записи.\n\n{reasoning[:300]}"
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
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("calendars", cmd_calendars))
    app.add_handler(CommandHandler("today", cmd_today))
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
    
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    logger.info("Bot is running! Polling for messages...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=False)


if __name__ == "__main__":
    main()
