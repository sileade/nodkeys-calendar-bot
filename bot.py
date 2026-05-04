#!/usr/bin/env python3
"""
Nodkeys Calendar & Life Bot v5.0
Telegram bot that analyzes messages using Claude AI and routes them:
- Events/Tasks/Reminders → Apple Calendar (iCloud CalDAV)
- Notes → Apple Notes (iCloud IMAP)
- Diary entries → Apple Notes diary with chronography (one note per day)
- Book search → Flibusta OPDS + Anna's Archive + Jackett + AI Rethink
- Audiobook search → RuTracker (auth + scraping + magnet links)
- Audiobook download → qBittorrent + SeaweedFS S3 cache + Telegram audio delivery
- Kindle: AI format detection, Calibre conversion, SMTP delivery
- X-Ray: AI-powered book analysis (characters, themes, timeline)
- Kindle Clippings: parse My Clippings.txt → key takeaways
- URL→Kindle: send web articles to Kindle as clean EPUB
- All through natural language — no commands needed
"""

VERSION = "10.1"

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
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

# --- New modules (v10.0): E2E encryption, subscriptions, onboarding, shortcuts ---
try:
    from crypto import encrypt_json, decrypt_json, generate_api_token, verify_api_token, get_user_fingerprint
    from user_store import (
        get_user, create_user, update_user, get_user_settings, save_user_settings,
        is_subscription_active, get_user_plan, activate_subscription, check_limit,
        set_custom_bot_token, get_custom_bot_token, PLANS
    )
    from subscription import cmd_subscribe, callback_subscribe, precheckout_handler, successful_payment_handler
    from onboarding import cmd_settings, callback_settings, handle_settings_input
    from shortcuts_api import ShortcutsAPIHandler, handle_shortcut_request, get_shortcuts_list, generate_shortcut_instructions
    from user_bots import start_all_user_bots, start_user_bot, get_active_bots_count
    NEW_MODULES_LOADED = True
except ImportError as _import_err:
    import traceback as _tb
    _tb.print_exc()
    NEW_MODULES_LOADED = False

# --- v10.1 Commercial modules ---
try:
    from smart_reminders import (
        analyze_content_for_reminders, process_photo_for_reminders,
        process_message_for_reminders, generate_proactive_suggestions,
        add_suggestion, accept_suggestion, dismiss_suggestion,
        get_pending_suggestions, format_suggestion_message
    )
    from library import (
        add_book, add_audiobook, update_book_progress, update_audiobook_progress,
        rate_book, add_book_note, get_goal_progress, get_reading_stats,
        get_recommendations, format_library_message, format_book_list,
        format_book_detail, format_stats_message, set_reading_goal
    )
    from diary import (
        create_entry as diary_create_entry, get_today_entry, get_entries as diary_get_entries,
        get_evening_prompt, get_mood_stats, get_diary_insights,
        format_diary_overview, format_mood_report, get_diary_settings, update_diary_settings
    )
    from family import (
        create_family, invite_member, remove_member, get_members,
        is_family_member, create_list as family_create_list,
        add_list_item, check_list_item, get_lists as family_get_lists,
        assign_task as family_assign_task, complete_task as family_complete_task,
        get_family_tasks, add_family_event, get_upcoming_family_events,
        format_family_overview, format_list_message as family_format_list
    )
    from yearly_review import cmd_yearly_review
    from voice_assistant import (
        text_to_speech, speech_to_text, should_send_as_voice,
        clean_text_for_tts, generate_voice_briefing, get_voice_settings,
        save_voice_settings, format_voice_settings_message, cleanup_voice_cache
    )
    from podcasts import (
        search_podcasts, get_podcast_episodes, subscribe_podcast,
        unsubscribe_podcast, get_subscriptions as podcast_get_subscriptions,
        add_to_queue as podcast_add_to_queue, get_queue as podcast_get_queue,
        check_new_episodes, download_episode, format_search_results as podcast_format_results,
        format_podcast_detail, format_subscriptions_message as podcast_format_subs
    )
    from notion_sync import (
        verify_notion_connection, list_notion_databases, run_full_sync,
        export_diary_to_obsidian, export_books_to_obsidian,
        format_sync_settings, format_sync_result
    )
    V101_MODULES_LOADED = True
except ImportError as _v101_err:
    import traceback as _tb101
    _tb101.print_exc()
    V101_MODULES_LOADED = False

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

# ──────────────────── RUTRACKER AUDIOBOOK CONFIG ────────────────────
RUTRACKER_BASE_URL = os.environ.get("RUTRACKER_BASE_URL", "https://rutracker.net/forum")
RUTRACKER_LOGIN = os.environ.get("RUTRACKER_LOGIN", "ILEA112")
RUTRACKER_PASSWORD = os.environ.get("RUTRACKER_PASSWORD", "2qAP4")

# RuTracker audiobook forum IDs
RUTRACKER_AUDIOBOOK_FORUMS = [
    402,   # [Аудио] Русская литература
    400,   # [Аудио] История, культурология, философия
    399,   # Аудиокниги на русском (общий)
    2387,  # [Аудио] Зарубежная фантастика, фэнтези, мистика, ужасы
    2388,  # [Аудио] Детективы, Триллеры
    2389,  # [Аудио] Современная проза
    2327,  # [Аудио] Классика
    661,   # [Аудио] Историческая проза
    2325,  # [Аудио] Научная фантастика
    2326,  # [Аудио] Приключения
    2324,  # [Аудио] Юмор
    530,   # Аудиокниги на английском
    2391,  # Аудиокниги на других языках
    148,   # Аудио (общий)
    403,   # [Аудио] Зарубежная литература
    716,   # [Аудио] Бизнес, саморазвитие
    2165,  # [Аудио] Детская литература
]

# Persistent httpx client for RuTracker (with session cookies)
_rt_client: _flib_httpx.Client | None = None
_rt_logged_in = False

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


# ══════════════════════════════════════════════════════════
# ██  RUTRACKER AUDIOBOOK SEARCH
# ══════════════════════════════════════════════════════════

def _ensure_rutracker_login() -> _flib_httpx.Client:
    """Ensure we have an authenticated RuTracker session."""
    global _rt_client, _rt_logged_in

    if _rt_client is None:
        _rt_kwargs = {
            "headers": {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            },
            "follow_redirects": True,
            "timeout": 20,
        }
        if FLIBUSTA_PROXY_URL:
            _rt_kwargs["proxy"] = FLIBUSTA_PROXY_URL
            logger.info("RuTracker using proxy: %s", FLIBUSTA_PROXY_URL)
        _rt_client = _flib_httpx.Client(**_rt_kwargs)

    if not _rt_logged_in:
        try:
            resp = _rt_client.post(
                f"{RUTRACKER_BASE_URL}/login.php",
                data={
                    "login_username": RUTRACKER_LOGIN,
                    "login_password": RUTRACKER_PASSWORD,
                    "login": "Вход",
                },
            )
            cookies = dict(_rt_client.cookies)
            if "bb_session" in cookies or "bb_data" in cookies or RUTRACKER_LOGIN.lower() in resp.text.lower():
                _rt_logged_in = True
                logger.info("RuTracker login successful")
            else:
                logger.warning("RuTracker login may have failed")
        except Exception as e:
            logger.error("RuTracker login error: %s", e)

    return _rt_client


def search_rutracker_audiobooks(query: str, limit: int = 10) -> list[dict]:
    """Search for audiobooks on RuTracker.

    Args:
        query: Search query (book title, author, etc.)
        limit: Maximum number of results

    Returns:
        List of dicts with keys: title, topic_id, url, size, seeds, forum, date, source
    """
    results = []

    try:
        client = _ensure_rutracker_login()

        # Build forum filter
        forum_params = "&".join([f"f[]={f}" for f in RUTRACKER_AUDIOBOOK_FORUMS])
        search_url = f"{RUTRACKER_BASE_URL}/tracker.php?nm={quote(query)}&{forum_params}"

        logger.info("RuTracker audiobook search: %s", query)
        resp = client.get(search_url)

        if resp.status_code != 200:
            logger.error("RuTracker search failed: HTTP %d", resp.status_code)
            return results

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "html.parser")
        tbl = soup.select_one("#tor-tbl")
        if not tbl:
            logger.warning("RuTracker: no results table found")
            return results

        rows = tbl.select("tr")[1:]  # Skip header
        logger.info("RuTracker: found %d raw results", len(rows))

        for row in rows[:limit]:
            tds = row.select("td")
            if len(tds) < 8:
                continue

            try:
                # td[3] = title with link (a.tLink)
                title_link = tds[3].select_one("a.tLink")
                if not title_link:
                    continue

                title = title_link.get_text(strip=True)
                topic_id = title_link.get("data-topic_id", "")
                if not topic_id:
                    href = title_link.get("href", "")
                    m = re.search(r"t=(\d+)", href)
                    topic_id = m.group(1) if m else ""

                if not topic_id:
                    continue

                # td[2] = forum name
                forum_el = tds[2].select_one("a")
                forum_name = forum_el.get_text(strip=True) if forum_el else "Аудио"

                # td[5] = size (a.dl-stub)
                size_el = tds[5].select_one("a.dl-stub")
                size_text = size_el.get_text(strip=True).replace("↓", "").strip() if size_el else "N/A"

                # td[6] = seeds (b.seedmed)
                seeds_el = tds[6].select_one("b.seedmed")
                seeds = int(seeds_el.get_text(strip=True)) if seeds_el else 0

                # td[9] = date
                date_text = tds[9].get_text(strip=True) if len(tds) > 9 else ""

                results.append({
                    "title": title,
                    "topic_id": topic_id,
                    "url": f"{RUTRACKER_BASE_URL}/viewtopic.php?t={topic_id}",
                    "download_url": f"{RUTRACKER_BASE_URL}/dl.php?t={topic_id}",
                    "size": size_text,
                    "seeds": seeds,
                    "forum": forum_name,
                    "date": date_text,
                    "source": "rutracker",
                })
            except Exception as e:
                logger.warning("RuTracker parse row error: %s", e)
                continue

        # Sort by seeds (most seeded first)
        results.sort(key=lambda x: x["seeds"], reverse=True)

    except Exception as e:
        logger.error("RuTracker search error: %s", e)

    return results


def get_audiobook_magnet(topic_id: str) -> str | None:
    """Get magnet link from a RuTracker topic page."""
    try:
        client = _ensure_rutracker_login()
        url = f"{RUTRACKER_BASE_URL}/viewtopic.php?t={topic_id}"
        resp = client.get(url)

        # Find magnet link in page
        magnet_match = re.search(r'magnet:\?xt=urn:btih:[a-fA-F0-9]+[^"\'\'<>\s]*', resp.text)
        if magnet_match:
            return magnet_match.group(0)

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "html.parser")
        magnet_el = soup.select_one("a[href^='magnet:']")
        if magnet_el:
            return magnet_el.get("href")

        return None
    except Exception as e:
        logger.error("RuTracker magnet error: %s", e)
        return None


# ══════════════════════════════════════════════════════════
# ██  AUDIOBOOK DOWNLOAD PIPELINE: qBittorrent → S3 → Telegram
# ══════════════════════════════════════════════════════════

QBITTORRENT_URL = os.environ.get("QBITTORRENT_URL", "http://qbittorrent:8080")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://10.250.1.201:8333")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "media")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "audiobooks")
AUDIOBOOK_DOWNLOAD_DIR = "/downloads/audiobooks"
AUDIOBOOK_CACHE_FILE = "/app/data/audiobook_cache.json"
AUDIO_EXTENSIONS = {".mp3", ".m4a", ".m4b", ".ogg", ".opus", ".flac", ".wav", ".aac", ".wma"}
TELEGRAM_MAX_AUDIO_SIZE = 49 * 1024 * 1024  # 49MB (Telegram limit ~50MB)

# Mini App player HTML — loaded from file at startup
_AUDIOBOOK_PLAYER_HTML = ""
_player_html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'audiobook_player.html')
if os.path.exists(_player_html_path):
    with open(_player_html_path, 'r', encoding='utf-8') as _f:
        _AUDIOBOOK_PLAYER_HTML = _f.read()
else:
    logger.warning('audiobook_player.html not found at %s', _player_html_path)

# S3 client (lazy init)
_s3_client = None

def _get_s3_client():
    """Get or create boto3 S3 client."""
    global _s3_client
    if _s3_client is None:
        try:
            import boto3
            from botocore.config import Config
            _s3_client = boto3.client(
                's3',
                endpoint_url=S3_ENDPOINT,
                aws_access_key_id=S3_ACCESS_KEY,
                aws_secret_access_key=S3_SECRET_KEY,
                region_name='us-east-1',
                config=Config(signature_version='s3v4'),
            )
        except Exception as e:
            logger.error("S3 client init error: %s", e)
    return _s3_client


def _load_audiobook_cache() -> dict:
    """Load audiobook cache: {info_hash: {title, s3_prefix, files, timestamp}}."""
    try:
        if os.path.exists(AUDIOBOOK_CACHE_FILE):
            with open(AUDIOBOOK_CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logger.error("Audiobook cache load error: %s", e)
    return {}


def _save_audiobook_cache(cache: dict):
    """Save audiobook cache to disk."""
    try:
        os.makedirs(os.path.dirname(AUDIOBOOK_CACHE_FILE), exist_ok=True)
        with open(AUDIOBOOK_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error("Audiobook cache save error: %s", e)


def _extract_info_hash(magnet: str) -> str | None:
    """Extract info_hash from magnet link."""
    m = re.search(r'btih:([a-fA-F0-9]{40})', magnet)
    if m:
        return m.group(1).lower()
    # Base32 encoded hash
    m = re.search(r'btih:([A-Za-z2-7]{32})', magnet)
    if m:
        import base64
        try:
            decoded = base64.b32decode(m.group(1).upper())
            return decoded.hex().lower()
        except Exception:
            pass
    return None


def _check_s3_cache(info_hash: str) -> list[dict] | None:
    """Check if audiobook files exist in S3 cache.
    Returns list of {key, size, filename} or None."""
    s3 = _get_s3_client()
    if not s3:
        return None
    try:
        prefix = f"{info_hash}/"
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        contents = resp.get('Contents', [])
        if not contents:
            return None
        files = []
        for obj in contents:
            key = obj['Key']
            fname = key.split('/')[-1]
            ext = os.path.splitext(fname)[1].lower()
            if ext in AUDIO_EXTENSIONS:
                files.append({
                    'key': key,
                    'size': obj['Size'],
                    'filename': fname,
                })
        if files:
            files.sort(key=lambda x: x['filename'])
            return files
        return None
    except Exception as e:
        logger.error("S3 cache check error: %s", e)
        return None


def _qbt_add_magnet(magnet: str, info_hash: str) -> bool:
    """Add magnet link to qBittorrent with specific save path and force start."""
    try:
        import httpx
        save_path = f"{AUDIOBOOK_DOWNLOAD_DIR}/{info_hash}"
        resp = httpx.post(
            f"{QBITTORRENT_URL}/api/v2/torrents/add",
            data={
                'urls': magnet,
                'savepath': save_path,
                'category': 'audiobooks',
            },
            timeout=15,
        )
        if resp.status_code == 200 and resp.text.strip() in ('Ok.', 'Fails.'):
            logger.info("qBittorrent add torrent: %s → %s", info_hash[:8], resp.text.strip())
            if resp.text.strip() == 'Ok.':
                # Force start to avoid stuck in 'error' state
                _time.sleep(2)  # Wait for torrent to be registered
                try:
                    httpx.post(
                        f"{QBITTORRENT_URL}/api/v2/torrents/setForceStart",
                        data={'hashes': info_hash, 'value': 'true'},
                        timeout=5,
                    )
                    logger.info("qBittorrent force start: %s", info_hash[:8])
                except Exception as e2:
                    logger.warning("qBittorrent force start failed: %s", e2)
                return True
            return False
        logger.warning("qBittorrent add unexpected: HTTP %d %s", resp.status_code, resp.text[:100])
        return False
    except Exception as e:
        logger.error("qBittorrent add error: %s", e)
        return False


def _qbt_get_torrent_status(info_hash: str) -> dict | None:
    """Get torrent status from qBittorrent."""
    try:
        import httpx
        resp = httpx.get(
            f"{QBITTORRENT_URL}/api/v2/torrents/info",
            params={'hashes': info_hash},
            timeout=10,
        )
        if resp.status_code == 200:
            torrents = resp.json()
            if torrents:
                t = torrents[0]
                return {
                    'state': t.get('state', ''),
                    'progress': t.get('progress', 0),
                    'size': t.get('size', 0),
                    'downloaded': t.get('downloaded', 0),
                    'dlspeed': t.get('dlspeed', 0),
                    'eta': t.get('eta', 0),
                    'num_seeds': t.get('num_seeds', 0),
                    'save_path': t.get('save_path', ''),
                    'name': t.get('name', ''),
                    'content_path': t.get('content_path', ''),
                }
        return None
    except Exception as e:
        logger.error("qBittorrent status error: %s", e)
        return None


def _qbt_get_torrent_files(info_hash: str) -> list[dict]:
    """Get list of files in a torrent."""
    try:
        import httpx
        resp = httpx.get(
            f"{QBITTORRENT_URL}/api/v2/torrents/files",
            params={'hash': info_hash},
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json()
        return []
    except Exception as e:
        logger.error("qBittorrent files error: %s", e)
        return []


def _upload_audiobook_to_s3(info_hash: str, local_dir: str) -> list[dict]:
    """Upload audio files from local directory to S3 using parallel threads.
    Returns list of {key, size, filename}."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    s3 = _get_s3_client()
    if not s3:
        return []

    # Collect files to upload
    files_to_upload = []
    for root, dirs, files in os.walk(local_dir):
        for fname in sorted(files):
            ext = os.path.splitext(fname)[1].lower()
            if ext not in AUDIO_EXTENSIONS:
                continue
            local_path = os.path.join(root, fname)
            rel_path = os.path.relpath(local_path, local_dir)
            s3_key = f"{info_hash}/{rel_path}"
            file_size = os.path.getsize(local_path)
            files_to_upload.append((local_path, s3_key, file_size, fname))

    if not files_to_upload:
        return []

    logger.info("S3 upload: %d files to upload for %s", len(files_to_upload), info_hash)
    uploaded = []
    failed = 0

    def _upload_one(item):
        local_path, s3_key, file_size, fname = item
        try:
            # Each thread needs its own S3 client
            thread_s3 = _get_s3_client()
            thread_s3.upload_file(local_path, S3_BUCKET, s3_key)
            return {'key': s3_key, 'size': file_size, 'filename': fname}
        except Exception as e:
            logger.error("S3 upload error for %s: %s", s3_key, e)
            return None

    # Use 8 parallel threads for upload
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_upload_one, item): item for item in files_to_upload}
        for future in as_completed(futures):
            result = future.result()
            if result:
                uploaded.append(result)
            else:
                failed += 1

    # Sort by filename for consistent ordering
    uploaded.sort(key=lambda x: x['filename'])
    logger.info("S3 upload done: %d uploaded, %d failed", len(uploaded), failed)
    return uploaded


def _download_from_s3(s3_key: str, local_path: str) -> bool:
    """Download a file from S3 to local path."""
    s3 = _get_s3_client()
    if not s3:
        return False
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3.download_file(S3_BUCKET, s3_key, local_path)
        return True
    except Exception as e:
        logger.error("S3 download error: %s", e)
        return False


def _format_size(size_bytes: int) -> str:
    """Format bytes to human-readable size."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


def _format_eta(seconds: int) -> str:
    """Format ETA seconds to human-readable."""
    if seconds <= 0 or seconds >= 8640000:
        return "∞"
    if seconds < 60:
        return f"{seconds}с"
    elif seconds < 3600:
        return f"{seconds // 60}м {seconds % 60}с"
    else:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}ч {m}м"


async def audiobook_download_pipeline(
    bot,
    chat_id: int,
    message_id: int,
    magnet: str,
    info_hash: str,
    title: str,
):
    """Full audiobook download pipeline:
    1. Check S3 cache
    2. If not cached: add to qBittorrent, wait for download
    3. Upload to S3
    4. Send audio files to Telegram
    """
    async def _edit(text: str, reply_markup=None):
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                parse_mode="HTML",
                reply_markup=reply_markup,
            )
        except Exception as e:
            logger.warning("Pipeline _edit error: %s", e)

    # ── Step 1: Check S3 cache ──
    logger.info("Audiobook pipeline started: hash=%s title=%s", info_hash, title[:40])
    await _edit(f"🔍 <b>Проверяю кэш...</b>\n\n🎧 {title[:60]}")
    try:
        cached_files = await asyncio.to_thread(_check_s3_cache, info_hash)
    except Exception as e:
        logger.error("S3 cache check failed: %s", e)
        cached_files = None
    logger.info("Audiobook cache result: %s files", len(cached_files) if cached_files else 0)

    if cached_files:
        # Found in cache — send player button
        logger.info("Audiobook found in cache: %d files", len(cached_files))
        webapp_url = f"https://bot.nodkeys.com/audiobook/player?hash={info_hash}"
        download_url = f"https://bot.nodkeys.com/audiobook/download/{info_hash}"
        total_size = 0
        try:
            total_size = sum(f['size'] for f in cached_files)
        except Exception as e:
            logger.error("Cache size calc error: %s, files sample: %s", e, cached_files[:2])
        # Player keyboard with play/prev/next + full player link
        keyboard = _make_player_keyboard(info_hash, 0, len(cached_files), webapp_url)
        try:
            await _edit(
                f"\u2705 <b>Аудиокнига готова!</b>\n\n"
                f"\U0001f3a7 {title[:60]}\n"
                f"\U0001f4c1 {len(cached_files)} аудиофайлов ({_format_size(total_size)})\n\n"
                f"\u25b6\ufe0f Нажмите Play для прослушивания в чате",
                reply_markup=keyboard,
            )
        except Exception as e:
            logger.error("Failed to send player button (cache): %s", e)
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"\u2705 <b>Аудиокнига готова!</b>\n\n"
                        f"\U0001f3a7 {title[:60]}\n"
                        f"\U0001f4c1 {len(cached_files)} аудиофайлов ({_format_size(total_size)})\n\n"
                        f"\u25b6\ufe0f Нажмите Play для прослушивания в чате"
                    ),
                    parse_mode="HTML",
                    reply_markup=keyboard,
                )
            except Exception as e2:
                logger.error("Fallback send also failed: %s", e2)
        return

    # ── Step 2: Add to qBittorrent ──
    logger.info("Pipeline step 2: adding to qBittorrent hash=%s", info_hash)
    await _edit(
        f"📥 <b>Начинаю скачивание...</b>\n\n"
        f"🎧 {title[:60]}\n"
        f"🧲 Добавляю в торрент-клиент..."
    )
    added = await asyncio.to_thread(_qbt_add_magnet, magnet, info_hash)
    logger.info("Pipeline step 2 result: added=%s", added)
    if not added:
        await _edit(
            f"❌ <b>Не удалось добавить торрент</b>\n\n"
            f"🎧 {title[:60]}\n\n"
            f"🧲 <b>Magnet-ссылка:</b>\n"
            f"<code>{magnet}</code>\n\n"
            f"💡 Скопируйте и откройте в торрент-клиенте вручную"
        )
        return

    # ── Step 3: Monitor download progress ──
    await _edit(
        f"⏳ <b>Скачивание запущено</b>\n\n"
        f"🎧 {title[:60]}\n"
        f"🔄 Ожидаю подключения к пирам..."
    )

    max_wait = 3600  # 1 hour max
    poll_interval = 10  # seconds
    elapsed = 0
    last_progress_text = ""

    while elapsed < max_wait:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

        status = await asyncio.to_thread(_qbt_get_torrent_status, info_hash)
        if not status:
            if elapsed > 120:
                await _edit(
                    f"⚠️ <b>Торрент не найден</b>\n\n"
                    f"🎧 {title[:60]}\n"
                    f"Возможно, скачивание не началось. Попробуйте позже."
                )
                return
            continue

        state = status['state']
        progress = status['progress']
        dlspeed = status['dlspeed']
        eta = status['eta']
        seeds = status['num_seeds']

        # Check if completed
        if state in ('uploading', 'pausedUP', 'stalledUP', 'queuedUP', 'forcedUP') or progress >= 1.0:
            break

        # Update progress message (not too often)
        progress_pct = int(progress * 100)
        speed_str = _format_size(dlspeed) + "/с" if dlspeed > 0 else "—"
        eta_str = _format_eta(eta)
        bar_filled = int(progress_pct / 5)
        bar = "█" * bar_filled + "░" * (20 - bar_filled)

        progress_text = (
            f"📥 <b>Скачивание аудиокниги</b>\n\n"
            f"🎧 {title[:50]}\n\n"
            f"[{bar}] {progress_pct}%\n"
            f"⚡ {speed_str} | 🌱 Seeds: {seeds}\n"
            f"⏱ ETA: {eta_str}"
        )

        if progress_text != last_progress_text:
            await _edit(progress_text)
            last_progress_text = progress_text

        # Slow down polling for large files
        if elapsed > 60:
            poll_interval = 15
        if elapsed > 300:
            poll_interval = 30
    else:
        # Timeout
        await _edit(
            f"⏰ <b>Превышено время ожидания (1 час)</b>\n\n"
            f"🎧 {title[:60]}\n\n"
            f"Скачивание продолжается в фоне. Попробуйте запросить снова позже."
        )
        return

    # ── Step 4: Upload to S3 ──
    logger.info("Pipeline step 4: uploading to S3 hash=%s", info_hash)
    await _edit(
        f"☁️ <b>Загружаю в облако...</b>\n\n"
        f"🎧 {title[:60]}\n"
        f"📦 Кэширую для быстрого доступа..."
    )

    # Find the actual download path
    status = await asyncio.to_thread(_qbt_get_torrent_status, info_hash)
    content_path = status.get('content_path', '') if status else ''
    save_path = status.get('save_path', '') if status else ''

    # The files should be in /downloads/audiobooks/{info_hash}/
    local_dir = f"{AUDIOBOOK_DOWNLOAD_DIR}/{info_hash}"
    if content_path and os.path.isdir(content_path):
        local_dir = content_path
    elif content_path and os.path.isfile(content_path):
        local_dir = os.path.dirname(content_path)
    elif save_path:
        local_dir = save_path

    logger.info("Pipeline step 4: local_dir=%s, exists=%s", local_dir, os.path.isdir(local_dir))
    uploaded = await asyncio.to_thread(_upload_audiobook_to_s3, info_hash, local_dir)
    logger.info("Pipeline step 4 result: %d files uploaded", len(uploaded) if uploaded else 0)

    if not uploaded:
        # Try to send files directly from disk
        await _edit(
            f"⚠️ <b>Не удалось загрузить в облако</b>\n\n"
            f"🎧 {title[:60]}\n"
            f"📤 Отправляю файлы напрямую..."
        )
        await _send_audio_files_from_disk(bot, chat_id, local_dir, title)
        return

    # Update cache
    cache = _load_audiobook_cache()
    cache[info_hash] = {
        'title': title,
        's3_prefix': f"{info_hash}/",
        'files': [{'filename': f['filename'], 'key': f['key'], 'size': f['size']} for f in uploaded],
        'total_size': sum(f['size'] for f in uploaded),
        'timestamp': datetime.now(TIMEZONE).isoformat(),
    }
    _save_audiobook_cache(cache)

    # ── Step 5: Send Mini App button ──
    logger.info("Pipeline step 5: sending player button")
    webapp_url = f"https://bot.nodkeys.com/audiobook/player?hash={info_hash}"
    download_url = f"https://bot.nodkeys.com/audiobook/download/{info_hash}"
    total_uploaded_size = sum(f['size'] for f in uploaded)
    # Player keyboard with play/prev/next + full player link
    keyboard = _make_player_keyboard(info_hash, 0, len(uploaded), webapp_url)
    try:
        await _edit(
            f"\u2705 <b>Аудиокнига готова!</b>\n\n"
            f"\U0001f3a7 {title[:60]}\n"
            f"\U0001f4c1 {len(uploaded)} файлов ({_format_size(total_uploaded_size)})\n\n"
            f"\u25b6\ufe0f Нажмите Play для прослушивания в чате",
            reply_markup=keyboard,
        )
    except Exception as e:
        logger.error("Step 5: Failed to send player button: %s", e)
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    f"\u2705 <b>Аудиокнига готова!</b>\n\n"
                    f"\U0001f3a7 {title[:60]}\n"
                    f"\U0001f4c1 {len(uploaded)} файлов ({_format_size(total_uploaded_size)})\n\n"
                    f"\u25b6\ufe0f Нажмите Play для прослушивания в чате"
                ),
                parse_mode="HTML",
                reply_markup=keyboard,
            )
        except Exception as e2:
            logger.error("Step 5 fallback also failed: %s", e2)

    # Clean up torrent (optional: pause seeding)
    try:
        import httpx
        httpx.post(
            f"{QBITTORRENT_URL}/api/v2/torrents/pause",
            data={'hashes': info_hash},
            timeout=5,
        )
    except Exception:
        pass


def _group_audio_files(filenames: list[str]) -> list[tuple[str, list[str]]]:
    """Group audio files by chapter/part prefix.
    
    Examples:
        01_01_01.mp3, 01_01_02.mp3 → group '01_01' (part 1, chapter 1)
        Chapter_01.mp3, Chapter_02.mp3 → each is its own group
    Returns list of (group_label, [filenames]).
    """
    if not filenames:
        return []
    
    sorted_files = sorted(filenames)
    
    # Try to detect grouping pattern
    # Pattern: XX_YY_ZZ.mp3 → group by XX_YY
    groups = {}
    for fname in sorted_files:
        name = os.path.splitext(fname)[0]
        parts = re.split(r'[_\-\s]+', name)
        if len(parts) >= 3 and all(p.isdigit() for p in parts[:2]):
            # Pattern like 01_01_01 → group by first two parts
            group_key = f"{parts[0]}_{parts[1]}"
        elif len(parts) >= 2 and parts[0].isdigit():
            # Pattern like 01_something → group by first part
            group_key = parts[0]
        else:
            # No clear pattern → each file is its own group
            group_key = name
        
        if group_key not in groups:
            groups[group_key] = []
        groups[group_key].append(fname)
    
    # Convert to sorted list of tuples
    result = []
    for key in sorted(groups.keys()):
        files = sorted(groups[key])
        # Generate human-readable label
        parts = key.split('_')
        if len(parts) == 2 and all(p.isdigit() for p in parts):
            label = f"Том {int(parts[0])}, Часть {int(parts[1])}"
        elif len(parts) == 1 and parts[0].isdigit():
            label = f"Часть {int(parts[0])}"
        else:
            label = key
        result.append((label, files))
    
    return result


async def _send_audio_files_from_s3(
    bot, chat_id: int, files: list[dict], title: str, info_hash: str = ""
):
    """Download files from S3 and send to Telegram as audio, grouped by chapters."""
    tmp_dir = f"/tmp/audiobook_{uuid.uuid4().hex[:8]}"
    os.makedirs(tmp_dir, exist_ok=True)

    sent_count = 0
    total = len(files)
    files_sorted = sorted(files, key=lambda x: x['filename'])
    files_by_name = {f['filename']: f for f in files_sorted}

    # Send S3 download link first
    if info_hash:
        download_url = f"https://bot.nodkeys.com/audiobook/download/{info_hash}"
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    f"\U0001f517 <b>Ссылка для скачивания:</b>\n"
                    f"<a href=\"{download_url}\">{title[:60]}</a>\n\n"
                    f"\U0001f4c1 {total} файлов | \U0001f4e6 {_format_size(sum(f['size'] for f in files_sorted))}\n\n"
                    f"\U0001f4e4 Отправляю аудиофайлы в чат..."
                ),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception as e:
            logger.error("Send download link error: %s", e)

    # Group files
    filenames = [f['filename'] for f in files_sorted]
    groups = _group_audio_files(filenames)
    
    global_idx = 0
    for group_label, group_files in groups:
        # Send group header if more than 1 group
        if len(groups) > 1:
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"\U0001f4d6 <b>{group_label}</b> ({len(group_files)} файлов)",
                    parse_mode="HTML",
                )
            except Exception:
                pass
            await asyncio.sleep(0.5)

        for fname in group_files:
            global_idx += 1
            f = files_by_name.get(fname)
            if not f:
                continue
            
            local_path = os.path.join(tmp_dir, fname)
            ok = await asyncio.to_thread(_download_from_s3, f['key'], local_path)
            if not ok:
                continue

            file_size = os.path.getsize(local_path)
            if file_size > TELEGRAM_MAX_AUDIO_SIZE:
                try:
                    with open(local_path, 'rb') as audio_file:
                        await bot.send_document(
                            chat_id=chat_id,
                            document=audio_file,
                            filename=fname,
                            caption=f"\U0001f3a7 {title[:40]} \u2014 {fname}",
                        )
                    sent_count += 1
                except Exception as e:
                    logger.error("Telegram send document error: %s", e)
            else:
                try:
                    track_title = os.path.splitext(fname)[0]
                    with open(local_path, 'rb') as audio_file:
                        await bot.send_audio(
                            chat_id=chat_id,
                            audio=audio_file,
                            title=track_title,
                            performer=title[:40],
                            caption=f"\U0001f4d6 {global_idx}/{total}",
                        )
                    sent_count += 1
                except Exception as e:
                    logger.error("Telegram send audio error: %s", e)
                    try:
                        with open(local_path, 'rb') as audio_file:
                            await bot.send_document(
                                chat_id=chat_id,
                                document=audio_file,
                                filename=fname,
                            )
                        sent_count += 1
                    except Exception:
                        pass

            # Clean up temp file after sending
            try:
                os.remove(local_path)
            except Exception:
                pass
            
            # Small delay to avoid Telegram rate limiting
            await asyncio.sleep(1)

    # Clean up temp dir
    try:
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)
    except Exception:
        pass

    if sent_count > 0:
        download_url = f"https://bot.nodkeys.com/audiobook/download/{info_hash}" if info_hash else ""
        dl_text = f"\n\U0001f517 <a href=\"{download_url}\">Скачать все файлы</a>" if download_url else ""
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    f"\u2705 <b>Аудиокнига отправлена!</b>\n\n"
                    f"\U0001f3a7 {title[:60]}\n"
                    f"\U0001f4c1 {sent_count}/{total} файлов{dl_text}\n\n"
                    f"\U0001f4a1 При повторном запросе файлы будут отправлены мгновенно из кэша."
                ),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception:
            pass
    else:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=f"\u274c Не удалось отправить аудиофайлы для: {title[:60]}",
            )
        except Exception:
            pass


async def _send_audio_files_from_disk(
    bot, chat_id: int, local_dir: str, title: str
):
    """Send audio files directly from local disk to Telegram, grouped by chapters."""
    sent_count = 0
    audio_files = []

    for root, dirs, files in os.walk(local_dir):
        for fname in sorted(files):
            ext = os.path.splitext(fname)[1].lower()
            if ext in AUDIO_EXTENSIONS:
                audio_files.append((fname, os.path.join(root, fname)))

    total = len(audio_files)
    filenames = [f[0] for f in audio_files]
    file_paths = {f[0]: f[1] for f in audio_files}
    groups = _group_audio_files(filenames)
    
    global_idx = 0
    for group_label, group_files in groups:
        if len(groups) > 1:
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"\U0001f4d6 <b>{group_label}</b> ({len(group_files)} файлов)",
                    parse_mode="HTML",
                )
            except Exception:
                pass
            await asyncio.sleep(0.5)

        for fname in group_files:
            global_idx += 1
            fpath = file_paths.get(fname)
            if not fpath:
                continue
            file_size = os.path.getsize(fpath)

            if file_size > TELEGRAM_MAX_AUDIO_SIZE:
                try:
                    with open(fpath, 'rb') as f:
                        await bot.send_document(
                            chat_id=chat_id,
                            document=f,
                            filename=fname,
                            caption=f"\U0001f3a7 {title[:40]} \u2014 {fname}",
                        )
                    sent_count += 1
                except Exception as e:
                    logger.error("Send from disk error: %s", e)
            else:
                try:
                    track_title = os.path.splitext(fname)[0]
                    with open(fpath, 'rb') as f:
                        await bot.send_audio(
                            chat_id=chat_id,
                            audio=f,
                            title=track_title,
                            performer=title[:40],
                            caption=f"\U0001f4d6 {global_idx}/{total}",
                        )
                    sent_count += 1
                except Exception as e:
                    logger.error("Send audio from disk error: %s", e)
            
            await asyncio.sleep(1)

    if sent_count > 0:
        await bot.send_message(
            chat_id=chat_id,
            text=f"\u2705 <b>Отправлено {sent_count}/{total} аудиофайлов</b>\n\U0001f3a7 {title[:60]}",
            parse_mode="HTML",
        )


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
        "name": "create_project",
        "description": "Create a new project for organizing tasks into a kanban board. Use when user wants to start a new project, plan something complex, or organize work.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Project name"},
                "description": {"type": "string", "description": "Brief project description"},
                "tasks": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "title": {"type": "string"},
                            "priority": {"type": "string", "enum": ["high", "medium", "low"]}
                        },
                        "required": ["title"]
                    },
                    "description": "Initial tasks to add to the project"
                }
            },
            "required": ["name"]
        }
    },
    {
        "name": "manage_project",
        "description": "Manage project tasks: add task, move task between columns (todo/in_progress/done), delete task, or show kanban board. Use when user talks about project progress, completing tasks within a project, or wants to see project status.",
        "input_schema": {
            "type": "object",
            "properties": {
                "action": {"type": "string", "enum": ["add_task", "move_task", "delete_task", "show_board", "list_projects", "delete_project"], "description": "Action to perform"},
                "project_name": {"type": "string", "description": "Project name (for context)"},
                "task_title": {"type": "string", "description": "Task title (for add/move/delete)"},
                "new_status": {"type": "string", "enum": ["todo", "in_progress", "done"], "description": "New status for move_task"},
                "priority": {"type": "string", "enum": ["high", "medium", "low"], "description": "Task priority"}
            },
            "required": ["action"]
        }
    },
    {
        "name": "search_audiobook",
        "description": "Search for an audiobook (аудиокнига) on RuTracker. Use when user wants to find, download, or listen to an audiobook. Returns results with magnet links for torrent download.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query — book title, author name, or both"},
                "search_queries": {"type": "array", "items": {"type": "string"}, "description": "2-3 search variants: exact title, author + title, transliteration"}
            },
            "required": ["query", "search_queries"]
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
        "name": "add_income",
        "description": "Record income/earnings. Use when user mentions receiving money, salary, freelance payment, gift, or any income.",
        "input_schema": {
            "type": "object",
            "properties": {
                "amount": {"type": "number", "description": "Amount in rubles"},
                "category": {"type": "string", "enum": ["salary", "freelance", "investment", "gift", "refund", "other"], "description": "Income category"},
                "description": {"type": "string", "description": "Income source description"},
                "date": {"type": "string", "description": "Date (YYYY-MM-DD), defaults to today"}
            },
            "required": ["amount", "category", "description"]
        }
    },
    {
        "name": "set_budget",
        "description": "Set a monthly budget limit for an expense category. Use when user wants to set spending limits, budget goals, or financial targets.",
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "enum": ["food", "transport", "housing", "utilities", "entertainment", "health", "clothing", "subscriptions", "education", "other"], "description": "Expense category"},
                "monthly_limit": {"type": "number", "description": "Monthly budget limit in rubles"}
            },
            "required": ["category", "monthly_limit"]
        }
    },
    {
        "name": "finance_chart",
        "description": "Generate a visual chart of expenses. Use when user asks for financial graphs, visual reports, or spending visualization.",
        "input_schema": {
            "type": "object",
            "properties": {
                "chart_type": {"type": "string", "enum": ["bar", "pie"], "description": "Chart type: bar (daily/monthly spending) or pie (by category)"},
                "period": {"type": "string", "enum": ["month", "year"], "description": "Time period"}
            },
            "required": ["chart_type"]
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
    memory_facts = _get_memory_facts()
    if memory_facts:
        prompt += "\n\n## Что я знаю о пользователе:\n"
        for fact in memory_facts[-20:]:
            prompt += f"- {fact}\n"
    
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


def _make_player_keyboard(info_hash: str, track_idx: int, total_tracks: int, webapp_url: str) -> InlineKeyboardMarkup:
    """Create inline keyboard with play/prev/next buttons for audiobook player."""
    buttons_row = []
    # Prev button
    if track_idx > 0:
        buttons_row.append(InlineKeyboardButton(
            "\u23ee", callback_data=f"abook:prev:{info_hash}:{track_idx}"
        ))
    # Play button
    buttons_row.append(InlineKeyboardButton(
        f"\u25b6\ufe0f {track_idx + 1}/{total_tracks}",
        callback_data=f"abook:play:{info_hash}:{track_idx}"
    ))
    # Next button
    if track_idx < total_tracks - 1:
        buttons_row.append(InlineKeyboardButton(
            "\u23ed", callback_data=f"abook:next:{info_hash}:{track_idx}"
        ))
    return InlineKeyboardMarkup([
        buttons_row,
        [InlineKeyboardButton("\U0001f4e5 \u0421\u043a\u0430\u0447\u0430\u0442\u044c \u043e\u0434\u043d\u0438\u043c \u0444\u0430\u0439\u043b\u043e\u043c", callback_data=f"abook:merge:{info_hash}")],
        [InlineKeyboardButton("\U0001f3a7 \u041e\u0442\u043a\u0440\u044b\u0442\u044c \u043f\u043b\u0435\u0435\u0440", url=webapp_url)],
    ])


async def callback_audiobook(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle audiobook selection callback — download and send audio files."""
    query = update.callback_query
    data = query.data

    # ── Play track: abook:play:{hash}:{idx} ──
    if data.startswith("abook:play:"):
        await query.answer("\u23f3 \u0417\u0430\u0433\u0440\u0443\u0436\u0430\u044e \u0433\u043b\u0430\u0432\u044b...")
        parts = data.split(":")
        info_hash = parts[2]
        track_idx = int(parts[3])
        cache = _load_audiobook_cache()
        book = cache.get(info_hash)
        if not book or not book.get('files'):
            await query.answer("\u274c \u041a\u043d\u0438\u0433\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430 \u0432 \u043a\u044d\u0448\u0435", show_alert=True)
            return
        files = book['files']
        if track_idx >= len(files):
            track_idx = 0
        title = book.get('title', '\u0410\u0443\u0434\u0438\u043e\u043a\u043d\u0438\u0433\u0430')
        import tempfile
        import shutil
        tmp_dir = tempfile.mkdtemp(prefix='abook_')
        BATCH_SIZE = 10
        try:
            # Send up to BATCH_SIZE tracks starting from track_idx
            end_idx = min(track_idx + BATCH_SIZE, len(files))
            last_sent_idx = None  # Track the last successfully sent file index
            for i in range(track_idx, end_idx):
                f = files[i]
                s3_key = f.get('key', '')
                filename = f.get('filename', f'track_{i}.mp3')
                file_size = f.get('size', 0)
                local_path = os.path.join(tmp_dir, filename)
                ok = await asyncio.to_thread(_download_from_s3, s3_key, local_path)
                if not ok:
                    logger.warning("abook:play S3 download failed for %s", s3_key)
                    continue
                # Determine if file is too large for send_audio (Telegram limit ~50MB)
                actual_size = os.path.getsize(local_path) if os.path.exists(local_path) else file_size
                is_large = actual_size > TELEGRAM_MAX_AUDIO_SIZE
                try:
                    with open(local_path, 'rb') as audio_file:
                        if is_large:
                            # Send as document for files > 49MB
                            await context.bot.send_document(
                                chat_id=query.message.chat_id,
                                document=audio_file,
                                filename=filename,
                                caption=f"\U0001f3a7 {title[:50]}\n\U0001f4c4 {filename} ({i + 1}/{len(files)})\n\u26a0\ufe0f \u0411\u043e\u043b\u044c\u0448\u043e\u0439 \u0444\u0430\u0439\u043b ({_format_size(actual_size)})",
                            )
                        else:
                            await context.bot.send_audio(
                                chat_id=query.message.chat_id,
                                audio=audio_file,
                                title=filename,
                                performer=title[:40],
                                caption=f"\U0001f3a7 {title[:50]}\n\U0001f4c4 {filename} ({i + 1}/{len(files)})",
                            )
                    last_sent_idx = i
                except Exception as send_err:
                    logger.error("abook:play send error for %s (size=%d): %s", filename, actual_size, send_err)
                    # If send_audio fails (e.g. file too large despite check), try as document
                    if not is_large:
                        try:
                            with open(local_path, 'rb') as audio_file:
                                await context.bot.send_document(
                                    chat_id=query.message.chat_id,
                                    document=audio_file,
                                    filename=filename,
                                    caption=f"\U0001f3a7 {title[:50]}\n\U0001f4c4 {filename} ({i + 1}/{len(files)})",
                                )
                            last_sent_idx = i
                        except Exception as doc_err:
                            logger.error("abook:play document fallback also failed: %s", doc_err)
                # Clean up file after sending
                try:
                    os.remove(local_path)
                except Exception:
                    pass
            # Send "Load more" button after the batch
            if last_sent_idx is not None:
                if end_idx < len(files):
                    next_kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton(
                            f"\u23ed\ufe0f \u0421\u043b\u0435\u0434\u0443\u044e\u0449\u0438\u0435 10 \u0433\u043b\u0430\u0432 ({end_idx + 1}/{len(files)})",
                            callback_data=f"abook:play:{info_hash}:{end_idx}"
                        )]
                    ])
                else:
                    next_kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton("\u2705 \u041a\u043d\u0438\u0433\u0430 \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u0430", callback_data=f"abook:done:{info_hash}")]
                    ])
                # Send the "load more" button as a separate message
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=f"\U0001f4e4 \u041e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e {last_sent_idx - track_idx + 1} \u0438\u0437 {len(files)} \u0433\u043b\u0430\u0432",
                    reply_markup=next_kb,
                )
            elif end_idx < len(files):
                # Nothing sent in this batch (all failed), offer to skip ahead
                skip_kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton(
                        f"\u23ed\ufe0f \u041f\u0440\u043e\u043f\u0443\u0441\u0442\u0438\u0442\u044c \u0438 \u043f\u0440\u043e\u0434\u043e\u043b\u0436\u0438\u0442\u044c ({end_idx + 1}/{len(files)})",
                        callback_data=f"abook:play:{info_hash}:{end_idx}"
                    )]
                ])
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=f"\u26a0\ufe0f \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u044c \u0433\u043b\u0430\u0432\u044b {track_idx+1}-{end_idx}. \u0424\u0430\u0439\u043b\u044b \u0441\u043b\u0438\u0448\u043a\u043e\u043c \u0431\u043e\u043b\u044c\u0448\u0438\u0435.",
                    reply_markup=skip_kb,
                )
            # Update main message keyboard to reflect new position
            if end_idx < len(files):
                webapp_url = f"https://bot.nodkeys.com/audiobook/player?hash={info_hash}"
                kb = _make_player_keyboard(info_hash, end_idx, len(files), webapp_url)
                try:
                    await query.edit_message_reply_markup(reply_markup=kb)
                except Exception:
                    pass
        except Exception as e:
            logger.error("abook:play error: %s", e)
            await query.answer(f"\u274c \u041e\u0448\u0438\u0431\u043a\u0430: {str(e)[:100]}", show_alert=True)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        return

    # ── Merge & send as one file: abook:merge:{hash} ──
    if data.startswith("abook:merge:"):
        await query.answer("\u23f3 \u041e\u0431\u044a\u0435\u0434\u0438\u043d\u044f\u044e \u0444\u0430\u0439\u043b\u044b...")
        info_hash = data.split(":")[2]
        cache = _load_audiobook_cache()
        book = cache.get(info_hash)
        if not book or not book.get('files'):
            await query.answer("\u274c \u041a\u043d\u0438\u0433\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430", show_alert=True)
            return
        files = book['files']
        title = book.get('title', '\u0410\u0443\u0434\u0438\u043e\u043a\u043d\u0438\u0433\u0430')
        import tempfile
        import shutil
        import subprocess
        tmp_dir = tempfile.mkdtemp(prefix='abook_merge_')
        try:
            # Update message to show progress
            try:
                await query.edit_message_text(
                    f"\u23f3 \u041e\u0431\u044a\u0435\u0434\u0438\u043d\u044f\u044e {len(files)} \u0444\u0430\u0439\u043b\u043e\u0432 \u0432 \u043e\u0434\u0438\u043d...\n"
                    f"\U0001f3a7 {title}\n"
                    f"\u2139\ufe0f \u042d\u0442\u043e \u043c\u043e\u0436\u0435\u0442 \u0437\u0430\u043d\u044f\u0442\u044c \u043d\u0435\u0441\u043a\u043e\u043b\u044c\u043a\u043e \u043c\u0438\u043d\u0443\u0442"
                )
            except Exception:
                pass
            # Download all files from S3
            file_list_path = os.path.join(tmp_dir, 'filelist.txt')
            downloaded = []
            for i, f in enumerate(files):
                s3_key = f.get('key', '')
                filename = f.get('filename', f'track_{i}.mp3')
                local_path = os.path.join(tmp_dir, f'{i:04d}_{filename}')
                ok = await asyncio.to_thread(_download_from_s3, s3_key, local_path)
                if ok:
                    downloaded.append(local_path)
            if not downloaded:
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="\u274c \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0441\u043a\u0430\u0447\u0430\u0442\u044c \u0444\u0430\u0439\u043b\u044b"
                )
                return
            # Create ffmpeg concat file
            with open(file_list_path, 'w') as fl:
                for p in downloaded:
                    fl.write(f"file '{p}'\n")
            # Merge with ffmpeg
            safe_title = title.replace('/', '_').replace('\\', '_')[:60]
            output_path = os.path.join(tmp_dir, f'{safe_title}.mp3')
            cmd = ['ffmpeg', '-y', '-f', 'concat', '-safe', '0', '-i', file_list_path, '-c', 'copy', output_path]
            result = await asyncio.to_thread(
                subprocess.run, cmd, capture_output=True, timeout=600
            )
            if result.returncode != 0 or not os.path.exists(output_path):
                logger.error("ffmpeg merge failed: %s", result.stderr[:500] if result.stderr else 'unknown')
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="\u274c \u041e\u0448\u0438\u0431\u043a\u0430 \u043e\u0431\u044a\u0435\u0434\u0438\u043d\u0435\u043d\u0438\u044f \u0444\u0430\u0439\u043b\u043e\u0432"
                )
                return
            # Check file size
            file_size = os.path.getsize(output_path)
            if file_size > 2 * 1024 * 1024 * 1024:  # 2 GB limit
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="\u274c \u0424\u0430\u0439\u043b \u0441\u043b\u0438\u0448\u043a\u043e\u043c \u0431\u043e\u043b\u044c\u0448\u043e\u0439 (>2 GB). \u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0439\u0442\u0435 \u043f\u043b\u0435\u0435\u0440."
                )
                return
            # Send as document
            size_mb = file_size / (1024 * 1024)
            with open(output_path, 'rb') as merged_file:
                await context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=merged_file,
                    filename=f'{safe_title}.mp3',
                    caption=f"\U0001f3a7 {title}\n\U0001f4be {size_mb:.0f} MB | {len(files)} \u0433\u043b\u0430\u0432 \u0432 \u043e\u0434\u043d\u043e\u043c \u0444\u0430\u0439\u043b\u0435",
                )
            # Restore original message
            webapp_url = f"https://bot.nodkeys.com/audiobook/player?hash={info_hash}"
            kb = _make_player_keyboard(info_hash, 0, len(files), webapp_url)
            try:
                await query.edit_message_text(
                    f"\u2705 \u0410\u0443\u0434\u0438\u043e\u043a\u043d\u0438\u0433\u0430 \u0433\u043e\u0442\u043e\u0432\u0430!\n\n"
                    f"\U0001f3a7 {title}\n"
                    f"\U0001f4c4 {len(files)} \u0430\u0443\u0434\u0438\u043e\u0444\u0430\u0439\u043b\u043e\u0432\n\n"
                    f"\u25b6\ufe0f \u041d\u0430\u0436\u043c\u0438\u0442\u0435 Play \u0434\u043b\u044f \u043f\u0440\u043e\u0441\u043b\u0443\u0448\u0438\u0432\u0430\u043d\u0438\u044f \u0432 \u0447\u0430\u0442\u0435",
                    reply_markup=kb,
                )
            except Exception:
                pass
        except Exception as e:
            logger.error("abook:merge error: %s", e)
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"\u274c \u041e\u0448\u0438\u0431\u043a\u0430: {str(e)[:100]}"
            )
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        return

    # ── Next track: abook:next:{hash}:{idx} ──
    if data.startswith("abook:next:"):
        await query.answer()
        parts = data.split(":")
        info_hash = parts[2]
        track_idx = int(parts[3])
        cache = _load_audiobook_cache()
        book = cache.get(info_hash)
        if not book:
            return
        files = book.get('files', [])
        next_idx = min(track_idx + 1, len(files) - 1)
        webapp_url = f"https://bot.nodkeys.com/audiobook/player?hash={info_hash}"
        new_kb = _make_player_keyboard(info_hash, next_idx, len(files), webapp_url)
        try:
            await query.edit_message_reply_markup(reply_markup=new_kb)
        except Exception:
            pass
        return

    # ── Prev track: abook:prev:{hash}:{idx} ──
    if data.startswith("abook:prev:"):
        await query.answer()
        parts = data.split(":")
        info_hash = parts[2]
        track_idx = int(parts[3])
        cache = _load_audiobook_cache()
        book = cache.get(info_hash)
        if not book:
            return
        files = book.get('files', [])
        prev_idx = max(track_idx - 1, 0)
        webapp_url = f"https://bot.nodkeys.com/audiobook/player?hash={info_hash}"
        new_kb = _make_player_keyboard(info_hash, prev_idx, len(files), webapp_url)
        try:
            await query.edit_message_reply_markup(reply_markup=new_kb)
        except Exception:
            pass
        return

    await query.answer()

    if data == "abook:cancel":
        await query.edit_message_text("❌ Поиск отменён")
        context.user_data.pop("audiobook_search_results", None)
        return

    # Handle magnet show callback: abook:mag:{index}
    if data.startswith("abook:mag:"):
        ab_data = context.user_data.get("audiobook_download_data", {})
        magnet = ab_data.get("magnet", "")
        title = ab_data.get("title", "")
        if magnet:
            await query.edit_message_text(
                f"\U0001f3a7 <b>{title[:60]}</b>\n\n"
                f"\U0001f9f2 <b>Magnet-\u0441\u0441\u044b\u043b\u043a\u0430:</b>\n"
                f"<code>{magnet}</code>\n\n"
                f"\U0001f4a1 \u0421\u043a\u043e\u043f\u0438\u0440\u0443\u0439\u0442\u0435 \u0438 \u043e\u0442\u043a\u0440\u043e\u0439\u0442\u0435 \u0432 \u0442\u043e\u0440\u0440\u0435\u043d\u0442-\u043a\u043b\u0438\u0435\u043d\u0442\u0435",
                parse_mode="HTML",
            )
        else:
            await query.edit_message_text("\u274c \u0414\u0430\u043d\u043d\u044b\u0435 \u0443\u0441\u0442\u0430\u0440\u0435\u043b\u0438. \u041f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u0435 \u043f\u043e\u0438\u0441\u043a.")
        return

    # Handle download callback: abook:dl:{info_hash}
    if data.startswith("abook:dl:"):
        info_hash = data.split(":", 2)[2]
        logger.info("abook:dl callback: hash=%s", info_hash)
        ab_data = context.user_data.get("audiobook_download_data", {})
        magnet = ab_data.get("magnet", "")
        title = ab_data.get("title", "Аудиокнига")
        logger.info("abook:dl data: magnet=%s title=%s", bool(magnet), title[:40] if title else "?")
        if not magnet:
            logger.warning("abook:dl: no magnet in user_data, keys=%s", list(ab_data.keys()))
            await query.edit_message_text("❌ Данные устарели. Повторите поиск.")
            return
        # Launch download pipeline in background
        logger.info("abook:dl: launching pipeline for %s", info_hash)
        async def _safe_pipeline():
            try:
                await audiobook_download_pipeline(
                    bot=context.bot,
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id,
                    magnet=magnet,
                    info_hash=info_hash,
                    title=title,
                )
            except Exception as e:
                logger.error("PIPELINE CRASHED: %s", e, exc_info=True)
                try:
                    await context.bot.send_message(
                        chat_id=query.message.chat_id,
                        text=f"❌ <b>Ошибка pipeline</b>\n\n<code>{str(e)[:300]}</code>",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
        asyncio.create_task(_safe_pipeline())
        return

    # Parse callback: abook:{index}:magnet
    parts = data.split(":")
    if len(parts) < 3:
        await query.edit_message_text("❌ Ошибка данных")
        return

    try:
        ab_idx = int(parts[1])
    except (ValueError, IndexError):
        await query.edit_message_text("❌ Ошибка данных")
        return

    results = context.user_data.get("audiobook_search_results", [])
    if ab_idx >= len(results):
        await query.edit_message_text("❌ Аудиокнига не найдена в результатах")
        return

    ab = results[ab_idx]

    await query.edit_message_text(
        f"\u23f3 Получаю ссылку...\n\n"
        f"\U0001f3a7 <b>{ab['title'][:60]}</b>\n"
        f"\U0001f4e6 {ab['size']} | \U0001f331 Seeds: {ab['seeds']}",
        parse_mode="HTML",
    )

    # Get magnet link with fallback to next results
    magnet = None
    selected_ab = ab
    candidates = [ab_idx] + [i for i in range(len(results)) if i != ab_idx][:4]
    for try_idx in candidates:
        candidate = results[try_idx]
        try:
            magnet = await asyncio.to_thread(get_audiobook_magnet, candidate["topic_id"])
        except Exception as e:
            logger.error("Audiobook magnet error for topic %s: %s", candidate["topic_id"], e)
            magnet = None
        if magnet:
            selected_ab = candidate
            if try_idx != ab_idx:
                logger.info("Magnet fallback: original topic %s failed, using topic %s", ab["topic_id"], candidate["topic_id"])
                await query.edit_message_text(
                    f"\u23f3 Получаю ссылку...\n\n"
                    f"\U0001f3a7 <b>{selected_ab['title'][:60]}</b>\n"
                    f"\U0001f4e6 {selected_ab['size']} | \U0001f331 Seeds: {selected_ab['seeds']}\n\n"
                    f"\u2139\ufe0f Использую альтернативную раздачу (у первой нет magnet)",
                    parse_mode="HTML",
                )
            break
    ab = selected_ab

    if magnet:
        info_hash = _extract_info_hash(magnet)
        if not info_hash:
            info_hash = ab.get("topic_id", "unknown")

        # Store download data for the download callback
        context.user_data["audiobook_download_data"] = {
            "magnet": magnet,
            "title": ab["title"],
            "info_hash": info_hash,
        }

        # Check if already cached
        cached = await asyncio.to_thread(_check_s3_cache, info_hash)
        cache_status = "✅ Есть в кэше — отправлю мгновенно!" if cached else "⏳ Скачаю и отправлю"

        text = (
            f"\U0001f3a7 <b>{ab['title'][:60]}</b>\n\n"
            f"\U0001f4e6 Размер: {ab['size']}\n"
            f"\U0001f331 Seeds: {ab['seeds']}\n"
            f"\U0001f4c1 Раздел: {ab['forum']}\n"
            f"\U0001f4c5 Дата: {ab['date']}\n\n"
            f"\U0001f517 <a href=\"{ab['url']}\">Страница раздачи</a>\n\n"
            f"{cache_status}"
        )
        keyboard = [
            [InlineKeyboardButton(
                "\U0001f4e5 Скачать и прослушать",
                callback_data=f"abook:dl:{info_hash}"
            )],
            [InlineKeyboardButton(
                "\U0001f9f2 Показать magnet-ссылку",
                callback_data=f"abook:mag:{ab_idx}"
            )],
        ]
        await query.edit_message_text(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    else:
        # Fallback: send direct link to topic (all candidates failed)
        await query.edit_message_text(
            f"\U0001f3a7 <b>{ab['title'][:60]}</b>\n\n"
            f"\u26a0\ufe0f Magnet-ссылка не найдена (проверено {len(candidates)} раздач)\n\n"
            f"\U0001f517 <a href=\"{ab['url']}\">Открыть на RuTracker</a>\n"
            f"(скачайте .torrent файл вручную)",
            parse_mode="HTML",
        )

    context.user_data.pop("audiobook_search_results", None)


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
        
        if data.get("action") == "skip":
            await thinking.edit_text(
                f"🎤 <b>Распознано:</b>\n<i>{transcribed_text[:300]}</i>\n\n"
                f"🤔 <b>Не создаю запись</b>\n💭 <i>{data.get('reasoning', '')}</i>",
                parse_mode="HTML"
            )
            return
        
        confidence_val = data.get("confidence", 0)
        if confidence_val < 0.4:
            await thinking.edit_text(
                f"🎤 <b>Распознано:</b>\n<i>{transcribed_text[:300]}</i>\n\n"
                f"⚠️ Низкая уверенность ({confidence_val:.0%})",
                parse_mode="HTML"
            )
            return
        
        entry_type = data.get("type", "")
        data = apply_calendar_override(data, user_routing)
        
        # Add category tag for calendar events
        if entry_type in ("event", "task", "reminder"):
            category_key = data.get("category", "personal")
            CATEGORY_TAG_MAP = {
                "personal": "👤 [Личное]",
                "home": "🏠 [Дом]",
                "work": "💼 [Работа]",
                "longterm": "🎯 [Долгосрочные]",
            }
            cat_tag = CATEGORY_TAG_MAP.get(category_key, "👤 [Личное]")
            original_title = data.get("title", "")
            if not any(tag in original_title for tag in ["[Личное]", "[Дом]", "[Работа]", "[Долгосрочные]"]):
                data["title"] = f"{cat_tag} {original_title}"
            
            uid = await asyncio.to_thread(create_calendar_event, data)
            if uid:
                type_map = {"event": "📅 Событие", "task": "✅ Задача", "reminder": "🔔 Напоминание"}
                cal_map = {"work": "💼 Рабочий", "family": "🏠 Семейный", "reminders": "⚠️ Напоминания"}
                response_text = (
                    f"🎤 <b>Голосовое → Календарь!</b>\n\n"
                    f"<b>Текст:</b> <i>{transcribed_text[:200]}</i>\n\n"
                    f"<b>Тип:</b> {type_map.get(entry_type, entry_type)}\n"
                    f"<b>Название:</b> {data.get('title', '?')}\n"
                    f"<b>Дата:</b> {data.get('date', '?')}\n"
                    f"<b>Календарь:</b> {cal_map.get(data.get('calendar', ''), '')}\n"
                    f"\n🗑 <i>Ответьте «удали» чтобы удалить</i>"
                )
                await thinking.edit_text(response_text, parse_mode="HTML")
                _event_store[thinking.message_id] = {"uid": uid, "calendar": data.get("calendar", "reminders")}
                _save_event_store()
            else:
                await thinking.edit_text("❌ Не удалось создать запись в календаре")
        
        elif entry_type == "note":
            title = data.get("title", "Заметка")
            note_content = data.get("content", data.get("description", transcribed_text))
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
        
        elif entry_type == "diary":
            diary_content = data.get("content", transcribed_text)
            success = await asyncio.to_thread(create_diary_entry, diary_content)
            if success:
                await thinking.edit_text(
                    f"🎤📔 <b>Голосовое → Дневник!</b>\n\n{diary_content[:300]}",
                    parse_mode="HTML"
                )
            else:
                await thinking.edit_text("❌ Не удалось добавить в дневник")
        else:
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
def _summarize_youtube(url: str) -> str:
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

    # ═══ Determine response format ═══
    # New format: tool_name/tool_input (Tool Calling from Claude)
    # Old format: action/confidence/type (legacy, should not happen anymore)
    tool_name = data.get("tool_name", "")
    tool_input = data.get("tool_input", {})
    
    if tool_name:
        # New Tool Calling format — skip legacy confidence/action checks
        logger.info("Tool Calling format detected: tool=%s", tool_name)
    else:
        # Legacy format — check action=skip and confidence threshold
        if data.get("action") == "skip":
            reasoning = data.get("reasoning", "Сообщение слишком абстрактное")
            await thinking_msg.edit_text(
                f"🤔 <b>Не создаю запись</b>\n\n"
                f"💭 <i>{reasoning}</i>\n\n"
                f"💡 Попробуйте уточнить: что именно нужно сделать, когда и где.",
                parse_mode="HTML"
            )
            return
        
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
        
        # Streaming-like: show what was understood (legacy format has entry_type)
        entry_type = data.get("type", "")
        type_labels = {
            "event": "📅 Событие", "task": "✅ Задача", "reminder": "🔔 Напоминание",
            "note": "📝 Заметка", "diary": "📖 Дневник", "edit_event": "✏️ Редактирование",
        }
        if entry_type in type_labels:
            progress_text = f"{type_labels[entry_type]}: <b>{data.get('title', '...')}</b>"
            if data.get("date"):
                progress_text += f"\n📅 {data['date']}"
                if entry_type in ("event", "task"):
                    try:
                        overload = await asyncio.to_thread(_check_day_overload, data["date"])
                        if overload.get("is_overloaded"):
                            progress_text += f"\n⚠️ <i>День загружен: {overload['event_count']} событий ({overload['total_hours']}ч)</i>"
                    except Exception:
                        pass
            if data.get("time_start"):
                progress_text += f" в {data['time_start']}"
            try:
                await thinking_msg.edit_text(f"⏳ {progress_text}\n\n<i>Создаю...</i>", parse_mode="HTML")
            except Exception:
                pass

    original_text = update.message.text or ''
    
    logger.info("Dispatching tool: %s", tool_name)
    
    # ── chat_response ──
    if tool_name == "chat_response":
        response_msg = tool_input.get("message", "")
        if response_msg:
            await thinking_msg.edit_text(response_msg, parse_mode="HTML")
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
    
    # ── create_project ──
    if tool_name == "create_project":
        name = tool_input.get("name", "Новый проект")
        description = tool_input.get("description", "")
        tasks = tool_input.get("tasks", [])

        await thinking_msg.edit_text(f"📋 <b>Создаю проект:</b> {name}...", parse_mode="HTML")

        try:
            project = await asyncio.to_thread(_create_project, name, description)

            # Add initial tasks if provided
            added_tasks = []
            for t in tasks:
                title = t.get("title", "")
                priority = t.get("priority", "medium")
                if title:
                    task = await asyncio.to_thread(_add_project_task, project["id"], title, "", priority)
                    added_tasks.append(task)

            text = f"📋 <b>Проект создан!</b>\n\n"
            text += f"🟦 <b>{name}</b>\n"
            if description:
                text += f"<i>{description}</i>\n"
            text += f"\n🆔 ID: <code>{project['id']}</code>\n"

            if added_tasks:
                text += f"\n✅ Добавлено {len(added_tasks)} задач:\n"
                for t in added_tasks:
                    priority_icon = "🔴" if t.get("priority") == "high" else "🟡" if t.get("priority") == "medium" else "🟢"
                    text += f"  {priority_icon} {t['title']}\n"

            text += "\n💡 /projects — посмотреть канбан-доску"

            keyboard = [[InlineKeyboardButton("📋 Открыть доску", callback_data=f"proj:board:{project['id']}")]]
            await thinking_msg.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))
            _add_knowledge_entry(original_text, f"project:{name}", "create_project", f"Project: {name}", "general")
        except Exception as e:
            logger.error("Create project error: %s", e)
            await thinking_msg.edit_text(f"❌ Ошибка: {str(e)[:200]}")
        return

    # ── manage_project ──
    if tool_name == "manage_project":
        action = tool_input.get("action", "list_projects")
        project_name = tool_input.get("project_name", "")
        task_title = tool_input.get("task_title", "")
        new_status = tool_input.get("new_status", "")
        priority = tool_input.get("priority", "medium")

        try:
            data = await asyncio.to_thread(_load_projects)

            if action == "list_projects":
                projects = await asyncio.to_thread(_list_projects)
                if not projects:
                    await thinking_msg.edit_text("📋 Нет активных проектов. Напишите: \"Cоздай проект ...\"", parse_mode="HTML")
                else:
                    text = "📋 <b>Проекты:</b>\n\n"
                    keyboard = []
                    for p in projects:
                        progress = int(p["done"] / p["task_count"] * 100) if p["task_count"] > 0 else 0
                        text += f"{p.get('color', '🟦')} <b>{p['name']}</b> — {progress}% ({p['done']}/{p['task_count']})\n"
                        keyboard.append([InlineKeyboardButton(f"{p.get('color', '🟦')} {p['name']}", callback_data=f"proj:board:{p['id']}")])
                    await thinking_msg.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))
                return

            if action == "show_board":
                # Find project by name
                project = next((p for p in data["projects"] if project_name.lower() in p["name"].lower()), None)
                if not project:
                    await thinking_msg.edit_text(f"❌ Проект '{project_name}' не найден")
                    return
                board = await asyncio.to_thread(_get_project_board, project["id"])
                # Build board text
                text = f"{project.get('color', '🟦')} <b>{project['name']}</b>\n\n"
                for status in ["todo", "in_progress", "done"]:
                    label = PROJECT_STATUS_LABELS[status]
                    col_tasks = board["columns"].get(status, [])
                    text += f"<b>{label}</b> ({len(col_tasks)})\n"
                    for t in col_tasks[:8]:
                        pi = "🔴" if t.get("priority") == "high" else "🟡" if t.get("priority") == "medium" else "🟢"
                        text += f"  {pi} {t['title']}\n"
                    if not col_tasks:
                        text += "  <i>пусто</i>\n"
                    text += "\n"
                await thinking_msg.edit_text(text, parse_mode="HTML")
                return

            if action == "add_task":
                project = next((p for p in data["projects"] if project_name.lower() in p["name"].lower()), None)
                if not project:
                    await thinking_msg.edit_text(f"❌ Проект '{project_name}' не найден")
                    return
                task = await asyncio.to_thread(_add_project_task, project["id"], task_title, "", priority)
                pi = "🔴" if priority == "high" else "🟡" if priority == "medium" else "🟢"
                await thinking_msg.edit_text(
                    f"✅ Задача добавлена в <b>{project['name']}</b>\n\n"
                    f"{pi} {task_title}\n"
                    f"📋 Статус: To Do",
                    parse_mode="HTML"
                )
                return

            if action == "move_task":
                # Find task by title across all projects
                task = next((t for t in data["tasks"] if task_title.lower() in t["title"].lower()), None)
                if not task:
                    await thinking_msg.edit_text(f"❌ Задача '{task_title}' не найдена")
                    return
                task = await asyncio.to_thread(_move_project_task, task["id"], new_status)
                status_label = PROJECT_STATUS_LABELS.get(new_status, new_status)
                await thinking_msg.edit_text(
                    f"✔️ <b>{task['title']}</b> → {status_label}",
                    parse_mode="HTML"
                )
                return

            if action == "delete_task":
                task = next((t for t in data["tasks"] if task_title.lower() in t["title"].lower()), None)
                if not task:
                    await thinking_msg.edit_text(f"❌ Задача '{task_title}' не найдена")
                    return
                await asyncio.to_thread(_delete_project_task, task["id"])
                await thinking_msg.edit_text(f"🗑 Задача <b>{task['title']}</b> удалена", parse_mode="HTML")
                return

            if action == "delete_project":
                project = next((p for p in data["projects"] if project_name.lower() in p["name"].lower()), None)
                if not project:
                    await thinking_msg.edit_text(f"❌ Проект '{project_name}' не найден")
                    return
                await asyncio.to_thread(_delete_project, project["id"])
                await thinking_msg.edit_text(f"🗑 Проект <b>{project['name']}</b> удалён", parse_mode="HTML")
                return

        except Exception as e:
            logger.error("Manage project error: %s", e)
            await thinking_msg.edit_text(f"❌ Ошибка: {str(e)[:200]}")
        return

    # ── search_audiobook ──
    if tool_name == "search_audiobook":
        query = tool_input.get("query", "")
        search_queries = tool_input.get("search_queries", [query])

        await thinking_msg.edit_text(f"\U0001f3a7 <b>Ищу аудиокнигу:</b> {query}...", parse_mode="HTML")

        results = None
        for sq in search_queries:
            results = await asyncio.to_thread(search_rutracker_audiobooks, sq)
            if results:
                break

        if not results:
            await thinking_msg.edit_text(
                f"\U0001f3a7 <b>Аудиокнига не найдена:</b> {query}\n\n"
                f"\U0001f4a1 Попробуйте другое название, автора или проверьте написание.",
                parse_mode="HTML"
            )
            return

        # Store results for callback
        context.user_data["audiobook_search_results"] = results

        text = f"\U0001f3a7 <b>Найдено {len(results)} аудиокниг:</b>\n\n"
        keyboard = []

        for i, ab in enumerate(results[:8]):
            title = ab["title"][:70]
            size = ab["size"]
            seeds = ab["seeds"]

            # Seed indicator
            if seeds >= 50:
                seed_icon = "\U0001f7e2"
            elif seeds >= 10:
                seed_icon = "\U0001f7e1"
            elif seeds >= 1:
                seed_icon = "\U0001f7e0"
            else:
                seed_icon = "\U0001f534"

            text += f"{i+1}. <b>{title}</b>\n"
            text += f"   \U0001f4e6 {size} | {seed_icon} Seeds: {seeds}\n\n"

            keyboard.append([InlineKeyboardButton(
                f"\U0001f3a7 {i+1}. {ab['title'][:30]}",
                callback_data=f"abook:{i}:magnet"
            )])

        keyboard.append([InlineKeyboardButton("\u274c Отмена", callback_data="abook:cancel")])

        await thinking_msg.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        try:
            _add_knowledge_entry(original_text, f"audiobook_search:{query}", "search_audiobook", f"Audiobook: {query}", "general")
        except Exception:
            pass
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
    
    # ── add_income ──
    if tool_name == "add_income":
        amount = tool_input.get("amount", 0)
        category = tool_input.get("category", "other")
        description = tool_input.get("description", "")
        date = tool_input.get("date")

        try:
            entry = await asyncio.to_thread(_add_income, amount, category, description, date)
            cat_label = INCOME_CATEGORIES.get(category, category)
            await thinking_msg.edit_text(
                f"\U0001f4b5 <b>\u0414\u043e\u0445\u043e\u0434 \u0437\u0430\u043f\u0438\u0441\u0430\u043d!</b>\n\n"
                f"{cat_label}: <b>{description}</b>\n"
                f"\U0001f4b0 \u0421\u0443\u043c\u043c\u0430: <b>{amount:,.0f} \u20bd</b>\n"
                f"\U0001f4c5 \u0414\u0430\u0442\u0430: {entry['date']}",
                parse_mode="HTML"
            )
            _add_knowledge_entry(original_text, f"income:{amount}:{category}", "add_income", f"{description}: {amount}RUB", "finance")
        except Exception as e:
            logger.error("Add income error: %s", e)
            await thinking_msg.edit_text(f"\u274c \u041e\u0448\u0438\u0431\u043a\u0430: {str(e)[:200]}")
        return

    # ── set_budget ──
    if tool_name == "set_budget":
        category = tool_input.get("category", "other")
        monthly_limit = tool_input.get("monthly_limit", 0)

        try:
            result = await asyncio.to_thread(_set_budget, category, monthly_limit)
            cat_label = EXPENSE_CATEGORIES.get(category, category)
            await thinking_msg.edit_text(
                f"\U0001f3af <b>\u0411\u044e\u0434\u0436\u0435\u0442 \u0443\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d!</b>\n\n"
                f"{cat_label}: <b>{monthly_limit:,.0f} \u20bd/\u043c\u0435\u0441</b>\n\n"
                f"\U0001f4a1 \u041f\u0440\u0438 \u043f\u0440\u0435\u0432\u044b\u0448\u0435\u043d\u0438\u0438 \u0431\u0443\u0434\u0443 \u043f\u0440\u0435\u0434\u0443\u043f\u0440\u0435\u0436\u0434\u0430\u0442\u044c!",
                parse_mode="HTML"
            )
            _add_knowledge_entry(original_text, f"budget:{category}:{monthly_limit}", "set_budget", f"{cat_label}: {monthly_limit}RUB/month", "finance")
        except Exception as e:
            logger.error("Set budget error: %s", e)
            await thinking_msg.edit_text(f"\u274c \u041e\u0448\u0438\u0431\u043a\u0430: {str(e)[:200]}")
        return

    # ── finance_chart ──
    if tool_name == "finance_chart":
        chart_type = tool_input.get("chart_type", "bar")
        period = tool_input.get("period", "month")

        await thinking_msg.edit_text("\U0001f4ca <b>\u0413\u0435\u043d\u0435\u0440\u0438\u0440\u0443\u044e \u0433\u0440\u0430\u0444\u0438\u043a...</b>", parse_mode="HTML")

        try:
            if chart_type == "pie":
                chart_path = await asyncio.to_thread(_generate_category_pie, period)
            else:
                chart_path = await asyncio.to_thread(_generate_finance_chart, period)

            if chart_path and os.path.exists(chart_path):
                chart_label = "Расходы по категориям" if chart_type == "pie" else "Расходы по дням"
                caption = f"\U0001f4ca {chart_label} ({period})"
                await thinking_msg.delete()
                await update.effective_chat.send_photo(
                    photo=open(chart_path, "rb"),
                    caption=caption,
                )
            else:
                await thinking_msg.edit_text("\u274c \u041d\u0435\u0434\u043e\u0441\u0442\u0430\u0442\u043e\u0447\u043d\u043e \u0434\u0430\u043d\u043d\u044b\u0445 \u0434\u043b\u044f \u0433\u0440\u0430\u0444\u0438\u043a\u0430. \u0414\u043e\u0431\u0430\u0432\u044c\u0442\u0435 \u0440\u0430\u0441\u0445\u043e\u0434\u044b \u0441\u043d\u0430\u0447\u0430\u043b\u0430.")
        except Exception as e:
            logger.error("Finance chart error: %s", e)
            await thinking_msg.edit_text(f"\u274c \u041e\u0448\u0438\u0431\u043a\u0430: {str(e)[:200]}")
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
# ██  PROJECTS & KANBAN (Trello-killer)
# ══════════════════════════════════════════════════════════

PROJECTS_FILE = "/app/data/projects.json"

PROJECT_STATUSES = ["todo", "in_progress", "done", "archived"]
PROJECT_STATUS_LABELS = {
    "todo": "📋 To Do",
    "in_progress": "🔧 In Progress",
    "done": "✅ Done",
    "archived": "📦 Archived",
}
PROJECT_STATUS_EMOJI = {
    "todo": "⬜",
    "in_progress": "🟡",
    "done": "✅",
    "archived": "📦",
}


def _load_projects() -> dict:
    """Load projects data: {projects: [...], tasks: [...]}"""
    try:
        with open(PROJECTS_FILE, "r") as f:
            return json.loads(f.read())
    except (FileNotFoundError, json.JSONDecodeError):
        return {"projects": [], "tasks": []}


def _save_projects(data: dict):
    os.makedirs(os.path.dirname(PROJECTS_FILE), exist_ok=True)
    with open(PROJECTS_FILE, "w") as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=2))


def _create_project(name: str, description: str = "", color: str = "🟦") -> dict:
    """Create a new project."""
    data = _load_projects()
    project = {
        "id": str(uuid.uuid4())[:8],
        "name": name,
        "description": description,
        "color": color,
        "created": datetime.now(TIMEZONE).isoformat(),
        "status": "active",
    }
    data["projects"].append(project)
    _save_projects(data)
    return project


def _add_project_task(project_id: str, title: str, description: str = "", priority: str = "medium") -> dict:
    """Add a task to a project."""
    data = _load_projects()
    
    # Verify project exists
    project = next((p for p in data["projects"] if p["id"] == project_id), None)
    if not project:
        raise ValueError(f"Проект {project_id} не найден")
    
    task = {
        "id": str(uuid.uuid4())[:8],
        "project_id": project_id,
        "title": title,
        "description": description,
        "status": "todo",
        "priority": priority,
        "created": datetime.now(TIMEZONE).isoformat(),
        "updated": datetime.now(TIMEZONE).isoformat(),
    }
    data["tasks"].append(task)
    _save_projects(data)
    return task


def _move_project_task(task_id: str, new_status: str) -> dict:
    """Move a task to a new status column."""
    if new_status not in PROJECT_STATUSES:
        raise ValueError(f"Неверный статус: {new_status}")
    
    data = _load_projects()
    task = next((t for t in data["tasks"] if t["id"] == task_id), None)
    if not task:
        raise ValueError(f"Задача {task_id} не найдена")
    
    task["status"] = new_status
    task["updated"] = datetime.now(TIMEZONE).isoformat()
    _save_projects(data)
    return task


def _get_project_board(project_id: str = None) -> dict:
    """Get kanban board view for a project or all projects."""
    data = _load_projects()
    projects = data["projects"]
    tasks = data["tasks"]
    
    if project_id:
        projects = [p for p in projects if p["id"] == project_id]
        tasks = [t for t in tasks if t["project_id"] == project_id]
    
    board = {
        "projects": projects,
        "columns": {},
        "stats": {"total": len(tasks), "todo": 0, "in_progress": 0, "done": 0},
    }
    
    for status in PROJECT_STATUSES[:3]:  # Skip archived
        col_tasks = [t for t in tasks if t["status"] == status]
        board["columns"][status] = col_tasks
        board["stats"][status] = len(col_tasks)
    
    return board


def _delete_project(project_id: str) -> bool:
    """Delete a project and all its tasks."""
    data = _load_projects()
    data["projects"] = [p for p in data["projects"] if p["id"] != project_id]
    data["tasks"] = [t for t in data["tasks"] if t["project_id"] != project_id]
    _save_projects(data)
    return True


def _delete_project_task(task_id: str) -> bool:
    """Delete a single task."""
    data = _load_projects()
    data["tasks"] = [t for t in data["tasks"] if t["id"] != task_id]
    _save_projects(data)
    return True


def _list_projects() -> list:
    """List all active projects with task counts."""
    data = _load_projects()
    result = []
    for p in data["projects"]:
        if p.get("status") == "archived":
            continue
        tasks = [t for t in data["tasks"] if t["project_id"] == p["id"]]
        todo = len([t for t in tasks if t["status"] == "todo"])
        in_prog = len([t for t in tasks if t["status"] == "in_progress"])
        done = len([t for t in tasks if t["status"] == "done"])
        result.append({
            **p,
            "task_count": len(tasks),
            "todo": todo,
            "in_progress": in_prog,
            "done": done,
        })
    return result


async def cmd_projects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /projects command — show kanban board."""
    if ALLOWED_CHAT_IDS and update.effective_chat.id not in ALLOWED_CHAT_IDS:
        return
    
    projects = await asyncio.to_thread(_list_projects)
    
    if not projects:
        await update.message.reply_text(
            "📋 <b>Проекты</b>\n\n"
            "Нет активных проектов.\n\n"
            "💡 Напишите боту:\n"
            '<i>"Создай проект Ремонт квартиры"</i>',
            parse_mode="HTML"
        )
        return
    
    text = "📋 <b>Проекты:</b>\n\n"
    keyboard = []
    
    for p in projects:
        progress = 0
        if p["task_count"] > 0:
            progress = int(p["done"] / p["task_count"] * 100)
        
        bar_len = 10
        filled = int(progress / 100 * bar_len)
        bar = "█" * filled + "░" * (bar_len - filled)
        
        text += f"{p.get('color', '🟦')} <b>{p['name']}</b>\n"
        text += f"   [{bar}] {progress}%\n"
        text += f"   ⬜ {p['todo']} | 🟡 {p['in_progress']} | ✅ {p['done']}\n\n"
        
        keyboard.append([InlineKeyboardButton(
            f"{p.get('color', '🟦')} {p['name']}",
            callback_data=f"proj:board:{p['id']}"
        )])
    
    await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


async def callback_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle project callback queries."""
    query = update.callback_query
    await query.answer()
    
    data = query.data  # proj:board:{id} or proj:move:{task_id}:{status}
    parts = data.split(":")
    
    if len(parts) < 3:
        return
    
    action = parts[1]
    
    if action == "board":
        project_id = parts[2]
        board = await asyncio.to_thread(_get_project_board, project_id)
        
        if not board["projects"]:
            await query.edit_message_text("❌ Проект не найден")
            return
        
        proj = board["projects"][0]
        text = f"{proj.get('color', '🟦')} <b>{proj['name']}</b>\n"
        if proj.get("description"):
            text += f"<i>{proj['description']}</i>\n"
        text += f"\n──────────────────────────\n"
        
        keyboard = []
        
        for status in ["todo", "in_progress", "done"]:
            label = PROJECT_STATUS_LABELS[status]
            col_tasks = board["columns"].get(status, [])
            text += f"\n<b>{label}</b> ({len(col_tasks)})\n"
            
            if not col_tasks:
                text += "  <i>пусто</i>\n"
            else:
                for t in col_tasks[:5]:
                    priority_icon = "🔴" if t.get("priority") == "high" else "🟡" if t.get("priority") == "medium" else "🟢"
                    text += f"  {priority_icon} {t['title']}\n"
                    
                    # Add move buttons
                    move_buttons = []
                    if status != "todo":
                        prev_status = PROJECT_STATUSES[PROJECT_STATUSES.index(status) - 1]
                        move_buttons.append(InlineKeyboardButton(
                            f"⬅ {t['title'][:15]}",
                            callback_data=f"proj:move:{t['id']}:{prev_status}"
                        ))
                    if status != "done":
                        next_status = PROJECT_STATUSES[PROJECT_STATUSES.index(status) + 1]
                        move_buttons.append(InlineKeyboardButton(
                            f"{t['title'][:15]} ➡",
                            callback_data=f"proj:move:{t['id']}:{next_status}"
                        ))
                    if move_buttons:
                        keyboard.append(move_buttons)
        
        # Stats
        total = board["stats"]["total"]
        done = board["stats"]["done"]
        progress = int(done / total * 100) if total > 0 else 0
        text += f"\n──────────────────────────\n"
        text += f"📊 Прогресс: {done}/{total} ({progress}%)\n"
        
        keyboard.append([InlineKeyboardButton("🔙 Назад к проектам", callback_data="proj:list")])
        
        await query.edit_message_text(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    
    elif action == "move":
        if len(parts) < 4:
            return
        task_id = parts[2]
        new_status = parts[3]
        
        try:
            task = await asyncio.to_thread(_move_project_task, task_id, new_status)
            status_label = PROJECT_STATUS_LABELS.get(new_status, new_status)
            
            # Reload the board
            board = await asyncio.to_thread(_get_project_board, task["project_id"])
            proj = board["projects"][0] if board["projects"] else None
            
            if proj:
                # Rebuild the board view (same as above)
                text = f"{proj.get('color', '🟦')} <b>{proj['name']}</b>\n"
                text += f"\n✔️ <i>{task['title']}</i> → {status_label}\n"
                text += f"\n──────────────────────────\n"
                
                keyboard = []
                for status in ["todo", "in_progress", "done"]:
                    label = PROJECT_STATUS_LABELS[status]
                    col_tasks = board["columns"].get(status, [])
                    text += f"\n<b>{label}</b> ({len(col_tasks)})\n"
                    
                    if not col_tasks:
                        text += "  <i>пусто</i>\n"
                    else:
                        for t in col_tasks[:5]:
                            priority_icon = "🔴" if t.get("priority") == "high" else "🟡" if t.get("priority") == "medium" else "🟢"
                            text += f"  {priority_icon} {t['title']}\n"
                            
                            move_buttons = []
                            if status != "todo":
                                prev_s = PROJECT_STATUSES[PROJECT_STATUSES.index(status) - 1]
                                move_buttons.append(InlineKeyboardButton(
                                    f"⬅ {t['title'][:15]}",
                                    callback_data=f"proj:move:{t['id']}:{prev_s}"
                                ))
                            if status != "done":
                                next_s = PROJECT_STATUSES[PROJECT_STATUSES.index(status) + 1]
                                move_buttons.append(InlineKeyboardButton(
                                    f"{t['title'][:15]} ➡",
                                    callback_data=f"proj:move:{t['id']}:{next_s}"
                                ))
                            if move_buttons:
                                keyboard.append(move_buttons)
                
                total = board["stats"]["total"]
                done_count = board["stats"]["done"]
                progress = int(done_count / total * 100) if total > 0 else 0
                text += f"\n──────────────────────────\n"
                text += f"📊 Прогресс: {done_count}/{total} ({progress}%)\n"
                
                keyboard.append([InlineKeyboardButton("🔙 Назад к проектам", callback_data="proj:list")])
                
                await query.edit_message_text(
                    text,
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                )
            else:
                await query.edit_message_text(f"✔️ Задача перемещена в {status_label}")
        except Exception as e:
            logger.error("Move task error: %s", e)
            await query.edit_message_text(f"❌ Ошибка: {str(e)[:200]}")
    
    elif action == "list":
        # Show all projects list
        projects = await asyncio.to_thread(_list_projects)
        if not projects:
            await query.edit_message_text("📋 Нет активных проектов")
            return
        
        text = "📋 <b>Проекты:</b>\n\n"
        keyboard = []
        for p in projects:
            progress = int(p["done"] / p["task_count"] * 100) if p["task_count"] > 0 else 0
            bar_len = 10
            filled = int(progress / 100 * bar_len)
            bar = "█" * filled + "░" * (bar_len - filled)
            text += f"{p.get('color', '🟦')} <b>{p['name']}</b>\n"
            text += f"   [{bar}] {progress}%\n"
            text += f"   ⬜ {p['todo']} | 🟡 {p['in_progress']} | ✅ {p['done']}\n\n"
            keyboard.append([InlineKeyboardButton(
                f"{p.get('color', '🟦')} {p['name']}",
                callback_data=f"proj:board:{p['id']}"
            )])
        
        await query.edit_message_text(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    
    elif action == "del":
        if len(parts) < 3:
            return
        task_id = parts[2]
        try:
            await asyncio.to_thread(_delete_project_task, task_id)
            await query.edit_message_text("🗑 Задача удалена")
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка: {str(e)[:200]}")


# ══════════════════════════════════════════════════════════
# ██  FINANCE TRACKER (Zenmoney-killer)
# ══════════════════════════════════════════════════════════

FINANCE_FILE = "/app/data/finance.json"

EXPENSE_CATEGORIES = {
    "food": "🍔 Еда",
    "transport": "🚗 Транспорт",
    "housing": "🏠 Жильё",
    "utilities": "💡 Коммуналка",
    "entertainment": "🎬 Развлечения",
    "health": "🏥 Здоровье",
    "clothing": "👕 Одежда",
    "subscriptions": "📱 Подписки",
    "education": "📚 Образование",
    "other": "📦 Другое",
}

INCOME_CATEGORIES = {
    "salary": "💼 Зарплата",
    "freelance": "💻 Фриланс",
    "investment": "📈 Инвестиции",
    "gift": "🎁 Подарок",
    "refund": "🔄 Возврат",
    "other": "💵 Другое",
}


def _load_finance() -> dict:
    """Load finance data."""
    try:
        with open(FINANCE_FILE, "r") as f:
            return json.loads(f.read())
    except (FileNotFoundError, json.JSONDecodeError):
        return {
            "transactions": [],
            "budgets": {},
            "recurring": [],
        }


def _save_finance(data: dict):
    os.makedirs(os.path.dirname(FINANCE_FILE), exist_ok=True)
    with open(FINANCE_FILE, "w") as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=2))


def _add_expense(amount: float, category: str, description: str, date: str = None) -> dict:
    """Add an expense transaction."""
    data = _load_finance()
    entry = {
        "id": str(uuid.uuid4())[:8],
        "type": "expense",
        "amount": amount,
        "category": category,
        "description": description,
        "date": date or datetime.now(TIMEZONE).strftime("%Y-%m-%d"),
        "created": datetime.now(TIMEZONE).isoformat(),
    }
    data["transactions"].append(entry)
    _save_finance(data)
    return entry


def _add_income(amount: float, category: str, description: str, date: str = None) -> dict:
    """Add an income transaction."""
    data = _load_finance()
    entry = {
        "id": str(uuid.uuid4())[:8],
        "type": "income",
        "amount": amount,
        "category": category,
        "description": description,
        "date": date or datetime.now(TIMEZONE).strftime("%Y-%m-%d"),
        "created": datetime.now(TIMEZONE).isoformat(),
    }
    data["transactions"].append(entry)
    _save_finance(data)
    return entry


def _set_budget(category: str, monthly_limit: float) -> dict:
    """Set a monthly budget for a category."""
    data = _load_finance()
    data["budgets"][category] = {
        "limit": monthly_limit,
        "updated": datetime.now(TIMEZONE).isoformat(),
    }
    _save_finance(data)
    return {"category": category, "limit": monthly_limit}


def _get_expense_report(period: str = "month", category: str = None) -> dict:
    """Generate expense report for a given period."""
    data = _load_finance()
    transactions = data["transactions"]
    now = datetime.now(TIMEZONE)

    # Filter by period
    if period == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        period_label = f"за сегодня ({now.strftime('%d.%m.%Y')})"
    elif period == "week":
        start = now - timedelta(days=now.weekday())
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        period_label = "за неделю"
    elif period == "month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        period_label = f"за {now.strftime('%B %Y')}"
    elif period == "year":
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        period_label = f"за {now.year} год"
    else:
        start = now - timedelta(days=30)
        period_label = "за 30 дней"

    start_str = start.strftime("%Y-%m-%d")

    # Filter transactions
    filtered = [
        t for t in transactions
        if t["type"] == "expense" and t["date"] >= start_str
    ]
    if category:
        filtered = [t for t in filtered if t["category"] == category]

    # Calculate totals
    total = sum(t["amount"] for t in filtered)
    by_category = {}
    for t in filtered:
        cat = t["category"]
        by_category[cat] = by_category.get(cat, 0) + t["amount"]

    # Budget check
    budgets = data.get("budgets", {})
    budget_status = {}
    for cat, budget_info in budgets.items():
        limit = budget_info["limit"]
        spent = by_category.get(cat, 0)
        remaining = limit - spent
        pct = (spent / limit * 100) if limit > 0 else 0
        budget_status[cat] = {
            "limit": limit,
            "spent": spent,
            "remaining": remaining,
            "percent": pct,
            "over": remaining < 0,
        }

    # Income for the same period
    income_filtered = [
        t for t in transactions
        if t["type"] == "income" and t["date"] >= start_str
    ]
    total_income = sum(t["amount"] for t in income_filtered)

    return {
        "period": period_label,
        "total": total,
        "total_income": total_income,
        "balance": total_income - total,
        "count": len(filtered),
        "by_category": by_category,
        "budget_status": budget_status,
        "recent": sorted(filtered, key=lambda x: x["date"], reverse=True)[:10],
    }


def _get_finance_chart_data(period: str = "month") -> dict:
    """Get data for finance charts."""
    data = _load_finance()
    transactions = data["transactions"]
    now = datetime.now(TIMEZONE)

    if period == "month":
        # Daily spending for current month
        start = now.replace(day=1)
        days = {}
        for t in transactions:
            if t["type"] == "expense" and t["date"] >= start.strftime("%Y-%m-%d"):
                day = t["date"]
                days[day] = days.get(day, 0) + t["amount"]

        return {"type": "daily", "data": days, "period": now.strftime("%B %Y")}

    elif period == "year":
        # Monthly spending for current year
        months = {}
        for t in transactions:
            if t["type"] == "expense" and t["date"][:4] == str(now.year):
                month = t["date"][:7]  # YYYY-MM
                months[month] = months.get(month, 0) + t["amount"]

        return {"type": "monthly", "data": months, "period": str(now.year)}

    return {"type": "empty", "data": {}}


def _generate_finance_chart(period: str = "month") -> str | None:
    """Generate a finance chart image and return file path."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        chart_data = _get_finance_chart_data(period)

        if not chart_data["data"]:
            return None

        fig, ax = plt.subplots(figsize=(10, 5))

        if chart_data["type"] == "daily":
            dates = sorted(chart_data["data"].keys())
            values = [chart_data["data"][d] for d in dates]
            ax.bar(dates, values, color="#FF6B6B", alpha=0.8)
            ax.set_title(f"Расходы по дням — {chart_data['period']}", fontsize=14, fontweight="bold")
            ax.set_xlabel("Дата")
            plt.xticks(rotation=45, ha="right")

        elif chart_data["type"] == "monthly":
            months = sorted(chart_data["data"].keys())
            values = [chart_data["data"][m] for m in months]
            ax.bar(months, values, color="#4ECDC4", alpha=0.8)
            ax.set_title(f"Расходы по месяцам — {chart_data['period']}", fontsize=14, fontweight="bold")
            ax.set_xlabel("Месяц")

        ax.set_ylabel("Рубли (₽)")
        ax.grid(axis="y", alpha=0.3)
        plt.tight_layout()

        chart_path = "/app/data/finance_chart.png"
        os.makedirs(os.path.dirname(chart_path), exist_ok=True)
        fig.savefig(chart_path, dpi=150, bbox_inches="tight")
        plt.close(fig)

        return chart_path
    except Exception as e:
        logger.error("Finance chart error: %s", e)
        return None


def _generate_category_pie(period: str = "month") -> str | None:
    """Generate a pie chart of expenses by category."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        report = _get_expense_report(period)
        by_cat = report["by_category"]

        if not by_cat:
            return None

        labels = []
        sizes = []
        colors = ["#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4", "#FFEAA7", "#DDA0DD", "#98D8C8", "#F7DC6F", "#BB8FCE", "#85C1E9"]

        sorted_cats = sorted(by_cat.items(), key=lambda x: x[1], reverse=True)
        for cat, amount in sorted_cats:
            cat_label = EXPENSE_CATEGORIES.get(cat, cat)
            labels.append(f"{cat_label}\n{amount:,.0f} ₽")
            sizes.append(amount)

        fig, ax = plt.subplots(figsize=(8, 8))
        wedges, texts, autotexts = ax.pie(
            sizes,
            labels=labels,
            colors=colors[:len(sizes)],
            autopct="%1.0f%%",
            startangle=90,
            textprops={"fontsize": 10},
        )
        ax.set_title(f"Расходы по категориям\nИтого: {report['total']:,.0f} ₽", fontsize=14, fontweight="bold")

        chart_path = "/app/data/finance_pie.png"
        os.makedirs(os.path.dirname(chart_path), exist_ok=True)
        fig.savefig(chart_path, dpi=150, bbox_inches="tight")
        plt.close(fig)

        return chart_path
    except Exception as e:
        logger.error("Finance pie chart error: %s", e)
        return None


def _delete_transaction(transaction_id: str) -> bool:
    """Delete a transaction by ID."""
    data = _load_finance()
    data["transactions"] = [t for t in data["transactions"] if t["id"] != transaction_id]
    _save_finance(data)
    return True


async def cmd_finance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /finance command — show financial dashboard."""
    if ALLOWED_CHAT_IDS and update.effective_chat.id not in ALLOWED_CHAT_IDS:
        return

    args = context.args if context.args else []
    period = args[0] if args else "month"

    await update.message.reply_text("💰 <b>Считаю финансы...</b>", parse_mode="HTML")

    try:
        report = await asyncio.to_thread(_get_expense_report, period)

        text = f"💰 <b>Финансовый отчёт {report['period']}:</b>\n\n"

        # Balance
        balance_icon = "🟢" if report["balance"] >= 0 else "🔴"
        text += f"💵 Доход: <b>{report['total_income']:,.0f} ₽</b>\n"
        text += f"💸 Расход: <b>{report['total']:,.0f} ₽</b>\n"
        text += f"{balance_icon} Баланс: <b>{report['balance']:,.0f} ₽</b>\n\n"

        # Categories
        if report["by_category"]:
            text += "<b>По категориям:</b>\n"
            sorted_cats = sorted(report["by_category"].items(), key=lambda x: x[1], reverse=True)
            for cat, amount in sorted_cats:
                cat_label = EXPENSE_CATEGORIES.get(cat, cat)
                pct = (amount / report["total"] * 100) if report["total"] > 0 else 0
                bar = "█" * max(1, int(pct / 5))
                text += f"  {cat_label}: <b>{amount:,.0f} ₽</b> ({pct:.0f}%) {bar}\n"

        # Budget alerts
        if report["budget_status"]:
            text += "\n<b>🎯 Бюджеты:</b>\n"
            for cat, bs in report["budget_status"].items():
                cat_label = EXPENSE_CATEGORIES.get(cat, cat)
                icon = "🔴" if bs["over"] else "🟡" if bs["percent"] > 80 else "🟢"
                text += f"  {icon} {cat_label}: {bs['spent']:,.0f}/{bs['limit']:,.0f} ₽ ({bs['percent']:.0f}%)\n"
                if bs["over"]:
                    text += f"    ⚠️ Превышение на {abs(bs['remaining']):,.0f} ₽!\n"

        # Recent
        if report["recent"]:
            text += "\n<b>Последние операции:</b>\n"
            for e in report["recent"][:5]:
                cat_label = EXPENSE_CATEGORIES.get(e["category"], "")
                text += f"  {e['date']} {cat_label} {e['description']}: <b>{e['amount']:,.0f} ₽</b>\n"

        keyboard = [
            [InlineKeyboardButton("📊 График расходов", callback_data=f"fin:chart:{period}")],
            [InlineKeyboardButton("🧩 По категориям", callback_data=f"fin:pie:{period}")],
        ]

        await update.message.reply_text(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Exception as e:
        logger.error("Finance command error: %s", e)
        await update.message.reply_text(f"❌ Ошибка: {str(e)[:200]}")


async def callback_finance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle finance callback queries (charts)."""
    query = update.callback_query
    await query.answer()

    data = query.data  # fin:chart:month or fin:pie:month
    parts = data.split(":")
    if len(parts) < 3:
        return

    action = parts[1]
    period = parts[2]

    if action == "chart":
        await query.edit_message_text("📊 <b>Генерирую график...</b>", parse_mode="HTML")
        chart_path = await asyncio.to_thread(_generate_finance_chart, period)
        if chart_path and os.path.exists(chart_path):
            await query.message.reply_photo(
                photo=open(chart_path, "rb"),
                caption=f"📊 Расходы по дням ({period})",
            )
        else:
            await query.edit_message_text("❌ Недостаточно данных для графика")

    elif action == "pie":
        await query.edit_message_text("🧩 <b>Генерирую диаграмму...</b>", parse_mode="HTML")
        chart_path = await asyncio.to_thread(_generate_category_pie, period)
        if chart_path and os.path.exists(chart_path):
            await query.message.reply_photo(
                photo=open(chart_path, "rb"),
                caption=f"🧩 Расходы по категориям ({period})",
            )
        else:
            await query.edit_message_text("❌ Недостаточно данных для диаграммы")


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


def _get_memory_facts() -> list:
    """Get memory facts as a list of strings."""
    mem = _load_memory()
    return mem.get("facts", [])


# ══════════════════════════════════════════════════════════
# ██  Knowledge Base (interaction history)
# ══════════════════════════════════════════════════════════
KNOWLEDGE_FILE = os.path.join(os.path.dirname(__file__), "data", "knowledge.json")


def _load_knowledge() -> list:
    """Load knowledge base: list of interaction entries."""
    try:
        with open(KNOWLEDGE_FILE, "r") as f:
            return json.loads(f.read())
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _save_knowledge(data: list):
    """Save knowledge base to disk."""
    os.makedirs(os.path.dirname(KNOWLEDGE_FILE), exist_ok=True)
    with open(KNOWLEDGE_FILE, "w") as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=2))


def _add_knowledge_entry(user_message: str, key: str, tool_used: str, result: str, category: str = "general"):
    """Add an interaction entry to the knowledge base."""
    try:
        kb = _load_knowledge()
        entry = {
            "timestamp": datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M"),
            "user_message": user_message[:200],
            "key": key,
            "tool_used": tool_used,
            "result": result[:500],
            "category": category,
        }
        kb.append(entry)
        # Keep max 500 entries
        if len(kb) > 500:
            kb = kb[-500:]
        _save_knowledge(kb)
    except Exception as e:
        logger.error("Failed to add knowledge entry: %s", e)


def _search_knowledge(query: str, category: str = "all") -> list:
    """Search knowledge base by query and optional category."""
    kb = _load_knowledge()
    query_lower = query.lower()
    results = []
    for entry in reversed(kb):  # newest first
        if category != "all" and entry.get("category") != category:
            continue
        text = f"{entry.get('user_message', '')} {entry.get('result', '')} {entry.get('key', '')}".lower()
        if query_lower in text:
            results.append(entry)
        if len(results) >= 20:
            break
    return results

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
                "version": VERSION,
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
        elif self.path.startswith('/audiobook/download/'):
            # Audiobook download page — list files from S3 for a given info_hash
            try:
                info_hash = self.path.split('/audiobook/download/')[1].split('/')[0].split('?')[0].strip()
                if not re.match(r'^[a-f0-9]{40}$', info_hash):
                    raise ValueError('Invalid info_hash')
                cache = _load_audiobook_cache()
                entry = cache.get(info_hash)
                if not entry:
                    self.send_response(404)
                    self.send_header('Content-Type', 'text/html; charset=utf-8')
                    self.end_headers()
                    self.wfile.write(b'<html><body style="background:#1a1c2e;color:#e0e0e0;font-family:sans-serif;padding:20px"><h2>Audiobook not found in cache</h2></body></html>')
                    return
                title = entry.get('title', 'Audiobook')
                raw_files = entry.get('files', [])
                files = sorted([f['filename'] if isinstance(f, dict) else f for f in raw_files])
                total_size = entry.get('total_size', 0)
                html = f'''<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>{title}</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #1a1c2e; color: #e0e0e0; margin: 0; padding: 16px; font-size: 14px; }}
h2 {{ color: #4ade80; margin-bottom: 4px; }}
.info {{ color: #94a3b8; margin-bottom: 16px; font-size: 13px; }}
.file {{ display: flex; padding: 6px 10px; border-bottom: 1px solid #2a2d42; align-items: center; }}
.file:hover {{ background: #2a2d42; }}
.num {{ color: #64748b; min-width: 35px; font-size: 12px; }}
.name {{ flex: 1; color: #e0e0e0; text-decoration: none; }}
.name:hover {{ color: #4ade80; }}
a.dl {{ color: #4ade80; text-decoration: none; padding: 3px 10px; border: 1px solid #4ade80; border-radius: 6px; font-size: 12px; margin-left: 8px; }}
a.dl:hover {{ background: #4ade80; color: #1a1c2e; }}
</style></head><body>
<h2>\U0001f3a7 {title}</h2>
<div class="info">\U0001f4c1 {len(files)} files | \U0001f4e6 {_format_size(total_size)}</div>\n'''
                for i, fname in enumerate(files, 1):
                    dl_url = f'/audiobook/file/{info_hash}/{quote(fname)}'
                    html += f'<div class="file"><span class="num">{i}.</span><a class="name" href="{dl_url}">{fname}</a><a class="dl" href="{dl_url}">\u2b07 Download</a></div>\n'
                html += '</body></html>'
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(html.encode())
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.end_headers()
                self.wfile.write(f'<html><body style="background:#1a1c2e;color:#e0e0e0;font-family:sans-serif;padding:20px">Error: {e}</body></html>'.encode())
        elif self.path.startswith('/audiobook/file/'):
            # Serve individual audiobook file from S3
            try:
                parts = self.path.split('/audiobook/file/')[1].split('/', 1)
                info_hash = parts[0]
                from urllib.parse import unquote as _unquote
                filename = _unquote(parts[1]) if len(parts) > 1 else ''
                if not re.match(r'^[a-f0-9]{40}$', info_hash) or not filename:
                    raise ValueError('Invalid path')
                # Prevent path traversal
                if '..' in filename:
                    raise ValueError('Invalid filename')
                s3 = _get_s3_client()
                if not s3:
                    raise RuntimeError('S3 not available')
                # Find real S3 key (files may be in subfolders)
                s3_key = None
                cache = _load_audiobook_cache()
                entry = cache.get(info_hash)
                if entry and entry.get('files'):
                    for f in entry['files']:
                        if isinstance(f, dict) and f.get('filename') == filename:
                            s3_key = f['key']
                            break
                if not s3_key:
                    try:
                        list_resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f'{info_hash}/')
                        for obj in list_resp.get('Contents', []):
                            if obj['Key'].endswith(f'/{filename}'):
                                s3_key = obj['Key']
                                break
                    except Exception:
                        pass
                if not s3_key:
                    raise FileNotFoundError(f'File {filename} not found in S3')
                # Stream from S3
                resp = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
                file_size = resp['ContentLength']
                self.send_response(200)
                content_type = 'audio/mpeg' if filename.lower().endswith('.mp3') else 'application/octet-stream'
                self.send_header('Content-Type', content_type)
                self.send_header('Content-Disposition', f'attachment; filename="{filename}"')
                self.send_header('Content-Length', str(file_size))
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                body = resp['Body']
                while True:
                    chunk = body.read(65536)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                body.close()
            except Exception as e:
                logger.warning('Audiobook file serve error: %s', e)
                try:
                    self.send_response(404)
                    self.send_header('Content-Type', 'text/html; charset=utf-8')
                    self.end_headers()
                    self.wfile.write(f'<html><body style="background:#1a1c2e;color:#e0e0e0;font-family:sans-serif;padding:20px">File not found: {e}</body></html>'.encode())
                except Exception:
                    pass
        elif self.path.startswith('/audiobook/player'):
            # Telegram Mini App — audiobook player
            try:
                html = _AUDIOBOOK_PLAYER_HTML
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Cache-Control', 'public, max-age=3600')
                self.end_headers()
                self.wfile.write(html.encode())
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.end_headers()
                self.wfile.write(f'<html><body>Error: {e}</body></html>'.encode())
        elif self.path.startswith('/audiobook/api/'):
            # JSON API — list audiobook files for Mini App
            try:
                info_hash = self.path.split('/audiobook/api/')[1].split('/')[0].split('?')[0].strip()
                if not re.match(r'^[a-f0-9]{40}$', info_hash):
                    raise ValueError('Invalid info_hash')
                cache = _load_audiobook_cache()
                entry = cache.get(info_hash)
                if not entry:
                    self.send_response(404)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(json.dumps({'error': 'not_found'}).encode())
                    return
                # Build file list with sizes from S3 or cache
                files_list = []
                s3 = _get_s3_client()
                if s3:
                    try:
                        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f'{info_hash}/')
                        for obj in resp.get('Contents', []):
                            fname = obj['Key'].split('/')[-1]
                            ext = os.path.splitext(fname)[1].lower()
                            if ext in AUDIO_EXTENSIONS:
                                files_list.append({'filename': fname, 'size': obj['Size'], 'key': obj['Key']})
                    except Exception:
                        pass
                if not files_list:
                    # Fallback to cache file list
                    for f in entry.get('files', []):
                        if isinstance(f, dict):
                            files_list.append({'filename': f.get('filename', ''), 'size': f.get('size', 0), 'key': f.get('key', '')})
                        else:
                            files_list.append({'filename': f, 'size': 0})
                files_list.sort(key=lambda x: x['filename'])
                data = {
                    'title': entry.get('title', 'Audiobook'),
                    'info_hash': info_hash,
                    'files': files_list,
                    'total_size': entry.get('total_size', sum(f['size'] for f in files_list)),
                    'total_files': len(files_list),
                }
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps(data, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': str(e)}).encode())
        elif self.path.startswith('/audiobook/stream/'):
            # Stream audio file (inline, no attachment header) for in-browser playback
            try:
                parts = self.path.split('/audiobook/stream/')[1].split('/', 1)
                info_hash = parts[0]
                from urllib.parse import unquote as _unquote
                filename = _unquote(parts[1]) if len(parts) > 1 else ''
                if not re.match(r'^[a-f0-9]{40}$', info_hash) or not filename:
                    raise ValueError('Invalid path')
                if '..' in filename:
                    raise ValueError('Invalid filename')
                s3 = _get_s3_client()
                if not s3:
                    raise RuntimeError('S3 not available')
                # Try to find the real S3 key - files may be in subfolders
                s3_key = None
                # First try cache
                cache = _load_audiobook_cache()
                entry = cache.get(info_hash)
                if entry and entry.get('files'):
                    for f in entry['files']:
                        if isinstance(f, dict) and f.get('filename') == filename:
                            s3_key = f['key']
                            break
                        elif isinstance(f, str) and f == filename:
                            # Old format - try with prefix
                            pass
                if not s3_key:
                    # Search S3 for the file
                    try:
                        list_resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f'{info_hash}/')
                        for obj in list_resp.get('Contents', []):
                            if obj['Key'].endswith(f'/{filename}'):
                                s3_key = obj['Key']
                                break
                    except Exception:
                        pass
                if not s3_key:
                    raise FileNotFoundError(f'File {filename} not found in S3')
                resp = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
                file_size = resp['ContentLength']
                ext = os.path.splitext(filename)[1].lower()
                ct_map = {'.mp3': 'audio/mpeg', '.ogg': 'audio/ogg', '.m4a': 'audio/mp4', '.m4b': 'audio/mp4', '.flac': 'audio/flac', '.wav': 'audio/wav'}
                content_type = ct_map.get(ext, 'audio/mpeg')
                self.send_response(200)
                self.send_header('Content-Type', content_type)
                self.send_header('Content-Length', str(file_size))
                self.send_header('Accept-Ranges', 'bytes')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Cache-Control', 'public, max-age=86400')
                self.end_headers()
                body = resp['Body']
                while True:
                    chunk = body.read(65536)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                body.close()
            except Exception as e:
                logger.warning('Audiobook stream error: %s', e)
                try:
                    self.send_response(404)
                    self.send_header('Content-Type', 'text/plain')
                    self.end_headers()
                    self.wfile.write(f'Not found: {e}'.encode())
                except Exception:
                    pass
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
        elif self.path == '/api/shortcuts/list':
            if NEW_MODULES_LOADED:
                shortcuts = get_shortcuts_list()
                self._json_response(200, {'ok': True, 'shortcuts': shortcuts})
            else:
                self._json_response(503, {'ok': False, 'error': 'Shortcuts not available'})
        else:
            self.send_response(404)
            self.end_headers()
      except BrokenPipeError:
          pass  # клиент отключился
      except Exception as e:
          logger.warning("Health handler error: %s", e)
    
    def do_POST(self):
      """Handle POST requests for Shortcuts API."""
      try:
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length) if content_length > 0 else b''
        
        if self.path == '/api/shortcut':
            # Shortcuts API endpoint
            auth_header = self.headers.get('Authorization', '')
            if not auth_header.startswith('Bearer '):
                self._json_response(401, {'ok': False, 'error': 'Authorization required'})
                return
            
            token = auth_header[7:]
            # Verify token
            user_id = None
            if NEW_MODULES_LOADED:
                from user_store import _load_users
                users = _load_users()
                for uid, user in users.items():
                    if user.get('api_token') == token:
                        user_id = int(uid)
                        break
            
            if not user_id:
                self._json_response(401, {'ok': False, 'error': 'Invalid token'})
                return
            
            try:
                data = json.loads(body)
            except Exception:
                self._json_response(400, {'ok': False, 'error': 'Invalid JSON'})
                return
            
            command = data.get('command', '')
            text = data.get('text', '')
            
            if not command or not text:
                self._json_response(400, {'ok': False, 'error': 'Missing command or text'})
                return
            
            # Queue the command for async processing
            self._json_response(202, {
                'ok': True,
                'status': 'processing',
                'message': f'Command {command} queued',
                'user_id': user_id,
                'command': command,
            })
        
        elif self.path == '/api/shortcuts/list':
            if NEW_MODULES_LOADED:
                shortcuts = get_shortcuts_list()
                self._json_response(200, {'ok': True, 'shortcuts': shortcuts})
            else:
                self._json_response(503, {'ok': False, 'error': 'Shortcuts not available'})
        
        else:
            self._json_response(404, {'ok': False, 'error': 'Not found'})
      except BrokenPipeError:
          pass
      except Exception as e:
          logger.warning("POST handler error: %s", e)
    
    def do_OPTIONS(self):
      """Handle CORS preflight."""
      self.send_response(204)
      self.send_header('Access-Control-Allow-Origin', '*')
      self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
      self.send_header('Access-Control-Allow-Headers', 'Authorization, Content-Type')
      self.end_headers()
    
    def _json_response(self, status, data):
      """Send JSON response."""
      self.send_response(status)
      self.send_header('Content-Type', 'application/json')
      self.send_header('Access-Control-Allow-Origin', '*')
      self.end_headers()
      self.wfile.write(json.dumps(data, ensure_ascii=False).encode())
    
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
    app.add_handler(CallbackQueryHandler(callback_audiobook, pattern=r"^abook:"))
    app.add_handler(CommandHandler("projects", cmd_projects))
    app.add_handler(CallbackQueryHandler(callback_project, pattern=r"^proj:"))
    app.add_handler(CommandHandler("finance", cmd_finance))
    app.add_handler(CallbackQueryHandler(callback_finance, pattern=r"^fin:"))
    app.add_handler(CallbackQueryHandler(callback_bookdev, pattern=r"^bookdev:"))
    app.add_handler(CallbackQueryHandler(callback_urlkindle, pattern=r"^urlkindle:"))

    # --- v10.0: Settings, Subscription, Shortcuts handlers ---
    if NEW_MODULES_LOADED:
        app.add_handler(CommandHandler("settings", cmd_settings))
        app.add_handler(CommandHandler("subscribe", cmd_subscribe))
        app.add_handler(CallbackQueryHandler(callback_settings, pattern=r"^(onb:|set:)"))
        app.add_handler(CallbackQueryHandler(callback_subscribe, pattern=r"^sub:"))
        # Payment handlers
        from telegram.ext import PreCheckoutQueryHandler
        app.add_handler(PreCheckoutQueryHandler(precheckout_handler))
        app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_handler))
        # Shortcuts command
        async def cmd_shortcuts(update: Update, context: ContextTypes.DEFAULT_TYPE):
            user_id = update.effective_user.id
            user = get_user(user_id)
            if not user:
                create_user(user_id, update.effective_user.first_name or "User")
                user = get_user(user_id)
            token = user.get('api_token')
            if not token:
                token = generate_api_token(user_id)
                update_user(user_id, api_token=token)
            shortcuts = get_shortcuts_list()
            msg = "\U0001f3af <b>\u0411\u044b\u0441\u0442\u0440\u044b\u0435 \u043a\u043e\u043c\u0430\u043d\u0434\u044b</b>\n\n"
            msg += "\u0414\u043e\u0431\u0430\u0432\u044c\u0442\u0435 \u043a\u043e\u043c\u0430\u043d\u0434\u044b \u043d\u0430 \u0440\u0430\u0431\u043e\u0447\u0438\u0439 \u0441\u0442\u043e\u043b iOS/Android:\n\n"
            for s in shortcuts:
                msg += f"{s['icon']} <b>{s['name']}</b> \u2014 {s['description']}\n"
            msg += f"\n\U0001f511 <b>\u0412\u0430\u0448 API \u0442\u043e\u043a\u0435\u043d:</b>\n<code>{token}</code>\n\n"
            msg += f"\U0001f310 <b>API URL:</b>\n<code>https://bot.nodkeys.com/api/shortcut</code>\n\n"
            msg += "\U0001f4f1 \u041d\u0430\u043f\u0438\u0448\u0438\u0442\u0435 <code>/shortcut \u043d\u0430\u0437\u0432\u0430\u043d\u0438\u0435</code> \u0434\u043b\u044f \u0438\u043d\u0441\u0442\u0440\u0443\u043a\u0446\u0438\u0439 \u043f\u043e \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0435"
            await update.message.reply_text(msg, parse_mode="HTML")
        app.add_handler(CommandHandler("shortcuts", cmd_shortcuts))
        logger.info("v10.0 modules registered: settings, subscribe, shortcuts")

    # --- v10.1: Commercial modules handlers ---
    if V101_MODULES_LOADED:
        # Library commands
        async def cmd_library(update: Update, context: ContextTypes.DEFAULT_TYPE):
            msg, buttons = format_library_message()
            kb = [[InlineKeyboardButton(b["text"], callback_data=b["callback_data"]) for b in row] for row in buttons] if buttons else []
            await update.message.reply_text(msg, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(kb) if kb else None)

        async def callback_library(update: Update, context: ContextTypes.DEFAULT_TYPE):
            query = update.callback_query
            await query.answer()
            data = query.data
            if data == "lib:stats":
                msg = format_stats_message()
                await query.edit_message_text(msg, parse_mode="HTML")
            elif data == "lib:goal":
                context.user_data["awaiting"] = "lib_goal"
                await query.edit_message_text("📚 Введите цель на год (количество книг):")
            elif data.startswith("lib:rate:"):
                book_id = data.split(":")[2]
                context.user_data["awaiting"] = f"lib_rate:{book_id}"
                await query.edit_message_text("⭐ Оцените книгу от 1 до 5:")
            elif data.startswith("lib:note:"):
                book_id = data.split(":")[2]
                context.user_data["awaiting"] = f"lib_note:{book_id}"
                await query.edit_message_text("📝 Введите заметку к книге:")
            elif data.startswith("lib:progress:"):
                book_id = data.split(":")[2]
                context.user_data["awaiting"] = f"lib_progress:{book_id}"
                await query.edit_message_text("📖 Введите текущую страницу или процент (например: 150 или 75%):")
            else:
                msg, buttons = format_library_message()
                kb = [[InlineKeyboardButton(b["text"], callback_data=b["callback_data"]) for b in row] for row in buttons] if buttons else []
                await query.edit_message_text(msg, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(kb) if kb else None)

        # Diary commands
        async def cmd_diary(update: Update, context: ContextTypes.DEFAULT_TYPE):
            msg, buttons = format_diary_overview()
            kb = [[InlineKeyboardButton(b["text"], callback_data=b["callback_data"]) for b in row] for row in buttons] if buttons else []
            await update.message.reply_text(msg, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(kb) if kb else None)

        async def callback_diary(update: Update, context: ContextTypes.DEFAULT_TYPE):
            query = update.callback_query
            await query.answer()
            data = query.data
            if data == "diary:write":
                context.user_data["awaiting"] = "diary_entry"
                prompt = get_evening_prompt()
                await query.edit_message_text(f"🌙 <b>Дневник</b>\n\n{prompt}\n\nНапишите свои мысли:", parse_mode="HTML")
            elif data == "diary:mood":
                kb = [[InlineKeyboardButton(f"{e} {i}", callback_data=f"diary:mood:{i}") for i, e in [(1,"😞"),(2,"😔"),(3,"😐"),(4,"🙂"),(5,"😊")]]]
                await query.edit_message_text("Как ваше настроение сегодня?",
                    reply_markup=InlineKeyboardMarkup(kb))
            elif data.startswith("diary:mood:"):
                mood = int(data.split(":")[2])
                entry = get_today_entry()
                if entry:
                    entry["mood"] = mood
                else:
                    diary_create_entry(mood=mood)
                mood_emojis = {5: "😊", 4: "🙂", 3: "😐", 2: "😔", 1: "😞"}
                await query.edit_message_text(f"{mood_emojis[mood]} Настроение записано!")
            elif data == "diary:stats":
                msg = format_mood_report()
                await query.edit_message_text(msg, parse_mode="HTML")
            else:
                msg, buttons = format_diary_overview()
                kb = [[InlineKeyboardButton(b["text"], callback_data=b["callback_data"]) for b in row] for row in buttons] if buttons else []
                await query.edit_message_text(msg, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(kb) if kb else None)

        # Family commands
        async def cmd_family(update: Update, context: ContextTypes.DEFAULT_TYPE):
            user_id = update.effective_user.id
            msg, buttons = format_family_overview(user_id)
            kb = [[InlineKeyboardButton(b["text"], callback_data=b["callback_data"]) for b in row] for row in buttons] if buttons else []
            await update.message.reply_text(msg, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(kb) if kb else None)

        async def callback_family(update: Update, context: ContextTypes.DEFAULT_TYPE):
            query = update.callback_query
            await query.answer()
            data = query.data
            user_id = update.effective_user.id
            if data == "fam:create":
                name = update.effective_user.first_name or "User"
                create_family(user_id, f"Семья {name}")
                msg, buttons = format_family_overview(user_id)
                kb = [[InlineKeyboardButton(b["text"], callback_data=b["callback_data"]) for b in row] for row in buttons] if buttons else []
                await query.edit_message_text(msg, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(kb) if kb else None)
            elif data == "fam:invite":
                context.user_data["awaiting"] = "fam_invite"
                await query.edit_message_text("👥 Перешлите сообщение от пользователя, которого хотите добавить, или введите его @username:")
            elif data == "fam:list:new":
                context.user_data["awaiting"] = "fam_list_name"
                await query.edit_message_text("📋 Введите название нового списка:")
            elif data.startswith("fam:list:"):
                list_id = data.split(":")[2]
                msg = family_format_list(list_id)
                await query.edit_message_text(msg, parse_mode="HTML")
            elif data == "fam:tasks":
                tasks = get_family_tasks(user_id)
                if tasks:
                    msg = "✅ <b>Семейные задачи:</b>\n\n"
                    for t in tasks[:10]:
                        status = "✅" if t.get("completed") else "⬜"
                        msg += f"{status} {t['title']} (@{t.get('assigned_to', '?')})\n"
                else:
                    msg = "Нет семейных задач."
                await query.edit_message_text(msg, parse_mode="HTML")

        # Podcast commands
        async def cmd_podcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
            args = context.args
            if args:
                query_text = " ".join(args)
                results = await search_podcasts(query_text)
                msg, buttons = podcast_format_results(results)
                kb = [[InlineKeyboardButton(b["text"], callback_data=b["callback_data"]) for b in row] for row in buttons] if buttons else []
                await update.message.reply_text(msg, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(kb) if kb else None)
            else:
                msg, buttons = podcast_format_subs()
                kb = [[InlineKeyboardButton(b["text"], callback_data=b["callback_data"]) for b in row] for row in buttons] if buttons else []
                await update.message.reply_text(msg, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(kb) if kb else None)

        async def callback_podcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
            query = update.callback_query
            await query.answer()
            data = query.data
            if data == "pod:main":
                msg, buttons = podcast_format_subs()
                kb = [[InlineKeyboardButton(b["text"], callback_data=b["callback_data"]) for b in row] for row in buttons] if buttons else []
                await query.edit_message_text(msg, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(kb) if kb else None)
            elif data == "pod:search":
                context.user_data["awaiting"] = "pod_search"
                await query.edit_message_text("🔍 Введите название подкаста:")
            elif data == "pod:check_new":
                new_eps = await check_new_episodes()
                if new_eps:
                    msg = "🆕 <b>Новые эпизоды:</b>\n\n"
                    for ep in new_eps[:5]:
                        msg += f"📻 {ep['podcast']}: {ep['episode']['title']}\n"
                else:
                    msg = "Нет новых эпизодов."
                await query.edit_message_text(msg, parse_mode="HTML")
            elif data.startswith("pod:sub:"):
                pod_id = data.split(":")[2]
                # Store podcast_id for subscription
                context.user_data["pod_subscribe"] = pod_id
                await query.edit_message_text("✅ Подписка оформлена!")
            elif data.startswith("pod:unsub:"):
                pod_id = data.split(":")[2]
                unsubscribe_podcast(pod_id)
                await query.edit_message_text("❌ Подписка отменена.")

        # Voice settings command
        async def cmd_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
            msg, buttons = format_voice_settings_message()
            kb = [[InlineKeyboardButton(b["text"], callback_data=b["callback_data"]) for b in row] for row in buttons] if buttons else []
            await update.message.reply_text(msg, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(kb) if kb else None)

        async def callback_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
            query = update.callback_query
            await query.answer()
            data = query.data
            settings = get_voice_settings()
            if data.startswith("voice:toggle:"):
                key = data.split(":")[2]
                settings[key] = not settings.get(key, False)
                save_voice_settings(settings)
                msg, buttons = format_voice_settings_message()
                kb = [[InlineKeyboardButton(b["text"], callback_data=b["callback_data"]) for b in row] for row in buttons] if buttons else []
                await query.edit_message_text(msg, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(kb) if kb else None)
            elif data == "voice:test":
                test_text = "Привет! Это тестовое сообщение голосового ассистента Nodkeys."
                audio_path = await text_to_speech(test_text)
                if audio_path:
                    await context.bot.send_voice(chat_id=query.message.chat_id, voice=open(audio_path, 'rb'))
                else:
                    await query.edit_message_text("❌ TTS недоступен. Проверьте API ключ.")
            elif data == "voice:change_voice":
                voices = ["alloy", "echo", "fable", "onyx", "nova", "shimmer"]
                current_idx = voices.index(settings.get("voice", "alloy")) if settings.get("voice") in voices else 0
                next_voice = voices[(current_idx + 1) % len(voices)]
                settings["voice"] = next_voice
                save_voice_settings(settings)
                msg, buttons = format_voice_settings_message()
                kb = [[InlineKeyboardButton(b["text"], callback_data=b["callback_data"]) for b in row] for row in buttons] if buttons else []
                await query.edit_message_text(msg, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(kb) if kb else None)
            elif data == "voice:speed":
                speeds = [0.75, 1.0, 1.25, 1.5, 2.0]
                current_speed = settings.get("speed", 1.0)
                current_idx = speeds.index(current_speed) if current_speed in speeds else 1
                next_speed = speeds[(current_idx + 1) % len(speeds)]
                settings["speed"] = next_speed
                save_voice_settings(settings)
                msg, buttons = format_voice_settings_message()
                kb = [[InlineKeyboardButton(b["text"], callback_data=b["callback_data"]) for b in row] for row in buttons] if buttons else []
                await query.edit_message_text(msg, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(kb) if kb else None)

        # Yearly review command
        async def cmd_review(update: Update, context: ContextTypes.DEFAULT_TYPE):
            await update.message.reply_text("🎉 Генерирую годовой обзор...")
            cards = await cmd_yearly_review(ai_call_fn=None)  # TODO: pass AI function
            for card in cards:
                await update.message.reply_text(card, parse_mode="HTML")
                await asyncio.sleep(1)

        # Sync/Notion command
        async def cmd_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
            msg, buttons = format_sync_settings()
            kb = [[InlineKeyboardButton(b["text"], callback_data=b["callback_data"]) for b in row] for row in buttons] if buttons else []
            await update.message.reply_text(msg, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(kb) if kb else None)

        async def callback_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
            query = update.callback_query
            await query.answer()
            data = query.data
            if data == "sync:notion:connect":
                context.user_data["awaiting"] = "notion_api_key"
                await query.edit_message_text("🔗 Введите ваш Notion Integration Token:\n\n"
                    "Создайте интеграцию на https://www.notion.so/my-integrations")
            elif data == "sync:notion:run":
                await query.edit_message_text("🔄 Синхронизация...")
                results = await run_full_sync()
                msg = format_sync_result(results)
                await query.edit_message_text(msg, parse_mode="HTML")
            elif data == "sync:obsidian:export":
                import tempfile, zipfile
                export_dir = os.path.join(tempfile.gettempdir(), "obsidian_export")
                os.makedirs(export_dir, exist_ok=True)
                diary_file = os.path.join(DATA_DIR, "diary.json")
                if os.path.exists(diary_file):
                    with open(diary_file, "r") as f:
                        diary_data = json.load(f)
                    export_diary_to_obsidian(diary_data.get("entries", []), os.path.join(export_dir, "diary"))
                library_file = os.path.join(DATA_DIR, "library.json")
                if os.path.exists(library_file):
                    with open(library_file, "r") as f:
                        lib_data = json.load(f)
                    export_books_to_obsidian(lib_data.get("books", []), os.path.join(export_dir, "books"))
                # Create zip
                zip_path = os.path.join(tempfile.gettempdir(), "nodkeys_obsidian_export.zip")
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for root, dirs, files in os.walk(export_dir):
                        for file in files:
                            filepath = os.path.join(root, file)
                            arcname = os.path.relpath(filepath, export_dir)
                            zf.write(filepath, arcname)
                await context.bot.send_document(chat_id=query.message.chat_id,
                    document=open(zip_path, 'rb'), filename="nodkeys_obsidian_export.zip",
                    caption="📥 Экспорт для Obsidian")

        # Register all v10.1 command handlers
        app.add_handler(CommandHandler("library", cmd_library))
        app.add_handler(CommandHandler("diary", cmd_diary))
        app.add_handler(CommandHandler("family", cmd_family))
        app.add_handler(CommandHandler("podcast", cmd_podcast))
        app.add_handler(CommandHandler("voice", cmd_voice))
        app.add_handler(CommandHandler("review", cmd_review))
        app.add_handler(CommandHandler("sync", cmd_sync))
        # Register callback handlers
        app.add_handler(CallbackQueryHandler(callback_library, pattern=r"^lib:"))
        app.add_handler(CallbackQueryHandler(callback_diary, pattern=r"^diary:"))
        app.add_handler(CallbackQueryHandler(callback_family, pattern=r"^fam:"))
        app.add_handler(CallbackQueryHandler(callback_podcast, pattern=r"^pod:"))
        app.add_handler(CallbackQueryHandler(callback_voice, pattern=r"^voice:"))
        app.add_handler(CallbackQueryHandler(callback_sync, pattern=r"^sync:"))
        logger.info("v10.1 modules registered: library, diary, family, podcast, voice, review, sync")

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
                                        evt_title = first_event['title']
                                        msg += f"📋 <b>К событию \"{evt_title}\":</b>\n<i>{brief[:300]}</i>\n\n"
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
