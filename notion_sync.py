"""
notion_sync.py — Notion/Obsidian Sync for Nodkeys Bot v10.1

Features:
- Export diary entries to Notion database
- Export book notes to Notion
- Sync tasks/reminders to Notion
- Export data as Obsidian-compatible Markdown
- Webhook for Notion → Bot sync
- Configurable sync schedule
"""

import os
import json
import asyncio
import logging
import urllib.request
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

TIMEZONE = ZoneInfo(os.environ.get("TIMEZONE", "Europe/Moscow"))
DATA_DIR = os.environ.get("DATA_DIR", "/app/data")
SYNC_FILE = os.path.join(DATA_DIR, "notion_sync.json")

NOTION_API_URL = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"


# ─── Storage ───────────────────────────────────────────────────────────────

def _load_sync_config() -> dict:
    """Load sync configuration."""
    try:
        if os.path.exists(SYNC_FILE):
            with open(SYNC_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error("Failed to load sync config: %s", e)
    return {
        "notion": {
            "enabled": False,
            "api_key": "",  # Encrypted via crypto.py
            "databases": {
                "diary": "",  # Database ID for diary entries
                "books": "",  # Database ID for book library
                "tasks": ""   # Database ID for tasks
            },
            "last_sync": None,
            "auto_sync": False,
            "sync_interval_hours": 24
        },
        "obsidian": {
            "enabled": False,
            "vault_path": "",  # For local export
            "export_format": "markdown",
            "last_export": None
        },
        "sync_history": []
    }


def _save_sync_config(data: dict):
    """Save sync configuration."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(SYNC_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─── Notion API ────────────────────────────────────────────────────────────

async def _notion_request(method: str, endpoint: str, api_key: str, 
                          body: dict = None) -> dict:
    """Make a request to Notion API."""
    url = f"{NOTION_API_URL}/{endpoint}"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }
    
    def _fetch():
        data = json.dumps(body).encode() if body else None
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            error_body = e.read().decode() if e.fp else ""
            logger.error("Notion API error %s: %s", e.code, error_body[:200])
            return {"error": True, "status": e.code, "message": error_body[:200]}
    
    return await asyncio.to_thread(_fetch)


async def verify_notion_connection(api_key: str) -> dict:
    """Verify Notion API key is valid."""
    result = await _notion_request("GET", "users/me", api_key)
    if result.get("error"):
        return {"valid": False, "error": result.get("message", "Unknown error")}
    return {"valid": True, "user": result.get("name", ""), "type": result.get("type", "")}


async def list_notion_databases(api_key: str) -> list:
    """List available Notion databases."""
    body = {
        "filter": {"property": "object", "value": "database"},
        "page_size": 20
    }
    result = await _notion_request("POST", "search", api_key, body)
    
    if result.get("error"):
        return []
    
    databases = []
    for item in result.get("results", []):
        if item.get("object") == "database":
            title_parts = item.get("title", [])
            title = "".join(t.get("plain_text", "") for t in title_parts) or "Untitled"
            databases.append({
                "id": item["id"],
                "title": title,
                "url": item.get("url", "")
            })
    
    return databases


# ─── Diary → Notion Sync ──────────────────────────────────────────────────

async def sync_diary_to_notion(entries: list, api_key: str, database_id: str) -> int:
    """Sync diary entries to Notion database."""
    synced = 0
    
    for entry in entries:
        # Create page in Notion database
        body = {
            "parent": {"database_id": database_id},
            "properties": {
                "Дата": {
                    "date": {"start": entry.get("date", "")}
                },
                "Настроение": {
                    "number": entry.get("mood", 0)
                },
                "Название": {
                    "title": [{"text": {"content": f"Дневник {entry.get('date', '')}"}}]
                }
            },
            "children": [
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{"type": "text", "text": {"content": entry.get("text", "")[:2000]}}]
                    }
                }
            ]
        }
        
        # Add gratitude block if present
        if entry.get("gratitude"):
            body["children"].append({
                "object": "block",
                "type": "callout",
                "callout": {
                    "icon": {"emoji": "🙏"},
                    "rich_text": [{"type": "text", "text": {"content": entry["gratitude"]}}]
                }
            })
        
        result = await _notion_request("POST", "pages", api_key, body)
        if not result.get("error"):
            synced += 1
        
        await asyncio.sleep(0.5)  # Rate limiting
    
    return synced


# ─── Books → Notion Sync ──────────────────────────────────────────────────

async def sync_books_to_notion(books: list, api_key: str, database_id: str) -> int:
    """Sync book library to Notion database."""
    synced = 0
    
    for book in books:
        status_map = {
            "to_read": "Хочу прочитать",
            "reading": "Читаю",
            "finished": "Прочитано",
            "abandoned": "Брошено"
        }
        
        body = {
            "parent": {"database_id": database_id},
            "properties": {
                "Название": {
                    "title": [{"text": {"content": book.get("title", "")}}]
                },
                "Автор": {
                    "rich_text": [{"text": {"content": book.get("author", "")}}]
                },
                "Статус": {
                    "select": {"name": status_map.get(book.get("status", ""), "Хочу прочитать")}
                },
                "Прогресс": {
                    "number": book.get("progress_pct", 0)
                }
            }
        }
        
        if book.get("rating"):
            body["properties"]["Оценка"] = {"number": book["rating"]}
        
        # Add notes as page content
        if book.get("notes"):
            body["children"] = []
            for note in book["notes"]:
                body["children"].append({
                    "object": "block",
                    "type": "quote",
                    "quote": {
                        "rich_text": [{"type": "text", "text": {"content": note.get("text", "")[:2000]}}]
                    }
                })
        
        result = await _notion_request("POST", "pages", api_key, body)
        if not result.get("error"):
            synced += 1
        
        await asyncio.sleep(0.5)
    
    return synced


# ─── Obsidian Export ───────────────────────────────────────────────────────

def export_diary_to_obsidian(entries: list, output_dir: str) -> int:
    """Export diary entries as Obsidian-compatible Markdown files."""
    os.makedirs(output_dir, exist_ok=True)
    exported = 0
    
    for entry in entries:
        date = entry.get("date", "unknown")
        filename = f"{date}.md"
        filepath = os.path.join(output_dir, filename)
        
        mood_emojis = {5: "😊", 4: "🙂", 3: "😐", 2: "😔", 1: "😞"}
        mood = entry.get("mood", 0)
        mood_str = mood_emojis.get(mood, "❓") if mood else ""
        
        content = f"---\n"
        content += f"date: {date}\n"
        content += f"mood: {mood}\n"
        content += f"tags: [diary"
        for tag in entry.get("tags", []):
            content += f", {tag}"
        content += "]\n"
        content += f"---\n\n"
        content += f"# Дневник — {date} {mood_str}\n\n"
        content += entry.get("text", "") + "\n"
        
        if entry.get("gratitude"):
            content += f"\n## 🙏 Благодарность\n\n{entry['gratitude']}\n"
        
        if entry.get("question"):
            content += f"\n> 💭 {entry['question']}\n"
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)
        exported += 1
    
    return exported


def export_books_to_obsidian(books: list, output_dir: str) -> int:
    """Export book library as Obsidian-compatible Markdown."""
    os.makedirs(output_dir, exist_ok=True)
    exported = 0
    
    for book in books:
        safe_title = "".join(c for c in book.get("title", "book") if c.isalnum() or c in " -_")[:60]
        filename = f"{safe_title}.md"
        filepath = os.path.join(output_dir, filename)
        
        status_map = {
            "to_read": "📋 Хочу прочитать",
            "reading": "📖 Читаю",
            "finished": "✅ Прочитано",
            "abandoned": "❌ Брошено"
        }
        
        content = f"---\n"
        content += f"title: \"{book.get('title', '')}\"\n"
        content += f"author: \"{book.get('author', '')}\"\n"
        content += f"status: {book.get('status', 'to_read')}\n"
        content += f"rating: {book.get('rating', 0)}\n"
        content += f"progress: {book.get('progress_pct', 0)}\n"
        content += f"tags: [book, {book.get('genre', 'other')}]\n"
        content += f"---\n\n"
        content += f"# {book.get('title', '')}\n\n"
        content += f"**Автор:** {book.get('author', 'Неизвестен')}\n"
        content += f"**Статус:** {status_map.get(book.get('status', ''), '?')}\n"
        content += f"**Прогресс:** {book.get('progress_pct', 0)}%\n"
        
        if book.get("rating"):
            content += f"**Оценка:** {'⭐' * book['rating']}\n"
        
        notes = book.get("notes", [])
        if notes:
            content += f"\n## Заметки\n\n"
            for note in notes:
                page_str = f" (стр. {note['page']})" if note.get("page") else ""
                content += f"> {note['text']}{page_str}\n\n"
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)
        exported += 1
    
    return exported


# ─── Full Sync ─────────────────────────────────────────────────────────────

async def run_full_sync(api_key: str = None) -> dict:
    """Run full sync to Notion."""
    config = _load_sync_config()
    
    if not config["notion"]["enabled"]:
        return {"error": "Notion sync not enabled"}
    
    api_key = api_key or config["notion"]["api_key"]
    if not api_key:
        return {"error": "No API key configured"}
    
    results = {"diary": 0, "books": 0, "tasks": 0, "errors": []}
    
    # Sync diary
    diary_db = config["notion"]["databases"].get("diary")
    if diary_db:
        try:
            diary_file = os.path.join(DATA_DIR, "diary.json")
            if os.path.exists(diary_file):
                with open(diary_file, "r") as f:
                    diary_data = json.load(f)
                
                # Only sync entries since last sync
                last_sync = config["notion"].get("last_sync")
                entries = diary_data.get("entries", [])
                if last_sync:
                    entries = [e for e in entries if e.get("date", "") > last_sync[:10]]
                
                results["diary"] = await sync_diary_to_notion(entries[-10:], api_key, diary_db)
        except Exception as e:
            results["errors"].append(f"Diary sync error: {e}")
    
    # Sync books
    books_db = config["notion"]["databases"].get("books")
    if books_db:
        try:
            library_file = os.path.join(DATA_DIR, "library.json")
            if os.path.exists(library_file):
                with open(library_file, "r") as f:
                    lib_data = json.load(f)
                results["books"] = await sync_books_to_notion(
                    lib_data.get("books", [])[-20:], api_key, books_db
                )
        except Exception as e:
            results["errors"].append(f"Books sync error: {e}")
    
    # Update last sync time
    config["notion"]["last_sync"] = datetime.now(TIMEZONE).isoformat()
    config["sync_history"].append({
        "timestamp": config["notion"]["last_sync"],
        "results": results
    })
    config["sync_history"] = config["sync_history"][-50:]
    _save_sync_config(config)
    
    return results


# ─── Telegram Formatting ──────────────────────────────────────────────────

def format_sync_settings() -> tuple:
    """Format sync settings message."""
    config = _load_sync_config()
    notion = config["notion"]
    obsidian = config["obsidian"]
    
    msg = "🔄 <b>Синхронизация</b>\n\n"
    
    # Notion
    notion_status = "✅ Подключено" if notion["enabled"] and notion["api_key"] else "❌ Не настроено"
    msg += f"<b>Notion:</b> {notion_status}\n"
    if notion["enabled"]:
        msg += f"  📝 Дневник: {'✅' if notion['databases'].get('diary') else '❌'}\n"
        msg += f"  📚 Книги: {'✅' if notion['databases'].get('books') else '❌'}\n"
        msg += f"  ✅ Задачи: {'✅' if notion['databases'].get('tasks') else '❌'}\n"
        if notion.get("last_sync"):
            msg += f"  🕐 Последняя синхр.: {notion['last_sync'][:16]}\n"
        msg += f"  🔄 Авто-синхр.: {'Вкл' if notion.get('auto_sync') else 'Выкл'}\n"
    msg += "\n"
    
    # Obsidian
    obsidian_status = "✅ Настроено" if obsidian["enabled"] else "❌ Не настроено"
    msg += f"<b>Obsidian:</b> {obsidian_status}\n"
    if obsidian["enabled"] and obsidian.get("last_export"):
        msg += f"  🕐 Последний экспорт: {obsidian['last_export'][:16]}\n"
    
    # Buttons
    buttons = [
        [{"text": "🔗 Подключить Notion", "callback_data": "sync:notion:connect"}],
        [{"text": "🔄 Синхронизировать", "callback_data": "sync:notion:run"},
         {"text": "⚙️ Настройки", "callback_data": "sync:notion:settings"}],
        [{"text": "📥 Экспорт в Obsidian", "callback_data": "sync:obsidian:export"}],
        [{"text": "🔙 Назад", "callback_data": "set:main"}]
    ]
    
    return msg, buttons


def format_sync_result(results: dict) -> str:
    """Format sync results message."""
    msg = "🔄 <b>Результаты синхронизации:</b>\n\n"
    
    if results.get("error"):
        msg += f"❌ {results['error']}\n"
        return msg
    
    msg += f"📝 Дневник: {results.get('diary', 0)} записей\n"
    msg += f"📚 Книги: {results.get('books', 0)} книг\n"
    msg += f"✅ Задачи: {results.get('tasks', 0)} задач\n"
    
    if results.get("errors"):
        msg += "\n⚠️ <b>Ошибки:</b>\n"
        for err in results["errors"]:
            msg += f"  • {err}\n"
    else:
        msg += "\n✅ Всё синхронизировано успешно!"
    
    return msg
