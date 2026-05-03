"""
Nodkeys Shortcuts API Module
REST API for iOS Shortcuts and Android automation (Tasker/Automate).

Endpoints:
- POST /api/shortcut — execute a command via API
- GET /api/shortcuts/list — get available shortcuts
- GET /api/shortcuts/install/{name} — get iOS Shortcut install link

Authentication: Bearer token (generated per user, stored in user profile)

iOS Shortcuts integration:
- User gets a unique URL + token
- Shortcut sends POST request with command text
- Bot processes it and returns result

Available shortcuts:
- add_task — add task/event to calendar
- add_note — add a note
- send_kindle — send URL/text to Kindle
- find_book — search for a book
- finance — log expense
- remind — set reminder
"""

import json
import logging
from typing import Optional
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from user_store import get_user, check_limit
from crypto import verify_api_token

logger = logging.getLogger(__name__)

# Base URL for shortcuts API
SHORTCUTS_BASE_URL = "https://bot.nodkeys.com/api"

# Available shortcuts definitions
SHORTCUTS = {
    "add_task": {
        "name": "Добавить задачу",
        "description": "Добавить событие или задачу в календарь",
        "icon": "📅",
        "input_label": "Что добавить?",
        "example": "Встреча с врачом завтра в 14:00",
    },
    "add_note": {
        "name": "Заметка",
        "description": "Создать заметку",
        "icon": "📝",
        "input_label": "Текст заметки",
        "example": "Идея: сделать приложение для трекинга привычек",
    },
    "send_kindle": {
        "name": "На Kindle",
        "description": "Отправить ссылку или текст на Kindle",
        "icon": "📖",
        "input_label": "URL или текст",
        "example": "https://habr.com/ru/articles/123456/",
    },
    "find_book": {
        "name": "Найти книгу",
        "description": "Поиск книги",
        "icon": "🔍",
        "input_label": "Название книги",
        "example": "Мастер и Маргарита",
    },
    "find_audiobook": {
        "name": "Аудиокнига",
        "description": "Найти и скачать аудиокнигу",
        "icon": "🎧",
        "input_label": "Название аудиокниги",
        "example": "Война и мир",
    },
    "finance": {
        "name": "Расход",
        "description": "Записать расход",
        "icon": "💰",
        "input_label": "Сумма и категория",
        "example": "500 еда обед",
    },
    "remind": {
        "name": "Напоминание",
        "description": "Установить напоминание",
        "icon": "⏰",
        "input_label": "О чём напомнить?",
        "example": "Позвонить маме в 18:00",
    },
    "diary": {
        "name": "Дневник",
        "description": "Запись в дневник",
        "icon": "📓",
        "input_label": "Что произошло?",
        "example": "Сегодня отличный день, закончил проект",
    },
}


def get_shortcuts_list() -> list:
    """Get list of available shortcuts for display."""
    return [
        {
            "id": sid,
            "name": s["name"],
            "description": s["description"],
            "icon": s["icon"],
            "input_label": s["input_label"],
            "example": s["example"],
        }
        for sid, s in SHORTCUTS.items()
    ]


def generate_ios_shortcut_url(user_id: int, token: str, shortcut_id: str) -> str:
    """Generate iOS Shortcut install URL.
    
    Creates a URL that opens Shortcuts app with pre-configured shortcut.
    Uses shortcuts:// URL scheme.
    """
    # iOS Shortcuts can be shared via iCloud links
    # For now, return instructions URL
    return f"{SHORTCUTS_BASE_URL}/shortcuts/install/{shortcut_id}?token={token}"


def generate_shortcut_instructions(user_id: int, token: str, shortcut_id: str) -> dict:
    """Generate instructions for setting up a shortcut on iOS/Android."""
    shortcut = SHORTCUTS.get(shortcut_id, {})
    
    return {
        "shortcut": shortcut,
        "api_url": f"{SHORTCUTS_BASE_URL}/shortcut",
        "method": "POST",
        "headers": {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        "body": {
            "command": shortcut_id,
            "text": "{{input}}",
        },
        "ios_steps": [
            "1. Откройте приложение 'Команды' (Shortcuts)",
            "2. Нажмите '+' для создания новой команды",
            f"3. Добавьте действие 'Запросить ввод' с подсказкой: '{shortcut.get('input_label', 'Текст')}'",
            "4. Добавьте действие 'Получить содержимое URL'",
            f"5. URL: {SHORTCUTS_BASE_URL}/shortcut",
            "6. Метод: POST",
            f"7. Заголовки: Authorization = Bearer {token}",
            "8. Тело (JSON): {\"command\": \"" + shortcut_id + "\", \"text\": \"Предоставленный ввод\"}",
            "9. Добавьте действие 'Показать результат'",
            f"10. Назовите команду: '{shortcut.get('name', shortcut_id)}'",
        ],
        "android_steps": [
            "1. Установите Tasker или HTTP Shortcuts из Play Store",
            "2. Создайте новый HTTP Shortcut",
            f"3. URL: {SHORTCUTS_BASE_URL}/shortcut",
            "4. Метод: POST",
            f"5. Header: Authorization = Bearer {token}",
            "6. Body (JSON): {\"command\": \"" + shortcut_id + "\", \"text\": \"{{input}}\"}",
            f"7. Назовите: '{shortcut.get('name', shortcut_id)}'",
            "8. Добавьте виджет на рабочий стол",
        ],
    }


async def handle_shortcut_request(user_id: int, command: str, text: str, bot_process_func) -> dict:
    """Process a shortcut API request.
    
    Args:
        user_id: authenticated user ID
        command: shortcut command name
        text: user input text
        bot_process_func: async function to process the message through bot logic
    
    Returns:
        dict with result
    """
    if command not in SHORTCUTS:
        return {"ok": False, "error": f"Unknown command: {command}"}
    
    # Check subscription limits
    if not check_limit(user_id, "shortcuts"):
        return {"ok": False, "error": "Shortcuts require Pro subscription. Use /subscribe"}
    
    # Map command to bot-understandable text
    prefix_map = {
        "add_task": "",
        "add_note": "заметка: ",
        "send_kindle": "отправь на kindle ",
        "find_book": "найди книгу ",
        "find_audiobook": "найди аудиокнигу ",
        "finance": "расход ",
        "remind": "напомни ",
        "diary": "дневник: ",
    }
    
    full_text = prefix_map.get(command, "") + text
    
    # Process through bot logic
    try:
        result = await bot_process_func(user_id, full_text)
        return {"ok": True, "result": result}
    except Exception as e:
        logger.error("Shortcut error: user=%d cmd=%s err=%s", user_id, command, e)
        return {"ok": False, "error": str(e)}


class ShortcutsAPIHandler:
    """Mixin for HTTP handler to process shortcuts API requests."""
    
    @staticmethod
    def handle_api_request(path: str, method: str, body: bytes, headers: dict) -> tuple:
        """Process API request and return (status_code, response_dict).
        
        Returns:
            (status_code: int, response: dict)
        """
        parsed = urlparse(path)
        route = parsed.path
        
        # GET /api/shortcuts/list
        if route == "/api/shortcuts/list" and method == "GET":
            return (200, {"ok": True, "shortcuts": get_shortcuts_list()})
        
        # GET /api/shortcuts/install/{id}
        if route.startswith("/api/shortcuts/install/") and method == "GET":
            shortcut_id = route.split("/")[-1]
            # Need auth
            token = _extract_token(headers)
            if not token:
                return (401, {"ok": False, "error": "Authorization required"})
            
            user_id = _verify_token(token)
            if not user_id:
                return (401, {"ok": False, "error": "Invalid token"})
            
            instructions = generate_shortcut_instructions(user_id, token, shortcut_id)
            return (200, {"ok": True, **instructions})
        
        # POST /api/shortcut
        if route == "/api/shortcut" and method == "POST":
            token = _extract_token(headers)
            if not token:
                return (401, {"ok": False, "error": "Authorization required"})
            
            user_id = _verify_token(token)
            if not user_id:
                return (401, {"ok": False, "error": "Invalid token"})
            
            try:
                data = json.loads(body)
            except Exception:
                return (400, {"ok": False, "error": "Invalid JSON"})
            
            command = data.get("command", "")
            text = data.get("text", "")
            
            if not command or not text:
                return (400, {"ok": False, "error": "Missing 'command' or 'text'"})
            
            # Return pending — actual processing is async
            return (202, {
                "ok": True, 
                "status": "processing",
                "message": f"Command '{command}' queued for processing",
                "user_id": user_id,
                "command": command,
                "text": text,
            })
        
        return (404, {"ok": False, "error": "Not found"})


def _extract_token(headers: dict) -> Optional[str]:
    """Extract Bearer token from headers."""
    auth = headers.get("Authorization", headers.get("authorization", ""))
    if auth.startswith("Bearer "):
        return auth[7:]
    return None


def _verify_token(token: str) -> Optional[int]:
    """Verify token and return user_id if valid."""
    # Load all users and check tokens
    from user_store import _load_users
    users = _load_users()
    for uid, user in users.items():
        if user.get("api_token") == token:
            return int(uid)
    return None
