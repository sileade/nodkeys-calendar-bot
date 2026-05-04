"""
smart_reminders.py — AI-powered Smart Reminders for Nodkeys Bot v10.1

Analyzes photos, documents, and messages to automatically suggest reminders:
- Medical prescriptions → medication schedule
- Tickets → departure/event reminders
- Receipts → warranty expiry reminders
- Conversations → follow-up reminders
- Deadlines mentioned in text → proactive alerts
"""

import os
import json
import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

TIMEZONE = ZoneInfo(os.environ.get("TIMEZONE", "Europe/Moscow"))
DATA_DIR = os.environ.get("DATA_DIR", "/app/data")
SMART_REMINDERS_FILE = os.path.join(DATA_DIR, "smart_reminders.json")


# ─── Storage ───────────────────────────────────────────────────────────────

def _load_smart_reminders() -> dict:
    """Load smart reminders database."""
    try:
        if os.path.exists(SMART_REMINDERS_FILE):
            with open(SMART_REMINDERS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error("Failed to load smart reminders: %s", e)
    return {"reminders": [], "suggestions": [], "patterns": []}


def _save_smart_reminders(data: dict):
    """Save smart reminders database."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(SMART_REMINDERS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─── AI Analysis ──────────────────────────────────────────────────────────

REMINDER_EXTRACTION_PROMPT = """Analyze this content and extract any actionable reminders or deadlines.

Content type: {content_type}
Content: {content}

Return a JSON array of suggested reminders. Each reminder should have:
- "title": short reminder text (in Russian)
- "due_date": ISO date (YYYY-MM-DD) when to remind
- "due_time": time (HH:MM) or null
- "category": one of "medical", "travel", "warranty", "deadline", "followup", "payment", "other"
- "priority": "high", "medium", "low"
- "recurring": null or object {"interval": "daily|weekly|monthly", "count": number}
- "context": brief explanation why this reminder was suggested

Today is {today}. If relative dates are mentioned (e.g., "через неделю", "в пятницу"), 
calculate the absolute date. For medical prescriptions, create recurring reminders.

Return ONLY valid JSON array, no other text. If no reminders found, return [].
"""

PROACTIVE_ANALYSIS_PROMPT = """Based on the user's recent activity patterns, suggest proactive reminders.

Recent events: {events}
Recent habits completion: {habits}
Pending tasks: {tasks}
Current date: {today}
Day of week: {weekday}

Suggest 1-3 proactive reminders that would be helpful. Consider:
- Tasks that are approaching deadline
- Habits that haven't been done today
- Upcoming events that need preparation
- Patterns (e.g., user always forgets X on Mondays)

Return JSON array with same format as above. Return [] if nothing useful to suggest.
"""


async def analyze_content_for_reminders(content: str, content_type: str, ai_call_fn) -> list:
    """
    Use AI to analyze content and extract potential reminders.
    
    Args:
        content: Text content to analyze (OCR text from photo, document text, message)
        content_type: Type of content (photo, document, message, url)
        ai_call_fn: Async function to call AI (takes prompt, returns text)
    
    Returns:
        List of suggested reminder dicts
    """
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    prompt = REMINDER_EXTRACTION_PROMPT.format(
        content_type=content_type,
        content=content[:3000],  # Limit content length
        today=today
    )
    
    try:
        response = await ai_call_fn(prompt)
        # Parse JSON from response
        response = response.strip()
        if response.startswith("```"):
            response = response.split("\n", 1)[1].rsplit("```", 1)[0]
        reminders = json.loads(response)
        if isinstance(reminders, list):
            return reminders
    except (json.JSONDecodeError, Exception) as e:
        logger.warning("Failed to parse AI reminder suggestions: %s", e)
    
    return []


async def generate_proactive_suggestions(events: list, habits: dict, tasks: list, ai_call_fn) -> list:
    """
    Generate proactive reminder suggestions based on user patterns.
    
    Called periodically (e.g., every 4 hours) to suggest helpful reminders.
    """
    today = datetime.now(TIMEZONE)
    weekday_names = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    
    prompt = PROACTIVE_ANALYSIS_PROMPT.format(
        events=json.dumps(events[:10], ensure_ascii=False),
        habits=json.dumps(habits, ensure_ascii=False)[:500],
        tasks=json.dumps(tasks[:10], ensure_ascii=False),
        today=today.strftime("%Y-%m-%d"),
        weekday=weekday_names[today.weekday()]
    )
    
    try:
        response = await ai_call_fn(prompt)
        response = response.strip()
        if response.startswith("```"):
            response = response.split("\n", 1)[1].rsplit("```", 1)[0]
        suggestions = json.loads(response)
        if isinstance(suggestions, list):
            return suggestions[:3]
    except (json.JSONDecodeError, Exception) as e:
        logger.warning("Failed to parse proactive suggestions: %s", e)
    
    return []


# ─── Suggestion Management ─────────────────────────────────────────────────

def add_suggestion(title: str, due_date: str, due_time: str = None,
                   category: str = "other", priority: str = "medium",
                   recurring: dict = None, context: str = "", source: str = "ai"):
    """Add a smart reminder suggestion (pending user approval)."""
    data = _load_smart_reminders()
    suggestion = {
        "id": f"sr_{datetime.now().strftime('%Y%m%d%H%M%S')}_{len(data['suggestions'])}",
        "title": title,
        "due_date": due_date,
        "due_time": due_time,
        "category": category,
        "priority": priority,
        "recurring": recurring,
        "context": context,
        "source": source,
        "created": datetime.now(TIMEZONE).isoformat(),
        "status": "pending"  # pending, accepted, dismissed
    }
    data["suggestions"].append(suggestion)
    _save_smart_reminders(data)
    return suggestion


def accept_suggestion(suggestion_id: str) -> dict:
    """Accept a suggestion and convert it to an active reminder."""
    data = _load_smart_reminders()
    for s in data["suggestions"]:
        if s["id"] == suggestion_id:
            s["status"] = "accepted"
            # Move to active reminders
            data["reminders"].append({
                **s,
                "status": "active",
                "accepted_at": datetime.now(TIMEZONE).isoformat()
            })
            _save_smart_reminders(data)
            return s
    return None


def dismiss_suggestion(suggestion_id: str) -> bool:
    """Dismiss a suggestion."""
    data = _load_smart_reminders()
    for s in data["suggestions"]:
        if s["id"] == suggestion_id:
            s["status"] = "dismissed"
            _save_smart_reminders(data)
            return True
    return False


def get_pending_suggestions() -> list:
    """Get all pending suggestions."""
    data = _load_smart_reminders()
    return [s for s in data["suggestions"] if s["status"] == "pending"]


def get_active_smart_reminders() -> list:
    """Get all active smart reminders."""
    data = _load_smart_reminders()
    return [r for r in data["reminders"] if r["status"] == "active"]


# ─── Pattern Learning ──────────────────────────────────────────────────────

def record_pattern(pattern_type: str, data_point: dict):
    """Record a user behavior pattern for future proactive suggestions."""
    data = _load_smart_reminders()
    data.setdefault("patterns", []).append({
        "type": pattern_type,
        "data": data_point,
        "timestamp": datetime.now(TIMEZONE).isoformat()
    })
    # Keep only last 100 patterns
    data["patterns"] = data["patterns"][-100:]
    _save_smart_reminders(data)


# ─── Telegram Handlers ─────────────────────────────────────────────────────

def format_suggestion_message(suggestions: list) -> tuple:
    """Format suggestions for Telegram message with inline buttons."""
    if not suggestions:
        return None, None
    
    category_emoji = {
        "medical": "🏥", "travel": "✈️", "warranty": "🛡️",
        "deadline": "⏰", "followup": "📞", "payment": "💰", "other": "📌"
    }
    priority_emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}
    
    msg = "💡 <b>Умные напоминания — предложения:</b>\n\n"
    buttons = []
    
    for i, s in enumerate(suggestions[:5]):
        cat_e = category_emoji.get(s.get("category", "other"), "📌")
        pri_e = priority_emoji.get(s.get("priority", "medium"), "🟡")
        time_str = f" в {s['due_time']}" if s.get("due_time") else ""
        
        msg += f"{pri_e} {cat_e} <b>{s['title']}</b>\n"
        msg += f"   📅 {s['due_date']}{time_str}\n"
        if s.get("context"):
            msg += f"   <i>{s['context']}</i>\n"
        if s.get("recurring"):
            interval = s["recurring"].get("interval", "")
            count = s["recurring"].get("count", "∞")
            msg += f"   🔄 Повтор: {interval} (×{count})\n"
        msg += "\n"
        
        sid = s.get("id", f"sr_{i}")
        buttons.append([
            {"text": f"✅ Принять", "callback_data": f"srem:accept:{sid}"},
            {"text": f"❌ Отклонить", "callback_data": f"srem:dismiss:{sid}"}
        ])
    
    buttons.append([{"text": "✅ Принять все", "callback_data": "srem:accept_all"}])
    
    return msg, buttons


async def process_photo_for_reminders(ocr_text: str, ai_call_fn) -> list:
    """Process OCR text from a photo and suggest reminders."""
    if not ocr_text or len(ocr_text) < 10:
        return []
    
    suggestions = await analyze_content_for_reminders(ocr_text, "photo/document", ai_call_fn)
    
    # Save suggestions
    saved = []
    for s in suggestions:
        saved.append(add_suggestion(
            title=s.get("title", "Напоминание"),
            due_date=s.get("due_date", ""),
            due_time=s.get("due_time"),
            category=s.get("category", "other"),
            priority=s.get("priority", "medium"),
            recurring=s.get("recurring"),
            context=s.get("context", "Из фото/документа"),
            source="photo"
        ))
    
    return saved


async def process_message_for_reminders(message_text: str, ai_call_fn) -> list:
    """Analyze a message for potential reminders (called selectively)."""
    # Only process messages that likely contain actionable info
    trigger_words = [
        "завтра", "послезавтра", "через", "до", "дедлайн", "deadline",
        "не забыть", "напомни", "нужно", "надо", "записаться", "позвонить",
        "оплатить", "купить", "забрать", "отправить", "сдать"
    ]
    
    if not any(w in message_text.lower() for w in trigger_words):
        return []
    
    suggestions = await analyze_content_for_reminders(message_text, "message", ai_call_fn)
    
    saved = []
    for s in suggestions:
        saved.append(add_suggestion(
            title=s.get("title", "Напоминание"),
            due_date=s.get("due_date", ""),
            due_time=s.get("due_time"),
            category=s.get("category", "other"),
            priority=s.get("priority", "medium"),
            recurring=s.get("recurring"),
            context=s.get("context", "Из сообщения"),
            source="message"
        ))
    
    return saved
