"""
diary.py — AI-Powered Evening Diary for Nodkeys Bot v10.1

Features:
- Evening prompts (21:00) with thoughtful questions
- Mood tracking and analysis
- Gratitude journaling
- Weekly/monthly mood reports
- AI-generated insights about patterns
- Integration with Apple Notes for storage
"""

import os
import json
import asyncio
import logging
import random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

TIMEZONE = ZoneInfo(os.environ.get("TIMEZONE", "Europe/Moscow"))
DATA_DIR = os.environ.get("DATA_DIR", "/app/data")
DIARY_FILE = os.path.join(DATA_DIR, "diary.json")


# ─── Evening Questions ─────────────────────────────────────────────────────

EVENING_QUESTIONS = [
    # Reflective
    "Что было самым значимым событием сегодня?",
    "Чему ты научился сегодня?",
    "Что бы ты сделал по-другому, если бы день начался заново?",
    "Какой момент сегодня принёс тебе радость?",
    "С какой трудностью ты столкнулся и как справился?",
    # Gratitude
    "За что ты благодарен сегодня?",
    "Кто сделал твой день лучше?",
    "Какая мелочь порадовала тебя сегодня?",
    # Forward-looking
    "Что ты хочешь сделать завтра в первую очередь?",
    "Какую одну вещь ты хочешь улучшить завтра?",
    # Emotional
    "Как бы ты описал свой день одним словом?",
    "Что заняло больше всего энергии сегодня?",
    "Был ли момент, когда ты чувствовал себя по-настоящему спокойно?",
    # Creative
    "Если бы сегодняшний день был цветом, какой бы это был цвет?",
    "Какую песню ты бы выбрал саундтреком к сегодняшнему дню?",
]

MOOD_OPTIONS = [
    ("😊", "Отлично", 5),
    ("🙂", "Хорошо", 4),
    ("😐", "Нормально", 3),
    ("😔", "Так себе", 2),
    ("😞", "Плохо", 1),
]


# ─── Storage ───────────────────────────────────────────────────────────────

def _load_diary() -> dict:
    """Load diary database."""
    try:
        if os.path.exists(DIARY_FILE):
            with open(DIARY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error("Failed to load diary: %s", e)
    return {
        "entries": [],
        "mood_log": {},
        "streaks": {"current": 0, "longest": 0},
        "settings": {
            "enabled": True,
            "time": "21:00",
            "questions_per_day": 2,
            "include_gratitude": True
        }
    }


def _save_diary(data: dict):
    """Save diary database."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(DIARY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─── Diary Entry Management ───────────────────────────────────────────────

def create_entry(text: str, mood: int = None, gratitude: str = None,
                 question: str = None) -> dict:
    """Create a new diary entry."""
    data = _load_diary()
    now = datetime.now(TIMEZONE)
    today = now.strftime("%Y-%m-%d")
    
    entry = {
        "id": f"diary_{now.strftime('%Y%m%d_%H%M%S')}",
        "date": today,
        "time": now.strftime("%H:%M"),
        "text": text,
        "mood": mood,
        "gratitude": gratitude,
        "question": question,
        "tags": _extract_tags(text),
        "word_count": len(text.split())
    }
    
    data["entries"].append(entry)
    
    # Update mood log
    if mood:
        data["mood_log"][today] = mood
    
    # Update streak
    _update_streak(data, today)
    
    _save_diary(data)
    return entry


def get_today_entry() -> dict:
    """Get today's diary entry if exists."""
    data = _load_diary()
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    
    for entry in reversed(data["entries"]):
        if entry["date"] == today:
            return entry
    return None


def get_entries(days: int = 7) -> list:
    """Get diary entries for the last N days."""
    data = _load_diary()
    cutoff = (datetime.now(TIMEZONE) - timedelta(days=days)).strftime("%Y-%m-%d")
    return [e for e in data["entries"] if e["date"] >= cutoff]


def _extract_tags(text: str) -> list:
    """Extract hashtags and keywords from diary text."""
    tags = []
    # Extract #hashtags
    import re
    hashtags = re.findall(r'#(\w+)', text)
    tags.extend(hashtags)
    
    # Auto-detect mood keywords
    positive_words = ["радость", "счастье", "успех", "достижение", "благодарен", "отлично"]
    negative_words = ["стресс", "усталость", "тревога", "грусть", "разочарование"]
    
    text_lower = text.lower()
    for w in positive_words:
        if w in text_lower:
            tags.append("позитив")
            break
    for w in negative_words:
        if w in text_lower:
            tags.append("негатив")
            break
    
    return list(set(tags))


def _update_streak(data: dict, today: str):
    """Update diary writing streak."""
    yesterday = (datetime.now(TIMEZONE) - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Check if wrote yesterday
    has_yesterday = any(e["date"] == yesterday for e in data["entries"])
    has_today = any(e["date"] == today for e in data["entries"][:-1])  # Exclude current
    
    if has_yesterday or has_today:
        data["streaks"]["current"] += 1
    else:
        data["streaks"]["current"] = 1
    
    data["streaks"]["longest"] = max(data["streaks"]["longest"], data["streaks"]["current"])


# ─── Evening Prompt ────────────────────────────────────────────────────────

def get_evening_prompt() -> tuple:
    """Generate evening diary prompt with questions and mood selector."""
    data = _load_diary()
    settings = data.get("settings", {})
    
    # Select questions not asked recently
    recent_questions = set()
    for entry in data["entries"][-14:]:
        if entry.get("question"):
            recent_questions.add(entry["question"])
    
    available = [q for q in EVENING_QUESTIONS if q not in recent_questions]
    if not available:
        available = EVENING_QUESTIONS
    
    num_questions = settings.get("questions_per_day", 2)
    selected = random.sample(available, min(num_questions, len(available)))
    
    # Build message
    now = datetime.now(TIMEZONE)
    streak = data["streaks"]["current"]
    
    msg = "🌙 <b>Вечерний дневник</b>\n\n"
    
    if streak > 1:
        msg += f"🔥 Серия: {streak} дней подряд!\n\n"
    
    msg += "Как прошёл твой день? Ответь на вопросы или просто напиши свои мысли.\n\n"
    
    for i, q in enumerate(selected, 1):
        msg += f"💭 <i>{q}</i>\n\n"
    
    if settings.get("include_gratitude", True):
        msg += "🙏 <i>За что ты благодарен сегодня?</i>\n\n"
    
    msg += "Выбери настроение и напиши ответ:"
    
    # Mood buttons
    buttons = []
    mood_row = []
    for emoji, label, value in MOOD_OPTIONS:
        mood_row.append({"text": f"{emoji}", "callback_data": f"diary:mood:{value}"})
    buttons.append(mood_row)
    buttons.append([{"text": "⏭ Пропустить сегодня", "callback_data": "diary:skip"}])
    
    return msg, buttons, selected


# ─── Mood Analysis ─────────────────────────────────────────────────────────

def get_mood_stats(days: int = 30) -> dict:
    """Get mood statistics for the last N days."""
    data = _load_diary()
    now = datetime.now(TIMEZONE)
    
    moods = []
    for i in range(days):
        d = (now - timedelta(days=i)).strftime("%Y-%m-%d")
        mood = data["mood_log"].get(d)
        if mood:
            moods.append({"date": d, "mood": mood})
    
    if not moods:
        return {"avg": 0, "trend": "neutral", "best_day": None, "worst_day": None}
    
    avg = sum(m["mood"] for m in moods) / len(moods)
    
    # Trend: compare last 7 days vs previous 7 days
    recent = [m["mood"] for m in moods[:7]]
    previous = [m["mood"] for m in moods[7:14]]
    
    if recent and previous:
        recent_avg = sum(recent) / len(recent)
        prev_avg = sum(previous) / len(previous)
        if recent_avg > prev_avg + 0.3:
            trend = "improving"
        elif recent_avg < prev_avg - 0.3:
            trend = "declining"
        else:
            trend = "stable"
    else:
        trend = "neutral"
    
    # Best/worst days of week
    weekday_moods = {}
    for m in moods:
        d = datetime.strptime(m["date"], "%Y-%m-%d")
        wd = d.weekday()
        weekday_moods.setdefault(wd, []).append(m["mood"])
    
    weekday_avgs = {wd: sum(ms)/len(ms) for wd, ms in weekday_moods.items()}
    best_day = max(weekday_avgs, key=weekday_avgs.get) if weekday_avgs else None
    worst_day = min(weekday_avgs, key=weekday_avgs.get) if weekday_avgs else None
    
    weekday_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    
    return {
        "avg": avg,
        "trend": trend,
        "best_day": weekday_names[best_day] if best_day is not None else None,
        "worst_day": weekday_names[worst_day] if worst_day is not None else None,
        "total_entries": len(moods),
        "moods": moods[:30],
        "weekday_avgs": {weekday_names[k]: v for k, v in weekday_avgs.items()}
    }


# ─── AI Insights ──────────────────────────────────────────────────────────

DIARY_INSIGHTS_PROMPT = """Analyze these diary entries and provide insights about the person's well-being and patterns.

Entries (last 2 weeks):
{entries}

Mood data:
{mood_data}

Provide a brief, empathetic analysis in Russian (3-5 sentences):
1. Overall emotional state
2. Patterns noticed (what makes good/bad days)
3. One gentle suggestion for improvement

Be warm, supportive, and non-judgmental. Keep it concise.
"""


async def get_diary_insights(ai_call_fn) -> str:
    """Get AI-generated insights from diary entries."""
    entries = get_entries(days=14)
    mood_stats = get_mood_stats(days=14)
    
    if len(entries) < 3:
        return "📝 Пока недостаточно записей для анализа. Продолжайте вести дневник!"
    
    entries_text = "\n".join(
        f"[{e['date']}] Mood:{e.get('mood', '?')} — {e['text'][:200]}"
        for e in entries
    )
    
    prompt = DIARY_INSIGHTS_PROMPT.format(
        entries=entries_text[:2000],
        mood_data=json.dumps(mood_stats, ensure_ascii=False)[:500]
    )
    
    try:
        response = await ai_call_fn(prompt)
        return response.strip()
    except Exception as e:
        logger.warning("Failed to get diary insights: %s", e)
        return "Не удалось получить анализ. Попробуйте позже."


# ─── Telegram Formatting ──────────────────────────────────────────────────

def format_diary_overview() -> tuple:
    """Format diary overview message."""
    data = _load_diary()
    stats = get_mood_stats(days=30)
    streak = data["streaks"]
    
    msg = "🌙 <b>Мой дневник</b>\n\n"
    
    # Streak
    if streak["current"] > 0:
        msg += f"🔥 <b>Серия:</b> {streak['current']} дн. (рекорд: {streak['longest']})\n\n"
    
    # Mood chart (last 7 days)
    mood_emojis = {5: "😊", 4: "🙂", 3: "😐", 2: "😔", 1: "😞"}
    recent_moods = stats.get("moods", [])[:7]
    if recent_moods:
        msg += "<b>Настроение (7 дней):</b>\n"
        for m in reversed(recent_moods):
            emoji = mood_emojis.get(m["mood"], "❓")
            day = datetime.strptime(m["date"], "%Y-%m-%d").strftime("%a %d.%m")
            bar = "█" * m["mood"] + "░" * (5 - m["mood"])
            msg += f"  {day}: {emoji} [{bar}]\n"
        msg += "\n"
    
    # Trend
    trend_text = {"improving": "📈 Улучшается", "declining": "📉 Снижается", 
                  "stable": "➡️ Стабильно", "neutral": "❓ Мало данных"}
    msg += f"<b>Тренд:</b> {trend_text.get(stats['trend'], '❓')}\n"
    
    if stats["avg"] > 0:
        msg += f"<b>Средний балл:</b> {stats['avg']:.1f}/5\n"
    if stats.get("best_day"):
        msg += f"<b>Лучший день:</b> {stats['best_day']}\n"
    
    msg += f"\n📝 <b>Записей за месяц:</b> {stats['total_entries']}\n"
    
    # Buttons
    buttons = [
        [{"text": "✍️ Написать", "callback_data": "diary:write"},
         {"text": "📊 Статистика", "callback_data": "diary:stats"}],
        [{"text": "🧠 AI-анализ", "callback_data": "diary:insights"},
         {"text": "📖 Записи", "callback_data": "diary:entries:0"}],
        [{"text": "⚙️ Настройки", "callback_data": "diary:settings"}]
    ]
    
    return msg, buttons


def format_mood_report(days: int = 30) -> str:
    """Format detailed mood report."""
    stats = get_mood_stats(days)
    
    msg = f"📊 <b>Отчёт о настроении ({days} дней)</b>\n\n"
    
    mood_emojis = {5: "😊", 4: "🙂", 3: "😐", 2: "😔", 1: "😞"}
    
    if stats["avg"] > 0:
        avg_emoji = mood_emojis.get(round(stats["avg"]), "❓")
        msg += f"<b>Средний балл:</b> {avg_emoji} {stats['avg']:.1f}/5\n\n"
    
    # Distribution
    if stats.get("moods"):
        distribution = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        for m in stats["moods"]:
            distribution[m["mood"]] = distribution.get(m["mood"], 0) + 1
        
        total = sum(distribution.values())
        msg += "<b>Распределение:</b>\n"
        for score in range(5, 0, -1):
            count = distribution[score]
            pct = int(count / total * 100) if total > 0 else 0
            bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
            msg += f"  {mood_emojis[score]} [{bar}] {pct}%\n"
        msg += "\n"
    
    # Weekly pattern
    if stats.get("weekday_avgs"):
        msg += "<b>По дням недели:</b>\n"
        for day, avg in sorted(stats["weekday_avgs"].items()):
            bar_len = int(avg * 4)
            msg += f"  {day}: {'█' * bar_len}{'░' * (20 - bar_len)} {avg:.1f}\n"
        msg += "\n"
    
    # Trend
    trend_text = {
        "improving": "📈 Настроение улучшается — отличная динамика!",
        "declining": "📉 Настроение снижается — обратите внимание на себя",
        "stable": "➡️ Настроение стабильно",
        "neutral": "❓ Недостаточно данных для определения тренда"
    }
    msg += f"\n<b>Тренд:</b> {trend_text.get(stats['trend'], '')}\n"
    
    return msg


# ─── Settings ──────────────────────────────────────────────────────────────

def get_diary_settings() -> dict:
    """Get diary settings."""
    data = _load_diary()
    return data.get("settings", {})


def update_diary_settings(**kwargs) -> dict:
    """Update diary settings."""
    data = _load_diary()
    for key, value in kwargs.items():
        if key in data["settings"]:
            data["settings"][key] = value
    _save_diary(data)
    return data["settings"]
