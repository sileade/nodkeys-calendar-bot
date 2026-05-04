"""
yearly_review.py — Yearly Review (Spotify Wrapped-style) for Nodkeys Bot v10.1

Generates a beautiful year-in-review summary:
- Total events, tasks, habits tracked
- Reading/listening stats
- Mood patterns over the year
- Top achievements
- Funny stats (most active hour, busiest day, etc.)
- AI-generated personal insights
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


# ─── Data Collection ──────────────────────────────────────────────────────

def _collect_year_data(year: int = None) -> dict:
    """Collect all data for the yearly review."""
    if year is None:
        year = datetime.now(TIMEZONE).year
    
    year_start = f"{year}-01-01"
    year_end = f"{year}-12-31"
    
    result = {
        "year": year,
        "events": {"total": 0, "by_month": {}, "busiest_day": None},
        "habits": {"total_checks": 0, "longest_streak": 0, "completion_rate": 0},
        "books": {"read": 0, "pages": 0, "favorite": None, "audiobooks": 0},
        "diary": {"entries": 0, "avg_mood": 0, "best_month": None, "total_words": 0},
        "reminders": {"created": 0, "completed": 0, "completion_rate": 0},
        "finance": {"total_tracked": 0},
        "fun_stats": {},
        "ai_insights": ""
    }
    
    # Load habits data
    habits_file = os.path.join(DATA_DIR, "habits.json")
    if os.path.exists(habits_file):
        try:
            with open(habits_file, "r") as f:
                habits_data = json.load(f)
            log = habits_data.get("log", {})
            habits = habits_data.get("habits", [])
            
            year_days = {d: v for d, v in log.items() if d.startswith(str(year))}
            total_possible = len(habits) * len(year_days) if habits else 1
            total_done = sum(
                sum(1 for h in habits if day_log.get(h["id"], False))
                for day_log in year_days.values()
            )
            result["habits"]["total_checks"] = total_done
            result["habits"]["completion_rate"] = int(total_done / max(1, total_possible) * 100)
            
            # Longest streak
            streak = 0
            max_streak = 0
            for d in sorted(year_days.keys()):
                day_log = year_days[d]
                if habits and all(day_log.get(h["id"], False) for h in habits):
                    streak += 1
                    max_streak = max(max_streak, streak)
                else:
                    streak = 0
            result["habits"]["longest_streak"] = max_streak
        except Exception as e:
            logger.debug("Habits data error: %s", e)
    
    # Load library data
    library_file = os.path.join(DATA_DIR, "library.json")
    if os.path.exists(library_file):
        try:
            with open(library_file, "r") as f:
                lib_data = json.load(f)
            
            books_read = [b for b in lib_data.get("books", [])
                         if b.get("finished_date", "").startswith(str(year))]
            result["books"]["read"] = len(books_read)
            result["books"]["pages"] = sum(b.get("total_pages", 0) for b in books_read)
            
            # Favorite book (highest rated)
            if books_read:
                rated = [b for b in books_read if b.get("rating")]
                if rated:
                    fav = max(rated, key=lambda x: x["rating"])
                    result["books"]["favorite"] = f"{fav['title']} ({fav.get('author', '')})"
            
            audiobooks = [ab for ab in lib_data.get("audiobooks", [])
                         if ab.get("finished_date", "").startswith(str(year))]
            result["books"]["audiobooks"] = len(audiobooks)
        except Exception as e:
            logger.debug("Library data error: %s", e)
    
    # Load diary data
    diary_file = os.path.join(DATA_DIR, "diary.json")
    if os.path.exists(diary_file):
        try:
            with open(diary_file, "r") as f:
                diary_data = json.load(f)
            
            year_entries = [e for e in diary_data.get("entries", [])
                          if e.get("date", "").startswith(str(year))]
            result["diary"]["entries"] = len(year_entries)
            result["diary"]["total_words"] = sum(e.get("word_count", 0) for e in year_entries)
            
            # Mood by month
            mood_log = diary_data.get("mood_log", {})
            year_moods = {d: v for d, v in mood_log.items() if d.startswith(str(year))}
            if year_moods:
                result["diary"]["avg_mood"] = sum(year_moods.values()) / len(year_moods)
                
                # Best month
                month_moods = {}
                for d, m in year_moods.items():
                    month = d[:7]
                    month_moods.setdefault(month, []).append(m)
                month_avgs = {m: sum(ms)/len(ms) for m, ms in month_moods.items()}
                if month_avgs:
                    best = max(month_avgs, key=month_avgs.get)
                    month_names = ["", "Январь", "Февраль", "Март", "Апрель", "Май",
                                  "Июнь", "Июль", "Август", "Сентябрь", "Октябрь",
                                  "Ноябрь", "Декабрь"]
                    result["diary"]["best_month"] = month_names[int(best.split("-")[1])]
        except Exception as e:
            logger.debug("Diary data error: %s", e)
    
    # Fun stats
    result["fun_stats"] = {
        "total_interactions": (result["habits"]["total_checks"] + 
                              result["diary"]["entries"] + 
                              result["books"]["read"]),
        "pages_equivalent": result["diary"]["total_words"] // 250,  # ~250 words per page
    }
    
    return result


# ─── AI Insights ──────────────────────────────────────────────────────────

YEARLY_INSIGHTS_PROMPT = """Generate a warm, personal year-in-review summary in Russian based on this data:

Year: {year}
Stats:
- Habits: {habits_checks} check-ins, {habits_rate}% completion, longest streak: {habits_streak} days
- Books read: {books_read}, audiobooks: {audiobooks}, pages: {pages}
- Favorite book: {favorite_book}
- Diary entries: {diary_entries}, average mood: {avg_mood}/5
- Best mood month: {best_month}
- Total words written in diary: {diary_words}

Write a brief (5-7 sentences), warm, encouraging year summary. Highlight achievements,
note patterns, and give a motivational outlook for next year. Use emoji sparingly.
Write in first person as if the user is reading about themselves.
"""


async def generate_yearly_insights(data: dict, ai_call_fn) -> str:
    """Generate AI insights for the yearly review."""
    prompt = YEARLY_INSIGHTS_PROMPT.format(
        year=data["year"],
        habits_checks=data["habits"]["total_checks"],
        habits_rate=data["habits"]["completion_rate"],
        habits_streak=data["habits"]["longest_streak"],
        books_read=data["books"]["read"],
        audiobooks=data["books"]["audiobooks"],
        pages=data["books"]["pages"],
        favorite_book=data["books"].get("favorite", "нет данных"),
        diary_entries=data["diary"]["entries"],
        avg_mood=f"{data['diary']['avg_mood']:.1f}" if data["diary"]["avg_mood"] else "нет данных",
        best_month=data["diary"].get("best_month", "нет данных"),
        diary_words=data["diary"]["total_words"]
    )
    
    try:
        response = await ai_call_fn(prompt)
        return response.strip()
    except Exception as e:
        logger.warning("Failed to generate yearly insights: %s", e)
        return ""


# ─── Telegram Formatting ──────────────────────────────────────────────────

def format_yearly_review(data: dict) -> list:
    """Format yearly review as a series of messages (like Spotify Wrapped cards)."""
    year = data["year"]
    cards = []
    
    # Card 1: Title
    card1 = f"🎉 <b>Ваш {year} год в цифрах</b>\n\n"
    card1 += f"За этот год вы провели <b>{data['fun_stats']['total_interactions']}</b> "
    card1 += "взаимодействий с ботом.\n\n"
    card1 += "Давайте посмотрим, чего вы достигли! 👇"
    cards.append(card1)
    
    # Card 2: Habits
    if data["habits"]["total_checks"] > 0:
        card2 = "💪 <b>Привычки</b>\n\n"
        card2 += f"✅ Отмечено: <b>{data['habits']['total_checks']}</b> раз\n"
        card2 += f"📊 Выполнение: <b>{data['habits']['completion_rate']}%</b>\n"
        card2 += f"🔥 Лучшая серия: <b>{data['habits']['longest_streak']}</b> дней\n"
        
        if data["habits"]["completion_rate"] >= 80:
            card2 += "\n🏆 Впечатляющая дисциплина!"
        elif data["habits"]["completion_rate"] >= 50:
            card2 += "\n👍 Хороший результат! Есть куда расти."
        else:
            card2 += "\n💡 В новом году можно улучшить!"
        cards.append(card2)
    
    # Card 3: Reading
    if data["books"]["read"] > 0 or data["books"]["audiobooks"] > 0:
        card3 = "📚 <b>Чтение</b>\n\n"
        card3 += f"📖 Прочитано книг: <b>{data['books']['read']}</b>\n"
        card3 += f"🎧 Прослушано аудиокниг: <b>{data['books']['audiobooks']}</b>\n"
        if data["books"]["pages"]:
            card3 += f"📄 Страниц: <b>{data['books']['pages']:,}</b>\n"
        if data["books"]["favorite"]:
            card3 += f"\n⭐ Любимая книга: <b>{data['books']['favorite']}</b>"
        cards.append(card3)
    
    # Card 4: Diary & Mood
    if data["diary"]["entries"] > 0:
        card4 = "🌙 <b>Дневник & Настроение</b>\n\n"
        card4 += f"📝 Записей: <b>{data['diary']['entries']}</b>\n"
        card4 += f"✍️ Написано слов: <b>{data['diary']['total_words']:,}</b>\n"
        
        if data["diary"]["total_words"] > 0:
            pages = data["diary"]["total_words"] // 250
            card4 += f"📖 Это как книга на <b>{pages}</b> страниц!\n"
        
        if data["diary"]["avg_mood"]:
            mood_emojis = {5: "😊", 4: "🙂", 3: "😐", 2: "😔", 1: "😞"}
            avg = data["diary"]["avg_mood"]
            emoji = mood_emojis.get(round(avg), "❓")
            card4 += f"\n{emoji} Среднее настроение: <b>{avg:.1f}/5</b>\n"
        
        if data["diary"]["best_month"]:
            card4 += f"☀️ Лучший месяц: <b>{data['diary']['best_month']}</b>"
        cards.append(card4)
    
    # Card 5: AI Summary (placeholder — filled later)
    if data.get("ai_insights"):
        card5 = "🧠 <b>AI-итоги года</b>\n\n"
        card5 += data["ai_insights"]
        cards.append(card5)
    
    # Final card
    final = f"🎊 <b>Спасибо за {year} год вместе!</b>\n\n"
    final += "Впереди новый год — новые цели, привычки и достижения.\n"
    final += "Удачи! 🚀"
    cards.append(final)
    
    return cards


async def cmd_yearly_review(year: int = None, ai_call_fn=None) -> list:
    """Generate full yearly review."""
    data = _collect_year_data(year)
    
    if ai_call_fn:
        data["ai_insights"] = await generate_yearly_insights(data, ai_call_fn)
    
    return format_yearly_review(data)
