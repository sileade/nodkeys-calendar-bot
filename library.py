"""
library.py — Personal Library for Nodkeys Bot v10.1

Manages user's personal book/audiobook shelf with:
- Reading progress tracking
- Book ratings and notes
- Reading statistics (books/month, pages/day)
- AI-powered recommendations
- Reading goals
- History of all downloaded/sent books
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
LIBRARY_FILE = os.path.join(DATA_DIR, "library.json")


# ─── Storage ───────────────────────────────────────────────────────────────

def _load_library() -> dict:
    """Load library database."""
    try:
        if os.path.exists(LIBRARY_FILE):
            with open(LIBRARY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error("Failed to load library: %s", e)
    return {
        "books": [],
        "audiobooks": [],
        "reading_goals": {"yearly": 24, "monthly": 2},
        "stats": {"total_books_read": 0, "total_audiobooks": 0, "total_pages": 0},
        "recommendations": []
    }


def _save_library(data: dict):
    """Save library database."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(LIBRARY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─── Book Management ──────────────────────────────────────────────────────

def add_book(title: str, author: str = "", book_type: str = "ebook",
             total_pages: int = 0, file_id: str = "", source: str = "manual",
             cover_url: str = "", genre: str = "") -> dict:
    """Add a book to the library."""
    data = _load_library()
    
    # Check if already exists
    for b in data["books"]:
        if b["title"].lower() == title.lower() and b.get("author", "").lower() == author.lower():
            return b  # Already in library
    
    book = {
        "id": f"book_{datetime.now().strftime('%Y%m%d%H%M%S')}_{len(data['books'])}",
        "title": title,
        "author": author,
        "type": book_type,  # ebook, audiobook, paper
        "genre": genre,
        "total_pages": total_pages,
        "current_page": 0,
        "progress_pct": 0,
        "status": "to_read",  # to_read, reading, finished, abandoned
        "rating": None,  # 1-5
        "notes": [],
        "file_id": file_id,
        "cover_url": cover_url,
        "source": source,
        "added_date": datetime.now(TIMEZONE).isoformat(),
        "started_date": None,
        "finished_date": None,
        "reading_sessions": []
    }
    
    data["books"].append(book)
    _save_library(data)
    return book


def add_audiobook(title: str, author: str = "", narrator: str = "",
                  total_chapters: int = 0, cache_key: str = "",
                  source: str = "rutracker") -> dict:
    """Add an audiobook to the library."""
    data = _load_library()
    
    # Check if already exists
    for ab in data["audiobooks"]:
        if ab["title"].lower() == title.lower():
            return ab
    
    audiobook = {
        "id": f"abook_{datetime.now().strftime('%Y%m%d%H%M%S')}_{len(data['audiobooks'])}",
        "title": title,
        "author": author,
        "narrator": narrator,
        "total_chapters": total_chapters,
        "current_chapter": 0,
        "progress_pct": 0,
        "status": "to_listen",  # to_listen, listening, finished, abandoned
        "rating": None,
        "cache_key": cache_key,
        "source": source,
        "added_date": datetime.now(TIMEZONE).isoformat(),
        "started_date": None,
        "finished_date": None,
        "last_played": None
    }
    
    data["audiobooks"].append(audiobook)
    _save_library(data)
    return audiobook


def update_book_progress(book_id: str, current_page: int = None, 
                         progress_pct: int = None, status: str = None) -> dict:
    """Update reading progress for a book."""
    data = _load_library()
    
    for b in data["books"]:
        if b["id"] == book_id:
            now = datetime.now(TIMEZONE).isoformat()
            
            if current_page is not None:
                b["current_page"] = current_page
                if b["total_pages"] > 0:
                    b["progress_pct"] = min(100, int(current_page / b["total_pages"] * 100))
            
            if progress_pct is not None:
                b["progress_pct"] = min(100, progress_pct)
            
            if status:
                b["status"] = status
                if status == "reading" and not b["started_date"]:
                    b["started_date"] = now
                elif status == "finished":
                    b["finished_date"] = now
                    b["progress_pct"] = 100
                    data["stats"]["total_books_read"] += 1
                    if b["total_pages"]:
                        data["stats"]["total_pages"] += b["total_pages"]
            
            # Record reading session
            b["reading_sessions"].append({
                "date": now,
                "page": b["current_page"],
                "progress": b["progress_pct"]
            })
            # Keep only last 50 sessions
            b["reading_sessions"] = b["reading_sessions"][-50:]
            
            _save_library(data)
            return b
    
    return None


def update_audiobook_progress(audiobook_id: str, current_chapter: int = None,
                              status: str = None) -> dict:
    """Update listening progress for an audiobook."""
    data = _load_library()
    
    for ab in data["audiobooks"]:
        if ab["id"] == audiobook_id:
            now = datetime.now(TIMEZONE).isoformat()
            
            if current_chapter is not None:
                ab["current_chapter"] = current_chapter
                if ab["total_chapters"] > 0:
                    ab["progress_pct"] = min(100, int(current_chapter / ab["total_chapters"] * 100))
            
            if status:
                ab["status"] = status
                if status == "listening" and not ab["started_date"]:
                    ab["started_date"] = now
                elif status == "finished":
                    ab["finished_date"] = now
                    ab["progress_pct"] = 100
                    data["stats"]["total_audiobooks"] += 1
            
            ab["last_played"] = now
            _save_library(data)
            return ab
    
    return None


def rate_book(book_id: str, rating: int, note: str = "") -> bool:
    """Rate a book (1-5 stars) and optionally add a note."""
    data = _load_library()
    
    # Search in both books and audiobooks
    for collection in [data["books"], data["audiobooks"]]:
        for item in collection:
            if item["id"] == book_id:
                item["rating"] = max(1, min(5, rating))
                if note:
                    item.setdefault("notes", []).append({
                        "text": note,
                        "date": datetime.now(TIMEZONE).isoformat()
                    })
                _save_library(data)
                return True
    
    return False


def add_book_note(book_id: str, note: str, page: int = None) -> bool:
    """Add a note/highlight to a book."""
    data = _load_library()
    
    for collection in [data["books"], data["audiobooks"]]:
        for item in collection:
            if item["id"] == book_id:
                item.setdefault("notes", []).append({
                    "text": note,
                    "page": page,
                    "date": datetime.now(TIMEZONE).isoformat()
                })
                _save_library(data)
                return True
    
    return False


# ─── Reading Goals ─────────────────────────────────────────────────────────

def set_reading_goal(yearly: int = None, monthly: int = None):
    """Set reading goals."""
    data = _load_library()
    if yearly is not None:
        data["reading_goals"]["yearly"] = yearly
    if monthly is not None:
        data["reading_goals"]["monthly"] = monthly
    _save_library(data)


def get_goal_progress() -> dict:
    """Get progress towards reading goals."""
    data = _load_library()
    now = datetime.now(TIMEZONE)
    year_start = now.replace(month=1, day=1).isoformat()
    month_start = now.replace(day=1).isoformat()
    
    books_this_year = sum(1 for b in data["books"] 
                         if b.get("finished_date") and b["finished_date"] >= year_start)
    books_this_month = sum(1 for b in data["books"]
                          if b.get("finished_date") and b["finished_date"] >= month_start)
    
    audiobooks_this_year = sum(1 for ab in data["audiobooks"]
                              if ab.get("finished_date") and ab["finished_date"] >= year_start)
    audiobooks_this_month = sum(1 for ab in data["audiobooks"]
                               if ab.get("finished_date") and ab["finished_date"] >= month_start)
    
    yearly_goal = data["reading_goals"].get("yearly", 24)
    monthly_goal = data["reading_goals"].get("monthly", 2)
    
    total_year = books_this_year + audiobooks_this_year
    total_month = books_this_month + audiobooks_this_month
    
    return {
        "yearly_goal": yearly_goal,
        "yearly_progress": total_year,
        "yearly_pct": min(100, int(total_year / yearly_goal * 100)) if yearly_goal > 0 else 0,
        "monthly_goal": monthly_goal,
        "monthly_progress": total_month,
        "monthly_pct": min(100, int(total_month / monthly_goal * 100)) if monthly_goal > 0 else 0,
        "books_this_year": books_this_year,
        "audiobooks_this_year": audiobooks_this_year,
        "on_track": total_year >= (yearly_goal * now.timetuple().tm_yday / 365)
    }


# ─── Statistics ────────────────────────────────────────────────────────────

def get_reading_stats() -> dict:
    """Get comprehensive reading statistics."""
    data = _load_library()
    now = datetime.now(TIMEZONE)
    
    all_books = data["books"]
    all_audiobooks = data["audiobooks"]
    
    finished_books = [b for b in all_books if b["status"] == "finished"]
    reading_books = [b for b in all_books if b["status"] == "reading"]
    listening_audiobooks = [ab for ab in all_audiobooks if ab["status"] == "listening"]
    
    # Average reading speed (pages per session)
    total_sessions = sum(len(b.get("reading_sessions", [])) for b in all_books)
    
    # Favorite genres
    genres = {}
    for b in finished_books:
        g = b.get("genre", "Другое")
        genres[g] = genres.get(g, 0) + 1
    top_genres = sorted(genres.items(), key=lambda x: x[1], reverse=True)[:5]
    
    # Favorite authors
    authors = {}
    for b in finished_books:
        a = b.get("author", "Неизвестен")
        authors[a] = authors.get(a, 0) + 1
    top_authors = sorted(authors.items(), key=lambda x: x[1], reverse=True)[:5]
    
    # Reading streak
    streak = 0
    check_date = now - timedelta(days=1)
    while streak < 365:
        d = check_date.strftime("%Y-%m-%d")
        has_session = any(
            any(s["date"].startswith(d) for s in b.get("reading_sessions", []))
            for b in all_books
        )
        if has_session:
            streak += 1
            check_date -= timedelta(days=1)
        else:
            break
    
    return {
        "total_books": len(all_books),
        "total_audiobooks": len(all_audiobooks),
        "finished_books": len(finished_books),
        "currently_reading": len(reading_books),
        "currently_listening": len(listening_audiobooks),
        "total_pages_read": data["stats"].get("total_pages", 0),
        "reading_streak": streak,
        "top_genres": top_genres,
        "top_authors": top_authors,
        "total_sessions": total_sessions,
        "avg_rating": (sum(b["rating"] for b in finished_books if b.get("rating")) / 
                      max(1, sum(1 for b in finished_books if b.get("rating"))))
    }


# ─── Recommendations ──────────────────────────────────────────────────────

RECOMMENDATION_PROMPT = """Based on the user's reading history, suggest 5 books they might enjoy.

Books they've read and liked (rated 4-5):
{liked_books}

Books they've read and disliked (rated 1-2):
{disliked_books}

Genres they prefer: {genres}
Authors they like: {authors}

Return a JSON array of 5 recommendations:
[{{"title": "...", "author": "...", "genre": "...", "reason": "short reason in Russian"}}]

Focus on books available in Russian. Return ONLY valid JSON.
"""


async def get_recommendations(ai_call_fn) -> list:
    """Get AI-powered book recommendations based on reading history."""
    data = _load_library()
    
    liked = [f"{b['title']} ({b.get('author', '?')})" 
             for b in data["books"] if b.get("rating") and b["rating"] >= 4]
    disliked = [f"{b['title']} ({b.get('author', '?')})" 
                for b in data["books"] if b.get("rating") and b["rating"] <= 2]
    
    stats = get_reading_stats()
    genres = ", ".join(g[0] for g in stats["top_genres"][:3]) or "разные"
    authors = ", ".join(a[0] for a in stats["top_authors"][:3]) or "разные"
    
    if not liked and not disliked:
        return []  # Not enough data
    
    prompt = RECOMMENDATION_PROMPT.format(
        liked_books="\n".join(liked[:10]) or "нет данных",
        disliked_books="\n".join(disliked[:5]) or "нет данных",
        genres=genres,
        authors=authors
    )
    
    try:
        response = await ai_call_fn(prompt)
        response = response.strip()
        if response.startswith("```"):
            response = response.split("\n", 1)[1].rsplit("```", 1)[0]
        recs = json.loads(response)
        if isinstance(recs, list):
            data["recommendations"] = recs
            _save_library(data)
            return recs
    except Exception as e:
        logger.warning("Failed to get recommendations: %s", e)
    
    return data.get("recommendations", [])


# ─── Telegram Formatting ──────────────────────────────────────────────────

def format_library_message() -> tuple:
    """Format library overview for Telegram."""
    data = _load_library()
    stats = get_reading_stats()
    goals = get_goal_progress()
    
    msg = "📚 <b>Моя библиотека</b>\n\n"
    
    # Goal progress
    yearly_bar = "█" * (goals["yearly_pct"] // 10) + "░" * (10 - goals["yearly_pct"] // 10)
    msg += f"🎯 <b>Цель на год:</b> {goals['yearly_progress']}/{goals['yearly_goal']}\n"
    msg += f"   [{yearly_bar}] {goals['yearly_pct']}%\n"
    
    track_emoji = "✅" if goals["on_track"] else "⚠️"
    msg += f"   {track_emoji} {'В графике' if goals['on_track'] else 'Отстаёте'}\n\n"
    
    # Currently reading
    reading = [b for b in data["books"] if b["status"] == "reading"]
    if reading:
        msg += "📖 <b>Сейчас читаю:</b>\n"
        for b in reading[:3]:
            progress_bar = "█" * (b["progress_pct"] // 10) + "░" * (10 - b["progress_pct"] // 10)
            msg += f"  • {b['title']}"
            if b.get("author"):
                msg += f" — {b['author']}"
            msg += f"\n    [{progress_bar}] {b['progress_pct']}%\n"
        msg += "\n"
    
    # Currently listening
    listening = [ab for ab in data["audiobooks"] if ab["status"] == "listening"]
    if listening:
        msg += "🎧 <b>Сейчас слушаю:</b>\n"
        for ab in listening[:3]:
            msg += f"  • {ab['title']}"
            if ab.get("narrator"):
                msg += f" (чит. {ab['narrator']})"
            msg += f"\n    Глава {ab['current_chapter']}/{ab['total_chapters']}\n"
        msg += "\n"
    
    # Stats
    msg += f"📊 <b>Статистика:</b>\n"
    msg += f"  📕 Прочитано: {stats['finished_books']} книг\n"
    msg += f"  🎧 Прослушано: {stats.get('total_audiobooks', 0)} аудиокниг\n"
    if stats["reading_streak"] > 0:
        msg += f"  🔥 Серия чтения: {stats['reading_streak']} дн.\n"
    if stats["avg_rating"] > 0:
        stars = "⭐" * int(stats["avg_rating"])
        msg += f"  {stars} Средняя оценка: {stats['avg_rating']:.1f}\n"
    
    # Buttons
    buttons = [
        [{"text": "📖 Книги", "callback_data": "lib:books:0"},
         {"text": "🎧 Аудиокниги", "callback_data": "lib:audio:0"}],
        [{"text": "📊 Статистика", "callback_data": "lib:stats"},
         {"text": "💡 Рекомендации", "callback_data": "lib:recs"}],
        [{"text": "🎯 Цели", "callback_data": "lib:goals"},
         {"text": "📝 Хочу прочитать", "callback_data": "lib:toread:0"}]
    ]
    
    return msg, buttons


def format_book_list(book_type: str = "books", page: int = 0, page_size: int = 5) -> tuple:
    """Format paginated book list."""
    data = _load_library()
    
    if book_type == "books":
        items = sorted(data["books"], key=lambda x: x.get("added_date", ""), reverse=True)
        emoji = "📖"
        title = "Книги"
    else:
        items = sorted(data["audiobooks"], key=lambda x: x.get("added_date", ""), reverse=True)
        emoji = "🎧"
        title = "Аудиокниги"
    
    total = len(items)
    start = page * page_size
    end = min(start + page_size, total)
    page_items = items[start:end]
    
    if not page_items:
        return f"{emoji} <b>{title}</b>\n\nПусто. Найдите книгу командой /book", []
    
    status_emoji = {
        "to_read": "📋", "to_listen": "📋",
        "reading": "📖", "listening": "🎧",
        "finished": "✅", "abandoned": "❌"
    }
    
    msg = f"{emoji} <b>{title}</b> ({total} всего)\n\n"
    buttons = []
    
    for item in page_items:
        s_emoji = status_emoji.get(item["status"], "📋")
        rating_str = "⭐" * item["rating"] if item.get("rating") else ""
        
        msg += f"{s_emoji} <b>{item['title']}</b>"
        if item.get("author"):
            msg += f" — {item['author']}"
        msg += "\n"
        
        if item["status"] in ("reading", "listening"):
            msg += f"   Прогресс: {item['progress_pct']}%\n"
        if rating_str:
            msg += f"   {rating_str}\n"
        msg += "\n"
        
        buttons.append([{"text": f"📋 {item['title'][:30]}", "callback_data": f"lib:detail:{item['id']}"}])
    
    # Pagination
    nav_buttons = []
    if page > 0:
        nav_buttons.append({"text": "⬅️ Назад", "callback_data": f"lib:{book_type}:{page-1}"})
    if end < total:
        nav_buttons.append({"text": "➡️ Далее", "callback_data": f"lib:{book_type}:{page+1}"})
    if nav_buttons:
        buttons.append(nav_buttons)
    
    buttons.append([{"text": "🔙 Библиотека", "callback_data": "lib:main"}])
    
    return msg, buttons


def format_book_detail(book_id: str) -> tuple:
    """Format detailed view of a single book."""
    data = _load_library()
    
    item = None
    for b in data["books"] + data["audiobooks"]:
        if b["id"] == book_id:
            item = b
            break
    
    if not item:
        return "❌ Книга не найдена", []
    
    is_audio = book_id.startswith("abook_")
    emoji = "🎧" if is_audio else "📖"
    
    msg = f"{emoji} <b>{item['title']}</b>\n"
    if item.get("author"):
        msg += f"✍️ {item['author']}\n"
    if item.get("narrator"):
        msg += f"🎙 Чтец: {item['narrator']}\n"
    if item.get("genre"):
        msg += f"📂 Жанр: {item['genre']}\n"
    msg += "\n"
    
    # Progress
    status_text = {
        "to_read": "📋 Хочу прочитать", "to_listen": "📋 Хочу послушать",
        "reading": "📖 Читаю", "listening": "🎧 Слушаю",
        "finished": "✅ Прочитано", "abandoned": "❌ Брошено"
    }
    msg += f"<b>Статус:</b> {status_text.get(item['status'], item['status'])}\n"
    
    if item["progress_pct"] > 0:
        bar = "█" * (item["progress_pct"] // 10) + "░" * (10 - item["progress_pct"] // 10)
        msg += f"<b>Прогресс:</b> [{bar}] {item['progress_pct']}%\n"
    
    if item.get("rating"):
        msg += f"<b>Оценка:</b> {'⭐' * item['rating']}\n"
    
    msg += f"\n<b>Добавлено:</b> {item['added_date'][:10]}\n"
    if item.get("started_date"):
        msg += f"<b>Начато:</b> {item['started_date'][:10]}\n"
    if item.get("finished_date"):
        msg += f"<b>Завершено:</b> {item['finished_date'][:10]}\n"
    
    # Notes
    notes = item.get("notes", [])
    if notes:
        msg += f"\n📝 <b>Заметки ({len(notes)}):</b>\n"
        for n in notes[-3:]:
            page_str = f" (стр. {n['page']})" if n.get("page") else ""
            msg += f"  • {n['text'][:100]}{page_str}\n"
    
    # Action buttons
    buttons = []
    if item["status"] in ("to_read", "to_listen"):
        buttons.append([{"text": "▶️ Начать", "callback_data": f"lib:start:{book_id}"}])
    elif item["status"] in ("reading", "listening"):
        buttons.append([
            {"text": "📝 Прогресс", "callback_data": f"lib:progress:{book_id}"},
            {"text": "✅ Завершить", "callback_data": f"lib:finish:{book_id}"}
        ])
    
    if not item.get("rating"):
        buttons.append([{"text": "⭐ Оценить", "callback_data": f"lib:rate:{book_id}"}])
    
    buttons.append([
        {"text": "📝 Заметка", "callback_data": f"lib:note:{book_id}"},
        {"text": "🗑 Удалить", "callback_data": f"lib:delete:{book_id}"}
    ])
    buttons.append([{"text": "🔙 Назад", "callback_data": "lib:main"}])
    
    return msg, buttons


def format_stats_message() -> str:
    """Format detailed statistics message."""
    stats = get_reading_stats()
    goals = get_goal_progress()
    
    msg = "📊 <b>Статистика чтения</b>\n\n"
    
    msg += f"📕 Всего книг в библиотеке: {stats['total_books']}\n"
    msg += f"🎧 Всего аудиокниг: {stats['total_audiobooks']}\n"
    msg += f"✅ Прочитано книг: {stats['finished_books']}\n"
    msg += f"📖 Сейчас читаю: {stats['currently_reading']}\n"
    msg += f"🎧 Сейчас слушаю: {stats['currently_listening']}\n"
    msg += f"📄 Всего страниц: {stats['total_pages_read']:,}\n"
    
    if stats["reading_streak"] > 0:
        msg += f"\n🔥 <b>Серия чтения:</b> {stats['reading_streak']} дней подряд!\n"
    
    if stats["avg_rating"] > 0:
        msg += f"\n⭐ <b>Средняя оценка:</b> {stats['avg_rating']:.1f}/5\n"
    
    if stats["top_genres"]:
        msg += "\n📂 <b>Любимые жанры:</b>\n"
        for genre, count in stats["top_genres"]:
            msg += f"  • {genre} ({count})\n"
    
    if stats["top_authors"]:
        msg += "\n✍️ <b>Любимые авторы:</b>\n"
        for author, count in stats["top_authors"]:
            msg += f"  • {author} ({count})\n"
    
    # Year progress
    msg += f"\n🎯 <b>Цель на год:</b>\n"
    msg += f"  📚 {goals['yearly_progress']}/{goals['yearly_goal']} "
    msg += f"({'✅ в графике' if goals['on_track'] else '⚠️ отстаёте'})\n"
    msg += f"  📅 Этот месяц: {goals['monthly_progress']}/{goals['monthly_goal']}\n"
    
    return msg
