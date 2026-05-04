"""
podcasts.py — Podcast Search & Streaming for Nodkeys Bot v10.1

Features:
- Search podcasts via iTunes/Podcast Index API
- Subscribe to podcasts
- Get new episodes notifications
- Stream episodes in Telegram (send as audio)
- Episode history and progress
- AI-powered episode summaries
"""

import os
import json
import asyncio
import logging
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

TIMEZONE = ZoneInfo(os.environ.get("TIMEZONE", "Europe/Moscow"))
DATA_DIR = os.environ.get("DATA_DIR", "/app/data")
PODCASTS_FILE = os.path.join(DATA_DIR, "podcasts.json")

# iTunes Search API (free, no auth needed)
ITUNES_SEARCH_URL = "https://itunes.apple.com/search"
ITUNES_LOOKUP_URL = "https://itunes.apple.com/lookup"


# ─── Storage ───────────────────────────────────────────────────────────────

def _load_podcasts() -> dict:
    """Load podcasts database."""
    try:
        if os.path.exists(PODCASTS_FILE):
            with open(PODCASTS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error("Failed to load podcasts: %s", e)
    return {
        "subscriptions": [],
        "episodes_history": [],
        "queue": [],
        "settings": {
            "auto_download": False,
            "notify_new_episodes": True,
            "max_queue_size": 20
        }
    }


def _save_podcasts(data: dict):
    """Save podcasts database."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(PODCASTS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─── Podcast Search ───────────────────────────────────────────────────────

async def search_podcasts(query: str, limit: int = 5) -> list:
    """Search podcasts via iTunes API."""
    params = {
        "term": query,
        "media": "podcast",
        "limit": limit,
        "country": "RU",
        "lang": "ru_ru"
    }
    
    url = f"{ITUNES_SEARCH_URL}?{urllib.parse.urlencode(params)}"
    
    try:
        def _fetch():
            req = urllib.request.Request(url, headers={"User-Agent": "NodkeysBot/10.1"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        
        result = await asyncio.to_thread(_fetch)
        
        podcasts = []
        for item in result.get("results", []):
            podcasts.append({
                "id": str(item.get("collectionId", "")),
                "title": item.get("collectionName", ""),
                "author": item.get("artistName", ""),
                "description": item.get("description", "") or item.get("collectionName", ""),
                "artwork_url": item.get("artworkUrl100", ""),
                "feed_url": item.get("feedUrl", ""),
                "genre": item.get("primaryGenreName", ""),
                "episode_count": item.get("trackCount", 0),
                "last_release": item.get("releaseDate", "")
            })
        
        return podcasts
    except Exception as e:
        logger.error("Podcast search error: %s", e)
        return []


async def get_podcast_episodes(feed_url: str, limit: int = 10) -> list:
    """Fetch recent episodes from podcast RSS feed."""
    try:
        import xml.etree.ElementTree as ET
        
        def _fetch_feed():
            req = urllib.request.Request(feed_url, headers={"User-Agent": "NodkeysBot/10.1"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                return resp.read().decode("utf-8", errors="replace")
        
        xml_text = await asyncio.to_thread(_fetch_feed)
        root = ET.fromstring(xml_text)
        
        episodes = []
        channel = root.find("channel")
        if channel is None:
            return []
        
        items = channel.findall("item")
        for item in items[:limit]:
            # Get enclosure (audio URL)
            enclosure = item.find("enclosure")
            audio_url = enclosure.get("url", "") if enclosure is not None else ""
            audio_length = enclosure.get("length", "0") if enclosure is not None else "0"
            
            # Get duration
            duration = ""
            for ns in ["itunes", "{http://www.itunes.com/dtds/podcast-1.0.dtd}"]:
                dur_elem = item.find(f"{ns}:duration" if ":" not in ns else f"{ns}duration")
                if dur_elem is not None and dur_elem.text:
                    duration = dur_elem.text
                    break
            
            # Get description
            desc = ""
            desc_elem = item.find("description")
            if desc_elem is not None and desc_elem.text:
                desc = desc_elem.text[:500]
            
            title_elem = item.find("title")
            pub_date_elem = item.find("pubDate")
            
            episodes.append({
                "title": title_elem.text if title_elem is not None else "Без названия",
                "description": desc,
                "audio_url": audio_url,
                "duration": duration,
                "pub_date": pub_date_elem.text if pub_date_elem is not None else "",
                "size_bytes": int(audio_length) if audio_length.isdigit() else 0
            })
        
        return episodes
    except Exception as e:
        logger.error("Feed fetch error: %s", e)
        return []


# ─── Subscription Management ──────────────────────────────────────────────

def subscribe_podcast(podcast: dict) -> bool:
    """Subscribe to a podcast."""
    data = _load_podcasts()
    
    # Check if already subscribed
    for sub in data["subscriptions"]:
        if sub["id"] == podcast["id"]:
            return False  # Already subscribed
    
    subscription = {
        **podcast,
        "subscribed_date": datetime.now(TIMEZONE).isoformat(),
        "last_checked": None,
        "last_episode_date": None
    }
    data["subscriptions"].append(subscription)
    _save_podcasts(data)
    return True


def unsubscribe_podcast(podcast_id: str) -> bool:
    """Unsubscribe from a podcast."""
    data = _load_podcasts()
    original_len = len(data["subscriptions"])
    data["subscriptions"] = [s for s in data["subscriptions"] if s["id"] != podcast_id]
    if len(data["subscriptions"]) < original_len:
        _save_podcasts(data)
        return True
    return False


def get_subscriptions() -> list:
    """Get all podcast subscriptions."""
    data = _load_podcasts()
    return data["subscriptions"]


# ─── Episode Queue ─────────────────────────────────────────────────────────

def add_to_queue(episode: dict, podcast_title: str = "") -> bool:
    """Add episode to listening queue."""
    data = _load_podcasts()
    
    queue_item = {
        "id": f"ep_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "podcast": podcast_title,
        "title": episode.get("title", ""),
        "audio_url": episode.get("audio_url", ""),
        "duration": episode.get("duration", ""),
        "added": datetime.now(TIMEZONE).isoformat(),
        "listened": False
    }
    
    data["queue"].append(queue_item)
    # Limit queue size
    max_size = data["settings"].get("max_queue_size", 20)
    data["queue"] = data["queue"][-max_size:]
    _save_podcasts(data)
    return True


def get_queue() -> list:
    """Get listening queue."""
    data = _load_podcasts()
    return [ep for ep in data["queue"] if not ep.get("listened")]


def mark_episode_listened(episode_id: str):
    """Mark episode as listened."""
    data = _load_podcasts()
    for ep in data["queue"]:
        if ep["id"] == episode_id:
            ep["listened"] = True
            break
    
    # Also add to history
    data["episodes_history"].append({
        "episode_id": episode_id,
        "listened_date": datetime.now(TIMEZONE).isoformat()
    })
    data["episodes_history"] = data["episodes_history"][-200:]
    _save_podcasts(data)


# ─── New Episodes Check ────────────────────────────────────────────────────

async def check_new_episodes() -> list:
    """Check all subscriptions for new episodes. Returns list of new episodes."""
    data = _load_podcasts()
    new_episodes = []
    
    for sub in data["subscriptions"]:
        if not sub.get("feed_url"):
            continue
        
        try:
            episodes = await get_podcast_episodes(sub["feed_url"], limit=3)
            if not episodes:
                continue
            
            last_checked = sub.get("last_checked")
            for ep in episodes:
                # Simple check: if pub_date is after last_checked
                if last_checked and ep.get("pub_date"):
                    # Just check if it's a new episode we haven't seen
                    pass  # For now, just return latest
                
                new_episodes.append({
                    "podcast": sub["title"],
                    "podcast_id": sub["id"],
                    "episode": ep
                })
                break  # Only latest episode per podcast
            
            sub["last_checked"] = datetime.now(TIMEZONE).isoformat()
        except Exception as e:
            logger.debug("Check episodes error for %s: %s", sub["title"], e)
    
    _save_podcasts(data)
    return new_episodes


# ─── Episode Download ──────────────────────────────────────────────────────

async def download_episode(audio_url: str, title: str = "episode") -> str:
    """Download podcast episode audio file. Returns local path."""
    if not audio_url:
        return None
    
    # Create temp file
    import tempfile
    safe_title = "".join(c for c in title if c.isalnum() or c in " -_")[:50]
    ext = ".mp3"
    if ".m4a" in audio_url:
        ext = ".m4a"
    elif ".ogg" in audio_url:
        ext = ".ogg"
    
    output_path = os.path.join(tempfile.gettempdir(), f"podcast_{safe_title}{ext}")
    
    try:
        def _download():
            req = urllib.request.Request(audio_url, headers={"User-Agent": "NodkeysBot/10.1"})
            with urllib.request.urlopen(req, timeout=300) as resp:
                with open(output_path, "wb") as f:
                    while True:
                        chunk = resp.read(65536)
                        if not chunk:
                            break
                        f.write(chunk)
            return output_path
        
        return await asyncio.to_thread(_download)
    except Exception as e:
        logger.error("Episode download error: %s", e)
        return None


# ─── Telegram Formatting ──────────────────────────────────────────────────

def format_search_results(podcasts: list) -> tuple:
    """Format podcast search results."""
    if not podcasts:
        return "🎙 Подкасты не найдены. Попробуйте другой запрос.", []
    
    msg = "🎙 <b>Результаты поиска:</b>\n\n"
    buttons = []
    
    for i, p in enumerate(podcasts[:5]):
        msg += f"<b>{i+1}. {p['title']}</b>\n"
        msg += f"   👤 {p['author']}\n"
        msg += f"   📂 {p['genre']} • {p['episode_count']} эпизодов\n\n"
        
        buttons.append([{"text": f"📻 {p['title'][:30]}", 
                        "callback_data": f"pod:view:{p['id']}"}])
    
    return msg, buttons


def format_podcast_detail(podcast: dict, episodes: list = None) -> tuple:
    """Format podcast detail view with episodes."""
    msg = f"🎙 <b>{podcast['title']}</b>\n"
    msg += f"👤 {podcast['author']}\n"
    msg += f"📂 {podcast['genre']}\n\n"
    
    if podcast.get("description"):
        desc = podcast["description"][:300]
        msg += f"<i>{desc}</i>\n\n"
    
    buttons = []
    
    if episodes:
        msg += "<b>Последние эпизоды:</b>\n\n"
        for i, ep in enumerate(episodes[:5]):
            duration = ep.get("duration", "?")
            msg += f"  {i+1}. {ep['title'][:50]}\n"
            msg += f"     ⏱ {duration}\n\n"
            
            buttons.append([{"text": f"▶️ {ep['title'][:25]}", 
                           "callback_data": f"pod:play:{podcast['id']}:{i}"}])
    
    # Subscribe/unsubscribe button
    data = _load_podcasts()
    is_subscribed = any(s["id"] == podcast["id"] for s in data["subscriptions"])
    
    if is_subscribed:
        buttons.append([{"text": "❌ Отписаться", "callback_data": f"pod:unsub:{podcast['id']}"}])
    else:
        buttons.append([{"text": "➕ Подписаться", "callback_data": f"pod:sub:{podcast['id']}"}])
    
    buttons.append([{"text": "🔙 Назад", "callback_data": "pod:main"}])
    
    return msg, buttons


def format_subscriptions_message() -> tuple:
    """Format subscriptions overview."""
    subs = get_subscriptions()
    queue = get_queue()
    
    msg = "🎙 <b>Мои подкасты</b>\n\n"
    
    if not subs:
        msg += "Нет подписок. Найдите подкаст:\n"
        msg += "/podcast <i>название</i>\n"
        return msg, []
    
    msg += f"<b>Подписки ({len(subs)}):</b>\n"
    for s in subs:
        msg += f"  📻 {s['title']}\n"
    
    if queue:
        msg += f"\n<b>Очередь ({len(queue)}):</b>\n"
        for ep in queue[:3]:
            msg += f"  ▶️ {ep['podcast']}: {ep['title'][:30]}\n"
    
    buttons = [
        [{"text": "🔍 Найти подкаст", "callback_data": "pod:search"},
         {"text": "📋 Очередь", "callback_data": "pod:queue"}],
        [{"text": "🔄 Проверить новые", "callback_data": "pod:check_new"}]
    ]
    
    for s in subs[:5]:
        buttons.append([{"text": f"📻 {s['title'][:30]}", 
                        "callback_data": f"pod:view:{s['id']}"}])
    
    return msg, buttons
