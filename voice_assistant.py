"""
voice_assistant.py — Voice Assistant for Nodkeys Bot v10.1

Features:
- Text-to-Speech (TTS) for bot responses
- Voice message transcription (STT)
- Voice briefing (morning/evening summary as audio)
- Configurable voice and speed
- Smart response: short answers as text, long as voice
"""

import os
import json
import asyncio
import logging
import tempfile
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

logger = logging.getLogger(__name__)

TIMEZONE = ZoneInfo(os.environ.get("TIMEZONE", "Europe/Moscow"))
DATA_DIR = os.environ.get("DATA_DIR", "/app/data")
VOICE_CACHE_DIR = os.path.join(DATA_DIR, "voice_cache")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

# TTS Settings
DEFAULT_VOICE = "alloy"  # alloy, echo, fable, onyx, nova, shimmer
DEFAULT_SPEED = 1.0
MAX_TTS_LENGTH = 4096  # Max characters for TTS
VOICE_THRESHOLD = 200  # Characters threshold to auto-send as voice


# ─── Configuration ─────────────────────────────────────────────────────────

def get_voice_settings() -> dict:
    """Get voice assistant settings."""
    settings_file = os.path.join(DATA_DIR, "voice_settings.json")
    try:
        if os.path.exists(settings_file):
            with open(settings_file, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return {
        "enabled": False,
        "voice": DEFAULT_VOICE,
        "speed": DEFAULT_SPEED,
        "auto_voice_replies": False,  # Auto-send long replies as voice
        "voice_briefing": False,  # Morning briefing as voice
        "transcribe_voice": True  # Auto-transcribe incoming voice messages
    }


def save_voice_settings(settings: dict):
    """Save voice assistant settings."""
    os.makedirs(DATA_DIR, exist_ok=True)
    settings_file = os.path.join(DATA_DIR, "voice_settings.json")
    with open(settings_file, "w") as f:
        json.dump(settings, f, ensure_ascii=False, indent=2)


# ─── Text-to-Speech ───────────────────────────────────────────────────────

async def text_to_speech(text: str, voice: str = None, speed: float = None) -> str:
    """
    Convert text to speech using OpenAI TTS API.
    Returns path to the generated audio file.
    """
    if not OPENAI_API_KEY:
        logger.warning("OpenAI API key not set, TTS unavailable")
        return None
    
    settings = get_voice_settings()
    voice = voice or settings.get("voice", DEFAULT_VOICE)
    speed = speed or settings.get("speed", DEFAULT_SPEED)
    
    # Truncate if too long
    if len(text) > MAX_TTS_LENGTH:
        text = text[:MAX_TTS_LENGTH] + "..."
    
    # Check cache
    cache_key = hashlib.md5(f"{text}:{voice}:{speed}".encode()).hexdigest()
    os.makedirs(VOICE_CACHE_DIR, exist_ok=True)
    cache_path = os.path.join(VOICE_CACHE_DIR, f"{cache_key}.ogg")
    
    if os.path.exists(cache_path):
        return cache_path
    
    try:
        import httpx
        
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                "https://api.openai.com/v1/audio/speech",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "tts-1",
                    "input": text,
                    "voice": voice,
                    "speed": speed,
                    "response_format": "opus"
                }
            )
            
            if response.status_code == 200:
                with open(cache_path, "wb") as f:
                    f.write(response.content)
                return cache_path
            else:
                logger.error("TTS API error: %s %s", response.status_code, response.text[:200])
                return None
    except ImportError:
        # Fallback to urllib
        import urllib.request
        
        req_data = json.dumps({
            "model": "tts-1",
            "input": text,
            "voice": voice,
            "speed": speed,
            "response_format": "opus"
        }).encode()
        
        req = urllib.request.Request(
            "https://api.openai.com/v1/audio/speech",
            data=req_data,
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json"
            }
        )
        
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                with open(cache_path, "wb") as f:
                    f.write(resp.read())
                return cache_path
        except Exception as e:
            logger.error("TTS urllib error: %s", e)
            return None
    except Exception as e:
        logger.error("TTS error: %s", e)
        return None


async def speech_to_text(audio_path: str) -> str:
    """
    Transcribe audio file using OpenAI Whisper API.
    Returns transcribed text.
    """
    if not OPENAI_API_KEY:
        logger.warning("OpenAI API key not set, STT unavailable")
        return None
    
    try:
        import httpx
        
        async with httpx.AsyncClient(timeout=120) as client:
            with open(audio_path, "rb") as f:
                response = await client.post(
                    "https://api.openai.com/v1/audio/transcriptions",
                    headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                    files={"file": (os.path.basename(audio_path), f, "audio/ogg")},
                    data={"model": "whisper-1", "language": "ru"}
                )
            
            if response.status_code == 200:
                result = response.json()
                return result.get("text", "")
            else:
                logger.error("STT API error: %s", response.status_code)
                return None
    except ImportError:
        # Fallback: use local whisper if available
        try:
            import subprocess
            result = subprocess.run(
                ["manus-speech-to-text", audio_path],
                capture_output=True, text=True, timeout=120
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        return None
    except Exception as e:
        logger.error("STT error: %s", e)
        return None


# ─── Smart Voice Response ──────────────────────────────────────────────────

def should_send_as_voice(text: str, user_sent_voice: bool = False) -> bool:
    """Determine if response should be sent as voice message."""
    settings = get_voice_settings()
    
    if not settings.get("enabled"):
        return False
    
    # If user sent voice, reply with voice
    if user_sent_voice and settings.get("auto_voice_replies"):
        return True
    
    # If auto-voice is on and text is long
    if settings.get("auto_voice_replies") and len(text) > VOICE_THRESHOLD:
        return True
    
    return False


def clean_text_for_tts(text: str) -> str:
    """Clean HTML/markdown formatting from text for TTS."""
    import re
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Remove markdown bold/italic
    text = re.sub(r'\*\*([^*]+)\*\*', r'\1', text)
    text = re.sub(r'\*([^*]+)\*', r'\1', text)
    text = re.sub(r'__([^_]+)__', r'\1', text)
    # Remove URLs
    text = re.sub(r'https?://\S+', '', text)
    # Remove emoji (optional, they're usually skipped by TTS)
    # Clean up extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text


# ─── Voice Briefing ───────────────────────────────────────────────────────

async def generate_voice_briefing(briefing_text: str) -> str:
    """Generate voice version of morning/evening briefing."""
    settings = get_voice_settings()
    
    if not settings.get("voice_briefing"):
        return None
    
    clean_text = clean_text_for_tts(briefing_text)
    if not clean_text:
        return None
    
    return await text_to_speech(clean_text)


# ─── Cache Management ──────────────────────────────────────────────────────

def cleanup_voice_cache(max_age_days: int = 7):
    """Remove old cached voice files."""
    if not os.path.exists(VOICE_CACHE_DIR):
        return
    
    now = datetime.now().timestamp()
    max_age = max_age_days * 86400
    
    removed = 0
    for f in os.listdir(VOICE_CACHE_DIR):
        path = os.path.join(VOICE_CACHE_DIR, f)
        if os.path.isfile(path):
            age = now - os.path.getmtime(path)
            if age > max_age:
                os.remove(path)
                removed += 1
    
    if removed:
        logger.info("Cleaned up %d old voice cache files", removed)


# ─── Telegram Formatting ──────────────────────────────────────────────────

def format_voice_settings_message() -> tuple:
    """Format voice settings message with toggle buttons."""
    settings = get_voice_settings()
    
    voices = {
        "alloy": "Alloy (нейтральный)",
        "echo": "Echo (мужской)",
        "fable": "Fable (британский)",
        "onyx": "Onyx (глубокий)",
        "nova": "Nova (женский)",
        "shimmer": "Shimmer (мягкий)"
    }
    
    enabled = "✅" if settings["enabled"] else "❌"
    auto_voice = "✅" if settings.get("auto_voice_replies") else "❌"
    briefing = "✅" if settings.get("voice_briefing") else "❌"
    transcribe = "✅" if settings.get("transcribe_voice") else "❌"
    
    msg = "🎙 <b>Голосовой ассистент</b>\n\n"
    msg += f"<b>Статус:</b> {enabled}\n"
    msg += f"<b>Голос:</b> {voices.get(settings['voice'], settings['voice'])}\n"
    msg += f"<b>Скорость:</b> {settings['speed']}x\n\n"
    msg += f"<b>Авто-озвучка:</b> {auto_voice}\n"
    msg += f"<b>Голосовой брифинг:</b> {briefing}\n"
    msg += f"<b>Транскрибация:</b> {transcribe}\n"
    
    buttons = [
        [{"text": f"{'🔴 Выкл' if settings['enabled'] else '🟢 Вкл'}", 
          "callback_data": "voice:toggle:enabled"}],
        [{"text": "🔊 Сменить голос", "callback_data": "voice:change_voice"},
         {"text": "⏩ Скорость", "callback_data": "voice:speed"}],
        [{"text": f"{'❌' if settings.get('auto_voice_replies') else '✅'} Авто-озвучка",
          "callback_data": "voice:toggle:auto_voice_replies"},
         {"text": f"{'❌' if settings.get('voice_briefing') else '✅'} Брифинг",
          "callback_data": "voice:toggle:voice_briefing"}],
        [{"text": "🧪 Тест голоса", "callback_data": "voice:test"}],
        [{"text": "🔙 Назад", "callback_data": "set:main"}]
    ]
    
    return msg, buttons
