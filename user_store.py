"""
Nodkeys User Store
Manages user profiles, encrypted settings, and subscription status.

Storage: JSON file (can be migrated to SQLite/Postgres later)
All personal data is encrypted per-user via crypto module.
"""

import os
import json
import logging
import threading
from datetime import datetime, timedelta
from typing import Optional

from crypto import encrypt_json, decrypt_json, generate_api_token, get_user_fingerprint

logger = logging.getLogger(__name__)

# Storage path
USERS_FILE = os.environ.get("USERS_FILE", "/app/data/users.json")

# Thread-safe lock for file operations
_lock = threading.Lock()


def _load_users() -> dict:
    """Load users database from file."""
    if not os.path.exists(USERS_FILE):
        return {}
    try:
        with open(USERS_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error("Failed to load users: %s", e)
        return {}


def _save_users(users: dict):
    """Save users database to file."""
    os.makedirs(os.path.dirname(USERS_FILE), exist_ok=True)
    with open(USERS_FILE, 'w') as f:
        json.dump(users, f, ensure_ascii=False, indent=2)


def get_user(user_id: int) -> Optional[dict]:
    """Get user profile (public fields only, not decrypted)."""
    with _lock:
        users = _load_users()
        return users.get(str(user_id))


def create_user(user_id: int, username: str = "", first_name: str = "") -> dict:
    """Create a new user profile."""
    with _lock:
        users = _load_users()
        uid = str(user_id)
        if uid in users:
            return users[uid]
        
        user = {
            "user_id": user_id,
            "username": username,
            "first_name": first_name,
            "created_at": datetime.now().isoformat(),
            "subscription": {
                "plan": "free",
                "active_until": None,
                "payment_method": None,
            },
            "onboarding_complete": False,
            "encrypted_settings": None,  # encrypted JSON with integrations
            "api_token": generate_api_token(user_id),
            "fingerprint": get_user_fingerprint(user_id),
            "custom_bot_token": None,  # user's own bot token (encrypted)
        }
        users[uid] = user
        _save_users(users)
        return user


def update_user(user_id: int, **kwargs) -> dict:
    """Update user profile fields."""
    with _lock:
        users = _load_users()
        uid = str(user_id)
        if uid not in users:
            return {}
        for key, value in kwargs.items():
            users[uid][key] = value
        _save_users(users)
        return users[uid]


def get_user_settings(user_id: int) -> dict:
    """Get decrypted user settings (integrations, preferences)."""
    user = get_user(user_id)
    if not user or not user.get("encrypted_settings"):
        return _default_settings()
    try:
        return decrypt_json(user_id, user["encrypted_settings"])
    except Exception as e:
        logger.error("Failed to decrypt settings for user %d: %s", user_id, e)
        return _default_settings()


def save_user_settings(user_id: int, settings: dict):
    """Encrypt and save user settings."""
    encrypted = encrypt_json(user_id, settings)
    update_user(user_id, encrypted_settings=encrypted)


def _default_settings() -> dict:
    """Default settings template for new users."""
    return {
        "integrations": {
            "calendar": {
                "enabled": False,
                "caldav_url": "",
                "username": "",
                "password": "",  # app-specific password
                "calendars": {},  # name → url mapping
            },
            "kindle": {
                "enabled": False,
                "email": "",
            },
            "notes": {
                "enabled": False,
                "imap_server": "",
                "username": "",
                "password": "",
            },
        },
        "preferences": {
            "timezone": "Europe/Moscow",
            "language": "ru",
            "notifications": True,
            "daily_report": True,
            "daily_report_time": "09:00",
        },
        "finance": {
            "currency": "RUB",
            "categories": ["Еда", "Транспорт", "Развлечения", "Здоровье", "Дом", "Другое"],
            "monthly_budget": None,
        },
    }


# ──────────────────── Subscription Management ────────────────────

PLANS = {
    "free": {
        "name": "Free",
        "price": 0,
        "limits": {
            "audiobooks_per_month": 1,
            "tasks_per_day": 3,
            "kindle_sends_per_month": 3,
            "shortcuts": False,
            "custom_bot": False,
        }
    },
    "pro": {
        "name": "Pro",
        "price_rub": 299,
        "price_stars": 150,  # ~299 RUB in Stars
        "limits": {
            "audiobooks_per_month": -1,  # unlimited
            "tasks_per_day": -1,
            "kindle_sends_per_month": -1,
            "shortcuts": True,
            "custom_bot": False,
        }
    },
    "pro_plus": {
        "name": "Pro+",
        "price_rub": 499,
        "price_stars": 250,
        "limits": {
            "audiobooks_per_month": -1,
            "tasks_per_day": -1,
            "kindle_sends_per_month": -1,
            "shortcuts": True,
            "custom_bot": True,
        }
    },
}


def is_subscription_active(user_id: int) -> bool:
    """Check if user has active paid subscription."""
    user = get_user(user_id)
    if not user:
        return False
    sub = user.get("subscription", {})
    if sub.get("plan") == "free":
        return False
    active_until = sub.get("active_until")
    if not active_until:
        return False
    try:
        return datetime.fromisoformat(active_until) > datetime.now()
    except Exception:
        return False


def get_user_plan(user_id: int) -> str:
    """Get user's current plan name."""
    user = get_user(user_id)
    if not user:
        return "free"
    return user.get("subscription", {}).get("plan", "free")


def activate_subscription(user_id: int, plan: str, months: int = 1, payment_method: str = "stars"):
    """Activate or extend subscription."""
    user = get_user(user_id)
    if not user:
        return
    
    sub = user.get("subscription", {})
    current_until = sub.get("active_until")
    
    # If extending existing subscription
    if current_until:
        try:
            start = max(datetime.fromisoformat(current_until), datetime.now())
        except Exception:
            start = datetime.now()
    else:
        start = datetime.now()
    
    new_until = start + timedelta(days=30 * months)
    
    update_user(user_id, subscription={
        "plan": plan,
        "active_until": new_until.isoformat(),
        "payment_method": payment_method,
    })


def check_limit(user_id: int, feature: str) -> bool:
    """Check if user can use a feature based on their plan limits.
    
    Returns True if allowed, False if limit reached.
    """
    plan_name = get_user_plan(user_id)
    if is_subscription_active(user_id):
        plan_name = get_user_plan(user_id)
    else:
        plan_name = "free"
    
    plan = PLANS.get(plan_name, PLANS["free"])
    limit = plan["limits"].get(feature)
    
    if limit is None:
        return True
    if limit == -1:
        return True  # unlimited
    if isinstance(limit, bool):
        return limit
    
    # TODO: track usage counters per user per month
    return True  # for now, allow all


# ──────────────────── Custom Bot Management ────────────────────

def set_custom_bot_token(user_id: int, bot_token: str):
    """Store user's custom bot token (encrypted)."""
    from crypto import encrypt_data
    encrypted_token = encrypt_data(user_id, bot_token)
    update_user(user_id, custom_bot_token=encrypted_token)


def get_custom_bot_token(user_id: int) -> Optional[str]:
    """Get user's custom bot token (decrypted)."""
    from crypto import decrypt_data
    user = get_user(user_id)
    if not user or not user.get("custom_bot_token"):
        return None
    try:
        return decrypt_data(user_id, user["custom_bot_token"])
    except Exception:
        return None
