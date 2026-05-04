"""
family.py — Family Access Module for Nodkeys Bot v10.1

Features:
- Shared family calendar
- Shopping lists (collaborative)
- Family task assignment
- Shared reminders
- Family member management (invite by Telegram ID)
- Activity feed
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
FAMILY_FILE = os.path.join(DATA_DIR, "family.json")


# ─── Storage ───────────────────────────────────────────────────────────────

def _load_family() -> dict:
    """Load family data."""
    try:
        if os.path.exists(FAMILY_FILE):
            with open(FAMILY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error("Failed to load family data: %s", e)
    return {
        "members": [],
        "owner_id": None,
        "lists": [],
        "tasks": [],
        "events": [],
        "activity": [],
        "settings": {"name": "Семья", "max_members": 5}
    }


def _save_family(data: dict):
    """Save family data."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(FAMILY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _add_activity(data: dict, actor: str, action: str, details: str = ""):
    """Add activity to feed."""
    data["activity"].append({
        "actor": actor,
        "action": action,
        "details": details,
        "timestamp": datetime.now(TIMEZONE).isoformat()
    })
    # Keep last 100 activities
    data["activity"] = data["activity"][-100:]


# ─── Family Management ─────────────────────────────────────────────────────

def create_family(owner_id: int, owner_name: str, family_name: str = "Семья") -> dict:
    """Create a new family group."""
    data = _load_family()
    
    if data["owner_id"]:
        return data  # Already exists
    
    data["owner_id"] = owner_id
    data["settings"]["name"] = family_name
    data["members"] = [{
        "user_id": owner_id,
        "name": owner_name,
        "role": "owner",
        "joined": datetime.now(TIMEZONE).isoformat()
    }]
    
    _add_activity(data, owner_name, "created_family", family_name)
    _save_family(data)
    return data


def invite_member(user_id: int, name: str, invited_by: str) -> dict:
    """Invite a family member."""
    data = _load_family()
    
    # Check if already a member
    for m in data["members"]:
        if m["user_id"] == user_id:
            return {"error": "already_member"}
    
    # Check max members
    if len(data["members"]) >= data["settings"]["max_members"]:
        return {"error": "max_members"}
    
    member = {
        "user_id": user_id,
        "name": name,
        "role": "member",
        "joined": datetime.now(TIMEZONE).isoformat()
    }
    data["members"].append(member)
    _add_activity(data, invited_by, "invited", name)
    _save_family(data)
    return member


def remove_member(user_id: int, removed_by: str) -> bool:
    """Remove a family member."""
    data = _load_family()
    
    for i, m in enumerate(data["members"]):
        if m["user_id"] == user_id and m["role"] != "owner":
            removed_name = m["name"]
            data["members"].pop(i)
            _add_activity(data, removed_by, "removed", removed_name)
            _save_family(data)
            return True
    return False


def get_members() -> list:
    """Get all family members."""
    data = _load_family()
    return data["members"]


def is_family_member(user_id: int) -> bool:
    """Check if user is a family member."""
    data = _load_family()
    return any(m["user_id"] == user_id for m in data["members"])


# ─── Shopping Lists ────────────────────────────────────────────────────────

def create_list(name: str, created_by: str) -> dict:
    """Create a new shared list."""
    data = _load_family()
    
    new_list = {
        "id": f"list_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "name": name,
        "items": [],
        "created_by": created_by,
        "created": datetime.now(TIMEZONE).isoformat(),
        "pinned": False
    }
    data["lists"].append(new_list)
    _add_activity(data, created_by, "created_list", name)
    _save_family(data)
    return new_list


def add_list_item(list_id: str, item: str, added_by: str, quantity: str = "") -> dict:
    """Add item to a shared list."""
    data = _load_family()
    
    for lst in data["lists"]:
        if lst["id"] == list_id:
            new_item = {
                "id": f"item_{len(lst['items'])}_{datetime.now().strftime('%H%M%S')}",
                "text": item,
                "quantity": quantity,
                "checked": False,
                "added_by": added_by,
                "added": datetime.now(TIMEZONE).isoformat()
            }
            lst["items"].append(new_item)
            _add_activity(data, added_by, "added_item", f"{item} → {lst['name']}")
            _save_family(data)
            return new_item
    return None


def check_list_item(list_id: str, item_id: str, checked_by: str) -> bool:
    """Check/uncheck an item in a list."""
    data = _load_family()
    
    for lst in data["lists"]:
        if lst["id"] == list_id:
            for item in lst["items"]:
                if item["id"] == item_id:
                    item["checked"] = not item["checked"]
                    item["checked_by"] = checked_by
                    _save_family(data)
                    return True
    return False


def get_lists() -> list:
    """Get all shared lists."""
    data = _load_family()
    return data["lists"]


def get_list(list_id: str) -> dict:
    """Get a specific list."""
    data = _load_family()
    for lst in data["lists"]:
        if lst["id"] == list_id:
            return lst
    return None


def delete_list(list_id: str) -> bool:
    """Delete a list."""
    data = _load_family()
    data["lists"] = [lst for lst in data["lists"] if lst["id"] != list_id]
    _save_family(data)
    return True


# ─── Family Tasks ──────────────────────────────────────────────────────────

def assign_task(title: str, assigned_to: int, assigned_by: str,
                due_date: str = None, priority: str = "medium") -> dict:
    """Assign a task to a family member."""
    data = _load_family()
    
    # Find member name
    member_name = "?"
    for m in data["members"]:
        if m["user_id"] == assigned_to:
            member_name = m["name"]
            break
    
    task = {
        "id": f"ftask_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "title": title,
        "assigned_to": assigned_to,
        "assigned_to_name": member_name,
        "assigned_by": assigned_by,
        "due_date": due_date,
        "priority": priority,
        "status": "pending",  # pending, in_progress, done
        "created": datetime.now(TIMEZONE).isoformat(),
        "completed": None
    }
    data["tasks"].append(task)
    _add_activity(data, assigned_by, "assigned_task", f"{title} → {member_name}")
    _save_family(data)
    return task


def complete_task(task_id: str, completed_by: str) -> bool:
    """Mark a family task as done."""
    data = _load_family()
    
    for task in data["tasks"]:
        if task["id"] == task_id:
            task["status"] = "done"
            task["completed"] = datetime.now(TIMEZONE).isoformat()
            _add_activity(data, completed_by, "completed_task", task["title"])
            _save_family(data)
            return True
    return False


def get_family_tasks(user_id: int = None, status: str = None) -> list:
    """Get family tasks, optionally filtered."""
    data = _load_family()
    tasks = data["tasks"]
    
    if user_id:
        tasks = [t for t in tasks if t["assigned_to"] == user_id]
    if status:
        tasks = [t for t in tasks if t["status"] == status]
    
    return sorted(tasks, key=lambda x: x.get("due_date") or "9999", reverse=False)


# ─── Family Events ────────────────────────────────────────────────────────

def add_family_event(title: str, date: str, time: str = None,
                     created_by: str = "", notify_all: bool = True) -> dict:
    """Add a family event."""
    data = _load_family()
    
    event = {
        "id": f"fevent_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "title": title,
        "date": date,
        "time": time,
        "created_by": created_by,
        "notify_all": notify_all,
        "created": datetime.now(TIMEZONE).isoformat()
    }
    data["events"].append(event)
    _add_activity(data, created_by, "added_event", f"{title} ({date})")
    _save_family(data)
    return event


def get_upcoming_family_events(days: int = 7) -> list:
    """Get upcoming family events."""
    data = _load_family()
    now = datetime.now(TIMEZONE)
    cutoff = (now + timedelta(days=days)).strftime("%Y-%m-%d")
    today = now.strftime("%Y-%m-%d")
    
    return [e for e in data["events"] if today <= e["date"] <= cutoff]


# ─── Telegram Formatting ──────────────────────────────────────────────────

def format_family_overview() -> tuple:
    """Format family overview message."""
    data = _load_family()
    
    if not data["owner_id"]:
        msg = "👨‍👩‍👧‍👦 <b>Семейный доступ</b>\n\n"
        msg += "Семья ещё не создана.\n"
        msg += "Создайте семью, чтобы делиться списками, задачами и событиями.\n"
        buttons = [[{"text": "➕ Создать семью", "callback_data": "fam:create"}]]
        return msg, buttons
    
    family_name = data["settings"]["name"]
    members = data["members"]
    
    msg = f"👨‍👩‍👧‍👦 <b>{family_name}</b>\n\n"
    
    # Members
    msg += f"<b>Участники ({len(members)}/{data['settings']['max_members']}):</b>\n"
    for m in members:
        role_emoji = "👑" if m["role"] == "owner" else "👤"
        msg += f"  {role_emoji} {m['name']}\n"
    msg += "\n"
    
    # Active lists
    active_lists = [lst for lst in data["lists"] if any(not i["checked"] for i in lst["items"])]
    if active_lists:
        msg += f"📝 <b>Активные списки ({len(active_lists)}):</b>\n"
        for lst in active_lists[:3]:
            unchecked = sum(1 for i in lst["items"] if not i["checked"])
            msg += f"  • {lst['name']} ({unchecked} пунктов)\n"
        msg += "\n"
    
    # Pending tasks
    pending_tasks = [t for t in data["tasks"] if t["status"] == "pending"]
    if pending_tasks:
        msg += f"✅ <b>Задачи ({len(pending_tasks)}):</b>\n"
        for t in pending_tasks[:3]:
            msg += f"  • {t['title']} → {t['assigned_to_name']}\n"
        msg += "\n"
    
    # Upcoming events
    events = get_upcoming_family_events(7)
    if events:
        msg += f"📅 <b>Ближайшие события:</b>\n"
        for e in events[:3]:
            time_str = f" {e['time']}" if e.get("time") else ""
            msg += f"  • {e['date']}{time_str} — {e['title']}\n"
        msg += "\n"
    
    # Recent activity
    recent = data["activity"][-3:]
    if recent:
        msg += "<b>Последние действия:</b>\n"
        for a in reversed(recent):
            msg += f"  • {a['actor']}: {a['action']} {a.get('details', '')}\n"
    
    # Buttons
    buttons = [
        [{"text": "📝 Списки", "callback_data": "fam:lists"},
         {"text": "✅ Задачи", "callback_data": "fam:tasks"}],
        [{"text": "📅 События", "callback_data": "fam:events"},
         {"text": "👥 Участники", "callback_data": "fam:members"}],
        [{"text": "➕ Добавить в список", "callback_data": "fam:add_item"}]
    ]
    
    return msg, buttons


def format_list_message(list_id: str) -> tuple:
    """Format a shopping/todo list."""
    lst = get_list(list_id)
    if not lst:
        return "❌ Список не найден", []
    
    msg = f"📝 <b>{lst['name']}</b>\n"
    msg += f"<i>Создал: {lst['created_by']}</i>\n\n"
    
    if not lst["items"]:
        msg += "<i>Список пуст</i>\n"
    else:
        for item in lst["items"]:
            check = "✅" if item["checked"] else "⬜"
            qty = f" ({item['quantity']})" if item.get("quantity") else ""
            msg += f"{check} {item['text']}{qty}\n"
        
        checked = sum(1 for i in lst["items"] if i["checked"])
        total = len(lst["items"])
        msg += f"\n<i>{checked}/{total} выполнено</i>"
    
    buttons = []
    # Add uncheck buttons for unchecked items
    for item in lst["items"]:
        if not item["checked"]:
            buttons.append([{"text": f"✅ {item['text'][:25]}", 
                           "callback_data": f"fam:check:{list_id}:{item['id']}"}])
    
    buttons.append([
        {"text": "➕ Добавить", "callback_data": f"fam:add_to:{list_id}"},
        {"text": "🗑 Очистить ✅", "callback_data": f"fam:clear:{list_id}"}
    ])
    buttons.append([{"text": "🔙 Назад", "callback_data": "fam:lists"}])
    
    return msg, buttons
