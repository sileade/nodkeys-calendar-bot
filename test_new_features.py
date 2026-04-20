"""
Tests for new v5.0 features: X-Ray, Kindle Clippings, URL→Kindle
Run: python3 test_new_features.py
"""
import sys
import os
import json

# Add project to path
sys.path.insert(0, os.path.dirname(__file__))

PASS = 0
FAIL = 0

def test(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name}: {detail}")


# ═══════════════════════════════════════════
# Test 1: parse_kindle_clippings
# ═══════════════════════════════════════════
print("\n📎 Test: parse_kindle_clippings")

# Import the function
from bot import parse_kindle_clippings

SAMPLE_CLIPPINGS = """\ufeffМастер и Маргарита (Михаил Булгаков)
- Ваша закладка на странице 42 | Добавлено: понедельник, 15 апреля 2024 г. 10:30:00

Рукописи не горят.
==========
Мастер и Маргарита (Михаил Булгаков)
- Ваша закладка на странице 55 | Добавлено: понедельник, 15 апреля 2024 г. 11:00:00

Никогда и ничего не просите! Никогда и ничего, и в особенности у тех, кто сильнее вас.
==========
1984 (George Orwell)
- Your Highlight on page 12 | Added on Monday, April 15, 2024 10:30:00 AM

Big Brother is watching you.
==========
1984 (George Orwell)
- Your Highlight on page 45 | Added on Monday, April 15, 2024 11:00:00 AM

War is peace. Freedom is slavery. Ignorance is strength.
==========
"""

books = parse_kindle_clippings(SAMPLE_CLIPPINGS)

test("Parses 2 books", len(books) == 2, f"got {len(books)}")
test("Булгаков has 2 highlights", 
     "Мастер и Маргарита (Михаил Булгаков)" in books and 
     len(books.get("Мастер и Маргарита (Михаил Булгаков)", [])) == 2,
     f"got {len(books.get('Мастер и Маргарита (Михаил Булгаков)', []))}")
test("Orwell has 2 highlights",
     "1984 (George Orwell)" in books and
     len(books.get("1984 (George Orwell)", [])) == 2)
test("First highlight correct",
     "Рукописи не горят." in books.get("Мастер и Маргарита (Михаил Булгаков)", []))
test("BOM stripped from title",
     not any(t.startswith("\ufeff") for t in books.keys()))

# Test empty input
empty = parse_kindle_clippings("")
test("Empty input returns empty dict", empty == {})

# Test duplicate handling
dup_clippings = """Book Title (Author)
- Highlight on page 1

Same highlight text
==========
Book Title (Author)
- Highlight on page 2

Same highlight text
==========
"""
dup_books = parse_kindle_clippings(dup_clippings)
test("Duplicates removed", len(dup_books.get("Book Title (Author)", [])) == 1)


# ═══════════════════════════════════════════
# Test 2: url_to_epub (HTML cleaning)
# ═══════════════════════════════════════════
print("\n🌐 Test: url_to_epub (HTML cleaning logic)")

from bot import url_to_epub, extract_urls, get_url_domain

test("extract_urls finds URL", 
     extract_urls("Check https://example.com/article here") == ["https://example.com/article"])
test("extract_urls strips trailing punctuation",
     extract_urls("Visit https://example.com/page.") == ["https://example.com/page"])
test("extract_urls handles multiple",
     len(extract_urls("https://a.com and https://b.com")) == 2)
test("get_url_domain works",
     get_url_domain("https://www.habr.com/article/123") == "habr.com")


# ═══════════════════════════════════════════
# Test 3: X-Ray prompt formatting
# ═══════════════════════════════════════════
print("\n🔬 Test: X-Ray prompt formatting")

from bot import XRAY_PROMPT

# Test prompt can be formatted without errors
try:
    formatted = XRAY_PROMPT.format(
        book_title="Мастер и Маргарита",
        author_part=" by Михаил Булгаков",
        author_line="\n✍️ Михаил Булгаков",
        progress_instruction="The reader has finished the book.",
    )
    test("XRAY_PROMPT formats correctly", True)
    test("Contains book title", "Мастер и Маргарита" in formatted)
    test("Contains author", "Михаил Булгаков" in formatted)
except Exception as e:
    test("XRAY_PROMPT formats correctly", False, str(e))

# Test with progress
try:
    formatted_progress = XRAY_PROMPT.format(
        book_title="1984",
        author_part=" by George Orwell",
        author_line="\n✍️ George Orwell",
        progress_instruction="The reader is at 50% of the book. DO NOT reveal spoilers.",
    )
    test("XRAY_PROMPT with progress formats", True)
except Exception as e:
    test("XRAY_PROMPT with progress formats", False, str(e))


# ═══════════════════════════════════════════
# Test 4: VERSION constant
# ═══════════════════════════════════════════
print("\n📌 Test: VERSION constant")

from bot import VERSION

test("VERSION is defined", VERSION is not None)
test("VERSION is 5.2", VERSION == "5.2", f"got '{VERSION}'")


# ═══════════════════════════════════════════
# Test 5: Claude system prompt contains new types
# ═══════════════════════════════════════════
print("\n🤖 Test: Claude system prompt")

from bot import SYSTEM_PROMPT

test("Prompt contains xray type", "xray" in SYSTEM_PROMPT)
test("Prompt contains url_to_kindle type", "url_to_kindle" in SYSTEM_PROMPT)
test("Prompt contains book_search type", "book_search" in SYSTEM_PROMPT)


# ═══════════════════════════════════════════
# Test 6: Per-user routing
# ═══════════════════════════════════════════
print("\n👥 Test: Per-user routing")

from bot import (
    get_user_routing, apply_calendar_override,
    GROUP_USERS, ALLOWED_CHAT_IDS
)

# Test GROUP_USERS parsing (env not set, should be empty)
test("GROUP_USERS is dict", isinstance(GROUP_USERS, dict))
test("ALLOWED_CHAT_IDS is set", isinstance(ALLOWED_CHAT_IDS, set))

# Test get_user_routing with mock user
class MockUser:
    def __init__(self, user_id, username=None, first_name=None, is_bot=False):
        self.id = user_id
        self.username = username
        self.first_name = first_name
        self.is_bot = is_bot

# With empty GROUP_USERS, should return None
test("get_user_routing returns None when no config",
     get_user_routing(MockUser(123, "testuser")) is None)

# Simulate GROUP_USERS config
import bot as bot_module
original_group_users = bot_module.GROUP_USERS.copy()

bot_module.GROUP_USERS = {
    "vera": {"display_name": "Вера", "calendar_rule": "family"},
    "seleadi": {"display_name": "Ilea", "calendar_rule": "auto"},
    "12345": {"display_name": "TestByID", "calendar_rule": "work"},
}

# Test username match
routing = get_user_routing(MockUser(999, "vera"))
test("Vera matched by username", routing is not None and routing["calendar_rule"] == "family")

# Test username case-insensitive
routing2 = get_user_routing(MockUser(999, "Vera"))
test("Vera matched case-insensitive", routing2 is not None and routing2["calendar_rule"] == "family")

# Test @seleadi match
routing3 = get_user_routing(MockUser(888, "seleadi"))
test("@seleadi matched", routing3 is not None and routing3["calendar_rule"] == "auto")

# Test user_id match
routing4 = get_user_routing(MockUser(12345, "unknown"))
test("User matched by ID", routing4 is not None and routing4["calendar_rule"] == "work")

# Test unknown user
routing5 = get_user_routing(MockUser(777, "stranger"))
test("Unknown user defaults to family", routing5 is not None and routing5.get("calendar_rule") == "family", f"got {routing5}")

# Test apply_calendar_override
data_work = {"type": "event", "calendar": "work", "title": "Test"}
overridden = apply_calendar_override(
    data_work.copy(),
    {"display_name": "Вера", "calendar_rule": "family"}
)
test("Override work→family for Vera", overridden["calendar"] == "family")

# Test auto rule doesn't override
data_work2 = {"type": "task", "calendar": "work", "title": "Test"}
not_overridden = apply_calendar_override(
    data_work2.copy(),
    {"display_name": "Ilea", "calendar_rule": "auto"}
)
test("Auto rule keeps Claude's choice", not_overridden["calendar"] == "work")

# Test non-calendar types not affected
data_note = {"type": "note", "calendar": "work", "title": "Test"}
note_result = apply_calendar_override(
    data_note.copy(),
    {"display_name": "Вера", "calendar_rule": "family"}
)
test("Note type not overridden", note_result["calendar"] == "work")

# Test None routing
data_none = {"type": "event", "calendar": "work", "title": "Test"}
none_result = apply_calendar_override(data_none.copy(), None)
test("None routing keeps original", none_result["calendar"] == "work")

# Restore original
bot_module.GROUP_USERS = original_group_users


# ═══════════════════════════════════════════
# Test 7: ical_proxy.py structure
# ═══════════════════════════════════════════
print("\n📅 Test: ical_proxy.py")

import ical_proxy

test("ical_proxy has run_server", hasattr(ical_proxy, 'run_server'))
test("ical_proxy has ICalHandler", hasattr(ical_proxy, 'ICalHandler'))


# ═══════════════════════════════════════════
# Test 7: Import checks
# ═══════════════════════════════════════════
print("\n📦 Test: Import checks")

try:
    from bs4 import BeautifulSoup
    test("beautifulsoup4 available", True)
except ImportError:
    test("beautifulsoup4 available", False, "pip install beautifulsoup4")

try:
    import anthropic
    test("anthropic available", True)
except ImportError:
    test("anthropic available", False, "pip install anthropic")


# ═══════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════
print(f"\n{'='*50}")
print(f"Results: {PASS} passed, {FAIL} failed, {PASS+FAIL} total")
if FAIL == 0:
    print("🎉 All tests passed!")
else:
    print(f"⚠️  {FAIL} test(s) failed")
    sys.exit(1)
