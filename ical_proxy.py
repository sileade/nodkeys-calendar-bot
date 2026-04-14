"""
iCal Proxy - Serves iCloud CalDAV events as .ics format for Homepage calendar widget.
Runs alongside the calendar bot on port 8086.
"""

import os
import json
import logging
import threading
import time
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from zoneinfo import ZoneInfo

import caldav

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ical-proxy")

ICLOUD_USERNAME = os.environ.get("ICLOUD_USERNAME", "slilea@icloud.com")
ICLOUD_PASSWORD = os.environ.get("ICLOUD_PASSWORD", "")
CALDAV_URL = "https://caldav.icloud.com"
TZ = ZoneInfo("Europe/Kyiv")

# Cache
_ical_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL = 300  # 5 minutes


def fold_ical_line(line):
    """Fold long lines per RFC 5545 (max 75 octets per line)."""
    encoded = line.encode("utf-8")
    if len(encoded) <= 75:
        return line
    
    result = []
    # First line: up to 75 bytes
    chunk = encoded[:75]
    # Make sure we don't split a multi-byte character
    try:
        first = chunk.decode("utf-8")
    except UnicodeDecodeError:
        # Back off until we get a valid decode
        for i in range(1, 4):
            try:
                first = encoded[:75-i].decode("utf-8")
                chunk = encoded[:75-i]
                break
            except UnicodeDecodeError:
                continue
    
    result.append(first)
    pos = len(chunk)
    
    # Continuation lines: space + up to 74 bytes
    while pos < len(encoded):
        chunk = encoded[pos:pos+74]
        try:
            part = chunk.decode("utf-8")
        except UnicodeDecodeError:
            for i in range(1, 4):
                try:
                    part = encoded[pos:pos+74-i].decode("utf-8")
                    chunk = encoded[pos:pos+74-i]
                    break
                except UnicodeDecodeError:
                    continue
        result.append(" " + part)
        pos += len(chunk)
    
    return "\r\n".join(result)


def escape_ical_text(text):
    """Escape text for iCal format per RFC 5545."""
    if not text:
        return ""
    # Replace actual newlines with \n
    text = text.replace("\r\n", "\\n").replace("\r", "\\n").replace("\n", "\\n")
    # Escape commas and semicolons
    text = text.replace(";", "\\;")
    # Don't double-escape commas that are already escaped
    if "\\," not in text:
        text = text.replace(",", "\\,")
    return text


def fetch_ical_data():
    """Fetch all calendar events from iCloud and return as iCal string."""
    try:
        client = caldav.DAVClient(url=CALDAV_URL, username=ICLOUD_USERNAME, password=ICLOUD_PASSWORD)
        principal = client.principal()
        calendars = principal.calendars()

        now = datetime.now(TZ)
        start = now - timedelta(days=30)
        end = now + timedelta(days=365)

        lines = []
        lines.append("BEGIN:VCALENDAR")
        lines.append("VERSION:2.0")
        lines.append("PRODID:-//Nodkeys//Calendar Bot//EN")
        lines.append("CALSCALE:GREGORIAN")
        lines.append("METHOD:PUBLISH")
        lines.append("X-WR-TIMEZONE:Europe/Kyiv")

        for cal in calendars:
            cal_name = cal.name or "Unknown"
            if "Напоминания" in cal_name:
                continue  # Skip read-only reminders

            try:
                events = cal.search(start=start, end=end, event=True, expand=True)
                for event in events:
                    try:
                        vcal = event.vobject_instance
                        for component in vcal.contents.get("vevent", []):
                            lines.append("BEGIN:VEVENT")
                            
                            if hasattr(component, "uid"):
                                lines.append(fold_ical_line(f"UID:{component.uid.value}"))
                            
                            if hasattr(component, "summary"):
                                summary = escape_ical_text(component.summary.value)
                                lines.append(fold_ical_line(f"SUMMARY:{summary}"))
                            
                            if hasattr(component, "description"):
                                desc = escape_ical_text(component.description.value)
                                lines.append(fold_ical_line(f"DESCRIPTION:{desc}"))
                            
                            if hasattr(component, "dtstart"):
                                dt = component.dtstart.value
                                if hasattr(dt, "strftime"):
                                    if hasattr(dt, "hour"):
                                        lines.append(f"DTSTART:{dt.strftime('%Y%m%dT%H%M%S')}")
                                    else:
                                        lines.append(f"DTSTART;VALUE=DATE:{dt.strftime('%Y%m%d')}")
                            
                            if hasattr(component, "dtend"):
                                dt = component.dtend.value
                                if hasattr(dt, "strftime"):
                                    if hasattr(dt, "hour"):
                                        lines.append(f"DTEND:{dt.strftime('%Y%m%dT%H%M%S')}")
                                    else:
                                        lines.append(f"DTEND;VALUE=DATE:{dt.strftime('%Y%m%d')}")
                            
                            if hasattr(component, "location"):
                                loc = escape_ical_text(component.location.value)
                                lines.append(fold_ical_line(f"LOCATION:{loc}"))
                            
                            # Add DTSTAMP (required by RFC)
                            lines.append(f"DTSTAMP:{now.strftime('%Y%m%dT%H%M%SZ')}")
                            
                            # Calendar name as category
                            cat_name = escape_ical_text(cal_name.strip())
                            lines.append(fold_ical_line(f"CATEGORIES:{cat_name}"))
                            
                            lines.append("END:VEVENT")
                    except Exception as e:
                        logger.warning("Error parsing event: %s", e)
                        continue
            except Exception as e:
                logger.warning("Error fetching events from %s: %s", cal_name, e)
                continue

        lines.append("END:VCALENDAR")
        return "\r\n".join(lines)

    except Exception as e:
        logger.error("Failed to fetch iCloud calendars: %s", e)
        return None


def get_cached_ical():
    """Get cached iCal data or fetch fresh."""
    with _cache_lock:
        cached = _ical_cache.get("data")
        cached_time = _ical_cache.get("time", 0)
        
        if cached and (time.time() - cached_time) < CACHE_TTL:
            return cached
    
    data = fetch_ical_data()
    if data:
        with _cache_lock:
            _ical_cache["data"] = data
            _ical_cache["time"] = time.time()
    return data


class ICalHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/calendar.ics") or self.path == "/":
            ical_data = get_cached_ical()
            if ical_data:
                self.send_response(200)
                self.send_header("Content-Type", "text/calendar; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
                self.end_headers()
                self.wfile.write(ical_data.encode("utf-8"))
            else:
                self.send_response(503)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"Failed to fetch calendar data")
        elif self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok", "service": "ical-proxy"}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress access logs


def run_server(port=8086):
    server = HTTPServer(("0.0.0.0", port), ICalHandler)
    logger.info("iCal proxy server started on port %d", port)
    server.serve_forever()


if __name__ == "__main__":
    run_server()
