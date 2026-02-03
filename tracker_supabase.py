#!/usr/bin/env python3
"""
Email Response Time Tracker - Supabase Edition
Fetches email response times and stores them in Supabase.

Usage:
    python3 tracker_supabase.py                    # Run for all tracked users
    python3 tracker_supabase.py --user email@x.com # Run for specific user
    python3 tracker_supabase.py --backfill         # Fetch more history (2000 threads)
    python3 tracker_supabase.py --user email@x.com --backfill  # Backfill specific user
"""

import argparse
import base64
import os
import threading
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta, time as dt_time
from email.utils import parsedate_to_datetime
from typing import Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from supabase import create_client, Client
from tqdm import tqdm

warnings.filterwarnings("ignore")

# Load environment variables
load_dotenv()

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")

MAX_THREADS_DEFAULT = 500   # Normal run
MAX_THREADS_BACKFILL = 2000 # Backfill run (covers ~90 days)
MAX_WORKERS = 20            # Parallel workers

# Noise filters - emails to exclude
EXCLUDE = [
    'mailer-daemon', 'postmaster', 'mixmax.com', 'notifications@',
    'noreply', 'no-reply', 'stellaconnect', 'calendar-notification',
    'newsletter', 'stripe.com', 'calsavers.com'
]

# Default internal domains (fallback if DB fetch fails)
DEFAULT_INTERNAL_DOMAINS = [
    'lumiere.education',
    'ladderinternships.com',
    'veritasai.com',
    'horizoninspires.com',
    'youngfounderslab.org',
    'wallstreetguide.net',
]

# Will be populated from tracked_users table
_internal_domains = None


def get_internal_domains() -> list:
    """Fetch internal domains from tracked_users table, with caching."""
    global _internal_domains
    if _internal_domains is not None:
        return _internal_domains

    try:
        supabase = get_supabase()
        result = supabase.table("tracked_users").select("domain").execute()
        if result.data:
            # Get unique domains from tracked users
            domains = set(row["domain"] for row in result.data if row.get("domain"))
            # Combine with defaults to ensure we don't miss any
            _internal_domains = list(domains.union(set(DEFAULT_INTERNAL_DOMAINS)))
        else:
            _internal_domains = DEFAULT_INTERNAL_DOMAINS
    except Exception:
        _internal_domains = DEFAULT_INTERNAL_DOMAINS

    return _internal_domains


def is_internal_email(email: str) -> bool:
    """Check if an email is from an internal domain."""
    return any(domain in email for domain in get_internal_domains())


def get_user_work_settings(user_email: str) -> dict:
    """Fetch work schedule settings for a user. Returns defaults if columns don't exist."""
    default_settings = {
        "timezone": "America/New_York",
        "exclude_weekends": True,
    }
    try:
        supabase = get_supabase()
        result = supabase.table("tracked_users").select(
            "timezone, exclude_weekends"
        ).eq("email", user_email).execute()

        if result.data:
            settings = result.data[0]
            return {
                "timezone": settings.get("timezone") or "America/New_York",
                "exclude_weekends": settings.get("exclude_weekends", True),
            }
    except Exception:
        # Columns don't exist yet - migration not run
        pass
    return default_settings


def get_user_ooo_dates(user_email: str) -> set:
    """Fetch all OOO dates for a user as a set of date objects. Returns empty set if table doesn't exist."""
    ooo_dates = set()
    try:
        supabase = get_supabase()
        result = supabase.table("user_out_of_office").select(
            "start_date, end_date"
        ).eq("user_email", user_email).execute()

        if result.data:
            for row in result.data:
                start = datetime.fromisoformat(row["start_date"]).date()
                end = datetime.fromisoformat(row["end_date"]).date()
                current = start
                while current <= end:
                    ooo_dates.add(current)
                    current += timedelta(days=1)
    except Exception:
        # Table doesn't exist yet - migration not run
        pass
    return ooo_dates


def calculate_adjusted_hours(
    received_at: datetime,
    replied_at: datetime,
    user_tz: str,
    exclude_weekends: bool,
    ooo_dates: set
) -> float:
    """
    Calculate adjusted hours between received_at and replied_at.

    Counts full 24-hour days, excluding weekends (if enabled) and OOO dates.
    On the received day, counts from received time to end of day.
    On the replied day, counts from start of day to replied time.
    On full intermediate days, counts 24 hours.
    """
    try:
        tz = ZoneInfo(user_tz)
    except Exception:
        tz = ZoneInfo("America/New_York")

    # Convert to user's timezone
    received_local = received_at.astimezone(tz)
    replied_local = replied_at.astimezone(tz)

    total_seconds = 0
    current_date = received_local.date()
    end_date = replied_local.date()

    while current_date <= end_date:
        # Skip weekends if configured
        if exclude_weekends and current_date.weekday() >= 5:
            current_date += timedelta(days=1)
            continue

        # Skip OOO dates
        if current_date in ooo_dates:
            current_date += timedelta(days=1)
            continue

        # Determine the counting window for this day
        day_start = datetime.combine(current_date, dt_time(0, 0), tzinfo=tz)
        day_end = datetime.combine(current_date, dt_time(23, 59, 59), tzinfo=tz) + timedelta(seconds=1)

        # Clamp to received/replied times
        if current_date == received_local.date():
            day_start = max(day_start, received_local)
        if current_date == replied_local.date():
            day_end = min(day_end, replied_local)

        # Only count if there's positive time
        if day_end > day_start:
            total_seconds += (day_end - day_start).total_seconds()

        current_date += timedelta(days=1)

    return total_seconds / 3600


# Thread-local storage for Gmail service
_thread_local = threading.local()


def get_supabase() -> Client:
    """Get Supabase client."""
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def get_gmail_service(user_email: str):
    """Create Gmail API service with impersonation."""
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=["https://www.googleapis.com/auth/gmail.readonly"]
    ).with_subject(user_email)
    return build("gmail", "v1", credentials=credentials)


def get_thread_local_service(user_email: str):
    """Get or create thread-local Gmail service."""
    if not hasattr(_thread_local, 'service') or _thread_local.user_email != user_email:
        _thread_local.service = get_gmail_service(user_email)
        _thread_local.user_email = user_email
    return _thread_local.service


def fetch_thread(user_email: str, thread_id: str) -> Optional[dict]:
    """Fetch a single thread with retry for rate limits."""
    import time
    for attempt in range(3):
        try:
            service = get_thread_local_service(user_email)
            return service.users().threads().get(
                userId="me", id=thread_id, format="full"
            ).execute()
        except HttpError as e:
            if e.resp.status == 429:
                time.sleep(2 ** attempt)
            else:
                return None
        except Exception:
            return None
    return None


def extract_body_preview(message: dict, max_chars: int = 1000) -> str:
    """Extract plain text body from a Gmail message, truncated to max_chars."""
    # Try snippet as fast fallback
    snippet = message.get("snippet", "")

    payload = message.get("payload", {})
    if not payload:
        return snippet

    def decode_body(data: str) -> str:
        try:
            return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
        except Exception:
            return ""

    def find_text_in_parts(parts: list) -> str:
        for part in parts:
            mime = part.get("mimeType", "")
            if mime == "text/plain":
                data = part.get("body", {}).get("data")
                if data:
                    return decode_body(data)
            # Recurse into nested parts (multipart/alternative, etc.)
            if part.get("parts"):
                text = find_text_in_parts(part["parts"])
                if text:
                    return text
        return ""

    # Case 1: Simple message with body directly on payload
    body_data = payload.get("body", {}).get("data")
    if body_data and payload.get("mimeType", "").startswith("text/"):
        text = decode_body(body_data)
        if text:
            return text[:max_chars]

    # Case 2: Multipart message - find text/plain part
    parts = payload.get("parts", [])
    if parts:
        text = find_text_in_parts(parts)
        if text:
            return text[:max_chars]

    # Fallback to snippet
    return snippet[:max_chars]


def process_thread(thread_data: dict, user_email: str, work_settings: dict = None, ooo_dates: set = None) -> dict:
    """Extract ALL external→user response pairs from a thread, plus email counts."""
    msgs = thread_data.get("messages", [])

    result = {"pairs": [], "received": [], "sent": [], "received_emails": []}

    if not msgs:
        return result

    thread_id = thread_data.get("id", "")

    # Get subject
    headers = msgs[0].get("payload", {}).get("headers", [])
    subject = next((h["value"] for h in headers if h["name"] == "Subject"), "")[:200]

    # Parse messages
    parsed = []
    for m in msgs:
        h = {x["name"]: x["value"] for x in m.get("payload", {}).get("headers", [])}
        sender = h.get("From", "")
        email = sender.split("<")[1].split(">")[0].lower() if "<" in sender else sender.lower()
        try:
            date = parsedate_to_datetime(h.get("Date", ""))
            # Ensure timezone-aware (some emails have naive datetimes)
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
        except:
            continue
        parsed.append({"email": email, "date": date, "raw_msg": m})

    parsed.sort(key=lambda x: x["date"])

    user_email_lower = user_email.lower()

    # Count emails received and sent, and build received_emails records
    for m in parsed:
        date_str = m["date"].date().isoformat()
        is_from_user = m["email"] == user_email_lower
        is_noise = any(ex in m["email"] for ex in EXCLUDE)

        if is_from_user:
            result["sent"].append(date_str)
        elif not is_noise and not is_internal_email(m["email"]):
            result["received"].append(date_str)
            # Build a received_email record (replied info backfilled after pairs loop)
            body_preview = extract_body_preview(m.get("raw_msg", {}))
            result["received_emails"].append({
                "user_email": user_email,
                "sender_email": m["email"][:200],
                "subject": subject,
                "received_at": m["date"].isoformat(),
                "thread_id": thread_id,
                "replied": False,
                "replied_at": None,
                "response_hours": None,
                "body_preview": body_preview,
            })

    # Find ALL external→user pairs (need at least 2 messages)
    if len(parsed) < 2:
        return result

    for i, m in enumerate(parsed):
        is_external = not is_internal_email(m["email"])
        is_noise = any(ex in m["email"] for ex in EXCLUDE)

        if is_external and not is_noise:
            for j in range(i + 1, len(parsed)):
                if parsed[j]["email"] == user_email_lower:
                    hours = (parsed[j]["date"] - m["date"]).total_seconds() / 3600

                    # Calculate adjusted hours if work settings provided
                    adjusted_hours = None
                    if work_settings:
                        adjusted_hours = calculate_adjusted_hours(
                            m["date"],
                            parsed[j]["date"],
                            work_settings["timezone"],
                            work_settings["exclude_weekends"],
                            ooo_dates or set()
                        )

                    result["pairs"].append({
                        "user_email": user_email,
                        "external_sender": m["email"][:200],
                        "subject": subject,
                        "received_at": m["date"].isoformat(),
                        "replied_at": parsed[j]["date"].isoformat(),
                        "response_hours": round(hours, 2),
                        "adjusted_response_hours": round(adjusted_hours, 2) if adjusted_hours is not None else None,
                        "thread_id": thread_id,
                    })
                    break  # Only first reply to each external message

    # Backfill replied info into received_emails from pairs
    reply_lookup = {}
    for p in result["pairs"]:
        key = (p["thread_id"], p["received_at"])
        reply_lookup[key] = p

    for rec in result["received_emails"]:
        key = (rec["thread_id"], rec["received_at"])
        if key in reply_lookup:
            p = reply_lookup[key]
            rec["replied"] = True
            rec["replied_at"] = p["replied_at"]
            rec["response_hours"] = p["response_hours"]

    return result


def fetch_user_responses(user_email: str, max_threads: int = MAX_THREADS_DEFAULT) -> dict:
    """Fetch all response pairs for a user."""
    print(f"\n{'='*60}")
    print(f"Processing: {user_email}")
    print(f"{'='*60}")

    try:
        gmail = get_gmail_service(user_email)
    except Exception as e:
        print(f"  Error authenticating: {e}")
        return {"pairs": [], "received": [], "sent": [], "received_emails": []}

    # Fetch thread IDs
    print(f"  Fetching thread IDs...")
    all_threads = []
    page_token = None
    # Build query to exclude internal domains and noise
    internal_domains = get_internal_domains()
    internal_excludes = " ".join([f"-from:{domain}" for domain in internal_domains])
    query = f"{internal_excludes} -from:mailer-daemon -from:postmaster -from:noreply -from:notifications"

    while len(all_threads) < max_threads:
        try:
            results = gmail.users().threads().list(
                userId="me", q=query, maxResults=100, pageToken=page_token
            ).execute()
            threads = results.get("threads", [])
            all_threads.extend(threads)
            page_token = results.get("nextPageToken")
            if not page_token:
                break
        except Exception as e:
            print(f"  Error listing threads: {e}")
            break

    all_threads = all_threads[:max_threads]
    print(f"  Found {len(all_threads)} threads")

    if not all_threads:
        return {"pairs": [], "received": [], "sent": [], "received_emails": []}

    # Fetch thread details in parallel
    print(f"  Fetching thread details ({MAX_WORKERS} workers)...")
    thread_data = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_thread, user_email, t["id"]): t["id"] for t in all_threads}

        with tqdm(total=len(futures), desc="  Fetching", unit="threads", leave=False) as pbar:
            for future in as_completed(futures):
                result = future.result()
                if result:
                    thread_data.append(result)
                pbar.update(1)

    print(f"  Fetched {len(thread_data)} thread details")

    # Fetch work settings and OOO dates for adjusted calculation
    work_settings = get_user_work_settings(user_email)
    ooo_dates = get_user_ooo_dates(user_email)

    # Process threads
    print(f"  Processing threads...")
    all_pairs = []
    all_received = []
    all_sent = []
    all_received_emails = []

    for data in thread_data:
        result = process_thread(data, user_email, work_settings, ooo_dates)
        all_pairs.extend(result["pairs"])
        all_received.extend(result["received"])
        all_sent.extend(result["sent"])
        all_received_emails.extend(result["received_emails"])

    print(f"  Found {len(all_pairs)} response pairs, {len(all_received)} received, {len(all_sent)} sent, {len(all_received_emails)} received emails")
    return {"pairs": all_pairs, "received": all_received, "sent": all_sent, "received_emails": all_received_emails}


def save_to_supabase(pairs: list) -> int:
    """Save response pairs to Supabase. Returns count of new records."""
    if not pairs:
        return 0

    # Dedupe by (thread_id, replied_at) - same key can appear multiple times
    seen = set()
    unique_pairs = []
    for p in pairs:
        key = (p["thread_id"], p["replied_at"])
        if key not in seen:
            seen.add(key)
            unique_pairs.append(p)

    supabase = get_supabase()
    new_count = 0

    # Insert in batches, using upsert to avoid duplicates
    batch_size = 100
    for i in range(0, len(unique_pairs), batch_size):
        batch = unique_pairs[i:i + batch_size]
        try:
            # Use upsert with the unique constraint (thread_id, replied_at)
            result = supabase.table("response_pairs").upsert(
                batch,
                on_conflict="thread_id,replied_at"
            ).execute()
            new_count += len(result.data) if result.data else 0
        except Exception as e:
            # If adjusted_response_hours column doesn't exist, retry without it
            if "adjusted_response_hours" in str(e):
                batch_without_adjusted = [
                    {k: v for k, v in p.items() if k != "adjusted_response_hours"}
                    for p in batch
                ]
                try:
                    result = supabase.table("response_pairs").upsert(
                        batch_without_adjusted,
                        on_conflict="thread_id,replied_at"
                    ).execute()
                    new_count += len(result.data) if result.data else 0
                except Exception as e2:
                    print(f"  Error inserting batch: {e2}")
            else:
                print(f"  Error inserting batch: {e}")

    return new_count


def save_received_emails(received_emails: list) -> int:
    """Save received emails to Supabase. Returns count of new records."""
    if not received_emails:
        return 0

    # Dedupe by (thread_id, received_at)
    seen = set()
    unique = []
    for r in received_emails:
        key = (r["thread_id"], r["received_at"])
        if key not in seen:
            seen.add(key)
            unique.append(r)

    supabase = get_supabase()
    new_count = 0

    batch_size = 100
    for i in range(0, len(unique), batch_size):
        batch = unique[i:i + batch_size]
        try:
            result = supabase.table("received_emails").upsert(
                batch,
                on_conflict="thread_id,received_at"
            ).execute()
            new_count += len(result.data) if result.data else 0
        except Exception as e:
            print(f"  Error inserting received_emails batch: {e}")

    return new_count


def update_daily_stats(user_email: str, pairs: list, received: list, sent: list):
    """Update daily stats for a user based on their response pairs and email counts."""
    if not pairs and not received and not sent:
        return

    supabase = get_supabase()

    # Group data by date
    from collections import defaultdict, Counter
    daily_hours = defaultdict(list)
    daily_adjusted_hours = defaultdict(list)
    received_counts = Counter(received)
    sent_counts = Counter(sent)

    for p in pairs:
        # Parse the replied_at date
        replied_at = datetime.fromisoformat(p["replied_at"].replace("Z", "+00:00"))
        date_str = replied_at.date().isoformat()
        daily_hours[date_str].append(p["response_hours"])
        # Track adjusted hours if available
        if p.get("adjusted_response_hours") is not None:
            daily_adjusted_hours[date_str].append(p["adjusted_response_hours"])

    # Get all dates that have any activity
    all_dates = set(daily_hours.keys()) | set(received_counts.keys()) | set(sent_counts.keys())

    # Upsert daily stats
    for date_str in all_dates:
        hours_list = daily_hours.get(date_str, [])

        stats = {
            "user_email": user_email,
            "date": date_str,
            "emails_received": received_counts.get(date_str, 0),
            "emails_sent": sent_counts.get(date_str, 0),
            "response_pairs_count": len(hours_list),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        # Only add response time stats if we have pairs
        if hours_list:
            sorted_hours = sorted(hours_list)
            n = len(sorted_hours)
            median = sorted_hours[n // 2] if n % 2 == 1 else (sorted_hours[n//2 - 1] + sorted_hours[n//2]) / 2
            stats["avg_response_hours"] = round(sum(hours_list) / n, 2)
            stats["median_response_hours"] = round(median, 2)
            stats["min_response_hours"] = round(min(hours_list), 2)
            stats["max_response_hours"] = round(max(hours_list), 2)

        # Add adjusted hours stats if available
        adjusted_list = daily_adjusted_hours.get(date_str, [])
        if adjusted_list:
            sorted_adjusted = sorted(adjusted_list)
            n_adj = len(sorted_adjusted)
            median_adj = sorted_adjusted[n_adj // 2] if n_adj % 2 == 1 else (sorted_adjusted[n_adj//2 - 1] + sorted_adjusted[n_adj//2]) / 2
            stats["avg_adjusted_hours"] = round(sum(adjusted_list) / n_adj, 2)
            stats["median_adjusted_hours"] = round(median_adj, 2)

        try:
            supabase.table("daily_stats").upsert(
                stats,
                on_conflict="user_email,date"
            ).execute()
        except Exception as e:
            # If adjusted columns don't exist, retry without them
            if "avg_adjusted_hours" in str(e) or "median_adjusted_hours" in str(e):
                stats.pop("avg_adjusted_hours", None)
                stats.pop("median_adjusted_hours", None)
                try:
                    supabase.table("daily_stats").upsert(
                        stats,
                        on_conflict="user_email,date"
                    ).execute()
                except Exception as e2:
                    print(f"  Error updating daily stats: {e2}")
            else:
                print(f"  Error updating daily stats: {e}")


def get_tracked_users() -> list:
    """Get list of active tracked users from Supabase."""
    supabase = get_supabase()
    result = supabase.table("tracked_users").select("email").eq("is_active", True).execute()
    return [row["email"] for row in result.data] if result.data else []


def exclude_response_pair(pair_data: dict):
    """Insert a response pair into the excluded_response_pairs table."""
    supabase = get_supabase()
    supabase.table("excluded_response_pairs").upsert(
        pair_data,
        on_conflict="thread_id,replied_at"
    ).execute()


def restore_response_pair(excluded_id: str):
    """Remove a pair from excluded_response_pairs by its id."""
    supabase = get_supabase()
    supabase.table("excluded_response_pairs").delete().eq("id", excluded_id).execute()


def get_excluded_pairs(user_email: str = None) -> list:
    """Fetch excluded pairs, optionally filtered by user."""
    supabase = get_supabase()
    query = supabase.table("excluded_response_pairs").select("*")
    if user_email:
        query = query.eq("user_email", user_email)
    result = query.order("excluded_at", desc=True).execute()
    return result.data if result.data else []


def whitelist_response_pair(pair_data: dict):
    """Add a response pair to the whitelist (override >7d filter)."""
    supabase = get_supabase()
    supabase.table("whitelisted_response_pairs").upsert(
        pair_data,
        on_conflict="thread_id,replied_at"
    ).execute()


def remove_whitelisted_pair(whitelist_id: str):
    """Remove a pair from the whitelist by its id."""
    supabase = get_supabase()
    supabase.table("whitelisted_response_pairs").delete().eq("id", whitelist_id).execute()


def get_whitelisted_pairs(user_email: str = None) -> list:
    """Fetch whitelisted pairs, optionally filtered by user."""
    supabase = get_supabase()
    query = supabase.table("whitelisted_response_pairs").select("*")
    if user_email:
        query = query.eq("user_email", user_email)
    result = query.execute()
    return result.data if result.data else []


def recalculate_daily_stats(user_email: str, dates: list):
    """Recalculate daily_stats for specific user+dates after exclusion/restoration."""
    from collections import defaultdict
    supabase = get_supabase()

    for date_str in dates:
        # Get all response pairs for this user+date
        pairs_result = supabase.table("response_pairs").select(
            "response_hours, thread_id, replied_at"
        ).eq(
            "user_email", user_email
        ).gte(
            "replied_at", date_str + "T00:00:00"
        ).lte(
            "replied_at", date_str + "T23:59:59"
        ).execute()

        # Get excluded pairs for this user
        excluded_result = supabase.table("excluded_response_pairs").select(
            "thread_id, replied_at"
        ).eq("user_email", user_email).execute()

        excluded_keys = set()
        if excluded_result.data:
            for ep in excluded_result.data:
                excluded_keys.add((ep["thread_id"], ep["replied_at"]))

        # Filter out excluded pairs
        hours_list = []
        if pairs_result.data:
            for p in pairs_result.data:
                if (p["thread_id"], p["replied_at"]) not in excluded_keys:
                    hours_list.append(p["response_hours"])

        # Update stats for this date
        stats_update = {
            "response_pairs_count": len(hours_list),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        if hours_list:
            sorted_hours = sorted(hours_list)
            n = len(sorted_hours)
            median = sorted_hours[n // 2] if n % 2 == 1 else (sorted_hours[n//2 - 1] + sorted_hours[n//2]) / 2
            stats_update["avg_response_hours"] = round(sum(hours_list) / n, 2)
            stats_update["median_response_hours"] = round(median, 2)
            stats_update["min_response_hours"] = round(min(hours_list), 2)
            stats_update["max_response_hours"] = round(max(hours_list), 2)
        else:
            stats_update["avg_response_hours"] = None
            stats_update["median_response_hours"] = None
            stats_update["min_response_hours"] = None
            stats_update["max_response_hours"] = None

        try:
            supabase.table("daily_stats").update(stats_update).eq(
                "user_email", user_email
            ).eq(
                "date", date_str
            ).execute()
        except Exception as e:
            print(f"  Error updating daily stats for {date_str}: {e}")


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Fetch email response times and store in Supabase")
    parser.add_argument("--user", help="Run for a specific user email only")
    parser.add_argument("--backfill", action="store_true", help="Fetch more history (2000 threads instead of 500)")
    args = parser.parse_args()

    max_threads = MAX_THREADS_BACKFILL if args.backfill else MAX_THREADS_DEFAULT

    print("=" * 60)
    print("Lumiere Email Tracker - Supabase Edition")
    if args.backfill:
        print("MODE: BACKFILL (fetching up to 2000 threads per user)")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Get tracked users from database
    if args.user:
        users = [args.user]
        print(f"\nRunning for specific user: {args.user}")
    else:
        users = get_tracked_users()
        print(f"\nTracking {len(users)} users: {', '.join(users)}")

    total_pairs = 0
    total_new = 0

    total_received_emails = 0

    failed_users = []

    for user_email in users:
        try:
            result = fetch_user_responses(user_email, max_threads=max_threads)
            pairs = result["pairs"]
            received = result["received"]
            sent = result["sent"]
            received_emails = result["received_emails"]

            if pairs:
                print(f"  Saving response pairs to Supabase...")
                new_count = save_to_supabase(pairs)
                print(f"  Saved {new_count} new response pair records")
                total_pairs += len(pairs)
                total_new += new_count

            if received_emails:
                print(f"  Saving received emails to Supabase...")
                re_count = save_received_emails(received_emails)
                print(f"  Saved {re_count} received email records")
                total_received_emails += len(received_emails)

            if pairs or received or sent:
                print(f"  Updating daily stats...")
                update_daily_stats(user_email, pairs, received, sent)

        except Exception as e:
            print(f"\n  ERROR processing {user_email}: {e}")
            print(f"  Skipping this user and continuing with others...")
            failed_users.append(user_email)

    print(f"\n{'='*60}")
    print(f"COMPLETE")
    print(f"{'='*60}")
    print(f"Total response pairs processed: {total_pairs}")
    print(f"New response pair records saved: {total_new}")
    print(f"Total received emails processed: {total_received_emails}")
    if failed_users:
        print(f"Failed users ({len(failed_users)}): {', '.join(failed_users)}")
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Exit with error if ALL users failed, but succeed if at least some data was saved
    if failed_users and len(failed_users) == len(users):
        print("\nERROR: All users failed. Exiting with error.")
        exit(1)


if __name__ == "__main__":
    main()
