#!/usr/bin/env python3
"""
Email Response Time Tracker - Supabase Edition
Fetches email response times and stores them in Supabase.
"""

import os
import threading
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional

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

MAX_THREADS = 500  # Threads to fetch per user
MAX_WORKERS = 20   # Parallel workers

# Noise filters - emails to exclude
EXCLUDE = [
    'mailer-daemon', 'postmaster', 'mixmax.com', 'notifications@',
    'noreply', 'no-reply', 'stellaconnect', 'calendar-notification',
    'newsletter', 'stripe.com', 'calsavers.com'
]

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
                userId="me", id=thread_id, format="metadata",
                metadataHeaders=["From", "Subject", "Date"]
            ).execute()
        except HttpError as e:
            if e.resp.status == 429:
                time.sleep(2 ** attempt)
            else:
                return None
        except Exception:
            return None
    return None


def process_thread(thread_data: dict, user_email: str) -> dict:
    """Extract ALL external→user response pairs from a thread, plus email counts."""
    msgs = thread_data.get("messages", [])

    result = {"pairs": [], "received": [], "sent": []}

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
        parsed.append({"email": email, "date": date})

    parsed.sort(key=lambda x: x["date"])

    user_email_lower = user_email.lower()

    # Count emails received and sent
    for m in parsed:
        date_str = m["date"].date().isoformat()
        is_from_user = m["email"] == user_email_lower
        is_noise = any(ex in m["email"] for ex in EXCLUDE)

        if is_from_user:
            result["sent"].append(date_str)
        elif not is_noise and "lumiere.education" not in m["email"]:
            result["received"].append(date_str)

    # Find ALL external→user pairs (need at least 2 messages)
    if len(parsed) < 2:
        return result

    for i, m in enumerate(parsed):
        is_external = "lumiere.education" not in m["email"]
        is_noise = any(ex in m["email"] for ex in EXCLUDE)

        if is_external and not is_noise:
            for j in range(i + 1, len(parsed)):
                if parsed[j]["email"] == user_email_lower:
                    hours = (parsed[j]["date"] - m["date"]).total_seconds() / 3600

                    result["pairs"].append({
                        "user_email": user_email,
                        "external_sender": m["email"][:200],
                        "subject": subject,
                        "received_at": m["date"].isoformat(),
                        "replied_at": parsed[j]["date"].isoformat(),
                        "response_hours": round(hours, 2),
                        "thread_id": thread_id,
                    })
                    break  # Only first reply to each external message

    return result


def fetch_user_responses(user_email: str) -> list:
    """Fetch all response pairs for a user."""
    print(f"\n{'='*60}")
    print(f"Processing: {user_email}")
    print(f"{'='*60}")

    try:
        gmail = get_gmail_service(user_email)
    except Exception as e:
        print(f"  Error authenticating: {e}")
        return []

    # Fetch thread IDs
    print(f"  Fetching thread IDs...")
    all_threads = []
    page_token = None
    query = "from:(-lumiere.education) -from:mailer-daemon -from:postmaster -from:noreply -from:notifications"

    while len(all_threads) < MAX_THREADS:
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

    all_threads = all_threads[:MAX_THREADS]
    print(f"  Found {len(all_threads)} threads")

    if not all_threads:
        return []

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

    # Process threads
    print(f"  Processing threads...")
    all_pairs = []
    all_received = []
    all_sent = []

    for data in thread_data:
        result = process_thread(data, user_email)
        all_pairs.extend(result["pairs"])
        all_received.extend(result["received"])
        all_sent.extend(result["sent"])

    print(f"  Found {len(all_pairs)} response pairs, {len(all_received)} received, {len(all_sent)} sent")
    return {"pairs": all_pairs, "received": all_received, "sent": all_sent}


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
            print(f"  Error inserting batch: {e}")

    return new_count


def update_daily_stats(user_email: str, pairs: list, received: list, sent: list):
    """Update daily stats for a user based on their response pairs and email counts."""
    if not pairs and not received and not sent:
        return

    supabase = get_supabase()

    # Group data by date
    from collections import defaultdict, Counter
    daily_hours = defaultdict(list)
    received_counts = Counter(received)
    sent_counts = Counter(sent)

    for p in pairs:
        # Parse the replied_at date
        replied_at = datetime.fromisoformat(p["replied_at"].replace("Z", "+00:00"))
        date_str = replied_at.date().isoformat()
        daily_hours[date_str].append(p["response_hours"])

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

        try:
            supabase.table("daily_stats").upsert(
                stats,
                on_conflict="user_email,date"
            ).execute()
        except Exception as e:
            print(f"  Error updating daily stats: {e}")


def get_tracked_users() -> list:
    """Get list of active tracked users from Supabase."""
    supabase = get_supabase()
    result = supabase.table("tracked_users").select("email").eq("is_active", True).execute()
    return [row["email"] for row in result.data] if result.data else []


def main():
    print("=" * 60)
    print("Lumiere Email Tracker - Supabase Edition")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Get tracked users from database
    users = get_tracked_users()
    print(f"\nTracking {len(users)} users: {', '.join(users)}")

    total_pairs = 0
    total_new = 0

    for user_email in users:
        result = fetch_user_responses(user_email)
        pairs = result["pairs"]
        received = result["received"]
        sent = result["sent"]

        if pairs:
            print(f"  Saving to Supabase...")
            new_count = save_to_supabase(pairs)
            print(f"  Saved {new_count} new records")
            total_pairs += len(pairs)
            total_new += new_count

        if pairs or received or sent:
            print(f"  Updating daily stats...")
            update_daily_stats(user_email, pairs, received, sent)

    print(f"\n{'='*60}")
    print(f"COMPLETE")
    print(f"{'='*60}")
    print(f"Total response pairs processed: {total_pairs}")
    print(f"New records saved: {total_new}")
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
