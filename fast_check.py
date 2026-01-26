#!/usr/bin/env python3
"""
Fast parallel spot check for email response times.
Uses ThreadPoolExecutor + tqdm for speed and visibility.
"""

import threading
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.utils import parsedate_to_datetime
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tqdm import tqdm

warnings.filterwarnings("ignore")

# Configuration
CREDENTIALS_FILE = "credentials.json"
USER_EMAIL = "contact@lumiere.education"
MAX_THREADS = 500
MAX_WORKERS = 20

# Noise filters - emails to exclude
EXCLUDE = [
    'mailer-daemon', 'postmaster', 'mixmax.com', 'notifications@',
    'noreply', 'no-reply', 'stellaconnect', 'calendar-notification',
    'newsletter', 'stripe.com', 'calsavers.com'
]

# Thread-local storage for Gmail service
_thread_local = threading.local()


def get_service():
    """Get or create thread-local Gmail service."""
    if not hasattr(_thread_local, 'service'):
        creds = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE,
            scopes=["https://www.googleapis.com/auth/gmail.readonly"]
        ).with_subject(USER_EMAIL)
        _thread_local.service = build("gmail", "v1", credentials=creds)
    return _thread_local.service


def fetch_thread(thread_id: str) -> Optional[dict]:
    """Fetch a single thread with retry for rate limits."""
    for attempt in range(3):
        try:
            service = get_service()
            return service.users().threads().get(
                userId="me", id=thread_id, format="metadata",
                metadataHeaders=["From", "Subject", "Date"]
            ).execute()
        except HttpError as e:
            if e.resp.status == 429:
                import time
                time.sleep(2 ** attempt)
            else:
                return None
        except Exception:
            return None
    return None


def process_thread(thread_data: dict) -> list:
    """Extract ALL external→Stephen response pairs from a thread."""
    msgs = thread_data.get("messages", [])
    if len(msgs) < 2:
        return []

    # Get subject
    headers = msgs[0].get("payload", {}).get("headers", [])
    subject = next((h["value"] for h in headers if h["name"] == "Subject"), "")[:40]

    # Parse messages
    parsed = []
    for m in msgs:
        h = {x["name"]: x["value"] for x in m.get("payload", {}).get("headers", [])}
        sender = h.get("From", "")
        email = sender.split("<")[1].split(">")[0].lower() if "<" in sender else sender.lower()
        try:
            date = parsedate_to_datetime(h.get("Date", ""))
        except:
            continue
        parsed.append({"email": email, "date": date})

    parsed.sort(key=lambda x: x["date"])

    # Find ALL external→Stephen pairs
    results = []
    for i, m in enumerate(parsed):
        is_external = "lumiere.education" not in m["email"]
        is_noise = any(ex in m["email"] for ex in EXCLUDE)

        if is_external and not is_noise:
            for j in range(i + 1, len(parsed)):
                if parsed[j]["email"] == USER_EMAIL.lower():
                    hours = (parsed[j]["date"] - m["date"]).total_seconds() / 3600
                    if hours < 24:
                        time_str = f"{int(hours)}h {int((hours % 1) * 60)}m"
                    else:
                        days = int(hours / 24)
                        time_str = f"{days}d {int(hours % 24)}h"

                    results.append({
                        "subject": subject,
                        "external": m["email"][:28],
                        "ext_date": m["date"],
                        "reply_date": parsed[j]["date"],
                        "response": time_str,
                        "hours": hours
                    })
                    break  # Only first reply to each external message

    return results


def main():
    print(f"Fast Email Response Check for {USER_EMAIL}")
    print("=" * 70)

    # Get initial service for thread list
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=["https://www.googleapis.com/auth/gmail.readonly"]
    ).with_subject(USER_EMAIL)
    gmail = build("gmail", "v1", credentials=creds)

    # Fetch thread IDs - query for threads with external senders
    print(f"\nFetching up to {MAX_THREADS} thread IDs (inbound from external)...")
    all_threads = []
    page_token = None

    # Query for threads where external people sent messages (not mailer-daemon, not internal)
    query = "from:(-lumiere.education) -from:mailer-daemon -from:postmaster -from:noreply -from:notifications"

    with tqdm(total=MAX_THREADS, desc="Listing threads", unit="threads") as pbar:
        while len(all_threads) < MAX_THREADS:
            results = gmail.users().threads().list(
                userId="me", q=query, maxResults=100, pageToken=page_token
            ).execute()

            threads = results.get("threads", [])
            all_threads.extend(threads)
            pbar.update(len(threads))

            page_token = results.get("nextPageToken")
            if not page_token:
                break

    all_threads = all_threads[:MAX_THREADS]
    print(f"Got {len(all_threads)} threads")

    # Fetch thread details in parallel
    print(f"\nFetching thread details ({MAX_WORKERS} workers)...")
    thread_data = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_thread, t["id"]): t["id"] for t in all_threads}

        with tqdm(total=len(futures), desc="Fetching details", unit="threads") as pbar:
            for future in as_completed(futures):
                result = future.result()
                if result:
                    thread_data.append(result)
                pbar.update(1)

    print(f"Fetched {len(thread_data)} thread details")

    # Process all threads to find response pairs
    print("\nProcessing threads for response pairs...")
    all_pairs = []

    for data in tqdm(thread_data, desc="Processing", unit="threads"):
        pairs = process_thread(data)
        all_pairs.extend(pairs)

    # Sort by reply date (most recent first)
    all_pairs.sort(key=lambda x: x["reply_date"], reverse=True)

    # Dedupe by external + reply time
    seen = set()
    deduped = []
    for p in all_pairs:
        key = (p["external"], p["reply_date"].strftime("%Y-%m-%d %H:%M"))
        if key not in seen:
            seen.add(key)
            deduped.append(p)

    # Display results
    print(f"\n{'=' * 100}")
    print(f"Found {len(deduped)} response pairs (sorted by your reply date)")
    print(f"{'=' * 100}\n")

    print(f"{'#':<3} {'Subject':<40} {'External Sender':<28} {'Ext Sent':<12} {'You Replied':<12} {'Response'}")
    print("-" * 110)

    for i, p in enumerate(deduped[:30], 1):
        print(f"{i:<3} {p['subject']:<40} {p['external']:<28} {p['ext_date'].strftime('%m-%d %H:%M'):<12} {p['reply_date'].strftime('%m-%d %H:%M'):<12} {p['response']}")

    # Stats
    if deduped:
        hours = [p["hours"] for p in deduped]
        avg = sum(hours) / len(hours)
        sorted_hours = sorted(hours)
        median = sorted_hours[len(sorted_hours) // 2]

        print(f"\n{'─' * 50}")
        print("Summary Statistics:")
        print(f"  Total Response Pairs: {len(deduped)}")
        print(f"  Average Response Time: {int(avg)}h {int((avg % 1) * 60)}m")
        print(f"  Median Response Time:  {int(median)}h {int((median % 1) * 60)}m")
        print(f"  Fastest: {int(min(hours))}h {int((min(hours) % 1) * 60)}m")
        print(f"  Slowest: {int(max(hours) / 24)}d {int(max(hours) % 24)}h")


if __name__ == "__main__":
    main()
