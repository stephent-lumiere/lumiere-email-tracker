#!/usr/bin/env python3
"""
Email Response Time Tracker for Lumiere Education
Uses Service Account with Domain-Wide Delegation to audit email performance.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Optional

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Thread-local storage for Gmail service instances
_thread_local = threading.local()

# Configuration
CREDENTIALS_FILE = "credentials.json"
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
INTERNAL_DOMAIN = "lumiere.education"
DAYS_TO_LOOK_BACK = 14
MAX_THREADS = None  # No limit - fetch all threads
MAX_WORKERS = 25

# Test users to audit
TARGET_USERS = [
    "dhruva.bhat@lumiere.education",
    "stephen.turban@lumiere.education",
    "program.manager@lumiere.education",
    "contact@lumiere.education",
]


def get_gmail_service(user_email: str):
    """Create Gmail API service with impersonation for the specified user."""
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE, scopes=SCOPES
    )
    delegated_credentials = credentials.with_subject(user_email)
    return build("gmail", "v1", credentials=delegated_credentials)


def get_thread_local_service(user_email: str):
    """Get or create a thread-local Gmail service instance."""
    if not hasattr(_thread_local, 'service') or _thread_local.user_email != user_email:
        _thread_local.service = get_gmail_service(user_email)
        _thread_local.user_email = user_email
    return _thread_local.service


def extract_email_address(header_value: str) -> str:
    """Extract email address from a header like 'Name <email@domain.com>'."""
    if "<" in header_value and ">" in header_value:
        return header_value.split("<")[1].split(">")[0].lower()
    return header_value.lower().strip()


def is_external_sender(sender_email: str) -> bool:
    """Check if sender is external (not from lumiere.education)."""
    return INTERNAL_DOMAIN not in sender_email.lower()


def is_internal_sender(sender_email: str) -> bool:
    """Check if sender is internal (from lumiere.education)."""
    return INTERNAL_DOMAIN in sender_email.lower()


def parse_message_date(headers: list) -> Optional[datetime]:
    """Extract and parse the Date header from message headers."""
    for header in headers:
        if header["name"].lower() == "date":
            try:
                return parsedate_to_datetime(header["value"])
            except Exception:
                return None
    return None


def get_header_value(headers: list, name: str) -> str:
    """Get a specific header value from message headers."""
    for header in headers:
        if header["name"].lower() == name.lower():
            return header["value"]
    return ""


def calculate_response_time(start: datetime, end: datetime) -> float:
    """Calculate raw time difference in hours."""
    if end <= start:
        return 0.0
    return (end - start).total_seconds() / 3600


def format_response_time(hours: float) -> str:
    """Format hours into a readable string."""
    if hours < 1:
        minutes = int(hours * 60)
        return f"{minutes}m"
    elif hours < 24:
        h = int(hours)
        m = int((hours - h) * 60)
        if m > 0:
            return f"{h}h {m}m"
        return f"{h}h"
    else:
        days = int(hours / 24)
        remaining_hours = hours - (days * 24)
        if remaining_hours >= 1:
            return f"{days}d {int(remaining_hours)}h"
        return f"{days}d"


def count_emails(service, user_email: str, days_back: int = 14) -> dict:
    """Count total emails received and sent by this user in the time period."""
    after_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y/%m/%d")

    received = 0
    sent = 0

    try:
        # Count received emails (in inbox, from external senders)
        query = f"after:{after_date} in:inbox"
        page_token = None
        while True:
            results = service.users().messages().list(
                userId="me", q=query, maxResults=500, pageToken=page_token
            ).execute()
            received += len(results.get("messages", []))
            page_token = results.get("nextPageToken")
            if not page_token:
                break
    except Exception:
        pass

    try:
        # Count sent emails
        query = f"after:{after_date} in:sent from:{user_email}"
        page_token = None
        while True:
            results = service.users().messages().list(
                userId="me", q=query, maxResults=500, pageToken=page_token
            ).execute()
            sent += len(results.get("messages", []))
            page_token = results.get("nextPageToken")
            if not page_token:
                break
    except Exception:
        pass

    return {"received": received, "sent": sent}


def fetch_threads(service, days_back: int = 30, max_threads: int = None) -> list:
    """Fetch email threads from the last N days. If max_threads is None, fetch all."""
    try:
        after_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y/%m/%d")
        # Query for threads with external senders (excluding bounces and internal-only)
        query = f"after:{after_date} (from:(-lumiere.education) -from:mailer-daemon -from:postmaster)"

        all_threads = []
        page_token = None

        while True:
            results = service.users().threads().list(
                userId="me",
                q=query,
                maxResults=100,
                pageToken=page_token
            ).execute()

            threads = results.get("threads", [])
            all_threads.extend(threads)

            page_token = results.get("nextPageToken")
            if not page_token:
                break

            # If max_threads is set, stop when we have enough
            if max_threads and len(all_threads) >= max_threads:
                break

        if max_threads:
            return all_threads[:max_threads]
        return all_threads
    except Exception as e:
        print(f"  Error fetching threads: {e}")
        return []


def fetch_thread_with_retry(user_email: str, thread_id: str, max_retries: int = 5) -> Optional[dict]:
    """Fetch a single thread with exponential backoff for rate limits."""
    for attempt in range(max_retries):
        try:
            service = get_thread_local_service(user_email)
            return service.users().threads().get(
                userId="me", id=thread_id, format="metadata",
                metadataHeaders=["From", "To", "Subject", "Date"]
            ).execute()
        except HttpError as e:
            if e.resp.status == 429:  # Rate limit
                wait_time = (2 ** attempt)  # Exponential backoff: 1, 2, 4, 8, 16 seconds
                time.sleep(wait_time)
            else:
                return None
        except Exception:
            return None
    return None


def process_thread_data(thread: dict, user_email: str) -> list:
    """Process a fetched thread to extract ALL response times for a specific user."""
    messages = thread.get("messages", [])
    if len(messages) < 2:
        return []

    # Get subject from first message
    subject = ""
    if messages:
        subject = get_header_value(
            messages[0].get("payload", {}).get("headers", []), "Subject"
        )

    # Parse message data
    message_data = []
    for msg in messages:
        headers = msg.get("payload", {}).get("headers", [])
        sender = get_header_value(headers, "From")
        date = parse_message_date(headers)
        if date and sender:
            message_data.append({
                "sender": sender,
                "sender_email": extract_email_address(sender),
                "date": date,
            })

    # Sort by date
    message_data.sort(key=lambda x: x["date"])

    # Find ALL external messages followed by a reply from THIS specific user
    results = []
    user_email_lower = user_email.lower()

    for i in range(len(message_data)):
        msg = message_data[i]
        if is_external_sender(msg["sender_email"]):
            # Look for the first reply from this specific user after this external message
            for j in range(i + 1, len(message_data)):
                reply = message_data[j]
                if reply["sender_email"] == user_email_lower:
                    response_hours = calculate_response_time(msg["date"], reply["date"])
                    results.append({
                        "subject": subject[:60] + "..." if len(subject) > 60 else subject,
                        "sender": msg["sender_email"],
                        "time_received": msg["date"].strftime("%Y-%m-%d %H:%M"),
                        "time_replied": reply["date"].strftime("%Y-%m-%d %H:%M"),
                        "response_hours": response_hours,
                        "response_time": format_response_time(response_hours),
                    })
                    break  # Only count first reply to each external message

    return results


def analyze_user_emails(user_email: str) -> tuple:
    """Analyze email response times for a specific user using parallel fetching."""
    print(f"\nAnalyzing: {user_email}")
    print("-" * 50)

    try:
        service = get_gmail_service(user_email)
    except Exception as e:
        print(f"  Authentication error: {e}")
        return [], {"received": 0, "sent": 0}

    # Count total emails received and sent
    print(f"  Counting total emails...")
    email_counts = count_emails(service, user_email, DAYS_TO_LOOK_BACK)
    print(f"  Total received: {email_counts['received']} | Total sent: {email_counts['sent']}")

    threads = fetch_threads(service, days_back=DAYS_TO_LOOK_BACK, max_threads=MAX_THREADS)
    if not threads:
        print("  No threads found")
        return [], email_counts

    print(f"  Fetching {len(threads)} threads in parallel ({MAX_WORKERS} workers)...")
    start_time = time.time()

    # Fetch all thread details in parallel using thread-local services
    thread_data = []
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {
            executor.submit(fetch_thread_with_retry, user_email, t["id"]): t["id"]
            for t in threads
        }

        for future in as_completed(future_to_id):
            result = future.result()
            if result:
                thread_data.append(result)
            completed += 1
            if completed % 100 == 0:
                print(f"  Fetched {completed}/{len(threads)} threads...")

    fetch_time = time.time() - start_time
    print(f"  Fetched {len(thread_data)} threads in {fetch_time:.1f}s")

    # Process all thread data - count ALL replies from THIS specific user
    results = []
    for thread in thread_data:
        thread_results = process_thread_data(thread, user_email)
        results.extend(thread_results)

    print(f"  Found {len(results)} response pairs (external msg → user reply)")
    return results, email_counts


def display_results(results: list, user_email: str, email_counts: dict):
    """Display results in a clean table format."""
    if not results:
        print(f"\n  Emails Received: {email_counts['received']} | Emails Sent: {email_counts['sent']}")
        print("  No response data to display.")
        return

    df = pd.DataFrame(results)
    df = df.rename(columns={
        "subject": "Subject",
        "sender": "Sender",
        "time_received": "Received",
        "time_replied": "Replied",
        "response_time": "Response Time",
    })

    display_df = df[["Subject", "Sender", "Received", "Replied", "Response Time"]]

    print(f"\n{'='*100}")
    print(f"Response Time Report: {user_email}")
    print(f"{'='*100}")
    print()

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", 40)

    print(display_df.to_string(index=False))

    if results:
        hours = [r["response_hours"] for r in results]
        avg_hours = sum(hours) / len(hours)
        min_hours = min(hours)
        max_hours = max(hours)

        # Calculate median
        sorted_hours = sorted(hours)
        n = len(sorted_hours)
        if n % 2 == 0:
            median_hours = (sorted_hours[n//2 - 1] + sorted_hours[n//2]) / 2
        else:
            median_hours = sorted_hours[n//2]

        print(f"\n{'─'*50}")
        print("Summary Statistics:")
        print(f"  Emails Received:       {email_counts['received']}")
        print(f"  Emails Sent:           {email_counts['sent']}")
        print(f"  Threads Analyzed:      {len(results)}")
        print(f"  Average Response Time: {format_response_time(avg_hours)}")
        print(f"  Median Response Time:  {format_response_time(median_hours)}")
        print(f"  Fastest Response:      {format_response_time(min_hours)}")
        print(f"  Slowest Response:      {format_response_time(max_hours)}")


def main():
    """Main entry point."""
    print("=" * 60)
    print("Lumiere Education - Email Response Time Tracker")
    print("=" * 60)
    print(f"Date range: Last {DAYS_TO_LOOK_BACK} days")
    print(f"Analyzing {len(TARGET_USERS)} users...")

    total_start = time.time()

    for user_email in TARGET_USERS:
        results, email_counts = analyze_user_emails(user_email)
        display_results(results, user_email, email_counts)
        print("\n")

    total_time = time.time() - total_start
    print(f"Total execution time: {total_time:.1f}s")


if __name__ == "__main__":
    main()
