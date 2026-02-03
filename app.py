import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import requests
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from supabase import create_client

def exclude_response_pair(pair_data: dict):
    """Insert a response pair into the excluded_response_pairs table."""
    supabase = get_supabase()
    supabase.table("excluded_response_pairs").upsert(
        pair_data, on_conflict="thread_id,replied_at"
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
        pair_data, on_conflict="thread_id,replied_at"
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
    supabase = get_supabase()
    for date_str in dates:
        pairs_result = supabase.table("response_pairs").select(
            "response_hours, thread_id, replied_at"
        ).eq("user_email", user_email).gte(
            "replied_at", date_str + "T00:00:00"
        ).lte("replied_at", date_str + "T23:59:59").execute()

        excluded_result = supabase.table("excluded_response_pairs").select(
            "thread_id, replied_at"
        ).eq("user_email", user_email).execute()

        excluded_keys = set()
        if excluded_result.data:
            for ep in excluded_result.data:
                excluded_keys.add((ep["thread_id"], ep["replied_at"]))

        hours_list = []
        if pairs_result.data:
            for p in pairs_result.data:
                if (p["thread_id"], p["replied_at"]) not in excluded_keys:
                    hours_list.append(p["response_hours"])

        stats_update = {
            "response_pairs_count": len(hours_list),
            "updated_at": datetime.now().isoformat(),
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
            ).eq("date", date_str).execute()
        except Exception as e:
            print(f"Error updating daily stats for {date_str}: {e}")

# Load environment variables
load_dotenv()

# Initialize Supabase client
@st.cache_resource
def get_supabase():
    return create_client(
        os.getenv("SUPABASE_URL"),
        os.getenv("SUPABASE_KEY")
    )


def check_gmail_access(user_email: str) -> tuple[bool, str]:
    """
    Check if we have Gmail API access for a user.
    Returns (success, message).
    """
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        credentials_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")

        # If credentials file doesn't exist (e.g., on Streamlit Cloud), skip the check
        # The actual verification will happen when GitHub Actions runs
        if not os.path.exists(credentials_file):
            return True, "Skipped (no local credentials)"

        credentials = service_account.Credentials.from_service_account_file(
            credentials_file,
            scopes=["https://www.googleapis.com/auth/gmail.readonly"]
        ).with_subject(user_email)

        service = build("gmail", "v1", credentials=credentials)

        # Try to list 1 thread to verify access
        service.users().threads().list(userId="me", maxResults=1).execute()

        return True, "Access verified"

    except Exception as e:
        error_msg = str(e)
        if "unauthorized_client" in error_msg.lower() or "access denied" in error_msg.lower():
            domain = user_email.split("@")[1] if "@" in user_email else "unknown"
            return False, f"domain_not_connected:{domain}"
        elif "invalid_grant" in error_msg.lower() or "user not found" in error_msg.lower():
            return False, "user_not_found"
        else:
            return False, f"error:{error_msg}"


def trigger_github_workflow(user_email: str = "", backfill: bool = True) -> bool:
    """Trigger the GitHub Actions workflow to fetch email data."""
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        return False, "GitHub token not configured"

    url = "https://api.github.com/repos/stephent-lumiere/lumiere-email-tracker/actions/workflows/daily-sync.yml/dispatches"

    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }

    data = {
        "ref": "main",
        "inputs": {
            "user_email": user_email,
            "backfill": "true" if backfill else "false"
        }
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 204:
            return True, "Workflow triggered successfully"
        else:
            return False, f"Error: {response.status_code} - {response.text}"
    except Exception as e:
        return False, f"Error: {str(e)}"

def get_stats_from_supabase(start_date: date, end_date: date, use_adjusted: bool = False, exclude_long_responses: bool = True) -> pd.DataFrame:
    """
    Fetch aggregated stats from Supabase daily_stats table.
    If exclude_long_responses is True, filters out response pairs > 168 hours (7 days).
    """
    import time

    # Retry logic for transient network errors
    for attempt in range(3):
        try:
            supabase = get_supabase()
            result = supabase.table("daily_stats").select("*").gte(
                "date", start_date.isoformat()
            ).lte(
                "date", end_date.isoformat()
            ).execute()
            break
        except Exception as e:
            if attempt < 2:
                time.sleep(1)
                continue
            raise e

    if not result.data:
        return pd.DataFrame()

    df = pd.DataFrame(result.data)

    # Choose columns based on mode
    avg_col = "avg_adjusted_hours" if use_adjusted else "avg_response_hours"
    median_col = "median_adjusted_hours" if use_adjusted else "median_response_hours"

    # Handle missing adjusted columns (for historical data)
    if use_adjusted:
        if avg_col not in df.columns:
            df[avg_col] = df.get("avg_response_hours", 0)
        if median_col not in df.columns:
            df[median_col] = df.get("median_response_hours", 0)

    # Aggregate email counts by user (these sums are correct)
    aggregated = df.groupby("user_email").agg({
        "emails_received": "sum",
        "emails_sent": "sum",
        "response_pairs_count": "sum",
    }).reset_index()

    # Compute true average and median from response_pairs table directly
    # (daily_stats aggregation loses accuracy due to equal-weight-per-day averaging)
    hours_col = "adjusted_response_hours" if use_adjusted else "response_hours"
    try:
        # Fetch ALL response pairs using pagination (Supabase caps at 1000 per request)
        all_pairs_data = []
        batch_size = 1000
        offset = 0
        while True:
            pairs_batch = supabase.table("response_pairs").select(
                "user_email, thread_id, replied_at, " + hours_col
            ).gte(
                "replied_at", start_date.isoformat()
            ).lte(
                "replied_at", end_date.isoformat() + "T23:59:59"
            ).range(offset, offset + batch_size - 1).execute()

            if not pairs_batch.data:
                break
            all_pairs_data.extend(pairs_batch.data)
            if len(pairs_batch.data) < batch_size:
                break  # Last page
            offset += batch_size

        if all_pairs_data:
            pairs_df = pd.DataFrame(all_pairs_data)

            # Try to filter out excluded pairs (table may not exist yet)
            try:
                excluded_result = supabase.table("excluded_response_pairs").select(
                    "thread_id, replied_at"
                ).execute()
                if excluded_result.data:
                    excluded_keys = {
                        (ep["thread_id"], ep["replied_at"]) for ep in excluded_result.data
                    }
                    pairs_df = pairs_df[
                        ~pairs_df.apply(
                            lambda r: (r["thread_id"], r["replied_at"]) in excluded_keys, axis=1
                        )
                    ]
            except Exception:
                pass  # Table doesn't exist yet, no exclusions to apply

            pairs_df[hours_col] = pd.to_numeric(pairs_df[hours_col], errors="coerce")
            pairs_df = pairs_df.dropna(subset=[hours_col])

            # Filter out responses > 7 days (168 hours) if enabled, but keep whitelisted pairs
            if exclude_long_responses:
                try:
                    wl_result = supabase.table("whitelisted_response_pairs").select(
                        "thread_id, replied_at"
                    ).execute()
                    if wl_result.data:
                        wl_keys = {(wp["thread_id"], wp["replied_at"]) for wp in wl_result.data}
                        pairs_df = pairs_df[
                            (pairs_df[hours_col] <= 168) |
                            pairs_df.apply(lambda r: (r["thread_id"], r["replied_at"]) in wl_keys, axis=1)
                        ]
                    else:
                        pairs_df = pairs_df[pairs_df[hours_col] <= 168]
                except Exception:
                    pairs_df = pairs_df[pairs_df[hours_col] <= 168]

            # Compute both average and median per user from raw pairs
            user_stats = pairs_df.groupby("user_email")[hours_col].agg(["mean", "median"]).reset_index()
            user_stats.columns = ["user_email", "avg_response_hours", "median_response_hours"]
            aggregated = aggregated.merge(user_stats, on="user_email", how="left")
        else:
            aggregated["avg_response_hours"] = None
            aggregated["median_response_hours"] = None
    except Exception as e:
        print(f"Error computing stats from response_pairs: {e}")
        # Fallback: compute weighted average from daily_stats
        def compute_weighted_avg(group):
            weights = group["response_pairs_count"]
            values = group[avg_col]
            mask = (weights > 0) & values.notna()
            if mask.any():
                return (values[mask] * weights[mask]).sum() / weights[mask].sum()
            return None

        weighted_avgs = df.groupby("user_email").apply(compute_weighted_avg).reset_index()
        weighted_avgs.columns = ["user_email", "avg_response_hours"]
        aggregated = aggregated.merge(weighted_avgs, on="user_email", how="left")
        aggregated["median_response_hours"] = aggregated["avg_response_hours"]

    # Fetch user info (domain, display_name, team_function) from tracked_users
    users_result = supabase.table("tracked_users").select("email, domain, display_name, team_function").execute()
    if users_result.data:
        users_df = pd.DataFrame(users_result.data)
        aggregated = aggregated.merge(
            users_df,
            left_on="user_email",
            right_on="email",
            how="left"
        )
        # Extract domain from email if not in tracked_users
        aggregated["domain"] = aggregated.apply(
            lambda row: row["domain"] if pd.notna(row.get("domain")) else row["user_email"].split("@")[1] if "@" in row["user_email"] else "unknown",
            axis=1
        )
        aggregated["display_name"] = aggregated.apply(
            lambda row: row["display_name"] if pd.notna(row.get("display_name")) else row["user_email"].split("@")[0],
            axis=1
        )
        # Handle team_function
        aggregated["team_function"] = aggregated.apply(
            lambda row: row["team_function"] if pd.notna(row.get("team_function")) else "unknown",
            axis=1
        )
    else:
        # Fallback: extract domain from email
        aggregated["domain"] = aggregated["user_email"].apply(lambda x: x.split("@")[1] if "@" in x else "unknown")
        aggregated["display_name"] = aggregated["user_email"].apply(lambda x: x.split("@")[0])
        aggregated["team_function"] = "unknown"

    # Rename columns to match dashboard format
    aggregated = aggregated.rename(columns={
        "user_email": "Email",
        "avg_response_hours": "Avg Response (hrs)",
        "median_response_hours": "Median Response (hrs)",
        "emails_received": "Emails Received",
        "emails_sent": "Emails Sent",
        "response_pairs_count": "Responses Tracked",
        "domain": "Domain",
        "display_name": "Name",
        "team_function": "Team",
    })

    # Round numeric columns (convert to numeric first to handle nulls)
    aggregated["Avg Response (hrs)"] = pd.to_numeric(aggregated["Avg Response (hrs)"], errors='coerce').round(1)
    aggregated["Median Response (hrs)"] = pd.to_numeric(aggregated["Median Response (hrs)"], errors='coerce').round(1)

    # Reorder columns
    column_order = ["Name", "Email", "Domain", "Team", "Median Response (hrs)", "Avg Response (hrs)", "Responses Tracked", "Emails Received", "Emails Sent"]
    # Only include columns that exist
    column_order = [c for c in column_order if c in aggregated.columns]
    aggregated = aggregated[column_order]

    return aggregated


def get_daily_trend(user_email: str, start_date: date, end_date: date) -> pd.DataFrame:
    """
    Fetch daily trend data for a specific user.
    """
    supabase = get_supabase()

    result = supabase.table("daily_stats").select("*").eq(
        "user_email", user_email
    ).gte(
        "date", start_date.isoformat()
    ).lte(
        "date", end_date.isoformat()
    ).order("date").execute()

    if not result.data:
        return pd.DataFrame()

    return pd.DataFrame(result.data)


def get_received_emails(user_email: str, start_date: date, end_date: date, limit: int = 50) -> pd.DataFrame:
    """
    Fetch all received emails for a user within a date range.
    """
    supabase = get_supabase()

    result = supabase.table("received_emails").select(
        "sender_email, subject, received_at, replied, replied_at, response_hours, body_preview"
    ).eq(
        "user_email", user_email
    ).gte(
        "received_at", start_date.isoformat()
    ).lte(
        "received_at", end_date.isoformat() + "T23:59:59"
    ).order(
        "received_at", desc=True
    ).limit(limit).execute()

    if not result.data:
        return pd.DataFrame()

    df = pd.DataFrame(result.data)

    # Format timestamps
    df['received_at'] = pd.to_datetime(df['received_at']).dt.strftime('%b %d, %H:%M')
    df['replied_at'] = df['replied_at'].apply(
        lambda x: pd.to_datetime(x).strftime('%b %d, %H:%M') if pd.notna(x) and x else ""
    )

    # Format response time
    def format_response_time(hours):
        if pd.isna(hours) or hours is None:
            return ""
        if hours < 1:
            return f"{int(hours * 60)}m"
        elif hours < 24:
            return f"{int(hours)}h {int((hours % 1) * 60)}m"
        else:
            days = int(hours / 24)
            remaining_hours = int(hours % 24)
            return f"{days}d {remaining_hours}h"

    df['response_time'] = df['response_hours'].apply(format_response_time)
    df['replied'] = df['replied'].apply(lambda x: "Yes" if x else "No")

    # Truncate long fields
    df['sender_email'] = df['sender_email'].str[:35]
    df['subject'] = df['subject'].str[:40]

    # Handle body_preview
    if 'body_preview' not in df.columns:
        df['body_preview'] = ""
    df['body_preview'] = df['body_preview'].fillna("").str[:300]

    return df


def get_received_emails_stats(user_email: str, start_date: date, end_date: date) -> dict:
    """
    Get summary stats for received emails (total, replied, reply rate).
    """
    supabase = get_supabase()

    # Get total count
    total_result = supabase.table("received_emails").select(
        "id", count="exact"
    ).eq(
        "user_email", user_email
    ).gte(
        "received_at", start_date.isoformat()
    ).lte(
        "received_at", end_date.isoformat() + "T23:59:59"
    ).execute()

    # Get replied count
    replied_result = supabase.table("received_emails").select(
        "id", count="exact"
    ).eq(
        "user_email", user_email
    ).eq(
        "replied", True
    ).gte(
        "received_at", start_date.isoformat()
    ).lte(
        "received_at", end_date.isoformat() + "T23:59:59"
    ).execute()

    total = total_result.count if total_result.count is not None else 0
    replied = replied_result.count if replied_result.count is not None else 0
    rate = (replied / total * 100) if total > 0 else 0

    return {"total": total, "replied": replied, "rate": rate}


def get_recent_response_pairs(user_email: str, start_date: date, end_date: date, limit: int = 10) -> pd.DataFrame:
    """
    Fetch the most recent response pairs for a specific user within a date range.
    Includes thread_id and exclusion status for the exclude/restore UI.
    """
    supabase = get_supabase()

    result = supabase.table("response_pairs").select(
        "thread_id, user_email, external_sender, subject, received_at, replied_at, response_hours, adjusted_response_hours"
    ).eq(
        "user_email", user_email
    ).gte(
        "replied_at", start_date.isoformat()
    ).lte(
        "replied_at", end_date.isoformat() + "T23:59:59"
    ).order(
        "replied_at", desc=True
    ).limit(limit).execute()

    if not result.data:
        return pd.DataFrame()

    df = pd.DataFrame(result.data)

    # Keep raw replied_at for exclusion logic
    df['raw_replied_at'] = df['replied_at']

    # Fetch excluded pairs and mark status
    try:
        excluded = get_excluded_pairs(user_email)
        excluded_keys = {(ep["thread_id"], ep["replied_at"]) for ep in excluded}
        # Build a lookup from (thread_id, replied_at) -> excluded row id
        excluded_id_map = {(ep["thread_id"], ep["replied_at"]): ep["id"] for ep in excluded}
        df['excluded'] = df.apply(
            lambda r: (r["thread_id"], r["raw_replied_at"]) in excluded_keys, axis=1
        )
        df['excluded_id'] = df.apply(
            lambda r: excluded_id_map.get((r["thread_id"], r["raw_replied_at"])), axis=1
        )
    except Exception:
        df['excluded'] = False
        df['excluded_id'] = None

    # Fetch whitelisted pairs (overrides for >7d filter)
    try:
        whitelisted = get_whitelisted_pairs(user_email)
        whitelisted_keys = {(wp["thread_id"], wp["replied_at"]) for wp in whitelisted}
        whitelisted_id_map = {(wp["thread_id"], wp["replied_at"]): wp["id"] for wp in whitelisted}
        df['whitelisted'] = df.apply(
            lambda r: (r["thread_id"], r["raw_replied_at"]) in whitelisted_keys, axis=1
        )
        df['whitelisted_id'] = df.apply(
            lambda r: whitelisted_id_map.get((r["thread_id"], r["raw_replied_at"])), axis=1
        )
    except Exception:
        df['whitelisted'] = False
        df['whitelisted_id'] = None

    # Format the data for display
    df['received_at'] = pd.to_datetime(df['received_at']).dt.strftime('%b %d, %H:%M')
    df['replied_at'] = pd.to_datetime(df['replied_at']).dt.strftime('%b %d, %H:%M')

    # Format response time
    def format_response_time(hours):
        if hours < 1:
            return f"{int(hours * 60)}m"
        elif hours < 24:
            return f"{int(hours)}h {int((hours % 1) * 60)}m"
        else:
            days = int(hours / 24)
            remaining_hours = int(hours % 24)
            return f"{days}d {remaining_hours}h"

    df['response_time'] = df['response_hours'].apply(format_response_time)

    # Truncate long fields
    df['external_sender'] = df['external_sender'].str[:35]
    df['subject'] = df['subject'].str[:40]

    return df


# Page configuration
st.set_page_config(
    page_title="Lumiere Email Response Dashboard",
    page_icon="📧",
    layout="wide"
)

# Initialize session state for data refresh
if 'refresh_counter' not in st.session_state:
    st.session_state.refresh_counter = 0

# Sidebar
with st.sidebar:
    st.header("Settings")

    # Time Window Dropdown
    time_window = st.selectbox(
        "Time Window",
        options=[
            'Yesterday',
            'Last 7 Days (Week)',
            'Last 14 Days (Sprint)',
            'Last 30 Days (Month)',
            'Last 90 Days (Quarter)',
            'Custom Range'
        ],
        index=2  # Default to Last 14 Days
    )

    # Calculate start_date and end_date based on selection
    today = date.today()

    if time_window == 'Yesterday':
        start_date = today - timedelta(days=1)
        end_date = today - timedelta(days=1)
    elif time_window == 'Last 7 Days (Week)':
        start_date = today - timedelta(days=7)
        end_date = today
    elif time_window == 'Last 14 Days (Sprint)':
        start_date = today - timedelta(days=14)
        end_date = today
    elif time_window == 'Last 30 Days (Month)':
        start_date = today - timedelta(days=30)
        end_date = today
    elif time_window == 'Last 90 Days (Quarter)':
        start_date = today - timedelta(days=90)
        end_date = today
    else:  # Custom Range
        col_start, col_end = st.columns(2)
        with col_start:
            start_date = st.date_input(
                "Start Date",
                value=today - timedelta(days=30),
                max_value=today
            )
        with col_end:
            end_date = st.date_input(
                "End Date",
                value=today,
                max_value=today
            )

    # Show selected date range
    st.caption(f"{start_date.strftime('%b %d, %Y')} - {end_date.strftime('%b %d, %Y')}")

    st.divider()

    # Response Time Mode Toggle
    response_time_mode = st.radio(
        "Response Time Mode",
        options=["Raw Time", "Working Hours Adjusted"],
        index=0,
        help="Raw shows actual elapsed time. Adjusted excludes weekends and out-of-office days."
    )
    use_adjusted = response_time_mode == "Working Hours Adjusted"

    # Exclude long responses toggle
    exclude_long_responses = st.checkbox(
        "Exclude responses > 7 days",
        value=True,
        help="Filter out response pairs where the reply took more than 7 days (168 hours)"
    )

    # Explainer for each time window
    st.divider()
    st.subheader("About This Data")

    days = (end_date - start_date).days

    filter_note = " Responses taking longer than 7 days are excluded." if exclude_long_responses else ""

    if use_adjusted:
        st.info(f"""
        **Data from the last {days} days** ({start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}) — **Working Hours Adjusted**

        Response times count full 24-hour days but **exclude weekends** (if configured for the user's timezone) and **out-of-office days**. On the day an email is received, time counts from when it arrived to end of day. On the reply day, time counts from start of day to when the reply was sent.

        *Example: An email received Friday at 4 PM with a reply Monday at 10 AM would show ~18 hours (8 hrs Friday + 10 hrs Monday), skipping Saturday and Sunday. If a user is marked as OOO for an entire week, none of those days count toward their response time.*

        This shows email threads between **external senders** and the tracked user. Internal emails (same domain) and automated messages are excluded.{filter_note}
        """)
    else:
        st.info(f"""
        **Data from the last {days} days** ({start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}) — **Raw Time**

        Response times are calculated as total elapsed time between receiving an email and sending a reply, including nights, weekends, holidays, and out-of-office days. OOO time is **not** excluded in this mode — switch to **Working Hours Adjusted** to account for OOO periods.

        *Example: An email received Friday at 4 PM with a reply Monday at 10 AM would show ~66 hours.*

        This shows email threads between **external senders** and the tracked user. Internal emails (same domain) and automated messages are excluded.{filter_note}
        """)

    st.divider()

    # Refresh Button
    if st.button("Refresh Data", type="primary", use_container_width=True):
        st.cache_resource.clear()
        st.session_state.refresh_counter += 1
        st.rerun()

# Main Area
st.title("Lumiere Email Response Dashboard")
st.caption("📅 Data refreshes automatically every day at 1:00 AM EST")

# Create tabs
tab_dashboard, tab_manage = st.tabs(["📊 Dashboard", "👥 Manage Team"])

with tab_manage:
    st.header("Manage Tracked Users")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Add New User")
        new_email = st.text_input("Email Address", placeholder="user@lumiere.education")
        new_name = st.text_input("Display Name (optional)", placeholder="John Smith")
        team_function = st.selectbox(
            "Team Function",
            options=["operations", "growth", "other"],
            index=0,
            help="Select the team this user belongs to"
        )

        timezone_options = [
            "America/New_York", "America/Chicago", "America/Denver",
            "America/Los_Angeles", "America/Phoenix", "Europe/London",
            "Europe/Paris", "Asia/Kolkata", "Asia/Tokyo", "Asia/Shanghai", "UTC"
        ]
        user_timezone = st.selectbox("Timezone", options=timezone_options, index=0, key="add_timezone")
        exclude_weekends = st.checkbox("Exclude weekends from adjusted time", value=True, key="add_exclude_weekends")

        fetch_history = st.checkbox("Fetch 90 days of email history", value=True)

        if st.button("Add User & Fetch Data", use_container_width=True, type="primary"):
            if new_email and "@" in new_email:
                # First check if we have Gmail access for this user
                with st.spinner("Checking Gmail access..."):
                    has_access, access_msg = check_gmail_access(new_email)

                if not has_access:
                    if access_msg.startswith("domain_not_connected:"):
                        domain = access_msg.split(":")[1]
                        st.error(f"❌ Cannot access Gmail for @{domain}")
                        st.markdown(f"""
                        ### Domain Not Connected

                        The **@{domain}** domain is not currently connected to this tool.

                        **To connect a new domain, you need to:**

                        1. **Enable Domain-Wide Delegation** in Google Workspace Admin Console
                           - Go to [Google Admin Console](https://admin.google.com) → Security → API Controls → Domain-wide Delegation
                           - Add the service account client ID
                           - Grant scope: `https://www.googleapis.com/auth/gmail.readonly`

                        2. **Service Account Client ID** (from your credentials.json):
                           - Contact your administrator to set this up

                        3. **After setup**, try adding this user again

                        *Note: Only Google Workspace admins for @{domain} can complete this setup.*
                        """)
                    elif access_msg == "user_not_found":
                        st.error(f"❌ User not found: {new_email}")
                        st.info("Make sure the email address is correct and the user exists in the domain.")
                    else:
                        st.error(f"❌ Could not verify access: {access_msg}")
                else:
                    # Access verified, add the user
                    try:
                        supabase = get_supabase()
                        domain = new_email.split("@")[1] if "@" in new_email else None
                        supabase.table("tracked_users").insert({
                            "email": new_email,
                            "display_name": new_name if new_name else None,
                            "domain": domain,
                            "team_function": team_function,
                            "is_active": True,
                            "timezone": user_timezone,
                            "exclude_weekends": exclude_weekends,
                        }).execute()
                        st.success(f"✅ Added {new_email} to tracked users!")

                        # Trigger GitHub workflow to fetch data
                        if os.getenv("GITHUB_TOKEN"):
                            success, message = trigger_github_workflow(new_email, backfill=fetch_history)
                            if success:
                                st.balloons()
                                st.markdown("""
                                ### 🚀 Data Fetch Started!

                                **What's happening now:**
                                1. Our system is connecting to Gmail for this user
                                2. Fetching up to 2000 email threads (~90 days)
                                3. Analyzing response times for each thread
                                4. Uploading results to the database

                                **⏱️ This takes about 3-5 minutes.**

                                **Next steps:**
                                - You can check progress at [GitHub Actions](https://github.com/stephent-lumiere/lumiere-email-tracker/actions)
                                - Or just wait 5 minutes and switch to the Dashboard tab
                                - Click "Refresh Data" in the sidebar to see the new user
                                """)
                            else:
                                st.warning(f"Could not auto-fetch: {message}")
                        else:
                            st.info("Auto-fetch not configured. Add GITHUB_TOKEN to enable.")

                        st.cache_resource.clear()
                    except Exception as e:
                        if "duplicate" in str(e).lower():
                            st.warning(f"{new_email} is already being tracked.")
                        else:
                            st.error(f"Error adding user: {e}")
            else:
                st.warning("Please enter a valid email address.")

    with col2:
        st.subheader("Currently Tracked")
        try:
            supabase = get_supabase()
            users = supabase.table("tracked_users").select("email, display_name, domain, is_active, team_function").order("domain").execute()
            if users.data:
                # Group by domain
                domains = {}
                for user in users.data:
                    domain = user.get("domain") or user["email"].split("@")[1]
                    if domain not in domains:
                        domains[domain] = []
                    domains[domain].append(user)

                for domain, domain_users in domains.items():
                    st.markdown(f"**@{domain}** ({len(domain_users)})")
                    for user in domain_users:
                        status = "✅" if user["is_active"] else "❌"
                        name = user.get('display_name') or user['email'].split('@')[0]
                        team = user.get('team_function') or ''
                        team_label = f" [{team}]" if team else ""
                        st.write(f"  {status} {name}{team_label}")
            else:
                st.write("No users being tracked yet.")
        except Exception as e:
            st.error(f"Error loading users: {e}")

    st.divider()

    st.subheader("Edit User Team")
    st.caption("Change the team assignment for an existing tracked user")
    try:
        supabase_edit = get_supabase()
        edit_users = supabase_edit.table("tracked_users").select("email, display_name, team_function").eq("is_active", True).order("email").execute()
        if edit_users.data:
            edit_col1, edit_col2, edit_col3 = st.columns([2, 2, 1])
            edit_options = {
                (u.get("display_name") or u["email"].split("@")[0]) + f" ({u['email']})": u["email"]
                for u in edit_users.data
            }
            with edit_col1:
                selected_label = st.selectbox("Select user", list(edit_options.keys()), key="edit_team_user")
            with edit_col2:
                new_team = st.selectbox(
                    "New team",
                    options=["operations", "growth", "other"],
                    key="edit_team_value"
                )
            with edit_col3:
                st.write("")  # spacing
                if st.button("Update Team", use_container_width=True, key="edit_team_btn"):
                    selected_email = edit_options[selected_label]
                    supabase_edit.table("tracked_users").update({"team_function": new_team}).eq("email", selected_email).execute()
                    st.success(f"Updated team for {selected_email} to {new_team}")
                    st.cache_resource.clear()
        else:
            st.write("No active users to edit.")
    except Exception as e:
        st.error(f"Error loading users: {e}")

    st.divider()

    st.subheader("Edit Timezone & Weekends")
    st.caption("Update timezone and weekend settings for a user")

    try:
        supabase_hours = get_supabase()
        hours_users = supabase_hours.table("tracked_users").select(
            "email, display_name, timezone, exclude_weekends"
        ).eq("is_active", True).order("email").execute()

        if hours_users.data:
            hours_options = {
                (u.get("display_name") or u["email"].split("@")[0]) + f" ({u['email']})": u
                for u in hours_users.data
            }

            selected_hours_label = st.selectbox("Select user", list(hours_options.keys()), key="hours_user")
            selected_hours_user = hours_options[selected_hours_label]

            timezone_options = [
                "America/New_York", "America/Chicago", "America/Denver",
                "America/Los_Angeles", "America/Phoenix", "Europe/London",
                "Europe/Paris", "Asia/Kolkata", "Asia/Tokyo", "Asia/Shanghai", "UTC"
            ]
            current_tz = selected_hours_user.get("timezone") or "America/New_York"
            current_tz_idx = timezone_options.index(current_tz) if current_tz in timezone_options else 0
            new_timezone = st.selectbox("Timezone", options=timezone_options, index=current_tz_idx, key="edit_timezone")

            new_exclude_weekends = st.checkbox(
                "Exclude weekends",
                value=selected_hours_user.get("exclude_weekends", True),
                key="edit_exclude_weekends"
            )

            if st.button("Update Settings", use_container_width=True, key="update_hours_btn"):
                supabase_hours.table("tracked_users").update({
                    "timezone": new_timezone,
                    "exclude_weekends": new_exclude_weekends,
                }).eq("email", selected_hours_user["email"]).execute()
                st.success(f"Updated settings for {selected_hours_user['email']}")
                st.cache_resource.clear()
        else:
            st.write("No active users to edit.")
    except Exception as e:
        st.error(f"Error: {e}")

    st.divider()

    st.subheader("Current Timezone & Weekend Settings")
    st.caption("Overview of timezone and weekend settings for all active users")

    try:
        supabase_view = get_supabase()
        view_users = supabase_view.table("tracked_users").select(
            "email, display_name, timezone, exclude_weekends"
        ).eq("is_active", True).order("email").execute()

        if view_users.data:
            # Build display data
            display_data = []
            for u in view_users.data:
                name = u.get("display_name") or u["email"].split("@")[0]
                tz = u.get("timezone") or "America/New_York"
                weekends = "No" if u.get("exclude_weekends", True) else "Yes"
                display_data.append({
                    "Name": name,
                    "Email": u["email"],
                    "Timezone": tz,
                    "Include Weekends": weekends,
                })

            view_df = pd.DataFrame(display_data)
            st.dataframe(
                view_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Name": st.column_config.TextColumn("Name", width="medium"),
                    "Email": st.column_config.TextColumn("Email", width="large"),
                    "Timezone": st.column_config.TextColumn("Timezone", width="medium"),
                    "Include Weekends": st.column_config.TextColumn("Weekends", width="small", help="Yes = weekends count toward response time. No = weekends are excluded from adjusted response time."),
                }
            )
        else:
            st.write("No active users found.")
    except Exception as e:
        st.error(f"Error loading settings: {e}")

    st.divider()

    st.subheader("Out of Office")
    st.caption("Mark dates when a user is unavailable")

    try:
        import time as time_module
        supabase_ooo = get_supabase()
        # Retry logic for transient errors
        for attempt in range(3):
            try:
                ooo_users = supabase_ooo.table("tracked_users").select("email, display_name").eq("is_active", True).order("email").execute()
                break
            except Exception as e:
                if attempt < 2:
                    time_module.sleep(1)
                    continue
                raise e

        if ooo_users.data:
            ooo_options = {
                (u.get("display_name") or u["email"].split("@")[0]) + f" ({u['email']})": u["email"]
                for u in ooo_users.data
            }

            ooo_col1, ooo_col2 = st.columns(2)

            with ooo_col1:
                st.markdown("**Add OOO Period**")
                selected_ooo_user = st.selectbox("User", list(ooo_options.keys()), key="ooo_user")
                ooo_email = ooo_options[selected_ooo_user]

                ooo_start = st.date_input("Start Date", key="ooo_start")
                ooo_end = st.date_input("End Date", key="ooo_end")
                ooo_description = st.text_input("Description (optional)", placeholder="Vacation, sick leave, etc.", key="ooo_desc")

                if st.button("Add OOO Period", use_container_width=True, key="add_ooo_btn"):
                    if ooo_end >= ooo_start:
                        supabase_ooo.table("user_out_of_office").insert({
                            "user_email": ooo_email,
                            "start_date": ooo_start.isoformat(),
                            "end_date": ooo_end.isoformat(),
                            "description": ooo_description if ooo_description else None,
                        }).execute()
                        st.success(f"Added OOO period for {ooo_email}")
                        st.cache_resource.clear()
                    else:
                        st.warning("End date must be on or after start date")

            with ooo_col2:
                st.markdown("**Current OOO Periods**")
                # Show existing OOO for selected user
                existing_ooo = supabase_ooo.table("user_out_of_office").select("*").eq(
                    "user_email", ooo_email
                ).order("start_date", desc=True).execute()

                if existing_ooo.data:
                    for ooo in existing_ooo.data:
                        start = datetime.fromisoformat(ooo["start_date"]).strftime("%b %d, %Y")
                        end = datetime.fromisoformat(ooo["end_date"]).strftime("%b %d, %Y")
                        desc = ooo.get("description") or "No description"
                        col_a, col_b = st.columns([3, 1])
                        with col_a:
                            st.write(f"{start} - {end}: {desc}")
                        with col_b:
                            if st.button("Delete", key=f"del_ooo_{ooo['id']}"):
                                supabase_ooo.table("user_out_of_office").delete().eq("id", ooo["id"]).execute()
                                st.rerun()
                else:
                    st.write("No OOO periods set")
        else:
            st.write("No active users.")
    except Exception as e:
        st.error(f"Error: {e}")

    st.divider()

    st.subheader("Sync Existing User")
    st.caption("Manually trigger a data refresh for an existing user")

    if os.getenv("GITHUB_TOKEN"):
        try:
            supabase = get_supabase()
            users_result = supabase.table("tracked_users").select("email").eq("is_active", True).execute()
            user_emails = [u["email"] for u in users_result.data] if users_result.data else []

            if user_emails:
                col_sync1, col_sync2 = st.columns([2, 1])
                with col_sync1:
                    sync_email = st.selectbox("Select user", user_emails)
                with col_sync2:
                    sync_backfill = st.checkbox("90 days history", value=False)

                if st.button("Fetch Latest Data", use_container_width=True):
                    success, message = trigger_github_workflow(sync_email, backfill=sync_backfill)
                    if success:
                        st.success(f"✅ Data fetch started for {sync_email}!")
                        st.info("⏱️ Takes 3-5 minutes. Refresh the Dashboard tab to see updated data.")
                    else:
                        st.error(message)
            else:
                st.write("No users being tracked yet.")
        except Exception as e:
            st.error(f"Error: {e}")
    else:
        st.info("Add GITHUB_TOKEN to .env to enable manual sync.")

with tab_dashboard:
    # Fetch data with spinner
    with st.spinner("Fetching data from Supabase..."):
        df = get_stats_from_supabase(start_date, end_date, use_adjusted=use_adjusted, exclude_long_responses=exclude_long_responses)

    if df.empty:
        st.warning("No data found for the selected date range.")
        st.stop()

    # Filters section
    st.markdown("### 🔍 Filters")
    filter_col1, filter_col2, filter_col3 = st.columns(3)

    with filter_col1:
        # Domain filter
        all_domains = ["All Domains"] + sorted(df['Domain'].unique().tolist())
        selected_domain = st.selectbox("Domain", options=all_domains, key="domain_filter")

    with filter_col2:
        # Team filter
        all_teams = ["All Teams"] + sorted([t for t in df['Team'].unique().tolist() if t != "unknown"])
        selected_team = st.selectbox("Team", options=all_teams, key="team_filter")

    with filter_col3:
        # Individual filter
        filtered_for_individual = df.copy()
        if selected_domain != "All Domains":
            filtered_for_individual = filtered_for_individual[filtered_for_individual['Domain'] == selected_domain]
        if selected_team != "All Teams":
            filtered_for_individual = filtered_for_individual[filtered_for_individual['Team'] == selected_team]

        individual_options = ["All Individuals"] + filtered_for_individual['Email'].tolist()
        selected_individual = st.selectbox("Individual", options=individual_options, key="individual_filter")

    # Apply filters
    df_filtered = df.copy()
    if selected_domain != "All Domains":
        df_filtered = df_filtered[df_filtered['Domain'] == selected_domain]
    if selected_team != "All Teams":
        df_filtered = df_filtered[df_filtered['Team'] == selected_team]
    if selected_individual != "All Individuals":
        df_filtered = df_filtered[df_filtered['Email'] == selected_individual]

    st.divider()

    # Summary metrics for filtered data
    filter_desc = []
    if selected_domain != "All Domains":
        filter_desc.append(f"@{selected_domain}")
    if selected_team != "All Teams":
        filter_desc.append(f"{selected_team.capitalize()}")
    if selected_individual != "All Individuals":
        filter_desc.append(selected_individual)

    filter_label = " | ".join(filter_desc) if filter_desc else "All Team Members"

    st.subheader(f"Summary: {filter_label}")
    st.caption(f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')} ({len(df_filtered)} people)")

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Median Response", f"{df_filtered['Median Response (hrs)'].mean():.1f} hrs")
    with col2:
        st.metric("Avg Response", f"{df_filtered['Avg Response (hrs)'].mean():.1f} hrs")
    with col3:
        st.metric("Responses Tracked", f"{int(df_filtered['Responses Tracked'].sum())}")
    with col4:
        st.metric("Emails Received", f"{int(df_filtered['Emails Received'].sum())}")
    with col5:
        st.metric("Emails Sent", f"{int(df_filtered['Emails Sent'].sum())}")

    st.divider()

    # Performance table (sortable)
    st.subheader("Performance Table")

    # Prepare display dataframe
    df_display = df_filtered[['Name', 'Email', 'Domain', 'Team', 'Median Response (hrs)', 'Avg Response (hrs)', 'Responses Tracked', 'Emails Received', 'Emails Sent']].copy()
    df_display = df_display.sort_values('Median Response (hrs)')

    # Use st.dataframe with sorting enabled
    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Name": st.column_config.TextColumn("Name", width="medium"),
            "Email": st.column_config.TextColumn("Email", width="large"),
            "Domain": st.column_config.TextColumn("Domain", width="medium"),
            "Team": st.column_config.TextColumn("Team", width="small"),
            "Median Response (hrs)": st.column_config.NumberColumn("Median (hrs)", format="%.1f"),
            "Avg Response (hrs)": st.column_config.NumberColumn("Avg (hrs)", format="%.1f"),
            "Responses Tracked": st.column_config.NumberColumn("Responses", format="%d"),
            "Emails Received": st.column_config.NumberColumn("Received", format="%d"),
            "Emails Sent": st.column_config.NumberColumn("Sent", format="%d"),
        }
    )

    # Chart - only show if more than 1 person
    if len(df_filtered) > 1:
        st.divider()
        st.subheader("Response Time Ranking")

        df_sorted = df_filtered.sort_values('Median Response (hrs)')

        fig_ranking = go.Figure()

        colors = ['#00CC96' if x < 4 else '#636EFA' if x < 12 else '#EF553B'
                  for x in df_sorted['Median Response (hrs)']]

        fig_ranking.add_trace(go.Bar(
            x=df_sorted['Median Response (hrs)'],
            y=df_sorted['Name'],
            orientation='h',
            marker_color=colors,
            text=[f"Median: {m:.1f}h | Avg: {a:.1f}h" for m, a in zip(df_sorted['Median Response (hrs)'], df_sorted['Avg Response (hrs)'])],
            textposition='outside'
        ))

        fig_ranking.update_layout(
            xaxis_title='Median Response Time (hours)',
            yaxis_title='',
            height=50 + len(df_filtered) * 40,
            showlegend=False
        )

        st.plotly_chart(fig_ranking, use_container_width=True)

    # Show response pairs when a single individual is selected
    if selected_individual != "All Individuals":
        st.divider()

        col_header, col_limit = st.columns([3, 1])
        with col_header:
            st.subheader("Recent Tracked Response Pairs")
            st.caption(f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}")
        with col_limit:
            pairs_option = st.selectbox(
                "Show",
                options=[10, 25, 50, 100, "All"],
                index=0,
                key="num_pairs_selector"
            )
            num_pairs = 10000 if pairs_option == "All" else pairs_option

        recent_pairs = get_recent_response_pairs(selected_individual, start_date, end_date, limit=num_pairs)

        if not recent_pairs.empty:
            st.caption(f"Showing {len(recent_pairs)} most recent response pairs")

            # Build display dataframe with Select checkbox
            display_pairs = recent_pairs[['external_sender', 'subject', 'received_at', 'replied_at', 'response_hours', 'response_time', 'excluded', 'thread_id', 'raw_replied_at', 'user_email', 'excluded_id', 'whitelisted', 'whitelisted_id']].copy()
            display_pairs.insert(0, 'Select', False)
            display_pairs['Response (hrs)'] = display_pairs['response_hours'].round(1)

            # Mark rows as excluded (manual, or >7d unless whitelisted)
            display_pairs['is_excluded'] = display_pairs.apply(
                lambda r: r['excluded'] or (exclude_long_responses and r['response_hours'] > 168 and not r['whitelisted']),
                axis=1
            )

            # Excluded column: checkmark for excluded pairs
            display_pairs['Excluded'] = display_pairs['is_excluded']

            # Rename visible columns
            display_pairs = display_pairs.rename(columns={
                'external_sender': 'External Sender',
                'subject': 'Subject',
                'received_at': 'Received',
                'replied_at': 'Replied',
                'response_time': 'Response Time',
            })

            edited_df = st.data_editor(
                display_pairs[['Select', 'Excluded', 'External Sender', 'Subject', 'Received', 'Replied', 'Response (hrs)', 'Response Time']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Select": st.column_config.CheckboxColumn("Select", default=False),
                    "Excluded": st.column_config.CheckboxColumn("Excluded", disabled=True),
                    "External Sender": st.column_config.TextColumn("External Sender", width="medium"),
                    "Subject": st.column_config.TextColumn("Subject", width="medium"),
                    "Received": st.column_config.TextColumn("Received", width="small"),
                    "Replied": st.column_config.TextColumn("Replied", width="small"),
                    "Response (hrs)": st.column_config.NumberColumn("Response (hrs)", format="%.1f", width="small"),
                    "Response Time": st.column_config.TextColumn("Response Time", width="small"),
                },
                disabled=["Excluded", "External Sender", "Subject", "Received", "Replied", "Response (hrs)", "Response Time"],
                key="response_pairs_editor",
            )

            # Buttons for Exclude / Restore
            btn_col1, btn_col2, _ = st.columns([1, 1, 3])
            with btn_col1:
                exclude_clicked = st.button("Exclude Selected", key="exclude_selected_btn")
            with btn_col2:
                restore_clicked = st.button("Restore Selected", key="restore_selected_btn")

            if exclude_clicked:
                selected_indices = edited_df.index[edited_df['Select']].tolist()
                # Filter to only active (non-excluded) pairs
                selected_indices = [i for i in selected_indices if not display_pairs.iloc[i]['is_excluded']]
                if not selected_indices:
                    st.warning("No active pairs selected to exclude.")
                else:
                    try:
                        affected_dates = set()
                        for idx in selected_indices:
                            row = display_pairs.iloc[idx]
                            pair_data = {
                                "thread_id": row['thread_id'],
                                "replied_at": row['raw_replied_at'],
                                "user_email": row['user_email'],
                                "external_sender": row['External Sender'],
                                "subject": row['Subject'],
                                "response_hours": row['response_hours'],
                            }
                            exclude_response_pair(pair_data)
                            # If this pair was whitelisted, remove the whitelist entry too
                            if row['whitelisted_id']:
                                remove_whitelisted_pair(str(row['whitelisted_id']))
                            replied_dt = pd.to_datetime(row['raw_replied_at'])
                            affected_dates.add(replied_dt.date().isoformat())
                        recalculate_daily_stats(selected_individual, list(affected_dates))
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error excluding pairs: {e}")
                        st.info("If this is an RLS error, disable Row Level Security on the `excluded_response_pairs` and `whitelisted_response_pairs` tables in Supabase.")

            if restore_clicked:
                selected_indices = edited_df.index[edited_df['Select']].tolist()
                # Filter to only excluded pairs (manual or >7d)
                selected_indices = [i for i in selected_indices if display_pairs.iloc[i]['is_excluded']]
                if not selected_indices:
                    st.warning("No excluded pairs selected to restore.")
                else:
                    try:
                        affected_dates = set()
                        for idx in selected_indices:
                            row = display_pairs.iloc[idx]
                            if row['excluded']:
                                # Manually excluded — remove from excluded table
                                exc_id = row['excluded_id']
                                if exc_id:
                                    restore_response_pair(str(exc_id))
                            elif exclude_long_responses and row['response_hours'] > 168:
                                # Excluded by >7d filter — whitelist it
                                whitelist_response_pair({
                                    "thread_id": row['thread_id'],
                                    "replied_at": row['raw_replied_at'],
                                    "user_email": row['user_email'],
                                })
                            replied_dt = pd.to_datetime(row['raw_replied_at'])
                            affected_dates.add(replied_dt.date().isoformat())
                        recalculate_daily_stats(selected_individual, list(affected_dates))
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error restoring pairs: {e}")
                        st.info("If this is an RLS error, disable Row Level Security on the `excluded_response_pairs` and `whitelisted_response_pairs` tables in Supabase.")
        else:
            st.info("No response pairs found for this user in this time period.")

    # Recent Emails Received section - shows for individual or all
    if selected_individual != "All Individuals":
        st.divider()

        col_recv_header, col_recv_limit = st.columns([3, 1])
        with col_recv_header:
            st.subheader("Recent Emails Received")
            st.caption(f"External emails received by {selected_individual}")
        with col_recv_limit:
            num_received = st.selectbox(
                "Show",
                options=[10, 25, 50, 100],
                index=1,
                key="num_received_selector"
            )

        st.caption("Excludes: internal emails (same domain), automated messages (newsletters, notifications, noreply, calendar alerts, Stripe, etc.)")

        # Show reply rate stats
        recv_stats = get_received_emails_stats(selected_individual, start_date, end_date)
        if recv_stats["total"] > 0:
            stat_col1, stat_col2, stat_col3 = st.columns(3)
            with stat_col1:
                st.metric("External Emails Received", recv_stats["total"])
            with stat_col2:
                st.metric("Replied To", recv_stats["replied"])
            with stat_col3:
                st.metric("Reply Rate", f"{recv_stats['rate']:.0f}%")

        received_df = get_received_emails(selected_individual, start_date, end_date, limit=num_received)

        if not received_df.empty:
            display_received = received_df[['sender_email', 'subject', 'received_at', 'replied', 'response_time', 'body_preview']].copy()
            display_received.columns = ['From', 'Subject', 'Received', 'Replied', 'Response Time', 'Email Preview']
            st.dataframe(
                display_received,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "From": st.column_config.TextColumn("From", width="medium"),
                    "Subject": st.column_config.TextColumn("Subject", width="medium"),
                    "Received": st.column_config.TextColumn("Received", width="small"),
                    "Replied": st.column_config.TextColumn("Replied", width="small"),
                    "Response Time": st.column_config.TextColumn("Response Time", width="small"),
                    "Email Preview": st.column_config.TextColumn("Email Preview", width="large"),
                }
            )
        else:
            st.info("No received email data yet. Data will appear after the next sync.")

    # Understanding metrics at the bottom
    st.divider()
    with st.expander("📊 Understanding the Metrics"):
        st.markdown("""
        - **Median Response Time**: The middle value of all response times - half of responses are faster, half are slower. Best indicator of typical behavior.
        - **Avg Response Time**: Mean time to respond to external emails. Can be skewed by a few very slow responses.
        - **Responses Tracked**: The number of external email → user reply pairs found. **This is what response time calculations are based on.** Each time an external person emails and the user replies, that's one tracked response.
        - **Emails Received**: External emails received (excludes internal @lumiere.education emails and automated messages).
        - **Emails Sent**: Emails sent by this user in tracked threads.

        **Excluded Response Pairs**

        Some response pairs are excluded from metric calculations and marked with a tag in the response pairs table:

        - **[excluded]** — Manually excluded via the checkbox selection and "Exclude Selected" button. Use "Restore Selected" to include them again.
        - **[>7d]** — Automatically excluded because the response took longer than 7 days (168 hours). This filter is controlled by the "Exclude responses > 7 days" checkbox in the sidebar. You can also restore individual >7d pairs using "Restore Selected" — restored pairs will stay included even with the >7d filter on.

        Excluded pairs still appear in the table for visibility but are not counted toward the summary metrics above.
        """)
