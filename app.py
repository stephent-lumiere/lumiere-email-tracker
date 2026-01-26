import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from supabase import create_client

# Load environment variables
load_dotenv()

# Initialize Supabase client
@st.cache_resource
def get_supabase():
    return create_client(
        os.getenv("SUPABASE_URL"),
        os.getenv("SUPABASE_KEY")
    )

def get_stats_from_supabase(start_date: date, end_date: date) -> pd.DataFrame:
    """
    Fetch aggregated stats from Supabase daily_stats table.
    """
    supabase = get_supabase()

    # Query daily_stats for the date range
    result = supabase.table("daily_stats").select("*").gte(
        "date", start_date.isoformat()
    ).lte(
        "date", end_date.isoformat()
    ).execute()

    if not result.data:
        return pd.DataFrame()

    df = pd.DataFrame(result.data)

    # Aggregate by user
    aggregated = df.groupby("user_email").agg({
        "emails_received": "sum",
        "emails_sent": "sum",
        "response_pairs_count": "sum",
        "avg_response_hours": "mean",
        "median_response_hours": "mean",
    }).reset_index()

    # Rename columns to match dashboard format
    aggregated = aggregated.rename(columns={
        "user_email": "Email",
        "avg_response_hours": "Avg Response (hrs)",
        "median_response_hours": "Median Response (hrs)",
        "emails_received": "Emails Received",
        "emails_sent": "Emails Sent",
        "response_pairs_count": "Responses Tracked",
    })

    # Round numeric columns
    aggregated["Avg Response (hrs)"] = aggregated["Avg Response (hrs)"].round(1)
    aggregated["Median Response (hrs)"] = aggregated["Median Response (hrs)"].round(1)

    # Reorder columns: Median first, then Avg, then Responses Tracked
    column_order = ["Email", "Median Response (hrs)", "Avg Response (hrs)", "Responses Tracked", "Emails Received", "Emails Sent"]
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


def get_recent_response_pairs(user_email: str, start_date: date, end_date: date, limit: int = 10) -> pd.DataFrame:
    """
    Fetch the most recent response pairs for a specific user within a date range.
    """
    supabase = get_supabase()

    result = supabase.table("response_pairs").select(
        "external_sender, subject, received_at, replied_at, response_hours"
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
            'Last 7 Days (Week)',
            'Last 14 Days (Sprint)',
            'Last 30 Days (Month)',
            'Last 90 Days (Quarter)',
            'Custom Range'
        ],
        index=1  # Default to Last 14 Days
    )

    # Calculate start_date and end_date based on selection
    today = date.today()

    if time_window == 'Last 7 Days (Week)':
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

    # Explainer for each time window
    st.divider()
    st.subheader("About This Data")

    days = (end_date - start_date).days

    st.info(f"""
    **Data from the last {days} days** ({start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')})

    This shows email threads between **external senders** and the tracked user. Internal emails (same domain, e.g. @lumiere.education) are excluded, as are automated messages (newsletters, notifications, etc.).
    """)

    st.divider()

    # Refresh Button
    if st.button("Refresh Data", type="primary", use_container_width=True):
        st.cache_resource.clear()
        st.session_state.refresh_counter += 1
        st.rerun()

# Main Area
st.title("Lumiere Email Response Dashboard")

# Fetch data with spinner
with st.spinner("Fetching data from Supabase..."):
    df = get_stats_from_supabase(start_date, end_date)

if df.empty:
    st.warning("No data found for the selected date range.")
    st.stop()

# Check for query param navigation (from button clicks)
query_user = st.query_params.get("user", None)
if query_user and query_user in df['Email'].tolist():
    selected_email = query_user
else:
    selected_email = 'All'

# Build options list
options_list = ['📊 All Team Members'] + df['Email'].tolist()

# Determine current index based on selection
if selected_email == 'All':
    current_index = 0
elif selected_email in df['Email'].tolist():
    current_index = df['Email'].tolist().index(selected_email) + 1
else:
    current_index = 0

# Prominent selector at top
st.markdown("### 👤 View Dashboard For:")
col_select, col_spacer = st.columns([2, 3])
with col_select:
    dropdown_selection = st.selectbox(
        "Select Individual",
        options=options_list,
        index=current_index,
        key='email_selector',
        label_visibility="collapsed"
    )

# Handle dropdown change
if dropdown_selection == '📊 All Team Members':
    if selected_email != 'All':
        st.query_params.clear()
        st.rerun()
    selected_email = 'All'
elif dropdown_selection != selected_email:
    st.query_params["user"] = dropdown_selection
    st.rerun()

st.divider()

if selected_email == 'All':
    # Team summary metrics
    st.subheader(f"Team Summary ({start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')})")
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Team Median Response", f"{df['Median Response (hrs)'].mean():.1f} hrs")
    with col2:
        st.metric("Team Avg Response", f"{df['Avg Response (hrs)'].mean():.1f} hrs")
    with col3:
        st.metric("Responses Tracked", f"{int(df['Responses Tracked'].sum())}")
    with col4:
        st.metric("Emails Received", f"{int(df['Emails Received'].sum())}")
    with col5:
        st.metric("Emails Sent", f"{int(df['Emails Sent'].sum())}")

    st.divider()

    # Show ranking table - sorted by response time (fastest first)
    st.subheader("Individual Performance Ranking")

    df_sorted = df.sort_values('Median Response (hrs)')

    # Response time ranking chart - showing both avg and median
    fig_ranking = go.Figure()

    colors = ['#00CC96' if x < 4 else '#636EFA' if x < 12 else '#EF553B'
              for x in df_sorted['Median Response (hrs)']]

    fig_ranking.add_trace(go.Bar(
        x=df_sorted['Median Response (hrs)'],
        y=df_sorted['Email'],
        orientation='h',
        marker_color=colors,
        text=[f"Median: {m:.1f}h | Avg: {a:.1f}h" for m, a in zip(df_sorted['Median Response (hrs)'], df_sorted['Avg Response (hrs)'])],
        textposition='outside'
    ))

    fig_ranking.update_layout(
        xaxis_title='Median Response Time (hours)',
        yaxis_title='',
        height=50 + len(df) * 60,
        showlegend=False
    )

    st.plotly_chart(fig_ranking, use_container_width=True)

    # Full table with clickable names
    st.subheader("All Individuals")

    # Create clickable table using columns
    header_cols = st.columns([3, 1.5, 1.5, 1.5, 1.5, 1.5])
    header_cols[0].markdown("**Email**")
    header_cols[1].markdown("**Median (hrs)**")
    header_cols[2].markdown("**Avg (hrs)**")
    header_cols[3].markdown("**Responses**")
    header_cols[4].markdown("**Received**")
    header_cols[5].markdown("**Sent**")

    st.divider()

    for idx, row in df_sorted.iterrows():
        cols = st.columns([3, 1.5, 1.5, 1.5, 1.5, 1.5])
        email = row['Email']
        if cols[0].button(f"→ {email}", key=f"btn_{email}", use_container_width=True, type="primary"):
            st.query_params["user"] = email
            st.rerun()
        cols[1].write(f"{row['Median Response (hrs)']:.1f}")
        cols[2].write(f"{row['Avg Response (hrs)']:.1f}")
        cols[3].write(f"{int(row['Responses Tracked'])}")
        cols[4].write(f"{int(row['Emails Received'])}")
        cols[5].write(f"{int(row['Emails Sent'])}")

else:
    # Individual detail view
    person = df[df['Email'] == selected_email].iloc[0]

    st.subheader(f"{selected_email}")

    # Get min/max response times for this user
    trend_df = get_daily_trend(selected_email, start_date, end_date)
    if not trend_df.empty:
        min_response = trend_df['min_response_hours'].min()
        max_response = trend_df['max_response_hours'].max()
    else:
        min_response = None
        max_response = None

    # Key metrics for this person
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Median Response", f"{person['Median Response (hrs)']:.1f} hrs")
    with col2:
        st.metric("Avg Response", f"{person['Avg Response (hrs)']:.1f} hrs")
    with col3:
        if min_response is not None:
            st.metric("Fastest Response", f"{min_response:.1f} hrs")
        else:
            st.metric("Fastest Response", "N/A")
    with col4:
        if max_response is not None:
            st.metric("Slowest Response", f"{max_response:.1f} hrs")
        else:
            st.metric("Slowest Response", "N/A")

    col5, col6, col7 = st.columns(3)
    with col5:
        st.metric("Responses Tracked", f"{int(person['Responses Tracked'])}")
    with col6:
        st.metric("Emails Received", f"{int(person['Emails Received'])}")
    with col7:
        st.metric("Emails Sent", f"{int(person['Emails Sent'])}")

    st.divider()

    # Recent response pairs
    st.subheader(f"Tracked Response Pairs ({start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')})")

    recent_pairs = get_recent_response_pairs(selected_email, start_date, end_date, limit=10)

    if not recent_pairs.empty:
        display_pairs = recent_pairs[['external_sender', 'subject', 'received_at', 'replied_at', 'response_time']].copy()
        display_pairs.columns = ['External Sender', 'Subject', 'Received', 'Replied', 'Response Time']
        st.dataframe(display_pairs, use_container_width=True, hide_index=True)
    else:
        st.info("No response pairs found for this user.")

    st.divider()

    # Compare to team
    st.subheader("Comparison to Team")

    fig_compare = go.Figure()

    # Highlight selected person
    colors = ['#636EFA' if email != selected_email else '#EF553B'
              for email in df['Email']]

    fig_compare.add_trace(go.Bar(
        x=df['Email'],
        y=df['Avg Response (hrs)'],
        marker_color=colors,
        text=[f"{x:.1f}" for x in df['Avg Response (hrs)']],
        textposition='outside'
    ))

    fig_compare.update_layout(
        xaxis_title='',
        yaxis_title='Avg Response Time (hrs)',
        height=400,
        showlegend=False
    )

    st.plotly_chart(fig_compare, use_container_width=True)

# Understanding metrics at the bottom
st.divider()
with st.expander("📊 Understanding the Metrics"):
    st.markdown("""
    - **Median Response Time**: The middle value of all response times - half of responses are faster, half are slower. Best indicator of typical behavior.
    - **Avg Response Time**: Mean time to respond to external emails. Can be skewed by a few very slow responses.
    - **Responses Tracked**: The number of external email → user reply pairs found. **This is what response time calculations are based on.** Each time an external person emails and the user replies, that's one tracked response.
    - **Emails Received**: External emails received (excludes internal @lumiere.education emails and automated messages).
    - **Emails Sent**: Emails sent by this user in tracked threads.
    """)
