import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import random
from datetime import datetime, timedelta, date

# Try to import from tracker module, fallback to dummy data
try:
    from tracker import get_domain_stats
except ImportError:
    def get_domain_stats(domain_name: str, start_date: date = None, end_date: date = None) -> pd.DataFrame:
        """
        Fallback function that generates mock data for testing.
        Returns a DataFrame with email response statistics per user.

        Args:
            domain_name: The domain to fetch stats for
            start_date: Start of date range filter
            end_date: End of date range filter
        """
        # Real email addresses per domain
        domain_users = {
            'Lumiere Education': [
                'dhruva@lumiere.education',
                'stephen@lumiere.education',
                'alex@lumiere.education',
                'maria@lumiere.education'
            ],
            'Ladder Internships': [
                'jordan@ladderinternships.com',
                'casey@ladderinternships.com',
                'taylor@ladderinternships.com'
            ],
            'Veritas AI': [
                'sam@veritasai.com',
                'jamie@veritasai.com',
                'riley@veritasai.com'
            ],
            'Horizon Academic': [
                'avery@horizonacademic.com',
                'dakota@horizonacademic.com'
            ]
        }

        emails = domain_users.get(domain_name, ['user1@example.com', 'user2@example.com'])

        # Calculate days in range for scaling mock data
        if start_date and end_date:
            days_in_range = (end_date - start_date).days
        else:
            days_in_range = 14  # default

        # Scale email volumes based on date range
        volume_scale = days_in_range / 30  # normalize to ~30 days

        # Realistic response time data (in hours)
        data = []
        for email in emails:
            avg_response = round(random.uniform(0.5, 8.0), 1)
            median_response = round(avg_response * random.uniform(0.5, 0.9), 1)
            base_received = random.randint(45, 180)
            base_sent = random.randint(40, 160)
            emails_received = max(1, int(base_received * volume_scale))
            emails_sent = max(1, int(base_sent * volume_scale))
            unanswered = max(0, int(random.randint(2, 15) * volume_scale))
            avg_reply_length = random.randint(50, 250)

            data.append({
                'Email': email,
                'Avg Response (hrs)': avg_response,
                'Median Response (hrs)': median_response,
                'Emails Received': emails_received,
                'Emails Sent': emails_sent,
                'Unanswered': unanswered,
                'Avg Reply Length': avg_reply_length
            })

        return pd.DataFrame(data)


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

    # Domain Dropdown
    domain = st.selectbox(
        "Select Domain",
        options=[
            'Lumiere Education',
            'Ladder Internships',
            'Veritas AI',
            'Horizon Academic'
        ]
    )

    # Time Window Dropdown
    time_window = st.selectbox(
        "Time Window",
        options=[
            'Last 14 Days (Sprint)',
            'Last 30 Days (Month)',
            'Last 90 Days (Quarter)',
            'Custom Range'
        ],
        index=0  # Default to Last 14 Days
    )

    # Calculate start_date and end_date based on selection
    today = date.today()

    if time_window == 'Last 14 Days (Sprint)':
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

    # Refresh Button
    if st.button("Refresh Data", type="primary", use_container_width=True):
        st.session_state.refresh_counter += 1
        st.rerun()

    # Manage Team Expander
    with st.expander("Manage Team"):
        new_email = st.text_input("New Email", placeholder="user@example.com")
        if st.button("Add User", use_container_width=True):
            if new_email:
                st.success(f"Successfully added {new_email} to the team!")
            else:
                st.warning("Please enter an email address.")

# Main Area
st.title("Lumiere Email Response Dashboard")

# Fetch data with spinner
with st.spinner("Fetching emails... this may take a moment"):
    df = get_domain_stats(domain, start_date=start_date, end_date=end_date)

# Individual selector
selected_email = st.selectbox(
    "Select Individual",
    options=['All'] + df['Email'].tolist()
)

st.divider()

if selected_email == 'All':
    # Show ranking table - sorted by response time (fastest first)
    st.subheader("Individual Performance Ranking")

    df_sorted = df.sort_values('Avg Response (hrs)')

    # Response time ranking chart
    fig_ranking = go.Figure()

    colors = ['#00CC96' if x < 2 else '#636EFA' if x < 4 else '#EF553B'
              for x in df_sorted['Avg Response (hrs)']]

    fig_ranking.add_trace(go.Bar(
        x=df_sorted['Avg Response (hrs)'],
        y=df_sorted['Email'],
        orientation='h',
        marker_color=colors,
        text=[f"{x:.1f} hrs" for x in df_sorted['Avg Response (hrs)']],
        textposition='outside'
    ))

    fig_ranking.update_layout(
        xaxis_title='Average Response Time (hours)',
        yaxis_title='',
        height=50 + len(df) * 50,
        showlegend=False
    )

    st.plotly_chart(fig_ranking, use_container_width=True)

    # Full table
    st.subheader("All Individuals")
    st.dataframe(
        df_sorted,
        use_container_width=True,
        hide_index=True,
        column_config={
            'Email': st.column_config.TextColumn(width='large'),
            'Avg Response (hrs)': st.column_config.NumberColumn(format="%.1f"),
            'Median Response (hrs)': st.column_config.NumberColumn(format="%.1f"),
            'Emails Received': st.column_config.NumberColumn(format="%d"),
            'Emails Sent': st.column_config.NumberColumn(format="%d"),
            'Unanswered': st.column_config.NumberColumn(format="%d"),
            'Avg Reply Length': st.column_config.NumberColumn(format="%d chars")
        }
    )

else:
    # Individual detail view
    person = df[df['Email'] == selected_email].iloc[0]

    st.subheader(f"{selected_email}")

    # Key metrics for this person
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Avg Response", f"{person['Avg Response (hrs)']:.1f} hrs")
    with col2:
        st.metric("Median Response", f"{person['Median Response (hrs)']:.1f} hrs")
    with col3:
        st.metric("Emails Received", f"{person['Emails Received']}")
    with col4:
        st.metric("Emails Sent", f"{person['Emails Sent']}")

    col5, col6 = st.columns(2)
    with col5:
        st.metric("Unanswered", f"{person['Unanswered']}")
    with col6:
        st.metric("Avg Reply Length", f"{person['Avg Reply Length']} chars")

    st.divider()

    # Compare to team
    st.subheader("Comparison to Others")

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
