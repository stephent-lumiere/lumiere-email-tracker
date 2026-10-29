"""
Email one-time-code login for the dashboard.

require_login() shows a sign-in screen and stops the script until the visitor
has entered a code emailed to an address on one of the internal domains.
Codes are stored hashed in the Supabase `otp_codes` table
(migrations/create_otp_codes.sql), so rate limits survive page reloads.

Secrets (env vars / Streamlit Cloud secrets):
    OTP_SECRET      - long random string used to HMAC the codes
    RESEND_API_KEY  - Resend API key (same Resend account as the portals)
    FROM_EMAIL      - sender on a Resend-verified domain,
                      e.g. "Lumiere Email Dashboard <dashboard@lumiere.education>"
"""

import hashlib
import hmac
import os
import secrets
from datetime import datetime, timedelta, timezone

import resend
import streamlit as st

from tracker_supabase import DEFAULT_INTERNAL_DOMAINS

CODE_TTL = timedelta(minutes=10)
MAX_ATTEMPTS = 5            # Wrong guesses before a code is locked
RESEND_COOLDOWN_SECONDS = 60
MAX_CODES_PER_HOUR = 5

REQUIRED_SECRETS = ("OTP_SECRET", "RESEND_API_KEY", "FROM_EMAIL")


def normalise_email(email: str) -> str:
    return (email or "").strip().lower()


def is_allowed_login_email(email: str) -> bool:
    """Exact domain match against the internal domains (no subdomains)."""
    if not email or any(c.isspace() for c in email) or email.count("@") != 1:
        return False
    local, domain = email.split("@")
    return bool(local) and domain in DEFAULT_INTERNAL_DOMAINS


def _now() -> datetime:
    # Whole seconds keep the stored timestamps easy to parse back
    return datetime.now(timezone.utc).replace(microsecond=0)


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _hash_code(email: str, code: str) -> str:
    key = os.getenv("OTP_SECRET").encode()
    return hmac.new(key, f"{email}:{code}".encode(), hashlib.sha256).hexdigest()


def _send_code_email(to_email: str, code: str) -> None:
    resend.api_key = os.getenv("RESEND_API_KEY")
    minutes = int(CODE_TTL.total_seconds() // 60)

    resend.Emails.send({
        "from": os.getenv("FROM_EMAIL"),
        "to": [to_email],
        "subject": f"{code} is your Lumiere dashboard sign-in code",
        "html": f"""
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2 style="color: #BE1E2D;">Lumiere Email Response Dashboard</h2>
            <p>Your sign-in code is:</p>
            <p style="font-size: 32px; font-weight: bold; letter-spacing: 6px; margin: 24px 0;">{code}</p>
            <p style="color: #64748B; font-size: 14px;">
                It expires in {minutes} minutes and can only be used once.<br>
                If you didn't request this code, you can safely ignore this email.
            </p>
        </div>
        """,
        "text": (
            f"Your sign-in code for the Lumiere Email Response Dashboard is {code}.\n\n"
            f"It expires in {minutes} minutes and can only be used once. "
            f"If you didn't request it, you can ignore this email."
        ),
    })


def request_code(supabase, email: str) -> tuple[bool, str]:
    """Create, store and email a new code. Returns (success, message)."""
    now = _now()

    recent = supabase.table("otp_codes").select("created_at").eq(
        "email", email
    ).gte("created_at", (now - timedelta(hours=1)).isoformat()).order(
        "created_at", desc=True
    ).execute().data or []

    if recent:
        since_last = (now - _parse_ts(recent[0]["created_at"])).total_seconds()
        wait = int(RESEND_COOLDOWN_SECONDS - since_last)
        if wait > 0:
            return False, f"Please wait {wait} seconds before requesting another code."

    if len(recent) >= MAX_CODES_PER_HOUR:
        oldest = _parse_ts(recent[MAX_CODES_PER_HOUR - 1]["created_at"])
        minutes = max(1, int((oldest + timedelta(hours=1) - now).total_seconds() // 60) + 1)
        return False, f"Too many codes requested for this email. Try again in {minutes} minutes."

    code = f"{secrets.randbelow(1_000_000):06d}"
    inserted = supabase.table("otp_codes").insert({
        "email": email,
        "code_hash": _hash_code(email, code),
        "created_at": now.isoformat(),
        "expires_at": (now + CODE_TTL).isoformat(),
        "attempts": 0,
    }).execute().data

    try:
        _send_code_email(email, code)
    except Exception as e:
        print(f"Error sending sign-in code to {email}: {e}")
        # Don't let a failed send count against the user's hourly limit
        if inserted:
            supabase.table("otp_codes").delete().eq("id", inserted[0]["id"]).execute()
        return False, "Couldn't send the code email. Try again shortly, or contact an admin if it keeps failing."

    return True, f"We've sent a 6-digit code to {email}."


def has_active_code(supabase, email: str) -> bool:
    """True if the newest code for the email can still be used."""
    rows = supabase.table("otp_codes").select(
        "expires_at, attempts, consumed_at"
    ).eq("email", email).order("created_at", desc=True).limit(1).execute().data
    if not rows:
        return False
    row = rows[0]
    return (
        not row["consumed_at"]
        and row["attempts"] < MAX_ATTEMPTS
        and _parse_ts(row["expires_at"]) > _now()
    )


def verify_code(supabase, email: str, code: str) -> tuple[bool, str]:
    """Check a code against the most recent one issued for the email."""
    code = (code or "").strip()
    if len(code) != 6 or not code.isdigit():
        return False, "Enter the 6-digit code from the email."

    # Only the newest code counts: requesting a new one replaces the old
    rows = supabase.table("otp_codes").select(
        "id, code_hash, expires_at, attempts, consumed_at"
    ).eq("email", email).order("created_at", desc=True).limit(1).execute().data

    if not rows:
        return False, "No code found for this email. Request a new one."
    row = rows[0]

    if row["consumed_at"]:
        return False, "That code has already been used. Request a new one."
    if row["attempts"] >= MAX_ATTEMPTS:
        return False, "Too many wrong attempts. Request a new code."
    if _parse_ts(row["expires_at"]) <= _now():
        return False, "That code has expired. Request a new one."

    if not hmac.compare_digest(row["code_hash"], _hash_code(email, code)):
        attempts = row["attempts"] + 1
        supabase.table("otp_codes").update({"attempts": attempts}).eq("id", row["id"]).execute()
        left = MAX_ATTEMPTS - attempts
        if left <= 0:
            return False, "Too many wrong attempts. Request a new code."
        return False, f"Incorrect code. {left} attempt{'s' if left != 1 else ''} left."

    # Only succeed if this request is the one that marks the code used
    consumed = supabase.table("otp_codes").update(
        {"consumed_at": _now().isoformat()}
    ).eq("id", row["id"]).is_("consumed_at", "null").execute().data
    if not consumed:
        return False, "That code has already been used. Request a new one."

    return True, "Signed in."


def require_login(supabase) -> str:
    """Render the sign-in screen and stop the script until the visitor signs in.

    Must be called right after st.set_page_config(). Returns the signed-in email.
    """
    if st.session_state.get("auth_email"):
        return st.session_state["auth_email"]

    # One container for the whole sign-in screen, cleared before each rerun so
    # it doesn't linger greyed out while the dashboard loads
    screen = st.empty()
    with screen.container():
        st.title("Lumiere Email Response Dashboard")

        missing = [name for name in REQUIRED_SECRETS if not os.getenv(name)]
        if missing:
            st.error(f"Sign-in isn't configured yet. Missing secrets: {', '.join(missing)}.")
            st.stop()

        pending_email = st.session_state.get("otp_email")

        if not pending_email:
            st.caption("Sign in with your work email to continue.")
            with st.form("otp_request"):
                email = st.text_input("Work email", placeholder="you@lumiere.education")
                submitted = st.form_submit_button("Send code", type="primary")

            if submitted:
                email = normalise_email(email)
                if not is_allowed_login_email(email):
                    st.error("Use your work email address. Only Lumiere team domains can sign in.")
                else:
                    try:
                        with st.spinner("Sending code..."):
                            ok, message = request_code(supabase, email)
                        # A refresh or double-click can land here while an earlier
                        # code is still valid: go to the code box rather than strand them
                        if not ok and has_active_code(supabase, email):
                            ok, message = True, f"We already sent a code to {email}. Enter it below."
                    except Exception as e:
                        print(f"Error requesting sign-in code for {email}: {e}")
                        ok, message = False, "Something went wrong. Please try again."
                    if ok:
                        st.session_state["otp_email"] = email
                        st.session_state["otp_notice"] = message
                        screen.empty()
                        st.rerun()
                    st.error(message)
        else:
            notice = st.session_state.pop("otp_notice", None)
            if notice:
                st.success(notice)
            st.caption(
                f"Enter the code sent to **{pending_email}**. "
                f"It expires in {int(CODE_TTL.total_seconds() // 60)} minutes."
            )
            with st.form("otp_verify", clear_on_submit=True):
                code = st.text_input("6-digit code", max_chars=6)
                verify = st.form_submit_button("Sign in", type="primary")

            if verify:
                try:
                    ok, message = verify_code(supabase, pending_email, code)
                except Exception as e:
                    print(f"Error verifying sign-in code for {pending_email}: {e}")
                    ok, message = False, "Something went wrong. Please try again."
                if ok:
                    st.session_state["auth_email"] = pending_email
                    st.session_state.pop("otp_email", None)
                    screen.empty()
                    st.rerun()
                st.error(message)

            col_resend, col_change = st.columns(2)
            with col_resend:
                if st.button("Resend code", use_container_width=True):
                    try:
                        with st.spinner("Sending code..."):
                            ok, message = request_code(supabase, pending_email)
                    except Exception as e:
                        print(f"Error requesting sign-in code for {pending_email}: {e}")
                        ok, message = False, "Something went wrong. Please try again."
                    (st.success if ok else st.error)(message)
            with col_change:
                if st.button("Use a different email", use_container_width=True):
                    st.session_state.pop("otp_email", None)
                    screen.empty()
                    st.rerun()

        st.stop()


def render_account_sidebar() -> None:
    """'Signed in as … / Log out' control. Call inside `with st.sidebar:`."""
    st.caption(f"Signed in as **{st.session_state.get('auth_email', '')}**")
    if st.button("Log out", use_container_width=True):
        for key in ("auth_email", "otp_email", "otp_notice"):
            st.session_state.pop(key, None)
        st.rerun()
    st.divider()
