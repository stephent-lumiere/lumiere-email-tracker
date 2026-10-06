"""
Weekly email figures -> Airtable (baseline competency system).

Writes one row per person per week into the Metric Tracking table in the
Research Programs Evaluations base. Runs at the end of the daily GitHub
Action, after the Gmail sync, and re-writes the last few completed weeks
each time so late-arriving data settles on its own. Safe to re-run: rows
are matched on "Record Key" (<staff record id>|<week starting>), never
duplicated.

Figures match the dashboard's "Working Hours Adjusted" mode, which is what
the standards were set against:
  - each response's time skips weekends (if the person's setting says so)
    and their out-of-office days, in their own timezone
  - responses over 5 working days (120h) are dropped unless whitelisted
  - responses excluded in the dashboard are left out
  - median / average come from every individual response that week,
    never from averaging daily medians
  - emails sent is the sum of daily_stats for the week
Only Active people on the Staff Table whose Last Updated Vertical includes
Lumiere Education or Horizon Academics get a row (VERTICALS below).
A person can have two inboxes: their Staff Email, and a second one (e.g. a
white-label or Horizon inbox) in the Staff Table's "Second Inbox Email"
field. Each inbox gets its own figures on the same row (the "Second Email"
fields for the second inbox), because each inbox is judged separately.

Once the weekly competency check has judged a person's week (Email
Responsiveness - Result is filled in), that row is left alone, so the
figures on it always match what was judged.

Pass/fail is NOT decided here. The weekly competency check reads these
figures and judges them against the Core Competencies table.

Environment:
  SUPABASE_URL, SUPABASE_KEY   already used by the tracker
  AIRTABLE_TOKEN               personal access token, read/write on the base
  AIRTABLE_WEEKS               completed weeks to (re)write, default 2
                               (set 12 for a one-off backfill)

Usage:
  python airtable_weekly_sync.py            # last AIRTABLE_WEEKS weeks
  python airtable_weekly_sync.py --weeks 12
  python airtable_weekly_sync.py --dry-run  # print, write nothing
"""

import argparse
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, time as dt_time, timedelta, timezone
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from supabase import create_client

BASE_ID = "apptbsqs9gIs3Aiva"            # Research Programs Evaluations
STAFF_TABLE = "tbljITYVsEwqZSuzI"        # Staff Table (synced)
METRIC_TABLE = "tblNaK1sQM5LQxu3k"       # Metric Tracking

F_STAFF_EMAIL = "Staff Email"
F_STAFF_SECOND = "Second Inbox Email"
F_STAFF_NAME = "Name"
F_STAFF_STATUS = "Status"
F_STAFF_VERTICAL = "Last Updated Vertical"
VERTICALS = ("lumiere education", "horizon academics")  # who gets a row

# Metric Tracking fields written by this script
F_KEY = "Record Key"
F_NAME = "Name"
F_MEMBER = "Team Member"
F_WEEK = "Week Starting"
F_MEDIAN = "Email Response Time - Median"
F_AVG = "Email Response Time - Average"
F_SENT = "Emails Sent"
F_TRACKED = "Email Responses Tracked"
# Second inbox
F_RESULT = "Email Responsiveness - Result"
F2_MEDIAN = "Second Email Response Time - Median"
F2_AVG = "Second Email Response Time - Average"
F2_SENT = "Emails Sent - Second Email"
F2_TRACKED = "Email Responses Tracked - Second Email"


# ---------------------------------------------------------------- Airtable

def _airtable(method, path, token, params=None, body=None):
    url = f"https://api.airtable.com/v0/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params, doseq=True)
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 or e.code >= 500:
                time.sleep(2 * (attempt + 1))
                continue
            raise RuntimeError(f"Airtable {e.code}: {e.read().decode()[:500]}")
    raise RuntimeError("Airtable: too many retries")


def load_staff(token):
    """Map each inbox address (lower-case) -> (record id, name, slot 1 or 2).

    Only Active people; records named "Test ..." are ignored. If an address is
    someone's Second Inbox Email, that wins over another record that has the
    same address as its Staff Email (e.g. a separate white-label record).
    """
    second, first, conflicts = {}, {}, []
    offset = None
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        page = _airtable("GET", f"{BASE_ID}/{STAFF_TABLE}", token, params=params)
        for rec in page.get("records", []):
            f = rec.get("fields", {})
            if str(f.get(F_STAFF_STATUS) or "").strip().lower() != "active":
                continue
            vertical = str(f.get(F_STAFF_VERTICAL) or "").lower()
            if not any(v in vertical for v in VERTICALS):
                continue
            name = str(f.get(F_STAFF_NAME) or rec["id"]).strip()
            if name.lower().startswith("test"):
                continue
            for slot, field, target in ((1, F_STAFF_EMAIL, first), (2, F_STAFF_SECOND, second)):
                e = str(f.get(field) or "").strip().lower()
                if not e:
                    continue
                if e in target and target[e][0] != rec["id"]:
                    conflicts.append(f"{e}: {target[e][1]} / {name}")
                    continue
                target[e] = (rec["id"], name, slot)
        offset = page.get("offset")
        if not offset:
            break
        time.sleep(0.25)
    by_email = dict(first)
    by_email.update(second)
    for c in conflicts:
        print("Same inbox on two Active Staff records (first one used):", c)
    return by_email


def locked_keys(token, mondays):
    """Record Keys of rows for these weeks that already have an email result."""
    weeks = ",".join(
        f"DATETIME_FORMAT({{{F_WEEK}}},'YYYY-MM-DD')='{m.isoformat()}'" for m in mondays)
    formula = f"AND({{{F_RESULT}}}!='',OR({weeks}))"
    keys, offset = set(), None
    while True:
        params = {"pageSize": 100, "fields[]": F_KEY, "filterByFormula": formula}
        if offset:
            params["offset"] = offset
        page = _airtable("GET", f"{BASE_ID}/{METRIC_TABLE}", token, params=params)
        for rec in page.get("records", []):
            k = rec.get("fields", {}).get(F_KEY)
            if k:
                keys.add(k)
        offset = page.get("offset")
        if not offset:
            return keys
        time.sleep(0.25)


def upsert(token, records, dry_run):
    for i in range(0, len(records), 10):
        chunk = records[i:i + 10]
        if dry_run:
            for r in chunk:
                print("  would write:", json.dumps(r["fields"]))
            continue
        _airtable("PATCH", f"{BASE_ID}/{METRIC_TABLE}", token, body={
            "performUpsert": {"fieldsToMergeOn": [F_KEY]},
            "typecast": True,
            "records": chunk,
        })
        time.sleep(0.25)


# ---------------------------------------------------------------- Supabase

def _norm_ts(ts):
    try:
        dt = datetime.fromisoformat(str(ts))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return str(ts)


def _paged(query_fn):
    """Read every row. Supabase silently caps a response at 1,000 rows."""
    rows, offset = [], 0
    while True:
        batch = query_fn().range(offset, offset + 999).execute()
        if not batch.data:
            break
        rows.extend(batch.data)
        offset += len(batch.data)
    return rows


def adjusted_hours(received_at, replied_at, tz_name, exclude_weekends, ooo_dates):
    """Same rule as the dashboard's Working Hours Adjusted mode."""
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("America/New_York")
    recv, repl = received_at.astimezone(tz), replied_at.astimezone(tz)
    total, day = 0.0, recv.date()
    while day <= repl.date():
        if (exclude_weekends and day.weekday() >= 5) or day in ooo_dates:
            day += timedelta(days=1)
            continue
        start = datetime.combine(day, dt_time(0, 0), tzinfo=tz)
        end = start + timedelta(days=1)
        if day == recv.date():
            start = max(start, recv)
        if day == repl.date():
            end = min(end, repl)
        if end > start:
            total += (end - start).total_seconds()
        day += timedelta(days=1)
    return total / 3600


def _ts(value):
    dt = datetime.fromisoformat(str(value))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def load_settings(sb):
    """Per-mailbox timezone, weekend setting and out-of-office dates."""
    settings = {}
    for u in sb.table("tracked_users").select("email, timezone, exclude_weekends").execute().data or []:
        settings[u["email"].lower()] = {
            "tz": u.get("timezone") or "America/New_York",
            # same as the dashboard: missing -> True, empty -> False
            "weekends": bool(u.get("exclude_weekends", True)),
            "ooo": set(),
        }
    try:
        rows = _paged(lambda: sb.table("user_out_of_office")
                      .select("user_email, start_date, end_date").order("user_email"))
    except Exception:
        rows = []
    for r in rows:
        st = settings.setdefault(r["user_email"].lower(),
                                 {"tz": "America/New_York", "weekends": True, "ooo": set()})
        d = datetime.fromisoformat(r["start_date"]).date()
        end = datetime.fromisoformat(r["end_date"]).date()
        while d <= end:
            st["ooo"].add(d)
            d += timedelta(days=1)
    return settings


def week_figures(sb, start, end, settings):
    """Per-mailbox figures for start..end (inclusive dates, UTC)."""
    s, e = start.isoformat(), end.isoformat() + "T23:59:59"

    stats = _paged(lambda: sb.table("daily_stats").select("user_email, date, emails_sent")
                   .gte("date", start.isoformat()).lte("date", end.isoformat())
                   .order("date").order("user_email"))
    pairs = _paged(lambda: sb.table("response_pairs")
                   .select("user_email, thread_id, received_at, replied_at, response_hours")
                   .gte("replied_at", s).lte("replied_at", e).order("id"))

    def keys(table):
        try:
            rows = sb.table(table).select("thread_id, replied_at") \
                .gte("replied_at", s).lte("replied_at", e).execute().data or []
        except Exception:
            rows = []
        return {(x["thread_id"], _norm_ts(x["replied_at"])) for x in rows}

    excluded, whitelisted = keys("excluded_response_pairs"), keys("whitelisted_response_pairs")
    default = {"tz": "America/New_York", "weekends": True, "ooo": set()}

    out = {}
    for row in stats:
        m = out.setdefault(row["user_email"].lower(), {"sent": 0, "hours": []})
        m["sent"] += row.get("emails_sent") or 0
    for p in pairs:
        key = (p["thread_id"], _norm_ts(p["replied_at"]))
        if key in excluded:
            continue
        email = p["user_email"].lower()
        st = settings.get(email, default)
        try:
            h = adjusted_hours(_ts(p["received_at"]), _ts(p["replied_at"]),
                               st["tz"], st["weekends"], st["ooo"])
        except Exception:
            try:
                h = float(p["response_hours"])
            except (TypeError, ValueError):
                continue
        if h > 120 and key not in whitelisted:
            continue
        out.setdefault(email, {"sent": 0, "hours": []})["hours"].append(h)
    return out


# ---------------------------------------------------------------- main

def completed_weeks(n, today=None):
    today = today or datetime.now(timezone.utc).date()
    this_monday = today - timedelta(days=today.weekday())
    return [this_monday - timedelta(weeks=k) for k in range(n, 0, -1)]


def main():
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--weeks", type=int, default=int(os.getenv("AIRTABLE_WEEKS") or 2))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    token = os.getenv("AIRTABLE_TOKEN")
    if not token:
        print("AIRTABLE_TOKEN not set; skipping Airtable sync.")
        return 0

    sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    active = {u["email"].lower() for u in
              (sb.table("tracked_users").select("email, is_active").execute().data or [])
              if u.get("is_active") is not False}
    staff = load_staff(token)
    settings = load_settings(sb)

    unmatched = set()
    mondays = completed_weeks(args.weeks)
    try:
        locked = locked_keys(token, mondays)
    except RuntimeError as e:
        print(f"Could not check for judged rows ({e}); writing all rows.")
        locked = set()
    skipped = 0
    for monday in mondays:
        figs = week_figures(sb, monday, monday + timedelta(days=6), settings)
        people = {}
        for email, m in figs.items():
            if email not in active:
                continue
            if email not in staff:
                unmatched.add(email)
                continue
            rec_id, name, slot = staff[email]
            people.setdefault(rec_id, {"name": name, "boxes": {}})["boxes"][slot] = (email, m)

        records = []
        for rec_id, p in people.items():
            key = f"{rec_id}|{monday.isoformat()}"
            if key in locked:
                skipped += 1
                continue
            fields = {
                F_KEY: key,
                F_NAME: f"{p['name']} – w/c {monday.isoformat()}",
                F_MEMBER: [rec_id],
                F_WEEK: monday.isoformat(),
            }
            for slot, names in ((1, (F_MEDIAN, F_AVG, F_SENT, F_TRACKED)),
                                (2, (F2_MEDIAN, F2_AVG, F2_SENT, F2_TRACKED))):
                if slot not in p["boxes"]:
                    continue
                email, m = p["boxes"][slot]
                f_med, f_avg, f_sent, f_tr = names
                h = m["hours"]
                fields[f_sent] = m["sent"]
                fields[f_tr] = len(h)
                fields[f_med] = round(statistics.median(h), 1) if h else None
                fields[f_avg] = round(statistics.fmean(h), 1) if h else None
            records.append({"fields": fields})
        print(f"Week of {monday}: {len(records)} people")
        upsert(token, records, args.dry_run)

    if skipped:
        print(f"{skipped} rows already judged (left unchanged)")
    if unmatched:
        print("Tracked mailboxes with no Staff Table match (add as Staff Email or Second Inbox Email if they belong to someone):")
        for e in sorted(unmatched):
            print("  ", e)
    return 0


if __name__ == "__main__":
    sys.exit(main())
