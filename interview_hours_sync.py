"""
Weekly interview figures -> Airtable (baseline competency system).

For every in-scope person (Staff Table "In Competency Scope" = 1) this writes
interview figures onto their Metric Tracking row for last week, this week and
next week, so you can see who is offering enough interview hours (and plan
ahead for next week). Rows are matched on "Record Key"
(<staff record id>|<week starting>), the same key the email and Slack syncs
use, so the figures land on the same weekly row.

Each person's "Interview Team" (Staff Table formula) decides which target
applies; targets, shortfall and met / not met are Airtable formulas:
  Lumiere      Lumiere student interviews + any other interviews (separately)
  White Label  JLI + White Label Research Scholar Program, combined
  Horizon      HARP only (on the Horizon Calendly account)
  (blank)      interns: figures written, no target

Where the numbers come from (Calendly):
  - Interview pools = active pooled event types (round robin / multi-pool)
    whose name looks like an interview, on the program.manager account
    (CALENDLY_TOKEN) and, if set, the Horizon account (HORIZON_CALENDLY_TOKEN).
    New pools are picked up automatically.
  - Pool categories: Lumiere student = "Lumiere Research Scholar Program -
    Interview" (main + Americas); White Label = JLI + "Research Program
    Interview Slot"; Horizon = every interview pool on the Horizon account;
    Other = every other interview pool.
  - Hours offered = each host's own availability on their interview pools
    for each day (weekly hours, date overrides taking priority, in the host's
    timezone), counted once: an hour open on two pools counts once, claimed
    first by the person's own team's pools, then Lumiere, White Label,
    Horizon, Other. Time their connected calendar shows as busy (Calendly
    "busy times", calendar events only) is then removed, so the figure is
    time genuinely free for interviews.
  - Each day is fixed once it has started for the person (stored per day in
    "Interview Hours - Daily (sync)"); only days still ahead are recalculated,
    because Calendly only reports current availability and future busy time.
  - Booked = bookings on those pools that week (not cancelled). Taken =
    booked, already finished, invitee not marked a no-show. No-show or
    cancelled = cancelled bookings + invitee no-shows.
  - Google Calendar (calendar_interviews.py, read-only via the tracker's
    service account): when a person's calendar can be read, their own
    calendar's busy time replaces Calendly's (it also covers days already
    past), and "Interviews Held (Calendar)" counts every finished interview
    event on their calendar, for any vertical, with hours and a breakdown.
    --recompute recalculates stored days; --weeks-back N rewrites N past weeks.
  - Cross-team = hours offered on another vertical's pools (e.g. a Lumiere PM
    on the Ladder pool). Information only; it doesn't affect any target.

Environment:
  CALENDLY_TOKEN          personal access token, program.manager account
  HORIZON_CALENDLY_TOKEN  personal access token, Horizon account (optional)
  AIRTABLE_TOKEN          personal access token, read/write on the base

Usage:
  python interview_hours_sync.py            # last week, this week, next week
  python interview_hours_sync.py --dry-run  # print, write nothing
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from datetime import time as dtime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

import calendar_interviews as gcal

BASE_ID = "apptbsqs9gIs3Aiva"            # Research Programs Evaluations
STAFF_TABLE = "tbljITYVsEwqZSuzI"        # Staff Table (synced)
METRIC_TABLE = "tblNaK1sQM5LQxu3k"       # Metric Tracking

# Staff Table
S_NAME, S_EMAIL, S_SECOND = "Name", "Staff Email", "Second Inbox Email"
S_SCOPE, S_TEAM = "In Competency Scope", "Interview Team"

# Metric Tracking fields written here
F_KEY, F_NAME, F_MEMBER, F_WEEK = "Record Key", "Name", "Team Member", "Week Starting"
F_LUM = "Interview Hours Offered - Lumiere"
F_OTH = "Interview Hours Offered - Other"
F_WL = "Interview Hours Offered - White Label"
F_HOR = "Interview Hours Offered - Horizon"
F_CROSS = "Interview Hours - Cross-Team"
F_BOOKED = "Interviews Booked"
F_TAKEN = "Interviews Taken"
F_NOSHOW = "Interviews No-show or Cancelled"
F_BREAKDOWN = "Interview Hours - Breakdown"
F_CAL_HELD = "Interviews Held (Calendar)"
F_CAL_HOURS = "Interview Hours Held (Calendar)"
F_CAL_BREAKDOWN = "Interviews Held - Breakdown (Calendar)"

INTERVIEW_POOL = re.compile(r"interview|sign-up|mentorship position|\bharp\b", re.I)
SKIP_POOL = re.compile(r"uceazy|20(1\d|2[0-3])", re.I)   # old one-off pools (e.g. UCEazy 2022)
LUMIERE_POOL = re.compile(r"^lumiere (research scholar program|rsp) - interview", re.I)
WHITE_LABEL_POOL = re.compile(r"^jli\b|^research program interview slot", re.I)

# Which vertical a pool serves (first match wins). Used for cross-team only.
POOL_VERTICAL = [
    (re.compile(r"ladder|online internship", re.I), "ladder"),
    (re.compile(r"\bwsg\b|wall street", re.I), "wsg"),
    (re.compile(r"young founders|\byfl\b", re.I), "yfl"),
    (re.compile(r"veritas", re.I), "veritas"),
]
DEFAULT_VERTICAL = "lumiere"   # Lumiere RSP, mentor, Foundation, professor pools
TEAM_VERTICAL = {"Lumiere": "lumiere", "White Label": "white label", "Horizon": "horizon"}

DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


# ---------------------------------------------------------------- HTTP

def _request(url, token, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        # Calendly sits behind Cloudflare, which blocks Python's default
        # user agent ("Python-urllib") with "error code: 1010".
        "User-Agent": "lumiere-interview-hours-sync/1.0",
    })
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 or e.code >= 500:
                time.sleep(2 * (attempt + 1))
                continue
            raise RuntimeError(f"{url.split('?')[0]} -> {e.code}: {e.read().decode()[:500]}")
    raise RuntimeError(f"{url}: too many retries")


def calendly(path_or_url, token, params=None):
    url = path_or_url if path_or_url.startswith("http") else f"https://api.calendly.com{path_or_url}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    return _request(url, token)


def calendly_all(path, token, params):
    """Every item of a paginated Calendly collection."""
    out, url = [], None
    page = calendly(path, token, params)
    while True:
        out.extend(page.get("collection", []))
        url = (page.get("pagination") or {}).get("next_page")
        if not url:
            return out
        time.sleep(0.2)
        page = calendly(url, token)


def airtable(method, path, token, params=None, body=None):
    url = f"https://api.airtable.com/v0/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params, doseq=True)
    return _request(url, token, method, body)


# ---------------------------------------------------------------- people

def _norm_name(s):
    return " ".join(re.sub(r"[^a-z ]", " ", (s or "").lower()).split())


def load_staff(token):
    """In-scope people: record id -> {name, team}; plus email and name lookups."""
    people, by_email, by_name, offset = {}, {}, {}, None
    while True:
        params = {"pageSize": 100, "filterByFormula": f"{{{S_SCOPE}}}=1"}
        if offset:
            params["offset"] = offset
        page = airtable("GET", f"{BASE_ID}/{STAFF_TABLE}", token, params=params)
        for rec in page.get("records", []):
            f = rec.get("fields", {})
            name = str(f.get(S_NAME) or "").strip()
            team = f.get(S_TEAM)
            team = (team[0] if isinstance(team, list) and team else team) or ""
            people[rec["id"]] = {"name": name, "team": team, "emails": []}
            for field in (S_EMAIL, S_SECOND):
                e = str(f.get(field) or "").strip().lower()
                if e:
                    by_email[e] = rec["id"]
                    people[rec["id"]]["emails"].append(e)
            by_name[_norm_name(name)] = rec["id"]
        offset = page.get("offset")
        if not offset:
            break
        time.sleep(0.25)
    return people, by_email, by_name


def match_person(cal_user, by_email, by_name):
    """Calendly user {email, name} -> Staff record id (email first, then full name)."""
    rid = by_email.get((cal_user.get("email") or "").lower())
    if rid:
        return rid
    n = _norm_name(cal_user.get("name"))
    if n in by_name:
        return by_name[n]
    # Staff names sometimes carry a middle name ("Lindiwe Kubheka Botha")
    parts = n.split()
    if len(parts) >= 2:
        for sn, rid in by_name.items():
            sp = sn.split()
            if sp and sp[0] == parts[0] and sp[-1] == parts[-1]:
                return rid
    return None


# ---------------------------------------------------------------- hours

def _minutes(hhmm):
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)


def interval_minutes(iv):
    start, end = _minutes(iv["from"]), _minutes(iv["to"])
    if end <= start:          # runs past midnight (e.g. 22:00 -> 01:00)
        end += 24 * 60
    return end - start


def pool_category(name, horizon_account):
    """(category, vertical) for an interview pool."""
    n = " ".join(name.split())
    if horizon_account:
        return "horizon", "horizon"
    if LUMIERE_POOL.search(n):
        return "lumiere", "lumiere"
    if WHITE_LABEL_POOL.search(n):
        return "white label", "white label"
    for pattern, vertical in POOL_VERTICAL:
        if pattern.search(n):
            return "other", vertical
    return "other", DEFAULT_VERTICAL


def short_name(name):
    n = " ".join(name.split())
    if LUMIERE_POOL.search(n):
        return "Lumiere RSP Americas" if "americas" in n.lower() else "Lumiere RSP"
    if re.match(r"(?i)research program interview slot", n):
        return "White Label RSP"
    if re.match(r"(?i)jli\b", n):
        return "JLI"
    n = re.sub(r"\s*\|.*$", "", n)                       # "Ladder Internships Interview | Upcoming Cohort"
    n = re.sub(r"(?i)\b(interviews?|sign-up|call|round|slot)\b", "", n)
    n = re.sub(r"[:\-–]+\s*$", "", " ".join(n.split())).strip(" -:")
    return n or name.strip()


# ---------------------------------------------------------------- hours (de-duplicated, minus busy time)

UTC = timezone.utc
F_DAILY = "Interview Hours - Daily (sync)"
CATEGORIES = ["lumiere", "white label", "horizon", "other"]


def _local_dt(d, minutes, tz):
    """Local date d + minutes past midnight (24:00 allowed) as a UTC datetime."""
    return (datetime.combine(d, dtime(0, 0), tzinfo=tz) + timedelta(minutes=minutes)).astimezone(UTC)


def rule_intervals(rule, d):
    """UTC (start, end) intervals a host's availability rule opens on local date d."""
    tz = ZoneInfo(rule.get("timezone") or "UTC")
    weekly, dated = {}, {}
    for r in rule.get("rules") or []:
        if r.get("type") == "wday" and r.get("wday"):
            weekly[r["wday"].lower()] = r.get("intervals") or []
        elif r.get("type") == "date" and r.get("date"):
            dated[r["date"]] = r.get("intervals") or []
    out = []
    for iv in dated.get(d.isoformat(), weekly.get(DAYS[d.weekday()], [])):
        start, end = _minutes(iv["from"]), _minutes(iv["to"])
        if end <= start:                      # runs past midnight
            end += 24 * 60
        out.append((_local_dt(d, start, tz), _local_dt(d, end, tz)))
    return out


def union(ivs):
    """Merge overlapping intervals."""
    out = []
    for s, e in sorted(ivs):
        if out and s <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], e))
        else:
            out.append((s, e))
    return out


def subtract(ivs, cut):
    """ivs minus cut (both lists of intervals)."""
    cut = union(cut)
    out = []
    for s, e in union(ivs):
        pieces = [(s, e)]
        for cs, ce in cut:
            nxt = []
            for ps, pe in pieces:
                if ce <= ps or cs >= pe:
                    nxt.append((ps, pe))
                    continue
                if cs > ps:
                    nxt.append((ps, cs))
                if ce < pe:
                    nxt.append((ce, pe))
            pieces = nxt
        out.extend(pieces)
    return out


def hours(ivs):
    return sum((e - s).total_seconds() for s, e in union(ivs)) / 3600


def busy_times(token, user_uri, lo, hi):
    """Times the host's connected calendar shows as busy between lo and hi.

    Only calendar ("external") events count. Calendly bookings are left in:
    a booked interview hour was offered. Calendar copies of Calendly bookings
    (same start and end) are dropped for the same reason.
    Returns None if Calendly will not give busy times for this host.
    """
    out, cal, start = [], set(), lo
    while start < hi:
        end = min(start + timedelta(days=7), hi)
        try:
            items = calendly_all("/user_busy_times", token, {
                "user": user_uri,
                "start_time": start.isoformat().replace("+00:00", "Z"),
                "end_time": end.isoformat().replace("+00:00", "Z")})
        except RuntimeError as e:
            print(f"  busy times unavailable for {user_uri.rsplit('/', 1)[-1]}: {str(e)[:200]}")
            return None
        for it in items:
            s = datetime.fromisoformat(it["start_time"].replace("Z", "+00:00"))
            e = datetime.fromisoformat(it["end_time"].replace("Z", "+00:00"))
            if it.get("type") == "calendly":
                cal.add((s, e))
            else:
                out.append((s, e))
        start = end
        time.sleep(0.2)
    return [iv for iv in out if iv not in cal]


def day_figures(entries, own, busy, after):
    """One person's hours for one day.

    entries: [(pool, intervals)] for the day. own: the person's team category
    (or None). busy: busy intervals (or None if unknown). after: busy time is
    only known from this moment on.
    Each hour counts once: the person's own team's pools claim it first, then
    Lumiere, White Label, Horizon, Other. Busy calendar time is then removed.
    """
    cut = [(max(s, after), e) for s, e in (busy or []) if e > after]
    order = ([own] if own in CATEGORIES else []) + [c for c in CATEGORIES if c != own]
    out, covered = {"pools": {}}, []
    for cat in order:
        ivs = union([iv for p, ivs in entries if p["cat"] == cat for iv in ivs])
        mine = subtract(ivs, covered)
        out[cat] = round(hours(subtract(mine, cut)), 2)
        covered = union(covered + ivs)
    if own:
        other_vert = [iv for p, ivs in entries if p["vertical"] != own for iv in ivs]
        out["cross"] = round(hours(subtract(other_vert, cut)), 2)
    else:
        out["cross"] = 0.0
    for p, ivs in entries:
        h = hours(subtract(ivs, cut))
        if h:
            out["pools"][p["short"]] = round(out["pools"].get(p["short"], 0) + h, 2)
    out["busy_checked"] = busy is not None
    return out


def load_daily(token, mondays):
    """Record Key -> stored per-day figures, for the given weeks."""
    formula = "OR(" + ",".join(
        f"IS_SAME({{{F_WEEK}}},'{m.isoformat()}','day')" for m in mondays) + ")"
    out, offset = {}, None
    while True:
        params = {"pageSize": 100, "filterByFormula": formula, "fields[]": [F_KEY, F_DAILY]}
        if offset:
            params["offset"] = offset
        page = airtable("GET", f"{BASE_ID}/{METRIC_TABLE}", token, params=params)
        for rec in page.get("records", []):
            f = rec.get("fields", {})
            if f.get(F_KEY) and f.get(F_DAILY):
                try:
                    out[f[F_KEY]] = json.loads(f[F_DAILY])
                except ValueError:
                    pass
        offset = page.get("offset")
        if not offset:
            return out
        time.sleep(0.25)


# ---------------------------------------------------------------- Calendly account

def read_account(token, horizon_account, by_email, by_name):
    """Interview pools and each host's availability on them, for one Calendly account."""
    me = calendly("/users/me", token)["resource"]
    org = me["current_organization"]
    users = {}
    for m in calendly_all("/organization_memberships", token, {"organization": org, "count": 100}):
        u = m.get("user") or {}
        users[u.get("uri")] = {"email": u.get("email"), "name": u.get("name")}

    pools = {}
    for et in calendly_all("/event_types", token, {"user": me["uri"], "active": "true", "count": 100}):
        name = et.get("name") or ""
        if et.get("pooling_type") and INTERVIEW_POOL.search(name) and not SKIP_POOL.search(name):
            cat, vert = pool_category(name, horizon_account)
            pools[et["uri"]] = {"name": name, "short": short_name(name), "cat": cat, "vertical": vert}

    host_rules, unmatched = [], set()
    for uri in pools:
        for sch in calendly_all("/event_type_availability_schedules", token, {"event_type": uri}):
            rule = sch.get("availability_rule") or {}
            u = users.get(rule.get("user"), {})
            rid = match_person(u, by_email, by_name)
            if rid:
                host_rules.append((rid, uri, rule))
            elif u.get("email") and u.get("email") != me.get("email"):
                unmatched.add(u.get("name") or u.get("email"))
        time.sleep(0.2)
    return {"token": token, "org": org, "pools": pools, "host_rules": host_rules,
            "unmatched": unmatched, "horizon": horizon_account}


def count_bookings(acct, lo, hi, fig, by_email, by_name):
    now = datetime.now(timezone.utc)
    for status in ("active", "canceled"):
        events = calendly_all("/scheduled_events", acct["token"], {
            "organization": acct["org"], "status": status, "count": 100,
            "min_start_time": lo.isoformat().replace("+00:00", "Z"),
            "max_start_time": hi.isoformat().replace("+00:00", "Z")})
        for ev in events:
            if ev.get("event_type") not in acct["pools"]:
                continue
            for mem in ev.get("event_memberships") or []:
                rid = match_person({"email": mem.get("user_email"), "name": mem.get("user_name")},
                                   by_email, by_name)
                if not rid or rid not in fig:
                    continue
                f = fig[rid]
                if status == "canceled":
                    f["noshow"] += 1
                    continue
                f["booked"] += 1
                end = datetime.fromisoformat(ev["end_time"].replace("Z", "+00:00"))
                if end > now:
                    continue
                invitees = calendly_all(f"{ev['uri']}/invitees", acct["token"], {"count": 100})
                if any(i.get("no_show") for i in invitees):
                    f["noshow"] += 1
                else:
                    f["taken"] += 1
                time.sleep(0.1)


# ---------------------------------------------------------------- main

def main():
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--weeks-back", type=int, default=1,
                    help="completed weeks to (re)write before this week (default 1)")
    ap.add_argument("--recompute", action="store_true",
                    help="recalculate days already stored (uses Google Calendar busy time for past days)")
    args = ap.parse_args()

    at_token = os.getenv("AIRTABLE_TOKEN")
    tokens = [(os.getenv("CALENDLY_TOKEN"), False), (os.getenv("HORIZON_CALENDLY_TOKEN"), True)]
    tokens = [(t, h) for t, h in tokens if t]
    if not at_token or not tokens:
        print("AIRTABLE_TOKEN or CALENDLY_TOKEN not set; skipping interview sync.")
        return 0

    people, by_email, by_name = load_staff(at_token)
    accounts = []
    for t, h in tokens:
        try:
            accounts.append(read_account(t, h, by_email, by_name))
        except RuntimeError as e:
            print(f"Skipping the {'Horizon' if h else 'Lumiere'} Calendly account: {e}")
    if not accounts:
        print("No Calendly account could be read; nothing written.")
        return 1
    have_horizon = any(a["horizon"] for a in accounts)
    for a in accounts:
        print(f"{'Horizon' if a['horizon'] else 'Lumiere'} Calendly: {len(a['pools'])} interview pools: "
              + "; ".join(sorted(f"{p['short']} ({p['cat']})" for p in a["pools"].values())))

    now = datetime.now(UTC)
    today = now.date()
    this_monday = today - timedelta(days=today.weekday())
    back = max(1, args.weeks_back)
    mondays = [this_monday - timedelta(weeks=k) for k in range(back, 0, -1)] + \
              [this_monday, this_monday + timedelta(weeks=1)]

    # Days already stored on the Metric Tracking rows. A day is fixed once it
    # has started for the person; only days still ahead are recalculated.
    stored = {} if args.recompute else load_daily(at_token, mondays)

    # Busy calendar time for every host, from now to the end of next week.
    busy_hi = datetime.combine(mondays[-1] + timedelta(days=8), dtime(0, 0), tzinfo=UTC)
    busy = {}
    for acct in accounts:
        for user_uri in {rule.get("user") for _, _, rule in acct["host_rules"]}:
            if user_uri and (id(acct), user_uri) not in busy:
                busy[(id(acct), user_uri)] = busy_times(acct["token"], user_uri, now, busy_hi)
    no_busy = sum(1 for v in busy.values() if v is None)
    if no_busy:
        print(f"Busy times unavailable for {no_busy} of {len(busy)} Calendly hosts; "
              "their hours are counted without removing calendar events.")

    # Google Calendar (read-only, via the tracker's service account): interviews
    # held, and busy time that also covers days already past.
    cal_lo = datetime.combine(mondays[0] - timedelta(days=1), dtime(0, 0), tzinfo=UTC)
    gevents = {}
    calendar = gcal.Calendar.from_env()
    if calendar:
        for rid in people:
            gevents[rid] = calendar.events(people[rid]["emails"], cal_lo, busy_hi)
        read = sum(1 for v in gevents.values() if v is not None)
        print(f"Google Calendar read for {read} of {len(people)} people.")

    # Each person's timezone (from their first availability rule) decides when a day has started.
    person_tz = {}
    for acct in accounts:
        for rid, _, rule in acct["host_rules"]:
            person_tz.setdefault(rid, ZoneInfo(rule.get("timezone") or "UTC"))

    records = []
    for monday in mondays:
        days = [monday + timedelta(days=i) for i in range(7)]
        lo = datetime.combine(monday, datetime.min.time(), tzinfo=UTC)
        hi = lo + timedelta(days=7)
        fig = {rid: {"booked": 0, "taken": 0, "noshow": 0} for rid in people}
        for acct in accounts:
            count_bookings(acct, lo, hi, fig, by_email, by_name)

        for rid in people:
            key = f"{rid}|{monday.isoformat()}"
            daily = dict(stored.get(key, {}))
            tz = person_tz.get(rid, UTC)
            own = TEAM_VERTICAL.get(people[rid]["team"])
            g_busy = gcal.busy(gevents.get(rid))
            for d in days:
                started = now >= datetime.combine(d, dtime(0, 0), tzinfo=tz)
                if started and d.isoformat() in daily:
                    continue                                   # fixed
                entries, person_busy, checked = [], [], True
                for acct in accounts:
                    for host, uri, rule in acct["host_rules"]:
                        if host != rid:
                            continue
                        ivs = rule_intervals(rule, d)
                        if ivs:
                            entries.append((acct["pools"][uri], ivs))
                        b = busy.get((id(acct), rule.get("user")))
                        if b is None:
                            checked = False
                        else:
                            person_busy.extend(b)
                if g_busy is not None:
                    # Google Calendar covers past days too, so nothing is left uncut.
                    daily[d.isoformat()] = day_figures(entries, own, g_busy, cal_lo)
                else:
                    daily[d.isoformat()] = day_figures(
                        entries, own, person_busy if checked else None, now)

            tot = {c: sum(v.get(c, 0) for v in daily.values()) for c in CATEGORIES + ["cross"]}
            pools = {}
            for v in daily.values():
                for n, h in v.get("pools", {}).items():
                    pools[n] = pools.get(n, 0) + h
            parts = sorted(pools.items(), key=lambda kv: -kv[1])
            unchecked = sum(1 for v in daily.values() if not v.get("busy_checked"))
            note = f" (calendar not checked for {unchecked} of 7 days)" if unchecked else ""
            fields = {
                F_KEY: key,
                F_NAME: f"{people[rid]['name']} – w/c {monday.isoformat()}",
                F_MEMBER: [rid],
                F_WEEK: monday.isoformat(),
                F_LUM: round(tot["lumiere"], 1),
                F_OTH: round(tot["other"], 1),
                F_WL: round(tot["white label"], 1),
                F_CROSS: round(tot["cross"], 1),
                F_BOOKED: fig[rid]["booked"],
                F_TAKEN: fig[rid]["taken"],
                F_NOSHOW: fig[rid]["noshow"],
                F_BREAKDOWN: (" · ".join(f"{n} {round(h, 1):g}h" for n, h in parts) or "none") + note,
                F_DAILY: json.dumps(daily, separators=(",", ":"), sort_keys=True),
            }
            h = gcal.held(gevents.get(rid), lo, hi, now)
            if h is not None:
                fields[F_CAL_HELD] = h["count"]
                fields[F_CAL_HOURS] = h["hours"]
                fields[F_CAL_BREAKDOWN] = " · ".join(
                    f"{k} {v}" for k, v in sorted(h["by"].items(), key=lambda kv: -kv[1])) or "none"
            if have_horizon:
                fields[F_HOR] = round(tot["horizon"], 1)
            records.append({"fields": fields})

    for i in range(0, len(records), 10):
        chunk = records[i:i + 10]
        if args.dry_run:
            for r in chunk:
                fl = r["fields"]
                print(f"  {fl[F_NAME]}: Lumiere {fl[F_LUM]}h, Other {fl[F_OTH]}h, WL {fl[F_WL]}h, "
                      f"Horizon {fl.get(F_HOR, '-')}h, cross-team {fl[F_CROSS]}h, booked {fl[F_BOOKED]}, "
                      f"taken {fl[F_TAKEN]}, no-show/cancelled {fl[F_NOSHOW]} | {fl[F_BREAKDOWN]}"
                      f" | calendar held {fl.get(F_CAL_HELD, '-')} ({fl.get(F_CAL_BREAKDOWN, 'not read')})")
            continue
        airtable("PATCH", f"{BASE_ID}/{METRIC_TABLE}", at_token, body={
            "performUpsert": {"fieldsToMergeOn": [F_KEY]},
            "typecast": True,
            "records": chunk,
        })
        time.sleep(0.25)

    print(f"{'Would write' if args.dry_run else 'Wrote'} {len(records)} rows "
          f"({len(people)} people x {len(mondays)} weeks).")
    unmatched = set().union(*(a["unmatched"] for a in accounts))
    if unmatched:
        print("Calendly hosts not in scope or not on the Staff Table (skipped):",
              ", ".join(sorted(unmatched)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
