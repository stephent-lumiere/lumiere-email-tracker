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
  - Hours offered = each host's own availability on that pool for each day
    of the week (weekly hours, date overrides taking priority), in the host's
    timezone - what they opened up, before Calendly removes slots that clash
    with their own calendar.
  - Booked = bookings on those pools that week (not cancelled). Taken =
    booked, already finished, invitee not marked a no-show. No-show or
    cancelled = cancelled bookings + invitee no-shows.
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

from dotenv import load_dotenv

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
            people[rec["id"]] = {"name": name, "team": team}
            for field in (S_EMAIL, S_SECOND):
                e = str(f.get(field) or "").strip().lower()
                if e:
                    by_email[e] = rec["id"]
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


def hours_offered(rule, days):
    """Hours a host's availability rule opens on the given dates.

    Dates are read in the host's own timezone, as Calendly does: a date
    override replaces that weekday's normal hours for that date.
    """
    weekly, dated = {}, {}
    for r in rule.get("rules") or []:
        if r.get("type") == "wday" and r.get("wday"):
            weekly[r["wday"].lower()] = r.get("intervals") or []
        elif r.get("type") == "date" and r.get("date"):
            dated[r["date"]] = r.get("intervals") or []
    total = 0
    for d in days:
        ivs = dated.get(d.isoformat(), weekly.get(DAYS[d.weekday()], []))
        total += sum(interval_minutes(iv) for iv in ivs)
    return total / 60


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
    args = ap.parse_args()

    at_token = os.getenv("AIRTABLE_TOKEN")
    tokens = [(os.getenv("CALENDLY_TOKEN"), False), (os.getenv("HORIZON_CALENDLY_TOKEN"), True)]
    tokens = [(t, h) for t, h in tokens if t]
    if not at_token or not tokens:
        print("AIRTABLE_TOKEN or CALENDLY_TOKEN not set; skipping interview sync.")
        return 0
    have_horizon = any(h for _, h in tokens)

    people, by_email, by_name = load_staff(at_token)
    accounts = [read_account(t, h, by_email, by_name) for t, h in tokens]
    for a in accounts:
        print(f"{'Horizon' if a['horizon'] else 'Lumiere'} Calendly: {len(a['pools'])} interview pools: "
              + "; ".join(sorted(f"{p['short']} ({p['cat']})" for p in a["pools"].values())))

    today = datetime.now(timezone.utc).date()
    this_monday = today - timedelta(days=today.weekday())
    mondays = [this_monday - timedelta(weeks=1), this_monday, this_monday + timedelta(weeks=1)]

    records = []
    for monday in mondays:
        days = [monday + timedelta(days=i) for i in range(7)]
        fig = {rid: {"lumiere": 0.0, "other": 0.0, "white label": 0.0, "horizon": 0.0,
                     "cross": 0.0, "by_pool": {}, "booked": 0, "taken": 0, "noshow": 0}
               for rid in people}
        lo = datetime.combine(monday, datetime.min.time(), tzinfo=timezone.utc)
        hi = lo + timedelta(days=7)

        for acct in accounts:
            for rid, uri, rule in acct["host_rules"]:
                h = hours_offered(rule, days)
                if not h:
                    continue
                p, f = acct["pools"][uri], fig[rid]
                f[p["cat"]] += h
                own = TEAM_VERTICAL.get(people[rid]["team"])
                if own and p["vertical"] != own:
                    f["cross"] += h
                f["by_pool"][p["short"]] = f["by_pool"].get(p["short"], 0) + h
            count_bookings(acct, lo, hi, fig, by_email, by_name)

        for rid, f in fig.items():
            parts = sorted(f["by_pool"].items(), key=lambda kv: -kv[1])
            fields = {
                F_KEY: f"{rid}|{monday.isoformat()}",
                F_NAME: f"{people[rid]['name']} – w/c {monday.isoformat()}",
                F_MEMBER: [rid],
                F_WEEK: monday.isoformat(),
                F_LUM: round(f["lumiere"], 1),
                F_OTH: round(f["other"], 1),
                F_WL: round(f["white label"], 1),
                F_CROSS: round(f["cross"], 1),
                F_BOOKED: f["booked"],
                F_TAKEN: f["taken"],
                F_NOSHOW: f["noshow"],
                F_BREAKDOWN: " · ".join(f"{n} {round(h, 1):g}h" for n, h in parts) or "none",
            }
            if have_horizon:
                fields[F_HOR] = round(f["horizon"], 1)
            records.append({"fields": fields})

    for i in range(0, len(records), 10):
        chunk = records[i:i + 10]
        if args.dry_run:
            for r in chunk:
                fl = r["fields"]
                print(f"  {fl[F_NAME]}: Lumiere {fl[F_LUM]}h, Other {fl[F_OTH]}h, WL {fl[F_WL]}h, "
                      f"Horizon {fl.get(F_HOR, '-')}h, cross-team {fl[F_CROSS]}h, booked {fl[F_BOOKED]}, "
                      f"taken {fl[F_TAKEN]}, no-show/cancelled {fl[F_NOSHOW]} | {fl[F_BREAKDOWN]}")
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
