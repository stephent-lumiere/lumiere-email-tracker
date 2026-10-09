"""
Weekly interview figures -> Airtable (baseline competency system).

For every in-scope person (Staff Table "In Competency Scope" = 1) this writes
interview figures onto their Metric Tracking row for last week, this week and
next week, so you can see who is offering enough interview hours (and plan
ahead for next week). Rows are matched on "Record Key"
(<staff record id>|<week starting>), the same key the email and Slack syncs
use, so the figures land on the same weekly row.

Where the numbers come from (Calendly, program.manager organisation):
  - Interview pools = active pooled event types (round robin / multi-pool) on
    the token's Calendly account whose name looks like an interview
    (INTERVIEW_POOL below). New pools are picked up automatically.
  - Lumiere pools = the "Lumiere Research Scholar Program - Interview" pools
    (main + Americas). Everything else counts as "Other".
  - Hours offered = each host's own availability on that pool for each day
    of the week (their weekly hours, with date overrides taking priority),
    in the host's timezone. This is what they opened up, before Calendly
    removes slots that clash with their own calendar.
  - Booked = Calendly bookings on those pools that week (not cancelled).
    Taken = booked and already finished, invitee not marked as a no-show.
    No-show or cancelled = cancelled bookings + invitee no-shows.
  - Cross-team = hours offered on pools for a vertical that is not one of the
    person's own verticals (e.g. a Lumiere PM on the Ladder pool).

Targets, shortfall and met / not met are formulas in Airtable, not here.

Environment:
  CALENDLY_TOKEN   personal access token from the program.manager account
  AIRTABLE_TOKEN   personal access token, read/write on the base

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
S_SCOPE, S_VERTICAL = "In Competency Scope", "Last Updated Vertical"

# Metric Tracking fields written here
F_KEY, F_NAME, F_MEMBER, F_WEEK = "Record Key", "Name", "Team Member", "Week Starting"
F_LUM = "Interview Hours Offered - Lumiere"
F_OTH = "Interview Hours Offered - Other"
F_CROSS = "Interview Hours - Cross-Team"
F_BOOKED = "Interviews Booked"
F_TAKEN = "Interviews Taken"
F_NOSHOW = "Interviews No-show or Cancelled"
F_BREAKDOWN = "Interview Hours - Breakdown"

INTERVIEW_POOL = re.compile(r"interview|sign-up|mentorship position", re.I)
SKIP_POOL = re.compile(r"uceazy|20(1\d|2[0-3])", re.I)   # old one-off pools (e.g. UCEazy 2022)
LUMIERE_POOL = re.compile(r"^lumiere (research scholar program|rsp) - interview", re.I)

# Which vertical a pool serves, by name (first match wins). Used for cross-team.
POOL_VERTICAL = [
    (re.compile(r"ladder|online internship", re.I), "ladder"),
    (re.compile(r"\bwsg\b|wall street", re.I), "wsg"),
    (re.compile(r"young founders|\byfl\b", re.I), "yfl"),
    (re.compile(r"veritas", re.I), "veritas"),
    (re.compile(r"horizon", re.I), "horizon"),
]
DEFAULT_VERTICAL = "lumiere"   # Lumiere RSP, mentor, Foundation, JLI, professor pools
PERSON_VERTICALS = {"lumiere education": "lumiere", "horizon academics": "horizon"}

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
    """In-scope people: record id -> {name, verticals}; plus email and name lookups."""
    people, by_email, by_name, offset = {}, {}, {}, None
    while True:
        params = {"pageSize": 100, "filterByFormula": f"{{{S_SCOPE}}}=1"}
        if offset:
            params["offset"] = offset
        page = airtable("GET", f"{BASE_ID}/{STAFF_TABLE}", token, params=params)
        for rec in page.get("records", []):
            f = rec.get("fields", {})
            name = str(f.get(S_NAME) or "").strip()
            verts = {v for k, v in PERSON_VERTICALS.items()
                     if k in str(f.get(S_VERTICAL) or "").lower()}
            people[rec["id"]] = {"name": name, "verticals": verts}
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


def pool_vertical(name):
    for pattern, vertical in POOL_VERTICAL:
        if pattern.search(name):
            return vertical
    return DEFAULT_VERTICAL


def short_name(name):
    n = " ".join(name.split())
    if LUMIERE_POOL.search(n):
        return "Lumiere RSP Americas" if "americas" in n.lower() else "Lumiere RSP"
    n = re.sub(r"\s*\|.*$", "", n)                       # "Ladder Internships Interview | Upcoming Cohort"
    n = re.sub(r"(?i)\b(interviews?|sign-up|call|round|slot)\b", "", n)
    n = re.sub(r"[:\-–]+\s*$", "", " ".join(n.split())).strip(" -:")
    return n or name.strip()


# ---------------------------------------------------------------- main

def main():
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cal_token, at_token = os.getenv("CALENDLY_TOKEN"), os.getenv("AIRTABLE_TOKEN")
    if not cal_token or not at_token:
        print("CALENDLY_TOKEN or AIRTABLE_TOKEN not set; skipping interview sync.")
        return 0

    me = calendly("/users/me", cal_token)["resource"]
    org = me["current_organization"]

    # Calendly users (uri -> email, name)
    users = {}
    for m in calendly_all("/organization_memberships", cal_token, {"organization": org, "count": 100}):
        u = m.get("user") or {}
        users[u.get("uri")] = {"email": u.get("email"), "name": u.get("name")}

    # Interview pools on this account
    pools = {}
    for et in calendly_all("/event_types", cal_token, {"user": me["uri"], "active": "true", "count": 100}):
        name = et.get("name") or ""
        if et.get("pooling_type") and INTERVIEW_POOL.search(name) and not SKIP_POOL.search(name):
            pools[et["uri"]] = {
                "name": et["name"],
                "short": short_name(et["name"]),
                "lumiere": bool(LUMIERE_POOL.search(" ".join(et["name"].split()))),
                "vertical": pool_vertical(et["name"]),
            }
    print(f"{len(pools)} interview pools:", "; ".join(sorted(p["short"] for p in pools.values())))

    people, by_email, by_name = load_staff(at_token)

    # Each host's availability on each pool (current rules)
    host_rules = []   # (staff record id, pool uri, rule)
    unmatched = set()
    for uri in pools:
        for s in calendly_all("/event_type_availability_schedules", cal_token, {"event_type": uri}):
            rule = s.get("availability_rule") or {}
            u = users.get(rule.get("user"), {})
            rid = match_person(u, by_email, by_name)
            if rid:
                host_rules.append((rid, uri, rule))
            elif u.get("email") and u.get("email") != me.get("email"):
                unmatched.add(u.get("name") or u.get("email"))
        time.sleep(0.2)

    today = datetime.now(timezone.utc).date()
    this_monday = today - timedelta(days=today.weekday())
    mondays = [this_monday - timedelta(weeks=1), this_monday, this_monday + timedelta(weeks=1)]

    records = []
    for monday in mondays:
        days = [monday + timedelta(days=i) for i in range(7)]
        fig = {rid: {"lum": 0.0, "oth": 0.0, "cross": 0.0, "by_pool": {},
                     "booked": 0, "taken": 0, "noshow": 0} for rid in people}

        for rid, uri, rule in host_rules:
            h = hours_offered(rule, days)
            if not h:
                continue
            p, f = pools[uri], fig[rid]
            f["lum" if p["lumiere"] else "oth"] += h
            if p["vertical"] not in people[rid]["verticals"]:
                f["cross"] += h
            f["by_pool"][p["short"]] = f["by_pool"].get(p["short"], 0) + h

        # Bookings that week (UTC Monday 00:00 to next Monday 00:00)
        lo = datetime.combine(monday, datetime.min.time(), tzinfo=timezone.utc)
        hi = lo + timedelta(days=7)
        now = datetime.now(timezone.utc)
        for status in ("active", "canceled"):
            events = calendly_all("/scheduled_events", cal_token, {
                "organization": org, "status": status, "count": 100,
                "min_start_time": lo.isoformat().replace("+00:00", "Z"),
                "max_start_time": hi.isoformat().replace("+00:00", "Z")})
            for ev in events:
                if ev.get("event_type") not in pools:
                    continue
                for mem in ev.get("event_memberships") or []:
                    rid = match_person({"email": mem.get("user_email"), "name": mem.get("user_name")},
                                       by_email, by_name)
                    if not rid:
                        continue
                    f = fig[rid]
                    if status == "canceled":
                        f["noshow"] += 1
                        continue
                    f["booked"] += 1
                    end = datetime.fromisoformat(ev["end_time"].replace("Z", "+00:00"))
                    if end > now:
                        continue
                    invitees = calendly_all(f"{ev['uri']}/invitees", cal_token, {"count": 100})
                    if any(i.get("no_show") for i in invitees):
                        f["noshow"] += 1
                    else:
                        f["taken"] += 1
                    time.sleep(0.1)

        for rid, f in fig.items():
            parts = sorted(f["by_pool"].items(), key=lambda kv: -kv[1])
            records.append({"fields": {
                F_KEY: f"{rid}|{monday.isoformat()}",
                F_NAME: f"{people[rid]['name']} – w/c {monday.isoformat()}",
                F_MEMBER: [rid],
                F_WEEK: monday.isoformat(),
                F_LUM: round(f["lum"], 1),
                F_OTH: round(f["oth"], 1),
                F_CROSS: round(f["cross"], 1),
                F_BOOKED: f["booked"],
                F_TAKEN: f["taken"],
                F_NOSHOW: f["noshow"],
                F_BREAKDOWN: " · ".join(f"{n} {round(h, 1):g}h" for n, h in parts) or "none",
            }})

    for i in range(0, len(records), 10):
        chunk = records[i:i + 10]
        if args.dry_run:
            for r in chunk:
                fl = r["fields"]
                print(f"  {fl[F_NAME]}: Lumiere {fl[F_LUM]}h, Other {fl[F_OTH]}h, "
                      f"cross-team {fl[F_CROSS]}h, booked {fl[F_BOOKED]}, taken {fl[F_TAKEN]}, "
                      f"no-show/cancelled {fl[F_NOSHOW]} | {fl[F_BREAKDOWN]}")
            continue
        airtable("PATCH", f"{BASE_ID}/{METRIC_TABLE}", at_token, body={
            "performUpsert": {"fieldsToMergeOn": [F_KEY]},
            "typecast": True,
            "records": chunk,
        })
        time.sleep(0.25)

    print(f"{'Would write' if args.dry_run else 'Wrote'} {len(records)} rows "
          f"({len(people)} people x {len(mondays)} weeks).")
    if unmatched:
        print("Calendly hosts not in scope or not on the Staff Table (skipped):",
              ", ".join(sorted(unmatched)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
