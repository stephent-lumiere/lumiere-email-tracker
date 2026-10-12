"""
Google Calendar figures for the interview-hours sync (baseline competency system).

Reads each in-scope person's own Google Calendar through the same service
account the email tracker uses (domain-wide delegation; calendar read-only
scope added by the Workspace admin). Two things come from it:

  1. Interviews held: interview events on the person's calendar that have
     finished, that they did not decline, and that had at least one other
     attendee. This catches interviews for every vertical (WSG, Ladder,
     hiring, Veritas...) including ones booked on Calendly accounts the sync
     can't read. Counted with hours and a breakdown by type.
  2. Busy time: every other timed event they haven't declined and that is
     not marked "free". Unlike Calendly's busy times this also covers days
     already past, so a day's hours offered can be worked out properly the
     first time it is calculated (or recalculated with --recompute).

Anything that can't be read (no access, a non-Lumiere address) returns None,
and the caller falls back to what it did before.
"""

import json
import os
import re
from datetime import datetime, timedelta, timezone

UTC = timezone.utc
SCOPES = ["https://www.googleapis.com/auth/calendar.readonly",
          "https://www.googleapis.com/auth/calendar.events.readonly"]
DOMAINS = ("lumiere.education",)

INTERVIEW = re.compile(r"interview|mentorship position|\bharp\b", re.I)
# (label, pattern): first match wins
TYPES = [
    ("WSG", re.compile(r"\bwsg\b|wall street", re.I)),
    ("Ladder", re.compile(r"ladder|online internship", re.I)),
    ("Horizon", re.compile(r"\bharp\b|horizon", re.I)),
    ("Veritas", re.compile(r"veritas", re.I)),
    ("White Label", re.compile(r"\bjli\b|research program interview slot|white label", re.I)),
    ("Hiring", re.compile(r"hiring|candidate|\bR[1-4]\b|round [1-4]", re.I)),
    ("Lumiere student", re.compile(r"research scholar program|\brsp\b", re.I)),
    ("Lumiere mentor", re.compile(r"mentor|professor|foundation", re.I)),
]


def _parse(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(UTC)


def interview_type(ev):
    text = " ".join([ev.get("summary") or "", ev.get("description") or ""])
    if not INTERVIEW.search(text):
        return None
    for label, pat in TYPES:
        if pat.search(text):
            return label
    return "Other"


class Calendar:
    def __init__(self, info):
        self.info = info
        self.scope = None
        self._svc = {}

    @classmethod
    def from_env(cls):
        path = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
        try:
            with open(path) as fh:
                info = json.loads(fh.read(), strict=False)
            if info.get("type") != "service_account":
                raise ValueError("not a service account file")
        except Exception as e:                                   # noqa: BLE001
            print(f"Google Calendar not used: credentials unreadable ({str(e)[:120]}).")
            return None
        return cls(info)

    def _service(self, email):
        if email in self._svc:
            return self._svc[email]
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        svc = None
        for scope in ([self.scope] if self.scope else SCOPES):
            try:
                creds = service_account.Credentials.from_service_account_info(
                    self.info, scopes=[scope]).with_subject(email)
                s = build("calendar", "v3", credentials=creds, cache_discovery=False)
                s.calendarList().get(calendarId="primary").execute()
                svc, self.scope = s, scope
                break
            except Exception as e:                               # noqa: BLE001
                last = str(e)[:160]
        if svc is None:
            print(f"  Google Calendar unavailable for {email}: {last}")
        self._svc[email] = svc
        return svc

    def events(self, emails, lo, hi):
        """Timed, non-cancelled events on the person's primary calendar, or None."""
        email = next((e for e in emails if e.endswith(DOMAINS)), None)
        if not email:
            return None
        svc = self._service(email)
        if svc is None:
            return None
        out, token = [], None
        try:
            while True:
                page = svc.events().list(
                    calendarId="primary", singleEvents=True, orderBy="startTime",
                    timeMin=lo.isoformat().replace("+00:00", "Z"),
                    timeMax=hi.isoformat().replace("+00:00", "Z"),
                    maxResults=250, pageToken=token,
                    fields="nextPageToken,items(status,summary,description,start,end,"
                           "attendees(self,email,responseStatus),transparency,eventType)").execute()
                for ev in page.get("items", []):
                    if ev.get("status") == "cancelled" or "dateTime" not in (ev.get("start") or {}):
                        continue
                    me = next((a for a in ev.get("attendees") or [] if a.get("self")), None)
                    if me and me.get("responseStatus") == "declined":
                        continue
                    ev["_s"], ev["_e"] = _parse(ev["start"]["dateTime"]), _parse(ev["end"]["dateTime"])
                    ev["_type"] = interview_type(ev)
                    ev["_others"] = sum(1 for a in ev.get("attendees") or [] if not a.get("self"))
                    out.append(ev)
                token = page.get("nextPageToken")
                if not token:
                    return out
        except Exception as e:                                   # noqa: BLE001
            print(f"  Google Calendar read failed for {email}: {str(e)[:160]}")
            return None


def busy(events):
    """Busy intervals: timed events that aren't booked interviews, aren't 'free', aren't working-location markers.

    A booked interview (interview event with an attendee) is left in, as with
    Calendly: a booked interview hour was offered. An interview *block* with
    no attendee (e.g. "WSG Interviews Block") is busy time for other pools.
    """
    if events is None:
        return None
    return [(ev["_s"], ev["_e"]) for ev in events
            if not (ev["_type"] and ev["_others"] >= 1) and ev.get("transparency") != "transparent"
            and ev.get("eventType", "default") in ("default", "focusTime", "outOfOffice")]


def held(events, lo, hi, now):
    """Interviews held in [lo, hi): count, hours, {type: count}. None if calendar unreadable."""
    if events is None:
        return None
    n, hrs, by = 0, 0.0, {}
    for ev in events:
        if not ev["_type"] or not (lo <= ev["_s"] < hi) or ev["_e"] > now or ev["_others"] < 1:
            continue
        mins = (ev["_e"] - ev["_s"]).total_seconds() / 60
        if mins < 5 or mins > 120:
            continue
        n += 1
        hrs += mins / 60
        by[ev["_type"]] = by.get(ev["_type"], 0) + 1
    return {"count": n, "hours": round(hrs, 1), "by": by}
