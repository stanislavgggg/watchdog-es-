"""Lead intake server — приём лидов из квиза без n8n/Node-RED.

POST /lead   — приём лида/телефона из квиза (CORS-ready под sendBeacon)
GET  /stats  — быстрая сводка по лидам (сегодня/всего/по зонам)
GET  /health — для watchdog/uptime-мониторинга

Хранит всё в той же SQLite, что и watchdog (таблица leads), форвардит
в Mailchimp (upsert + теги) и Slack в фоновом потоке — ответ квизу
мгновенный, внешние API не блокируют приём.

ENV:
  DB_PATH             (default: watchdog.db)
  MAILCHIMP_API_KEY   (формат xxxxx-us14)
  MAILCHIMP_LIST_ID
  SLACK_WEBHOOK_URL
  ALLOWED_ORIGIN      (домен квиза, например https://quiz.example.com; * для теста)
  PORT                (default: 8080)

Запуск:  pip install flask --break-system-packages && python lead_server.py
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import queue
import re
import sqlite3
import threading
import time
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s lead_server %(levelname)s %(message)s")
log = logging.getLogger("leads")

DB_PATH = os.environ.get("DB_PATH", "watchdog.db")
MC_KEY = os.environ.get("MAILCHIMP_API_KEY", "")
MC_DC = MC_KEY.rsplit("-", 1)[-1] if "-" in MC_KEY else ""
MC_LIST = os.environ.get("MAILCHIMP_LIST_ID", "")
SLACK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
ORIGIN = os.environ.get("ALLOWED_ORIGIN", "*")

EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]{2,}\.[^\s@]{2,}$")

SCHEMA = """
CREATE TABLE IF NOT EXISTS leads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    email TEXT NOT NULL,
    email_hash TEXT NOT NULL,
    event TEXT NOT NULL DEFAULT 'lead',
    phone TEXT, q1 TEXT, q2 TEXT, q3 TEXT, score TEXT,
    zoneid TEXT, campaignid TEXT, subid TEXT,
    quiz TEXT DEFAULT 'laliga',
    opened INTEGER DEFAULT 0,
    clicked INTEGER DEFAULT 0,
    open_pb INTEGER DEFAULT 0,
    is_duplicate INTEGER DEFAULT 0,
    forwarded INTEGER DEFAULT 0,
    raw TEXT
);
CREATE INDEX IF NOT EXISTS idx_leads_hash ON leads(email_hash);
CREATE INDEX IF NOT EXISTS idx_leads_ts ON leads(ts);
"""

app = Flask(__name__)
_local = threading.local()
_forward_q: "queue.Queue[int]" = queue.Queue()


def db() -> sqlite3.Connection:
    if not hasattr(_local, "db"):
        _local.db = sqlite3.connect(DB_PATH)
        _local.db.row_factory = sqlite3.Row
        _local.db.executescript(SCHEMA)
        for mig in ("ALTER TABLE leads ADD COLUMN quiz TEXT DEFAULT 'laliga'",
                    "ALTER TABLE leads ADD COLUMN opened INTEGER DEFAULT 0",
                    "ALTER TABLE leads ADD COLUMN clicked INTEGER DEFAULT 0",
                    "ALTER TABLE leads ADD COLUMN open_pb INTEGER DEFAULT 0"):
            try:
                _local.db.execute(mig)
                _local.db.commit()
            except sqlite3.OperationalError:
                pass
    return _local.db


def clean(v, n=100) -> str:
    return re.sub(r"[^\w@.+\-:/ ]", "", str(v or ""))[:n]


def cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = ORIGIN
    resp.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


@app.after_request
def _after(resp):
    return cors(resp)


@app.route("/lead", methods=["POST", "OPTIONS"])
def lead():
    if request.method == "OPTIONS":
        return "", 204

    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception:
        data = {}
    email = str(data.get("email", "")).strip().lower()
    if not EMAIL_RE.match(email):
        return jsonify({"ok": False, "error": "bad_email"}), 400

    h = hashlib.md5(email.encode()).hexdigest()
    event = "phone" if data.get("event") == "phone" else "lead"

    con = db()
    dup = 0
    if event == "lead":
        cur = con.execute(
            "SELECT 1 FROM leads WHERE email_hash=? AND event='lead' LIMIT 1", (h,))
        dup = 1 if cur.fetchone() else 0

    cur = con.execute(
        "INSERT INTO leads (ts,email,email_hash,event,phone,q1,q2,q3,score,"
        "zoneid,campaignid,subid,quiz,is_duplicate,raw) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (datetime.now(timezone.utc).isoformat(timespec="seconds"),
         email, h, event,
         clean(data.get("phone"), 20),
         clean(data.get("q1")), clean(data.get("q2")), clean(data.get("q3")),
         clean(data.get("score"), 10),
         clean(data.get("zoneid") or data.get("zone"), 20),
         clean(data.get("campaignid"), 20),
         clean(data.get("subid"), 64),
         clean(data.get("quiz"), 20) or "laliga",
         dup,
         json.dumps(data, ensure_ascii=False)[:2000]))
    con.commit()
    _forward_q.put(cur.lastrowid)
    return jsonify({"ok": True, "duplicate": bool(dup)})





# ---------- мостик Mailchimp -> Keitaro: открытия/клики welcome ----------
MC_WELCOME_WEB_ID = os.environ.get("MC_WELCOME_WEB_ID", "")        # id из URL отчёта
MC_CAMPAIGN_ID = os.environ.get("MC_CAMPAIGN_ID", "")             # либо сразу API id
KEITARO_PB_URL = os.environ.get(
    "KEITARO_POSTBACK_URL",
    "https://silveratlasic.com/c228fd1/postback?subid={subid}&status={status}")
OPEN_STATUS = os.environ.get("KEITARO_OPEN_STATUS", "sale")
SYNC_MINUTES = int(os.environ.get("OPEN_SYNC_MINUTES", "120"))


def _mc_get(path: str, params: dict) -> dict:
    r = requests.get(f"https://{MC_DC}.api.mailchimp.com/3.0{path}",
                     auth=("x", MC_KEY), params=params, timeout=30)
    if r.status_code >= 400:
        log.error("MC GET %s -> %d: %s", path, r.status_code, r.text[:200])
        return {}
    return r.json()


def resolve_campaign_id() -> str:
    """web_id (из URL отчёта) -> API campaign id; ищем и в campaigns, и в reports."""
    if MC_CAMPAIGN_ID:
        return MC_CAMPAIGN_ID
    if not MC_WELCOME_WEB_ID:
        return ""
    want = str(MC_WELCOME_WEB_ID)
    for path, key in (("/campaigns", "campaigns"), ("/reports", "reports")):
        data = _mc_get(path, {"count": 200, "fields": f"{key}.id,{key}.web_id,"
                                                      f"{key}.settings.title,{key}.campaign_title"})
        for c in data.get(key, []):
            if str(c.get("web_id", "")) == want:
                log.info("welcome campaign resolved: web_id=%s -> id=%s (%s)",
                         want, c["id"], c.get("settings", {}).get("title")
                         or c.get("campaign_title", ""))
                return c["id"]
    log.error("welcome campaign web_id=%s not found in /campaigns nor /reports", want)
    return ""


def sync_engagement():
    """Тянем open/click по welcome, матчим с лидами, шлём постбек за первое открытие."""
    cid = resolve_campaign_id()
    if not (cid and MC_KEY):
        return
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    offset, seen = 0, 0
    while offset < 10000:
        data = _mc_get(f"/reports/{cid}/email-activity",
                       {"count": 1000, "offset": offset})
        items = data.get("emails", [])
        if not items:
            break
        for it in items:
            email = (it.get("email_address") or "").lower()
            acts = {a.get("action") for a in it.get("activity", [])}
            if not email or not acts:
                continue
            opened = 1 if "open" in acts else 0
            clicked = 1 if "click" in acts else 0
            if not (opened or clicked):
                continue
            rows = con.execute(
                "SELECT id,subid,opened,clicked,open_pb FROM leads "
                "WHERE email=? AND event='lead' AND is_duplicate=0", (email,)).fetchall()
            for row in rows:
                con.execute("UPDATE leads SET opened=MAX(opened,?), clicked=MAX(clicked,?) "
                            "WHERE id=?", (opened, clicked, row["id"]))
                if opened and not row["open_pb"] and row["subid"]:
                    try:
                        url = KEITARO_PB_URL.format(subid=row["subid"], status=OPEN_STATUS)
                        pr = requests.get(url, timeout=15)
                        log.info("open postback %s -> %d", row["subid"], pr.status_code)
                        con.execute("UPDATE leads SET open_pb=1 WHERE id=?", (row["id"],))
                    except Exception as e:
                        log.error("open postback failed %s: %s", row["subid"], e)
            seen += 1
        con.commit()
        if len(items) < 1000:
            break
        offset += 1000
    con.close()
    log.info("engagement sync done: %d engaged emails processed", seen)


def engagement_loop():
    time.sleep(90)  # даём сервису подняться
    while True:
        try:
            sync_engagement()
        except Exception as e:
            log.error("engagement sync crashed: %s", e)
        time.sleep(SYNC_MINUTES * 60)


threading.Thread(target=engagement_loop, daemon=True).start()


@app.route("/quality")
def quality():
    """Зонная сводка качества: лиды/открытия/клики по (campaign, zone)."""
    con = db()
    rows = con.execute(
        "SELECT campaignid, zoneid, COUNT(*) leads, SUM(opened) opens, "
        "SUM(clicked) clicks FROM leads WHERE event='lead' AND is_duplicate=0 "
        "GROUP BY campaignid, zoneid ORDER BY leads DESC LIMIT 100").fetchall()
    return jsonify([dict(r) for r in rows])


# ---------- серверная валидация email (/validate) ----------
DISPOSABLE = {
    "mailinator.com","yopmail.com","guerrillamail.com","10minutemail.com",
    "tempmail.com","temp-mail.org","trashmail.com","sharklasers.com",
    "getnada.com","dispostable.com","maildrop.cc","fakeinbox.com",
    "mintemail.com","throwawaymail.com","mytemp.email","tempr.email",
    "emailondeck.com","spamgourmet.com","mohmal.com","tmpmail.net",
}
_domain_cache: dict = {}


def domain_deliverable(domain: str) -> bool:
    if domain in _domain_cache:
        return _domain_cache[domain]
    ok = False
    try:
        import dns.resolver
        r = dns.resolver.Resolver()
        r.lifetime = r.timeout = 2.5
        try:
            ok = len(r.resolve(domain, "MX")) > 0
        except Exception:
            ok = len(r.resolve(domain, "A")) > 0
    except ImportError:
        import socket
        try:
            socket.setdefaulttimeout(2.5)
            socket.gethostbyname(domain)
            ok = True
        except Exception:
            ok = False
    except Exception:
        ok = False
    _domain_cache[domain] = ok
    return ok


@app.route("/validate")
def validate():
    email = (request.args.get("email") or "").strip().lower()
    if not EMAIL_RE.match(email):
        return jsonify({"valid": False, "reason": "format"})
    domain = email.split("@")[1]
    if domain in DISPOSABLE:
        return jsonify({"valid": False, "reason": "disposable"})
    if not domain_deliverable(domain):
        return jsonify({"valid": False, "reason": "domain"})
    return jsonify({"valid": True})


@app.route("/stats")
def stats():
    con = db()
    today = datetime.now(timezone.utc).date().isoformat()
    total = con.execute(
        "SELECT COUNT(*) c FROM leads WHERE event='lead' AND is_duplicate=0").fetchone()["c"]
    today_n = con.execute(
        "SELECT COUNT(*) c FROM leads WHERE event='lead' AND is_duplicate=0 AND ts LIKE ?",
        (f"{today}%",)).fetchone()["c"]
    dups = con.execute(
        "SELECT COUNT(*) c FROM leads WHERE is_duplicate=1").fetchone()["c"]
    phones = con.execute(
        "SELECT COUNT(*) c FROM leads WHERE event='phone'").fetchone()["c"]
    zones = con.execute(
        "SELECT zoneid, COUNT(*) c FROM leads WHERE event='lead' AND is_duplicate=0 "
        "AND zoneid!='' GROUP BY zoneid ORDER BY c DESC LIMIT 15").fetchall()
    return jsonify({
        "leads_total": total, "leads_today": today_n,
        "duplicates": dups, "phones": phones,
        "top_zones": [{"zone": z["zoneid"], "leads": z["c"]} for z in zones],
    })


@app.route("/health")
def health():
    return jsonify({"ok": True, "queue": _forward_q.qsize()})


# ---------------- фоновый форвардер ----------------

def mc_list_for(quiz: str) -> str:
    # отдельная аудитория на квиз: MAILCHIMP_LIST_ID_KREPSINIS и т.п.; иначе общая
    return os.environ.get(f"MAILCHIMP_LIST_ID_{(quiz or 'laliga').upper()}", MC_LIST)


def mc_url(h: str, list_id: str = "") -> str:
    return f"https://{MC_DC}.api.mailchimp.com/3.0/lists/{list_id or MC_LIST}/members/{h}"


def _mc(method: str, url: str, auth, payload: dict) -> requests.Response:
    r = requests.request(method, url, auth=auth, timeout=20, json=payload)
    if r.status_code >= 400:
        log.error("Mailchimp %s %s -> %d: %s", method, url.split("/3.0/")[-1],
                  r.status_code, r.text[:300])
    return r


def forward_row(row: sqlite3.Row, con: sqlite3.Connection):
    auth = ("x", MC_KEY)
    h = row["email_hash"]
    quiz = (row["quiz"] if "quiz" in row.keys() else "") or "laliga"
    lst = mc_list_for(quiz)

    if MC_KEY and lst:
        if row["event"] == "phone":
            _mc("PATCH", mc_url(h, lst), auth, {
                "merge_fields": {"PHONE": row["phone"] or ""}})
            _mc("POST", mc_url(h, lst) + "/tags", auth, {
                "tags": [{"name": "sms-optin", "status": "active"}]})
        else:
            body = {
                "email_address": row["email"],
                "status_if_new": "subscribed", "status": "subscribed",
                "merge_fields": {"ZONE": row["zoneid"] or "",
                                 "CAMP": row["campaignid"] or "",
                                 "SUBID": row["subid"] or ""},
            }
            r = _mc("PUT", mc_url(h, lst), auth, body)
            if r.status_code == 400:
                # Скорее всего merge fields не созданы в аудитории —
                # повторяем без них, контакт важнее полей.
                body.pop("merge_fields")
                r = _mc("PUT", mc_url(h, lst), auth, body)
            if r.status_code < 400:
                tags = [f"quiz-{quiz}",
                        f"team-{row['q1'] or 'na'}", f"freq-{row['q2'] or 'na'}",
                        f"bookie-{row['q3'] or 'na'}", f"score-{row['score'] or 'warm'}"]
                _mc("POST", mc_url(h, lst) + "/tags", auth, {
                    "tags": [{"name": t, "status": "active"} for t in tags]})

    if SLACK_URL:
        if row["event"] == "phone":
            text = (f"📱 *SMS opt-in*: {row['email']} → {row['phone']} "
                    f"| zone {row['zoneid'] or '—'}")
        elif row["is_duplicate"]:
            text = (f"♻️ *Duplicate*: {row['email']} | zone {row['zoneid'] or '—'} "
                    f"— зона продаёт одних и тех же людей")
        else:
            text = (f"🆕 *Lead*: {row['email']} | {row['score']} "
                    f"| team {row['q1'] or '—'} | bookie {row['q3'] or '—'} "
                    f"| zone {row['zoneid'] or '—'} camp {row['campaignid'] or '—'}")
        requests.post(SLACK_URL, json={"text": text}, timeout=15)

    con.execute("UPDATE leads SET forwarded=1 WHERE id=?", (row["id"],))
    con.commit()


def forwarder():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    # добить неотправленное после рестарта
    for r in con.execute("SELECT id FROM leads WHERE forwarded=0").fetchall():
        _forward_q.put(r["id"])
    while True:
        lead_id = _forward_q.get()
        try:
            row = con.execute("SELECT * FROM leads WHERE id=?", (lead_id,)).fetchone()
            if row and not row["forwarded"]:
                forward_row(row, con)
        except Exception:
            log.exception("forward failed id=%s (останется forwarded=0, "
                          "уйдёт после рестарта)", lead_id)


threading.Thread(target=forwarder, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    log.info("Lead server on :%d  db=%s  mc=%s  slack=%s  origin=%s",
             port, DB_PATH, "on" if MC_KEY else "OFF",
             "on" if SLACK_URL else "OFF", ORIGIN)
    app.run(host="0.0.0.0", port=port)
