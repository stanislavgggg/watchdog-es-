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
        "zoneid,campaignid,subid,is_duplicate,raw) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (datetime.now(timezone.utc).isoformat(timespec="seconds"),
         email, h, event,
         clean(data.get("phone"), 20),
         clean(data.get("q1")), clean(data.get("q2")), clean(data.get("q3")),
         clean(data.get("score"), 10),
         clean(data.get("zoneid") or data.get("zone"), 20),
         clean(data.get("campaignid"), 20),
         clean(data.get("subid"), 64),
         dup,
         json.dumps(data, ensure_ascii=False)[:2000]))
    con.commit()
    _forward_q.put(cur.lastrowid)
    return jsonify({"ok": True, "duplicate": bool(dup)})


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

def mc_url(h: str) -> str:
    return f"https://{MC_DC}.api.mailchimp.com/3.0/lists/{MC_LIST}/members/{h}"


def forward_row(row: sqlite3.Row, con: sqlite3.Connection):
    auth = ("x", MC_KEY)
    h = row["email_hash"]

    if MC_KEY and MC_LIST:
        if row["event"] == "phone":
            requests.patch(mc_url(h), auth=auth, timeout=20, json={
                "merge_fields": {"PHONE": row["phone"] or ""}})
            requests.post(mc_url(h) + "/tags", auth=auth, timeout=20, json={
                "tags": [{"name": "sms-optin", "status": "active"}]})
        else:
            requests.put(mc_url(h), auth=auth, timeout=20, json={
                "email_address": row["email"],
                "status_if_new": "subscribed", "status": "subscribed",
                "merge_fields": {"ZONE": row["zoneid"] or "",
                                 "CAMP": row["campaignid"] or "",
                                 "SUBID": row["subid"] or ""}})
            tags = ["quiz-laliga",
                    f"team-{row['q1'] or 'na'}", f"freq-{row['q2'] or 'na'}",
                    f"bookie-{row['q3'] or 'na'}", f"score-{row['score'] or 'warm'}"]
            requests.post(mc_url(h) + "/tags", auth=auth, timeout=20, json={
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
