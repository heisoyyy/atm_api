# activity_log_routes.py
"""
Endpoint Activity Log:
  GET  /api/activity-log          — list log dengan filter & pagination
  GET  /api/activity-log/summary  — statistik (card + timeline + top user + top action)
"""

import json
from datetime import datetime, timedelta
from fastapi import APIRouter, Query, Depends, HTTPException
from database import get_conn
from auth import require_admin

router = APIRouter(prefix="/api/activity-log", tags=["Activity Log"])


# ─────────────────────────────────────────────────────────────────────────────
#  GET /api/activity-log
# ─────────────────────────────────────────────────────────────────────────────
@router.get("")
def list_activity_log(
    limit:     int = Query(50,  ge=1, le=200),
    offset:    int = Query(0,   ge=0),
    action:    str = Query(None),
    entity:    str = Query(None),
    status:    str = Query(None),           # "ok" | "error"
    username:  str = Query(None),           # pencarian username
    date_from: str = Query(None),           # "YYYY-MM-DD"
    date_to:   str = Query(None),           # "YYYY-MM-DD"
    _admin=Depends(require_admin),
):
    where, params = [], []

    if action:
        where.append("action = %s");           params.append(action)
    if entity:
        where.append("entity = %s");           params.append(entity)
    if status:
        where.append("status = %s");           params.append(status)
    if username:
        where.append("username LIKE %s");      params.append(f"%{username}%")
    if date_from:
        where.append("DATE(created_at) >= %s"); params.append(date_from)
    if date_to:
        where.append("DATE(created_at) <= %s"); params.append(date_to)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql_count = f"SELECT COUNT(*) AS cnt FROM activity_log {where_sql}"
    sql_data  = f"""
        SELECT id, user_id, username, role, action, entity, entity_id,
               detail, ip_address, status, error_msg, created_at
        FROM activity_log
        {where_sql}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """

    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql_count, params)
        total = cur.fetchone()["cnt"]
        cur.execute(sql_data, params + [limit, offset])
        rows = cur.fetchall()

    for r in rows:
        if r.get("created_at"):
            r["created_at"] = r["created_at"].isoformat()
        # detail disimpan sebagai JSON string di DB, parse balik ke dict
        if r.get("detail") and isinstance(r["detail"], str):
            try:
                r["detail"] = json.loads(r["detail"])
            except Exception:
                pass

    return {"total": total, "data": rows}


# ─────────────────────────────────────────────────────────────────────────────
#  GET /api/activity-log/summary
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/summary")
def activity_log_summary(
    days: int = Query(7, ge=1, le=90),
    _admin=Depends(require_admin),
):
    since = datetime.now() - timedelta(days=days)

    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)

        # ── Stat cards ──
        cur.execute("""
            SELECT
                COUNT(*)                                   AS total_actions,
                COUNT(DISTINCT user_id)                    AS active_users,
                SUM(action = 'UPLOAD')                     AS uploads,
                SUM(action LIKE 'CASHPLAN%')               AS cashplan_actions,
                SUM(action LIKE 'REKAP%')                  AS rekap_actions,
                SUM(action LIKE 'USER%')                   AS user_actions,
                SUM(status = 'error')                      AS errors
            FROM activity_log
            WHERE created_at >= %s
        """, (since,))
        cards = cur.fetchone()

        # ── Timeline (per hari) ──
        cur.execute("""
            SELECT DATE(created_at) AS tgl, COUNT(*) AS cnt
            FROM activity_log
            WHERE created_at >= %s
            GROUP BY DATE(created_at)
            ORDER BY tgl ASC
        """, (since,))
        timeline = [{"tgl": str(r["tgl"]), "cnt": r["cnt"]} for r in cur.fetchall()]

        # ── Top users ──
        cur.execute("""
            SELECT username, COUNT(*) AS cnt
            FROM activity_log
            WHERE created_at >= %s AND username IS NOT NULL
            GROUP BY username
            ORDER BY cnt DESC
            LIMIT 10
        """, (since,))
        by_user = cur.fetchall()

        # ── Top actions ──
        cur.execute("""
            SELECT action, COUNT(*) AS cnt
            FROM activity_log
            WHERE created_at >= %s
            GROUP BY action
            ORDER BY cnt DESC
            LIMIT 10
        """, (since,))
        by_action = cur.fetchall()

    def _i(v): return int(v or 0)

    return {
        "days":             days,
        "total_actions":    _i(cards["total_actions"]),
        "active_users":     _i(cards["active_users"]),
        "uploads":          _i(cards["uploads"]),
        "cashplan_actions": _i(cards["cashplan_actions"]),
        "rekap_actions":    _i(cards["rekap_actions"]),
        "user_actions":     _i(cards["user_actions"]),
        "errors":           _i(cards["errors"]),
        "timeline":         timeline,
        "by_user":          [{"username": r["username"], "cnt": r["cnt"]} for r in by_user],
        "by_action":        [{"action": r["action"], "cnt": r["cnt"]} for r in by_action],
    }