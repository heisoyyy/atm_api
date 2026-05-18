# activity_log.py
"""
Helper untuk mencatat activity log ke tabel activity_log.
Panggil log_activity() dari endpoint manapun.
"""

from datetime import datetime
from database import get_conn


def log_activity(
    action: str,
    *,
    user_id: int = None,
    username: str = None,
    role: str = None,
    entity: str = None,
    entity_id: str = None,
    detail: dict = None,
    ip_address: str = None,
    status: str = "ok",
    error_msg: str = None,
):
    """
    Insert satu baris ke activity_log.

    Contoh:
        log_activity("UPLOAD", username="budi", role="operator",
                     entity="upload", entity_id="upload_log_42",
                     detail={"filename": "data.zip", "rows": 500},
                     ip_address=request.client.host)
    """
    try:
        import json
        detail_json = json.dumps(detail, default=str) if detail else None

        sql = """
            INSERT INTO activity_log
                (user_id, username, role, action, entity, entity_id,
                 detail, ip_address, status, error_msg, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with get_conn() as conn:
            conn.cursor().execute(sql, (
                user_id,
                username,
                role,
                action,
                entity,
                str(entity_id) if entity_id is not None else None,
                detail_json,
                ip_address,
                status,
                error_msg,
                datetime.now(),
            ))
    except Exception as e:
        # Jangan sampai logging gagal → merusak endpoint utama
        print(f"[activity_log] WARNING: gagal catat log: {e}")


def get_client_ip(request) -> str:
    """Ambil IP dari request FastAPI, handle reverse proxy."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"