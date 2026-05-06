from passlib.context import CryptContext
from database import get_conn

pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")
hashed = pwd.hash("admin123")

with get_conn() as conn:
    conn.cursor().execute("""
        INSERT INTO users (username, email, password_hash, full_name, role)
        VALUES ('admin', 'admin@brks.co.id', %s, 'Administrator BRKS', 'admin')
    """, (hashed,))

print("Done:", hashed)