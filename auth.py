"""
auth.py — Sistem autentikasi SIPRAS v8
JWT-based auth dengan bcrypt password hashing.

Install dependencies:
    pip install python-jose[cryptography] passlib[bcrypt]
"""

from datetime import datetime, timedelta
from typing import Optional
import os

from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

from database import get_conn

# ── Config ────────────────────────────────────────────────────────────────────
SECRET_KEY  = os.getenv("JWT_SECRET_KEY", "sipras-brks-secret-key-ganti-di-production-2026")
ALGORITHM   = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 12

# ── Crypto ────────────────────────────────────────────────────────────────────
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
bearer_scheme = HTTPBearer(auto_error=False)


# ── Pydantic Models ───────────────────────────────────────────────────────────
class LoginRequest(BaseModel):
    username: str
    password: str

class RegisterRequest(BaseModel):
    username:  str
    email:     str
    password:  str
    full_name: Optional[str] = None
    role:      str = "viewer"
    wilayah:   Optional[str] = None

class TokenResponse(BaseModel):
    access_token: str
    token_type:   str = "bearer"
    user: dict

class UserInfo(BaseModel):
    id:        int
    username:  str
    email:     str
    full_name: Optional[str]
    role:      str
    wilayah:   Optional[str]
    is_active: bool


# ── Helpers ───────────────────────────────────────────────────────────────────
def hash_password(plain: str) -> str:
    return pwd_context.hash(plain)

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def _get_user_by_username(username: str) -> Optional[dict]:
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT * FROM users WHERE username=%s AND is_active=1",
            (username,)
        )
        return cur.fetchone()

def _get_user_by_id(user_id: int) -> Optional[dict]:
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id, username, email, full_name, role, wilayah, is_active "
            "FROM users WHERE id=%s",
            (user_id,)
        )
        return cur.fetchone()


# ── Dependency: get current user dari JWT ─────────────────────────────────────
def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme)
) -> dict:
    """
    FastAPI dependency — inject ke endpoint yang perlu auth.
    Contoh: user = Depends(get_current_user)
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token tidak ditemukan. Silakan login.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: int = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Token tidak valid.")
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired atau tidak valid. Silakan login ulang.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user = _get_user_by_id(int(user_id))
    if not user:
        raise HTTPException(status_code=401, detail="User tidak ditemukan.")
    if not user["is_active"]:
        raise HTTPException(status_code=403, detail="Akun dinonaktifkan.")
    return user


def require_admin(user: dict = Depends(get_current_user)) -> dict:
    """Dependency khusus admin."""
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Hanya admin yang diizinkan.")
    return user


# ── Auth Functions (dipanggil dari main.py) ───────────────────────────────────
def login_user(req: LoginRequest) -> TokenResponse:
    user = _get_user_by_username(req.username.strip())
    if not user or not verify_password(req.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Username atau password salah.",
        )

    # Update last_login
    with get_conn() as conn:
        conn.cursor().execute(
            "UPDATE users SET last_login=%s WHERE id=%s",
            (datetime.now(), user["id"])
        )

    token = create_access_token({"sub": str(user["id"]), "role": user["role"]})
    return {
        "access_token": token,
        "token_type":   "bearer",
        "user": {
            "id":        user["id"],
            "username":  user["username"],
            "email":     user["email"],
            "full_name": user["full_name"],
            "role":      user["role"],
            "wilayah":   user["wilayah"],
        },
    }


def register_user(req: RegisterRequest, created_by: dict = None) -> dict:
    """
    Register user baru.
    - Role 'admin' hanya bisa dibuat oleh admin.
    - Role 'operator'/'viewer' bisa dibuat oleh admin atau self-register.
    """
    if created_by is None and req.role == "admin":
        raise HTTPException(400, "Tidak bisa self-register sebagai admin.")

    if created_by and created_by["role"] != "admin" and req.role == "admin":
        raise HTTPException(403, "Hanya admin yang bisa membuat akun admin.")

    # Cek duplikat
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id FROM users WHERE username=%s OR email=%s",
            (req.username.strip(), req.email.strip())
        )
        if cur.fetchone():
            raise HTTPException(400, "Username atau email sudah terdaftar.")

    hashed = hash_password(req.password)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO users (username, email, password_hash, full_name, role, wilayah)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (
                req.username.strip(),
                req.email.strip(),
                hashed,
                req.full_name,
                req.role,
                req.wilayah,
            )
        )
        new_id = cur.lastrowid

    return {
        "message":  "Registrasi berhasil",
        "user_id":  new_id,
        "username": req.username,
        "role":     req.role,
    }