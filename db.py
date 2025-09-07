# db.py
import os, sys, time, logging
from time import monotonic
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base

logging.basicConfig(level=logging.INFO)
_q_cache = {"qid": None, "ts": 0.0}

def _load_env_once():
    """Load .env from sensible places (exe dir, _MEIPASS, source dir) if DATABASE_URL not set."""
    if os.getenv("DATABASE_URL"):
        return

    candidates = []

    # When bundled:
    if getattr(sys, "frozen", False):
        # next to the .exe
        candidates.append(os.path.join(os.path.dirname(sys.executable), ".env"))
        # PyInstaller onefile temp extraction dir (if you bundled .env)
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            candidates.append(os.path.join(meipass, ".env"))

    # When running from source
    candidates.append(os.path.join(os.path.abspath(os.path.dirname(__file__)), ".env"))

    for p in candidates:
        if os.path.isfile(p):
            load_dotenv(p, override=False)  # don't overwrite real env if set
            break

# Ensure env is available before reading it
_load_env_once()

DATABASE_URL = os.getenv("DATABASE_URL")  # Neon URL (or your DB)
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is not set.\n"
        "Put a .env file next to the executable (or in the project root) containing e.g.\n"
        "  DATABASE_URL=postgresql+psycopg://user:pass@host/db?sslmode=require\n"
        "Or set it as a system environment variable."
    )

# Single engine (remove duplicate line)
engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True)
Base = declarative_base()

def get_current_qid():
    now = monotonic()
    if _q_cache["qid"] is not None and (now - _q_cache["ts"] < 60):
        return _q_cache["qid"]
    row = fetch_one("SELECT id FROM quarters WHERE is_current = TRUE LIMIT 1")
    _q_cache["qid"] = row["id"] if row else None
    _q_cache["ts"] = now
    return _q_cache["qid"]

def fetch_all(sql, **params):
    t0 = time.perf_counter()
    with engine.begin() as conn:
        res = conn.execute(text(sql), params)
        rows = [dict(row._mapping) for row in res.all()]
    dt = (time.perf_counter() - t0) * 1000
    if dt > 100:
        logging.info("SQL %.1fms:  %s  params=%s", dt, sql.splitlines()[0], params)
    return rows

def fetch_one(sql, **params):
    t0 = time.perf_counter()
    with engine.begin() as conn:
        res = conn.execute(text(sql), params).first()
    dt = (time.perf_counter() - t0) * 1000
    if dt > 100:
        logging.info("SQL1 %.1fms: %s params=%s", dt, sql.splitlines()[0], params)
    return dict(res._mapping) if res is not None else None

def execute(sql, **params):
    with engine.begin() as conn:
        conn.execute(text(sql), params)
