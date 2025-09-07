# db.py
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sqlalchemy.orm import declarative_base
import time, logging
from time import monotonic
logging.basicConfig(level=logging.INFO)
_q_cache = {"qid": None, "ts": 0.0}

load_dotenv(override=True)
# db.py
DATABASE_URL = os.getenv("DATABASE_URL")  # your Neon URL

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is not set. Put it in a .env file in the project root, "
        "e.g. DATABASE_URL=postgresql+psycopg://user:pass@host/db?sslmode=require"
    )

engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True)

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
    if dt > 100:  # log queries that take >100ms
        logging.info("SQL %.1fms: %s params=%s", dt, sql.splitlines()[0], params)
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
