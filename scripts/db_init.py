# scripts/db_init.py
import os
from db import engine
from sqlalchemy import text

BASE = os.path.dirname(os.path.dirname(__file__))

def run_sql(path):
    p = os.path.join(BASE, path)
    with open(p, "r", encoding="utf-8") as f:
        sql = f.read()
    stmts = [s.strip() for s in sql.split(";") if s.strip()]
    with engine.begin() as conn:
        for i, s in enumerate(stmts, 1):
            conn.exec_driver_sql(s + ";")
            print(f"✔ Ran statement {i} from {path}")

if __name__ == "__main__":
    run_sql(r"sql\create_tables.sql")
    run_sql(r"sql\seed_sample.sql")
    print("✅ DB initialized/seeded")
