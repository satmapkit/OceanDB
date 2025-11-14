# OceanDB/utils/db_utils.py
from psycopg import OperationalError
from sqlalchemy import create_engine, text
import os

def test_connection(engine):
    # engine = get_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            print("✅ Connection successful! Database is reachable and authentication works.")
    except OperationalError as e:
        print(f"❌ Could not connect to database: {e}")
    except Exception as e:
        print(f"⚠️ Unexpected error: {e}")


def get_engine(echo: bool = False):
    """Return a SQLAlchemy engine connected to the OceanDB Postgres database."""
    host = os.getenv("DB_HOST", "postgres")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    db = os.getenv("DB_NAME", "ocean")

    url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(url, echo=echo)
    return engine

engine = get_engine()