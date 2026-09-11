import os

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker


DB_URL = os.getenv("POSTAMATS_DB_URL", "sqlite:///./app.db")
IS_SQLITE = DB_URL.startswith("sqlite")

connect_args = {}
if IS_SQLITE:
    connect_args = {
        "check_same_thread": False,
        # DB-API timeout дополняет PRAGMA busy_timeout и не даёт
        # сразу падать при короткой конкурирующей записи daemon/web.
        "timeout": 30.0,
    }

engine = create_engine(
    DB_URL,
    connect_args=connect_args,
)
SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
)


if IS_SQLITE:
    @event.listens_for(engine, "connect")
    def _set_sqlite_connection_pragmas(
        dbapi_connection,
        connection_record,
    ):
        """Настройки, которые должны применяться к каждому соединению."""
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA synchronous=NORMAL;")
            cursor.execute("PRAGMA busy_timeout=30000;")
        finally:
            cursor.close()


def init_sqlite_pragmas():
    if not IS_SQLITE:
        return

    with engine.connect() as connection:
        try:
            # journal_mode сохраняется в файле БД, поэтому достаточно
            # явно включить WAL при инициализации приложения.
            connection.exec_driver_sql("PRAGMA journal_mode=WAL;")
            connection.exec_driver_sql("PRAGMA synchronous=NORMAL;")
            connection.exec_driver_sql("PRAGMA busy_timeout=30000;")
        except Exception as exc:
            print("exception in db.py, reason:", exc)
