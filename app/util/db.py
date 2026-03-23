from sqlalchemy import create_engine
from sqlmodel import Session, text

from app.internal.env_settings import Settings

def _is_postgres() -> bool:
    return Settings().db.use_postgres


def _build_engine():
    db = Settings().db
    if db.use_postgres:
        return create_engine(
            f"postgresql://{db.postgres_user}:{db.postgres_password}@{db.postgres_host}:{db.postgres_port}/{db.postgres_db}?sslmode={db.postgres_ssl_mode}"
        )
    sqlite_path = Settings().get_sqlite_path()
    return create_engine(f"sqlite+pysqlite:///{sqlite_path}")


engine = _build_engine()


def get_session():
    with Session(engine) as session:
        if not _is_postgres():
            session.execute(text("PRAGMA foreign_keys=ON"))
        yield session
