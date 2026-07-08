from logging.config import fileConfig

from sqlalchemy import create_engine, pool, text

from alembic import context
from matchbot.config.settings import get_settings
from matchbot.storage.schema import build_metadata

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Connection info and schema name come from matchbot's own Settings (DATABASE_URL
# / DB_SCHEMA via .env), the same source every other entrypoint uses — never a
# second, separately-configured URL living only in alembic.ini.
settings = get_settings()
db_schema = settings.db_schema
target_metadata = build_metadata(db_schema)


def _normalize_url(url: str) -> str:
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (emits SQL, no live connection)."""
    context.configure(
        url=_normalize_url(settings.database_url),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table_schema=db_schema,
        include_schemas=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode against the configured schema.

    Pins ``search_path`` to ``db_schema`` first, same as
    ``PostgresRepository.init_schema()``, and tells Alembic to keep its own
    ``alembic_version`` bookkeeping table in that schema too (not ``public``)
    so multiple schemas (e.g. a future second environment sharing one
    database) each track their own migration history independently.
    """
    connectable = create_engine(
        _normalize_url(settings.database_url), poolclass=pool.NullPool
    )

    with connectable.connect() as connection:
        connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{db_schema}"'))
        connection.execute(text(f'SET search_path TO "{db_schema}"'))
        connection.commit()
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table_schema=db_schema,
            include_schemas=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
