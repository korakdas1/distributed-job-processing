# Database migrations

Alembic owns schema changes. Do not use `Base.metadata.create_all()` in the API.

```bash
source .venv/bin/activate
alembic upgrade head
alembic current
alembic history
```

The database URL is loaded from application settings / `.env`, not from `alembic.ini`.
