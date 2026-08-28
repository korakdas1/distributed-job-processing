# Docker assets

The root `Dockerfile` builds one runtime image: `job-platform-app:local`.

`docker/entrypoint-api.sh` waits for PostgreSQL, runs `alembic upgrade head`, then `exec`s Uvicorn so the API process receives SIGTERM.

Workers, publisher, and scheduler use the same image with different commands. See the root README architecture section.
