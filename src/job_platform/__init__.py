"""Distributed Job Processing Platform.

FastAPI persists jobs in PostgreSQL. A transactional outbox publisher dispatches
job IDs onto Redis Streams. Independent workers execute allowlisted handlers
with persist-before-XACK crash safety. Execution is at-least-once.
"""

__version__ = "0.1.0"
