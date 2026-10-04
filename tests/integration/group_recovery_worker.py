"""Subprocess-only barriers/fault injection for consumer-group acceptance tests."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ConnectionError

from job_platform.queue.streams import StreamMessage
from job_platform.worker import processor, runtime


def main() -> None:
    directory = Path(sys.argv[1])
    index = sys.argv[2]
    original_reclaim = runtime._reclaim_one
    original_ensure = runtime.ensure_consumer_groups
    original_handler = processor.execute_handler
    original_process = runtime.process_message
    original_ack = Redis.xack
    initialized = False

    async def wait_file(name: str) -> None:
        while not (directory / name).exists():
            await asyncio.sleep(0.01)

    async def reclaim(*args: Any, **kwargs: Any) -> tuple[StreamMessage | None, int]:
        await wait_file(f"poll-{index}.release")
        return await original_reclaim(*args, **kwargs)

    async def ensure() -> None:
        nonlocal initialized
        if initialized:
            (directory / f"repair-{index}.entered").touch()
            await wait_file("repair.release")
            if (directory / "repair.fail").exists():
                raise ConnectionError("injected Redis unavailability during group repair")
        await original_ensure()
        initialized = True

    async def handler(*args: Any, **kwargs: Any) -> dict[str, Any]:
        if args[1].get("text") == "blocked handler":
            (directory / f"handler-{index}.entered").touch()
            await wait_file("handler.release")
        return await original_handler(*args, **kwargs)

    async def process(*args: Any, **kwargs: Any) -> None:
        await original_process(*args, **kwargs)
        message = args[1]
        (directory / f"processed-{index}-{message.job_id}").touch()

    async def ack(self: Redis, *args: Any, **kwargs: Any) -> Any:
        result = await original_ack(self, *args, **kwargs)
        print(f"TEST_XACK_RESULT={result}", flush=True)
        return result

    runtime._reclaim_one = reclaim
    runtime.ensure_consumer_groups = ensure
    processor.execute_handler = handler
    runtime.process_message = process
    Redis.xack = ack
    runtime.main()


if __name__ == "__main__":
    main()
