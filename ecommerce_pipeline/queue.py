"""Redis-backed queue for ingest events."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass

from ecommerce_pipeline.config import ingest_queue_name, redis_host, redis_port


@dataclass
class IngestEvent:
    event_id: str
    file_path: str

    def dumps(self) -> str:
        return json.dumps({"event_id": self.event_id, "file_path": self.file_path})

    @staticmethod
    def loads(raw: str) -> "IngestEvent":
        payload = json.loads(raw)
        return IngestEvent(event_id=payload["event_id"], file_path=payload["file_path"])


def _client():
    import redis

    return redis.Redis(host=redis_host(), port=redis_port(), decode_responses=True)


def enqueue_file(file_path: str, event_id: str | None = None) -> IngestEvent:
    evt = IngestEvent(event_id=event_id or str(uuid.uuid4()), file_path=file_path)
    _client().rpush(ingest_queue_name(), evt.dumps())
    return evt


def dequeue_file() -> IngestEvent | None:
    try:
        raw = _client().lpop(ingest_queue_name())
    except Exception:
        # Queue is optional for ad-hoc local runs; fall back to static CSV path.
        return None
    if raw is None:
        return None
    return IngestEvent.loads(raw)
