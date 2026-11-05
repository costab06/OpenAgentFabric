"""Stable workflow IDs derived from canonical business-event identity.

The business key MUST distinguish a genuine new execution from a replay.
"""
from __future__ import annotations
import hashlib
import json
from uuid import UUID


class WorkflowIdentity:
    @staticmethod
    def derive(trigger_type: str, business_key: str, *, namespace: str = "default") -> UUID:
        if not all(isinstance(x, str) and x.strip() for x in (trigger_type, business_key, namespace)):
            raise ValueError("namespace, trigger_type and business_key must be nonempty strings")
        canonical = json.dumps([namespace, trigger_type, business_key], ensure_ascii=False, separators=(",", ":"))
        return UUID(bytes=hashlib.sha256(canonical.encode("utf-8")).digest()[:16])
