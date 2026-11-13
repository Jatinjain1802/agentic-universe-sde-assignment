"""PII protection at rest.

Two different treatments, on purpose:

* **Matching key** (``match_key``): a keyed HMAC-SHA256 of the canonical
  phone number. One-way, so the number itself is never stored, but events
  and leads can still be joined on it.
* **Encryption** (Fernet, i.e. AES-128-CBC + HMAC): for values the business
  may legitimately need back, like the customer name or the full phone
  number.

The secret comes from the ``AU_PII_KEY`` environment variable. When it is
missing we fall back to a fixed *development* key so the pipeline runs out
of the box; in production the key would live in a KMS/secret manager and
never in the repo or the database.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import os
import warnings

from cryptography.fernet import Fernet

_DEV_ONLY_SEED = b"agentic-universe-takehome-DEV-ONLY"


class PiiProtector:
    def __init__(self, secret: bytes):
        self._fernet = Fernet(base64.urlsafe_b64encode(hashlib.sha256(b"enc:" + secret).digest()))
        self._hmac_key = hashlib.sha256(b"match:" + secret).digest()

    @classmethod
    def from_env(cls) -> "PiiProtector":
        raw = os.environ.get("AU_PII_KEY")
        if raw:
            return cls(raw.encode("utf-8"))
        warnings.warn(
            "AU_PII_KEY is not set; using the built-in development key. "
            "Set AU_PII_KEY for any real data.",
            stacklevel=2,
        )
        return cls(_DEV_ONLY_SEED)

    def match_key(self, canonical_phone: str) -> str:
        """One-way key used to join events to leads by phone."""
        return hmac.new(self._hmac_key, canonical_phone.encode("utf-8"), hashlib.sha256).hexdigest()

    def encrypt(self, plaintext: str) -> str:
        return self._fernet.encrypt(plaintext.encode("utf-8")).decode("ascii")

    def decrypt(self, token: str) -> str:
        return self._fernet.decrypt(token.encode("ascii")).decode("utf-8")
