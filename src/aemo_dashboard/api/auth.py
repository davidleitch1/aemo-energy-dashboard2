"""Bearer token middleware.

Tokens loaded from `$API_TOKENS_FILE` (YAML mapping `token: name`). If unset,
any non-empty token is accepted (dev mode). In production, set the env var.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Set

import yaml
from fastapi import Request
from fastapi.responses import JSONResponse


def _load_tokens() -> Set[str]:
    path = os.environ.get("API_TOKENS_FILE")
    if not path:
        return set()
    p = Path(path)
    if not p.exists():
        return set()
    data = yaml.safe_load(p.read_text())
    if not isinstance(data, dict):
        return set()
    return {str(k) for k in data.keys()}


_TOKENS: Set[str] | None = None


def _get_tokens() -> Set[str]:
    global _TOKENS
    if _TOKENS is None:
        _TOKENS = _load_tokens()
    return _TOKENS


def reset_tokens_for_tests() -> None:
    global _TOKENS
    _TOKENS = None


async def bearer_token_middleware(request: Request, call_next):
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        return JSONResponse(
            status_code=401,
            content={"error": {"code": "UNAUTHORIZED", "message": "Missing bearer token"}},
        )

    token = auth[7:].strip()
    if not token:
        return JSONResponse(
            status_code=401,
            content={"error": {"code": "UNAUTHORIZED", "message": "Empty bearer token"}},
        )

    tokens = _get_tokens()
    if tokens and token not in tokens:
        return JSONResponse(
            status_code=401,
            content={"error": {"code": "UNAUTHORIZED", "message": "Invalid token"}},
        )

    return await call_next(request)
