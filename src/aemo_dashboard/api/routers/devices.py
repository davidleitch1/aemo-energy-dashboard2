"""POST /v1/devices/register — APNs token registration for the iOS app.

The iOS app posts on first launch (and on token refresh) with:

    {
      "token": "<64-hex-char APNs device token>",
      "user_label": "David's iPhone",            // optional
      "categories": ["price", "new-duid"]        // optional, default all
    }

The endpoint upserts a row into the JSON registry at the path given
by `APNS_TOKENS_PATH`. The collector's `ApnsPushSink` reads this file
each cycle to fan alerts out via APNs.

Re-registration of a previously-deactivated token (APNs returned 410
Unregistered → sink set `active=false`) re-activates the token. A
successful registration always refreshes `last_seen_at`.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field


router = APIRouter()


# Subset of categories the iOS app may opt into. Not yet enforced
# server-side beyond shape validation; ApnsPushSink fans out to all
# active tokens for now (Phase B). Per-device subscription filtering
# is Phase C.
VALID_CATEGORIES = {'price', 'new-duid', 'renewable-record'}
DEFAULT_CATEGORIES = ['price', 'new-duid']


class DeviceRegistration(BaseModel):
    token: str = Field(..., description='APNs device token (hex string)')
    user_label: Optional[str] = Field(None, description='Optional human-readable label')
    categories: Optional[list[str]] = Field(None, description='Notification categories to opt in to')


def _tokens_path() -> Path:
    raw = os.environ.get('APNS_TOKENS_PATH')
    if not raw:
        raise HTTPException(
            status_code=503,
            detail={'code': 'APNS_NOT_CONFIGURED',
                    'message': 'APNS_TOKENS_PATH not set on server'},
        )
    return Path(raw)


def _load(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _save(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(state, indent=2))
    os.replace(tmp, path)


@router.post('/devices/register')
def register_device(body: DeviceRegistration) -> dict:
    token = (body.token or '').strip()
    if not token:
        raise HTTPException(400, detail={'code': 'EMPTY_TOKEN',
                                         'message': 'token is required'})
    # APNs tokens are 64-hex-char on iOS 13-, 32-byte (64-hex) on
    # iOS 13+. We're not strict on the format — the iOS app builds
    # this from `application:didRegisterForRemoteNotificationsWithDeviceToken:`
    # — but reject anything obviously not a token.
    if len(token) < 16:
        raise HTTPException(400, detail={'code': 'INVALID_TOKEN_LENGTH',
                                         'message': 'token too short to be valid'})

    if body.categories is not None:
        bad = [c for c in body.categories if c not in VALID_CATEGORIES]
        if bad:
            raise HTTPException(
                400,
                detail={'code': 'INVALID_CATEGORY',
                        'message': f'unknown categories: {bad}',
                        'valid': sorted(VALID_CATEGORIES)},
            )
        categories = list(body.categories)
    else:
        categories = list(DEFAULT_CATEGORIES)

    path = _tokens_path()
    state = _load(path)
    now_iso = datetime.now(timezone.utc).isoformat()
    existing = state.get(token, {})
    state[token] = {
        'active':         True,  # always re-activate on re-registration
        'user_label':     body.user_label or existing.get('user_label'),
        'registered_at':  existing.get('registered_at') or now_iso,
        'last_seen_at':   now_iso,
        'categories':     categories,
    }
    _save(path, state)

    return {
        'token': token,
        'active': True,
        'user_label': state[token]['user_label'],
        'categories': categories,
        'registered_at': state[token]['registered_at'],
        'last_seen_at': state[token]['last_seen_at'],
    }
