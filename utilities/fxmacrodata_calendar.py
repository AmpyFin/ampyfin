"""FXMacroData release-calendar utility for strategy features."""

from __future__ import annotations

import os
from typing import Any, Optional

import requests

FXMACRODATA_BASE_URL = "https://api.fxmacrodata.com/v1"


def fetch_fxmacrodata_calendar(
    currency: str = "usd",
    *,
    limit: int = 50,
    min_tier: Optional[int] = 2,
    api_key: Optional[str] = None,
    base_url: str = FXMACRODATA_BASE_URL,
) -> list[dict[str, Any]]:
    """Fetch official macro release events for AmpyFin strategies."""

    limit_count = max(1, min(int(limit), 100))
    params: dict[str, str] = {"limit": str(limit_count)}
    token = api_key or os.getenv("FXMACRODATA_API_KEY")
    if token:
        params["api_key"] = token

    response = requests.get(
        f"{base_url.rstrip('/')}/calendar/{currency.lower()}",
        params=params,
        timeout=20,
    )
    response.raise_for_status()
    events = response.json().get("data", [])
    if min_tier is None:
        return events[:limit_count]

    return [
        event
        for event in events
        if int(event.get("market_tier") or 99) <= min_tier
    ][:limit_count]


def release_dates(events: list[dict[str, Any]]) -> set[str]:
    """Return the release-date set for calendar-aware entry filters."""

    return {event["date"] for event in events if event.get("date")}
