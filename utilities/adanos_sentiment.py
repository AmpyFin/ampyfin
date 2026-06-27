"""Optional Adanos market sentiment feature helpers.

AmpyFin's core price and strategy pipelines remain independent from Adanos.
Use these helpers when you explicitly want to enrich a ticker universe with
external sentiment, buzz, and attention features.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from math import isfinite
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import pandas as pd

ADANOS_BASE_URL = "https://api.adanos.org"
ADANOS_API_KEY_ENV = "ADANOS_API_KEY"

SOURCE_ENDPOINTS = {
    "reddit": "/reddit/stocks/v1/stock/{ticker}",
    "x": "/x/stocks/v1/stock/{ticker}",
    "news": "/news/stocks/v1/stock/{ticker}",
    "polymarket": "/polymarket/stocks/v1/stock/{ticker}",
}

FEATURE_COLUMNS = [
    "Ticker",
    "adanos_source",
    "adanos_sentiment_score",
    "adanos_buzz_score",
    "adanos_bullish_pct",
    "adanos_bearish_pct",
    "adanos_mentions",
    "adanos_trend",
]


class AdanosSentimentError(RuntimeError):
    """Raised when Adanos sentiment data cannot be fetched or normalized."""


def _to_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if isfinite(parsed) else None


def _to_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _format_date(value: date | datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _build_stock_url(
    ticker: str,
    source: str,
    base_url: str,
    start_date: date | datetime | str | None,
    end_date: date | datetime | str | None,
) -> str:
    if source not in SOURCE_ENDPOINTS:
        valid_sources = ", ".join(sorted(SOURCE_ENDPOINTS))
        raise ValueError(
            f"Unsupported Adanos source '{source}'. Expected one of: {valid_sources}"
        )

    ticker_path = quote(ticker.upper(), safe="")
    path = SOURCE_ENDPOINTS[source].format(ticker=ticker_path)
    params = {
        key: value
        for key, value in {
            "from": _format_date(start_date),
            "to": _format_date(end_date),
        }.items()
        if value
    }
    query = f"?{urlencode(params)}" if params else ""
    return f"{base_url.rstrip('/')}{path}{query}"


def normalize_adanos_stock_payload(
    ticker: str, payload: dict[str, Any], source: str = "reddit"
) -> dict[str, Any]:
    """Convert one Adanos stock sentiment response into AmpyFin feature columns."""

    if any(payload.get(key) for key in ("error", "errors", "detail", "message")):
        raise AdanosSentimentError(f"Adanos returned an error payload for {ticker}")

    symbol = _clean_text(payload.get("ticker") or payload.get("symbol") or ticker)
    if symbol is None:
        raise AdanosSentimentError("Adanos payload must include a ticker or symbol")

    mentions = _to_int(payload.get("mentions"))
    if mentions is None:
        mentions = _to_int(payload.get("trade_count"))

    return {
        "Ticker": symbol.upper(),
        "adanos_source": source,
        "adanos_sentiment_score": _to_float(payload.get("sentiment_score")),
        "adanos_buzz_score": _to_float(payload.get("buzz_score")),
        "adanos_bullish_pct": _to_float(payload.get("bullish_pct")),
        "adanos_bearish_pct": _to_float(payload.get("bearish_pct")),
        "adanos_mentions": mentions,
        "adanos_trend": _clean_text(payload.get("trend")),
    }


def adanos_features_from_payloads(
    payloads: dict[str, dict[str, Any]],
    source: str = "reddit",
) -> pd.DataFrame:
    """Build an AmpyFin-ready feature DataFrame from pre-fetched Adanos payloads."""

    rows = [
        normalize_adanos_stock_payload(ticker, payload, source=source)
        for ticker, payload in payloads.items()
    ]
    return pd.DataFrame(rows, columns=FEATURE_COLUMNS)


def fetch_adanos_features(
    tickers: list[str],
    api_key: str | None = None,
    source: str = "reddit",
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    base_url: str = ADANOS_BASE_URL,
    timeout: int = 10,
) -> pd.DataFrame:
    """Fetch Adanos stock sentiment features for a ticker list.

    Args:
        tickers: Stock ticker symbols to enrich.
        api_key: Optional Adanos API key. Falls back to the ``ADANOS_API_KEY``
            environment variable.
        source: One of ``reddit``, ``x``, ``news``, or ``polymarket``.
        start_date: Optional API ``from`` date.
        end_date: Optional API ``to`` date.
        base_url: Adanos API base URL, injectable for tests.
        timeout: Request timeout in seconds.

    Returns:
        DataFrame keyed by ``Ticker`` with ``adanos_*`` feature columns.
    """

    resolved_api_key = api_key or os.getenv(ADANOS_API_KEY_ENV)
    if not resolved_api_key:
        raise ValueError(
            f"Set {ADANOS_API_KEY_ENV} or pass api_key to fetch Adanos sentiment features"
        )

    payloads: dict[str, dict[str, Any]] = {}
    for ticker in tickers:
        url = _build_stock_url(ticker, source, base_url, start_date, end_date)
        request = Request(
            url, headers={"X-API-Key": resolved_api_key, "Accept": "application/json"}
        )
        try:
            with urlopen(request, timeout=timeout) as response:
                payloads[ticker] = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise AdanosSentimentError(
                f"Adanos request failed for {ticker}: HTTP {exc.code}"
            ) from exc
        except URLError as exc:
            raise AdanosSentimentError(
                f"Adanos request failed for {ticker}: {exc.reason}"
            ) from exc
        except json.JSONDecodeError as exc:
            raise AdanosSentimentError(
                f"Adanos returned invalid JSON for {ticker}"
            ) from exc

    return adanos_features_from_payloads(payloads, source=source)
