import json

import pandas as pd
import pytest

from utilities import adanos_sentiment as adanos


class DummyResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


def test_normalize_adanos_stock_payload():
    payload = {
        "ticker": "AAPL",
        "sentiment_score": "0.42",
        "buzz_score": 73.5,
        "bullish_pct": 61.2,
        "bearish_pct": 18.4,
        "mentions": "128",
        "trend": "rising",
    }

    result = adanos.normalize_adanos_stock_payload("AAPL", payload, source="reddit")

    assert result == {
        "Ticker": "AAPL",
        "adanos_source": "reddit",
        "adanos_sentiment_score": 0.42,
        "adanos_buzz_score": 73.5,
        "adanos_bullish_pct": 61.2,
        "adanos_bearish_pct": 18.4,
        "adanos_mentions": 128,
        "adanos_trend": "rising",
    }


def test_adanos_features_from_payloads_preserves_columns():
    payloads = {
        "AAPL": {"ticker": "AAPL", "sentiment_score": 0.2, "mentions": 10},
        "MSFT": {"ticker": "MSFT", "buzz_score": 66, "trend": "stable"},
    }

    result = adanos.adanos_features_from_payloads(payloads)

    assert list(result.columns) == adanos.FEATURE_COLUMNS
    assert result["Ticker"].tolist() == ["AAPL", "MSFT"]
    assert pd.isna(result.loc[1, "adanos_sentiment_score"])


def test_error_payload_is_rejected():
    with pytest.raises(adanos.AdanosSentimentError, match="error payload"):
        adanos.normalize_adanos_stock_payload("AAPL", {"detail": "Invalid API key"})


def test_fetch_adanos_features_uses_api_key_and_date_params(monkeypatch):
    requests = []

    def fake_urlopen(request, timeout):
        requests.append((request, timeout))
        return DummyResponse(
            {
                "ticker": "AAPL",
                "sentiment_score": 0.5,
                "buzz_score": 70,
                "mentions": 20,
            }
        )

    monkeypatch.setattr(adanos, "urlopen", fake_urlopen)

    result = adanos.fetch_adanos_features(
        ["AAPL"],
        api_key="sk_test",
        source="news",
        start_date="2026-06-01",
        end_date="2026-06-27",
        base_url="https://example.test",
        timeout=3,
    )

    request, timeout = requests[0]
    assert (
        request.full_url
        == "https://example.test/news/stocks/v1/stock/AAPL?from=2026-06-01&to=2026-06-27"
    )
    assert request.headers["X-api-key"] == "sk_test"
    assert timeout == 3
    assert result.loc[0, "adanos_buzz_score"] == 70


def test_fetch_adanos_features_requires_api_key(monkeypatch):
    monkeypatch.delenv(adanos.ADANOS_API_KEY_ENV, raising=False)

    with pytest.raises(ValueError, match=adanos.ADANOS_API_KEY_ENV):
        adanos.fetch_adanos_features(["AAPL"])
