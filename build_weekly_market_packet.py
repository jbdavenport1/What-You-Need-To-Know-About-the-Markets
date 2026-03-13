import os
import json
import requests
import datetime
from typing import Dict, Any

import yfinance as yf

NEWS_API_KEY = os.getenv("NEWSAPI_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")


OUTPUT_DIR = "output"


def get_weekly_return_pct_yf(symbol: str) -> float:
    try:
        hist = yf.Ticker(symbol).history(period="3mo", interval="1wk", auto_adjust=False)
    except Exception as e:
        raise RuntimeError(f"Yahoo Finance failed for {symbol}: {e}")

    if hist is None or hist.empty:
        raise RuntimeError(f"Yahoo Finance returned no data for {symbol}")

    if "Close" not in hist.columns:
        raise RuntimeError(f"Yahoo Finance did not return Close data for {symbol}")

    closes = hist["Close"].dropna()

    if len(closes) < 2:
        raise RuntimeError(f"Not enough weekly data returned for {symbol}")

    latest = float(closes.iloc[-1])
    previous = float(closes.iloc[-2])

    return round(((latest / previous) - 1) * 100, 2)


def fetch_us10y_from_fred():
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": "DGS10",
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 10,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    observations = data.get("observations", [])

    latest = None
    previous = None

    for obs in observations:
        value = obs.get("value")
        if value and value != ".":
            if latest is None:
                latest = float(value)
            else:
                previous = float(value)
                break

    if latest is None or previous is None:
        raise RuntimeError("Could not parse US10Y data from FRED")

    change_bps = round((latest - previous) * 100, 1)
    return latest, change_bps


def fetch_news(query: str) -> str:
    if not NEWS_API_KEY:
        return ""

    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 1,
        "apiKey": NEWS_API_KEY,
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        articles = data.get("articles", [])
        if not articles:
            return ""
        return articles[0].get("title", "")
    except Exception:
        return ""


def build_weekly_row() -> Dict[str, Any]:
    today = datetime.date.today()
    week_ending = today.strftime("%Y-%m-%d")

    sp500 = get_weekly_return_pct_yf("SPY")
    nasdaq = get_weekly_return_pct_yf("QQQ")
    dow = get_weekly_return_pct_yf("DIA")
    oil = get_weekly_return_pct_yf("USO")
    gold = get_weekly_return_pct_yf("GLD")

    us10y_yield, us10y_change = fetch_us10y_from_fred()

    fed_news = fetch_news("Federal Reserve interest rates")
    inflation_news = fetch_news("inflation CPI")
    jobs_news = fetch_news("US jobs report unemployment")
    geopolitics_news = fetch_news("geopolitical markets")
    corporate_news = fetch_news("stock market corporate earnings")

    return {
        "week_ending": week_ending,
        "sp500_weekly_return_pct": sp500,
        "nasdaq_weekly_return_pct": nasdaq,
        "dow_weekly_return_pct": dow,
        "oil_weekly_return_pct": oil,
        "gold_weekly_return_pct": gold,
        "us10y_yield_end_pct": us10y_yield,
        "us10y_yield_change_bps": us10y_change,
        "fed_headline": fed_news,
        "inflation_headline": inflation_news,
        "jobs_headline": jobs_news,
        "geopolitics_headline": geopolitics_news,
        "corporate_headline": corporate_news,
        "source_notes": "Yahoo Finance, FRED, NewsAPI",
    }


def build_commentary(row: Dict[str, Any]) -> Dict[str, Any]:
    week_ending = row["week_ending"]

    client_email = f"""What You Need To Know About the Markets

For the week ending {week_ending}, markets delivered a mixed picture.

The S&P 500 moved {row['sp500_weekly_return_pct']} percent.
The Nasdaq moved {row['nasdaq_weekly_return_pct']} percent.
The Dow moved {row['dow_weekly_return_pct']} percent.

The 10-year Treasury yield ended the week at {row['us10y_yield_end_pct']} percent, a change of {row['us10y_yield_change_bps']} basis points.

Oil moved {row['oil_weekly_return_pct']} percent and gold moved {row['gold_weekly_return_pct']} percent.

Key themes investors watched included Federal Reserve developments, inflation data, labor market updates, and broader geopolitical headlines.

As always, short-term market moves reflect changing expectations. Long-term discipline remains important.
"""

    newsletter = f"""Markets were mixed for the week ending {week_ending}. The S&P 500 moved {row['sp500_weekly_return_pct']} percent, the Nasdaq moved {row['nasdaq_weekly_return_pct']} percent, and the Dow moved {row['dow_weekly_return_pct']} percent. Investors continued to monitor rates, inflation, jobs data, and global developments."""

    linkedin = f"""What You Need To Know About the Markets for the week ending {week_ending}: S&P 500 {row['sp500_weekly_return_pct']} percent, Nasdaq {row['nasdaq_weekly_return_pct']} percent, Dow {row['dow_weekly_return_pct']} percent. Markets continue to respond to rates, inflation, and economic data. Long-term discipline still matters."""

    talking_points = [
        f"S&P 500 weekly move: {row['sp500_weekly_return_pct']} percent",
        f"Nasdaq weekly move: {row['nasdaq_weekly_return_pct']} percent",
        f"Dow weekly move: {row['dow_weekly_return_pct']} percent",
        f"10-year Treasury yield: {row['us10y_yield_end_pct']} percent",
        "Key client topics this week are rates, inflation, and market volatility",
    ]

    return {
        "week_ending": week_ending,
        "subject": "What You Need To Know About the Markets",
        "client_email": client_email,
        "newsletter_version": newsletter,
        "linkedin_version": linkedin,
        "advisor_talking_points": talking_points,
        "market_data": row,
    }


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def write_outputs(row: Dict[str, Any], commentary: Dict[str, Any]):
    ensure_output_dir()

    with open("weekly_market_data.json", "w") as f:
        json.dump(row, f, indent=2)

    filename = f"commentary_{row['week_ending']}.json"
    path = os.path.join(OUTPUT_DIR, filename)

    with open(path, "w") as f:
        json.dump(commentary, f, indent=2)

    print(f"Commentary written to {path}")


if __name__ == "__main__":
    row = build_weekly_row()
    commentary = build_commentary(row)
    write_outputs(row, commentary)
    print("Weekly market data and commentary built successfully.")


