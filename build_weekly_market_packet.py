import os
import json
import requests
import datetime
import pandas as pd
import yfinance as yf

from typing import Dict, Any
from dateutil import parser as date_parser

NEWS_API_KEY = os.getenv("NEWSAPI_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")


def get_weekly_return_pct_yf(symbol: str) -> float:
    hist = yf.Ticker(symbol).history(period="3mo", interval="1wk", auto_adjust=False)

    if hist is None or hist.empty:
        raise RuntimeError(f"Yahoo Finance returned no data for {symbol}")

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
        "limit": 10
    }

    response = requests.get(url, params=params)
    data = response.json()

    observations = data["observations"]

    latest = None
    previous = None

    for obs in observations:
        if obs["value"] != ".":
            if latest is None:
                latest = float(obs["value"])
            else:
                previous = float(obs["value"])
                break

    if latest is None or previous is None:
        raise RuntimeError("Could not parse US10Y data from FRED")

    change_bps = round((latest - previous) * 100, 1)

    return latest, change_bps


def fetch_news(query: str) -> str:

    url = "https://newsapi.org/v2/everything"

    params = {
        "q": query,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 1,
        "apiKey": NEWS_API_KEY
    }

    r = requests.get(url, params=params)
    data = r.json()

    if "articles" not in data or len(data["articles"]) == 0:
        return ""

    return data["articles"][0]["title"]


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

    row = {
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
        "source_notes": "Yahoo Finance, FRED, NewsAPI"
    }

    return row


def build_prompt_payload(row: Dict[str, Any], advisor_name: str = "", firm_name: str = "") -> Dict[str, Any]:

    return {
        "week_ending": row["week_ending"],
        "advisor_name": advisor_name,
        "firm_name": firm_name,
        "market_data": {
            "sp500_weekly_return_pct": row["sp500_weekly_return_pct"],
            "nasdaq_weekly_return_pct": row["nasdaq_weekly_return_pct"],
            "dow_weekly_return_pct": row["dow_weekly_return_pct"],
            "us10y_yield_end_pct": row["us10y_yield_end_pct"],
            "us10y_yield_change_bps": row["us10y_yield_change_bps"],
            "oil_weekly_return_pct": row["oil_weekly_return_pct"],
            "gold_weekly_return_pct": row["gold_weekly_return_pct"]
        },
        "macro_headlines": {
            "fed": row["fed_headline"],
            "inflation": row["inflation_headline"],
            "jobs": row["jobs_headline"],
            "geopolitics": row["geopolitics_headline"],
            "corporate": row["corporate_headline"]
        }
    }


if __name__ == "__main__":

    row = build_weekly_row()

    with open("weekly_market_data.json", "w") as f:
        json.dump(row, f, indent=2)

    print("Weekly market data built successfully.")
