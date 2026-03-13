import os
import json
import time
import datetime
from typing import Dict, Any, List, Optional

import requests
import yfinance as yf

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
NEWS_API_KEY = os.getenv("NEWSAPI_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")

OUTPUT_DIR = "output"


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def pct_change(current, previous):
    if previous == 0:
        return 0
    return round(((current / previous) - 1) * 100, 2)


def get_weekly_return(symbol):

    hist = yf.Ticker(symbol).history(period="6mo", interval="1wk")

    closes = hist["Close"].dropna()

    latest = float(closes.iloc[-1])
    prev = float(closes.iloc[-2])

    return pct_change(latest, prev)


def get_latest_close(symbol):

    hist = yf.Ticker(symbol).history(period="1mo", interval="1d")

    closes = hist["Close"].dropna()

    return round(float(closes.iloc[-1]), 2)


def fred(series):

    if not FRED_API_KEY:
        return None

    url = "https://api.stlouisfed.org/fred/series/observations"

    params = {
        "series_id": series,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 2,
    }

    r = requests.get(url, params=params)
    data = r.json()

    obs = data.get("observations", [])

    if len(obs) < 2:
        return None

    latest = float(obs[0]["value"])
    prev = float(obs[1]["value"])

    return {
        "latest": latest,
        "change": round(latest - prev, 2)
    }


def build_market_snapshot():

    sectors = {
        "technology": get_weekly_return("XLK"),
        "financials": get_weekly_return("XLF"),
        "health_care": get_weekly_return("XLV"),
        "energy": get_weekly_return("XLE"),
        "industrials": get_weekly_return("XLI"),
        "consumer_discretionary": get_weekly_return("XLY"),
        "utilities": get_weekly_return("XLU"),
        "real_estate": get_weekly_return("XLRE")
    }

    snapshot = {

        "equities": {

            "sp500": get_weekly_return("SPY"),
            "nasdaq": get_weekly_return("QQQ"),
            "dow": get_weekly_return("DIA"),
            "russell2000": get_weekly_return("IWM")

        },

        "rates": {

            "two_year": fred("DGS2"),
            "ten_year": fred("DGS10")

        },

        "volatility": {

            "vix": get_latest_close("^VIX")

        },

        "commodities": {

            "oil": get_weekly_return("USO"),
            "gold": get_weekly_return("GLD")

        },

        "sectors": sectors

    }

    return snapshot


def build_macro_snapshot():

    return {

        "inflation": fred("CPIAUCSL"),
        "unemployment": fred("UNRATE"),
        "payrolls": fred("PAYEMS"),
        "retail_sales": fred("RSAFS"),
        "fed_funds": fred("FEDFUNDS")

    }


def get_master_prompt():

    return """
You are a Chief Market Strategist writing a premium weekly market commentary product for financial advisors.

Your job is to transform structured market data into a professional market briefing advisors can use with clients.

Write with authority, clarity, and calm.

Avoid hype or sensational predictions.

Never recommend specific trades.

Always structure your reasoning as:

Observation
Interpretation
Implication

Observation
What happened in markets.

Interpretation
Why it likely happened.

Implication
What it may mean for investors.

Focus on cross-asset relationships:

Equities and interest rates
Sector leadership
Volatility signals
Macro data
Federal Reserve policy
Commodity moves

If signals conflict, explain the tension.

Write in a tone comparable to institutional research from large asset managers.

Return JSON only with the following structure:

{
  "week_ending": "",
  "market_dashboard": {
    "summary": "",
    "top_sectors": [],
    "bottom_sectors": []
  },
  "market_summary": "",
  "what_drove_markets": "",
  "under_the_surface": "",
  "macro_update": {
    "inflation": "",
    "labor_market": "",
    "growth": "",
    "federal_reserve": ""
  },
  "investor_implications": "",
  "advisor_talking_points": [],
  "client_email": "",
  "linkedin_post": "",
  "client_faq": [],
  "advisor_internal_notes": "",
  "risk_watch": "",
  "bottom_line": ""
}
"""


def call_openai(payload):

    url = "https://api.openai.com/v1/responses"

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }

    body = {

        "model": "gpt-5",

        "input": [

            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": get_master_prompt()
                    }
                ]
            },

            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(payload)
                    }
                ]
            }

        ]
    }

    r = requests.post(url, headers=headers, json=body)

    data = r.json()

    text = data.get("output_text")

    return json.loads(text)


def fallback_commentary(payload):

    return {

        "week_ending": payload["week_ending"],

        "market_dashboard": {

            "summary": "Markets reflected shifting rate expectations and mixed macro signals.",

            "top_sectors": [],
            "bottom_sectors": []

        },

        "market_summary": "Markets responded to changing expectations around interest rates and economic data.",

        "what_drove_markets": "Interest rates and macroeconomic data likely played the dominant role in market movements.",

        "under_the_surface": "Sector leadership and volatility patterns provide additional context for the week's market tone.",

        "macro_update": {

            "inflation": "Inflation remains central to market interpretation.",

            "labor_market": "Labor market conditions continue to influence growth expectations.",

            "growth": "Growth indicators remain mixed but resilient.",

            "federal_reserve": "Policy expectations continue to shape cross-asset pricing."

        },

        "investor_implications": "Short-term volatility should be viewed within the context of long-term investment discipline.",

        "advisor_talking_points": [

            "Short-term market moves often reflect shifting expectations rather than structural changes.",
            "Interest rates remain a key driver of asset pricing.",
            "Diversification remains important during periods of volatility."

        ],

        "client_email": "Markets moved this week as investors reacted to changing interest rate expectations and macroeconomic signals.",

        "linkedin_post": "Markets continue to respond to macro data and rate expectations.",

        "client_faq": [],

        "advisor_internal_notes": "Advisors should frame recent volatility in context of long-term strategy.",

        "risk_watch": "Watch interest rates and inflation signals.",

        "bottom_line": "Maintaining perspective remains critical for long-term investors."

    }


def build_payload():

    today = datetime.date.today()

    return {

        "week_ending": today.strftime("%Y-%m-%d"),

        "market_snapshot": build_market_snapshot(),

        "macro_snapshot": build_macro_snapshot()

    }


def main():

    ensure_output_dir()

    payload = build_payload()

    try:

        commentary = call_openai(payload)

    except Exception as e:

        print("OpenAI failed, using fallback")

        commentary = fallback_commentary(payload)

    path = os.path.join(OUTPUT_DIR, f"commentary_{payload['week_ending']}.json")

    with open(path, "w") as f:

        json.dump(commentary, f, indent=2)

    print("Commentary written to", path)


if __name__ == "__main__":

    main()
