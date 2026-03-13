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
WEEKLY_DATA_FILENAME = "weekly_market_data.json"


def ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None or value == "" or value == ".":
            return default
        return float(value)
    except Exception:
        return default


def pct_change(current: float, previous: float) -> float:
    if previous == 0:
        return 0.0
    return round(((current / previous) - 1.0) * 100.0, 2)


def fetch_history_with_retry(
    symbol: str,
    period: str = "6mo",
    interval: str = "1wk",
    retries: int = 3,
):
    last_error = None
    for attempt in range(retries):
        try:
            hist = yf.Ticker(symbol).history(
                period=period,
                interval=interval,
                auto_adjust=False,
            )
            if hist is not None and not hist.empty:
                return hist
        except Exception as e:
            last_error = e
        time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"Yahoo Finance failed for {symbol}: {last_error}")


def get_weekly_return_pct_yf(symbol: str) -> float:
    hist = fetch_history_with_retry(symbol, period="6mo", interval="1wk")

    if "Close" not in hist.columns:
        raise RuntimeError(f"Yahoo Finance did not return Close data for {symbol}")

    closes = hist["Close"].dropna()

    if len(closes) < 2:
        raise RuntimeError(f"Not enough weekly data returned for {symbol}")

    latest = float(closes.iloc[-1])
    previous = float(closes.iloc[-2])

    return pct_change(latest, previous)


def get_latest_close_yf(symbol: str) -> Optional[float]:
    try:
        hist = fetch_history_with_retry(symbol, period="1mo", interval="1d")
        closes = hist["Close"].dropna()
        if len(closes) == 0:
            return None
        return round(float(closes.iloc[-1]), 2)
    except Exception:
        return None


def fred_observations(series_id: str, limit: int = 12) -> List[Dict[str, Any]]:
    if not FRED_API_KEY:
        return []

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": limit,
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("observations", [])


def latest_two_valid_fred_values(series_id: str) -> List[float]:
    values: List[float] = []
    for obs in fred_observations(series_id, limit=20):
        v = safe_float(obs.get("value"))
        if v is not None:
            values.append(v)
        if len(values) >= 2:
            break
    return values


def get_fred_latest_and_change(series_id: str, unit: str = "level") -> Dict[str, Optional[float]]:
    vals = latest_two_valid_fred_values(series_id)

    if len(vals) < 2:
        return {"latest": None, "change": None}

    latest = vals[0]
    previous = vals[1]

    if unit == "bps":
        change = round((latest - previous) * 100.0, 1)
    else:
        change = round(latest - previous, 2)

    return {"latest": latest, "change": change}


def fetch_news(query: str, page_size: int = 3) -> List[str]:
    if not NEWS_API_KEY:
        return []

    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": page_size,
        "apiKey": NEWS_API_KEY,
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        articles = data.get("articles", [])
        titles = [a.get("title", "").strip() for a in articles if a.get("title")]
        return titles[:page_size]
    except Exception:
        return []


def latest_headline_or_blank(query: str) -> str:
    items = fetch_news(query, page_size=1)
    return items[0] if items else ""


def build_market_snapshot() -> Dict[str, Any]:
    sector_map = {
        "technology": "XLK",
        "financials": "XLF",
        "health_care": "XLV",
        "energy": "XLE",
        "industrials": "XLI",
        "consumer_discretionary": "XLY",
        "utilities": "XLU",
        "real_estate": "XLRE",
        "consumer_staples": "XLP",
        "materials": "XLB",
        "communication_services": "XLC",
    }

    sectors = {}
    for name, ticker in sector_map.items():
        try:
            sectors[name] = get_weekly_return_pct_yf(ticker)
        except Exception:
            sectors[name] = None

    two_year = get_fred_latest_and_change("DGS2", unit="bps")
    ten_year = get_fred_latest_and_change("DGS10", unit="bps")

    dashboard = {
        "equities": {
            "sp500_weekly_return_pct": get_weekly_return_pct_yf("SPY"),
            "nasdaq_weekly_return_pct": get_weekly_return_pct_yf("QQQ"),
            "dow_weekly_return_pct": get_weekly_return_pct_yf("DIA"),
            "russell2000_weekly_return_pct": get_weekly_return_pct_yf("IWM"),
            "msci_eafe_weekly_return_pct": get_weekly_return_pct_yf("EFA"),
            "msci_em_weekly_return_pct": get_weekly_return_pct_yf("EEM"),
        },
        "style_factor": {
            "large_growth_weekly_return_pct": get_weekly_return_pct_yf("IWF"),
            "large_value_weekly_return_pct": get_weekly_return_pct_yf("IWD"),
            "small_cap_weekly_return_pct": get_weekly_return_pct_yf("IWM"),
            "dividend_weekly_return_pct": get_weekly_return_pct_yf("VIG"),
            "momentum_weekly_return_pct": get_weekly_return_pct_yf("MTUM"),
        },
        "sectors": sectors,
        "rates": {
            "two_year_yield": two_year["latest"],
            "ten_year_yield": ten_year["latest"],
            "two_year_change_bps": two_year["change"],
            "ten_year_change_bps": ten_year["change"],
            "two_ten_spread": round(ten_year["latest"] - two_year["latest"], 2)
            if two_year["latest"] is not None and ten_year["latest"] is not None
            else None,
        },
        "fixed_income": {
            "agg_bond_return_pct": get_weekly_return_pct_yf("AGG"),
            "investment_grade_return_pct": get_weekly_return_pct_yf("LQD"),
            "high_yield_return_pct": get_weekly_return_pct_yf("HYG"),
            "tips_return_pct": get_weekly_return_pct_yf("TIP"),
        },
        "commodities": {
            "oil_return_pct": get_weekly_return_pct_yf("USO"),
            "gold_return_pct": get_weekly_return_pct_yf("GLD"),
        },
        "currency": {
            "dollar_index_proxy_return_pct": get_weekly_return_pct_yf("UUP"),
        },
        "volatility": {
            "vix_level": get_latest_close_yf("^VIX"),
            "vix_weekly_change_pct": get_weekly_return_pct_yf("^VIX"),
        },
    }

    return dashboard


def build_macro_snapshot() -> Dict[str, Any]:
    cpi = get_fred_latest_and_change("CPIAUCSL")
    unemployment = get_fred_latest_and_change("UNRATE")
    payrolls = get_fred_latest_and_change("PAYEMS")
    retail_sales = get_fred_latest_and_change("RSAFS")
    industrial_production = get_fred_latest_and_change("INDPRO")
    fedfunds = get_fred_latest_and_change("FEDFUNDS")

    return {
        "inflation": {
            "cpi_index_latest": cpi["latest"],
            "cpi_index_change": cpi["change"],
            "headline_context": latest_headline_or_blank("CPI inflation United States"),
        },
        "labor_market": {
            "unemployment_rate_latest": unemployment["latest"],
            "unemployment_rate_change": unemployment["change"],
            "nonfarm_payrolls_level_latest": payrolls["latest"],
            "nonfarm_payrolls_change": payrolls["change"],
            "headline_context": latest_headline_or_blank("US jobs report unemployment payrolls"),
        },
        "growth": {
            "retail_sales_latest": retail_sales["latest"],
            "retail_sales_change": retail_sales["change"],
            "industrial_production_latest": industrial_production["latest"],
            "industrial_production_change": industrial_production["change"],
            "headline_context": latest_headline_or_blank("US retail sales economy growth"),
        },
        "federal_reserve": {
            "fed_funds_latest": fedfunds["latest"],
            "fed_funds_change": fedfunds["change"],
            "headline_context": latest_headline_or_blank("Federal Reserve interest rates policy"),
        },
    }


def rank_sector_moves(sectors: Dict[str, Optional[float]]) -> Dict[str, List[Dict[str, Any]]]:
    valid = [{"sector": k, "return_pct": v} for k, v in sectors.items() if v is not None]
    sorted_valid = sorted(valid, key=lambda x: x["return_pct"], reverse=True)
    return {
        "top_sectors": sorted_valid[:3],
        "bottom_sectors": sorted_valid[-3:],
    }


def build_flow_snapshot() -> Dict[str, Any]:
    return {
        "top_etf_inflows": [],
        "top_etf_outflows": [],
        "flow_commentary": "ETF flow integration not yet connected. Add a dedicated flow source for a stronger institutional layer.",
    }


def likely_client_questions(market_snapshot: Dict[str, Any], macro_snapshot: Dict[str, Any]) -> List[str]:
    questions = []

    vix = market_snapshot.get("volatility", {}).get("vix_level")
    ten_year_change = market_snapshot.get("rates", {}).get("ten_year_change_bps")
    sp500 = market_snapshot.get("equities", {}).get("sp500_weekly_return_pct")
    unemployment = macro_snapshot.get("labor_market", {}).get("unemployment_rate_latest")

    if sp500 is not None and abs(sp500) >= 1.5:
        questions.append("Should I be worried about this week's stock market move?")
    if ten_year_change is not None and abs(ten_year_change) >= 10:
        questions.append("Why did bond yields move so much this week, and what does that mean for my portfolio?")
    if vix is not None and vix >= 20:
        questions.append("Does higher volatility mean we should reduce risk right now?")
    if unemployment is not None:
        questions.append("What does the latest labor market data say about the economy?")

    questions.append("Does any of this change our long-term plan?")
    questions.append("Should we be doing anything different right now?")

    deduped = []
    for q in questions:
        if q not in deduped:
            deduped.append(q)

    return deduped[:6]


def build_input_payload() -> Dict[str, Any]:
    today = datetime.date.today()
    week_ending = today.strftime("%Y-%m-%d")

    market_snapshot = build_market_snapshot()
    macro_snapshot = build_macro_snapshot()
    flow_snapshot = build_flow_snapshot()
    sector_rank = rank_sector_moves(market_snapshot["sectors"])

    headlines = {
        "fed": fetch_news("Federal Reserve interest rates policy", page_size=3),
        "inflation": fetch_news("CPI inflation PPI prices United States", page_size=3),
        "jobs": fetch_news("US jobs report unemployment payrolls", page_size=3),
        "growth": fetch_news("US economy retail sales manufacturing services", page_size=3),
        "geopolitics": fetch_news("geopolitics markets stocks bonds oil", page_size=3),
        "corporate": fetch_news("corporate earnings stock market outlook", page_size=3),
    }

    return {
        "week_ending": week_ending,
        "generated_at_utc": datetime.datetime.utcnow().isoformat(),
        "market_snapshot": market_snapshot,
        "sector_rank": sector_rank,
        "macro_snapshot": macro_snapshot,
        "flow_snapshot": flow_snapshot,
        "headlines": headlines,
        "likely_client_questions": likely_client_questions(market_snapshot, macro_snapshot),
        "house_view_settings": {
            "tone": "balanced",
            "audience": "financial advisors and long-term investors",
            "writing_style": "institutional, clear, calm, useful",
        },
        "source_notes": [
            "Yahoo Finance",
            "FRED",
            "NewsAPI",
        ],
    }


def get_master_prompt() -> str:
    return """
You are a Chief Market Strategist writing a premium weekly market note for financial advisors.

Your job is to turn structured market and macro data into a high-value weekly commentary product that advisors can actually use with clients.

Audience:
- financial advisors
- high-net-worth clients
- retirees
- long-term investors

Tone:
- professional
- calm
- analytical
- useful
- client-friendly
- institutional quality

Do not:
- make personalized investment recommendations
- recommend trades
- guarantee outcomes
- sound sensational
- use generic filler language

Always answer:
1. What happened this week?
2. Why did it happen?
3. What changed beneath the surface?
4. What does it mean for investors?
5. What should advisors say to clients?

Write in plain English, but keep the analysis sophisticated.

Use the supplied numbers and headlines.
If a specific data point is missing, do not invent it.
Be specific.
Interpret cross-asset relationships.
Highlight sector leadership, rates, volatility, and macro context.
Use concise paragraphs.
Make the commentary feel premium and useful.

Return ONLY valid JSON matching this schema exactly:

{
  "week_ending": "YYYY-MM-DD",
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
  "client_faq": [
    {
      "question": "",
      "answer": ""
    }
  ],
  "advisor_internal_notes": "",
  "risk_watch": "",
  "bottom_line": ""
}

Requirements:
- market_summary: 120 to 220 words
- what_drove_markets: 120 to 220 words
- under_the_surface: 120 to 220 words
- each macro_update field: 60 to 140 words
- investor_implications: 120 to 220 words
- advisor_talking_points: 5 to 8 bullets
- client_email: 250 to 450 words
- linkedin_post: 100 to 180 words
- client_faq: 4 to 6 items
- advisor_internal_notes: 120 to 220 words
- risk_watch: 80 to 160 words
- bottom_line: 60 to 120 words

The output should feel like a premium advisor communication product.
""".strip()


def call_openai_for_commentary(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        raise RuntimeError("Missing OPENAI_API_KEY")

    url = "https://api.openai.com/v1/responses"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    body = {
        "model": "gpt-5",
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": get_master_prompt(),
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(payload, indent=2),
                    }
                ],
            },
        ],
    }

    r = requests.post(url, headers=headers, json=body, timeout=120)
    r.raise_for_status()
    data = r.json()

    text = data.get("output_text", "").strip()
    if not text:
        raise RuntimeError("OpenAI response did not contain output_text")

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start:end + 1])
        raise RuntimeError("OpenAI returned non-JSON output")


def build_fallback_commentary(payload: Dict[str, Any]) -> Dict[str, Any]:
    week_ending = payload["week_ending"]
    eq = payload["market_snapshot"]["equities"]
    rates = payload["market_snapshot"]["rates"]
    sectors = payload["sector_rank"]

    return {
        "week_ending": week_ending,
        "market_dashboard": {
            "summary": (
                f"S&P 500 {eq.get('sp500_weekly_return_pct')}%, "
                f"Nasdaq {eq.get('nasdaq_weekly_return_pct')}%, "
                f"Dow {eq.get('dow_weekly_return_pct')}%, "
                f"Russell 2000 {eq.get('russell2000_weekly_return_pct')}%. "
                f"10-year Treasury yield {rates.get('ten_year_yield')}%, "
                f"weekly change {rates.get('ten_year_change_bps')} bps."
            ),
            "top_sectors": sectors.get("top_sectors", []),
            "bottom_sectors": sectors.get("bottom_sectors", []),
        },
        "market_summary": "Markets responded to a mix of rate expectations, economic data, and sector rotation. Equities, rates, and defensive assets reflected shifting expectations around growth, inflation, and Federal Reserve policy. This version is a fallback summary and should be replaced by the AI-generated premium note when the OpenAI step succeeds.",
        "what_drove_markets": "The primary drivers this week were changes in rate expectations, incoming macro data, and evolving investor sentiment. Equity performance was shaped not only by headline market direction but also by leadership beneath the surface, including sector rotation and relative performance across styles and asset classes.",
        "under_the_surface": "Looking below the headline indexes, market leadership and sector rotation offered important clues about investor positioning. Rates, volatility, and defensive asset behavior can often say as much as headline equity returns. Advisors should use this layer to explain that market moves are rarely just about one number or one headline.",
        "macro_update": {
            "inflation": "Inflation remains central to market interpretation because it influences rate expectations and valuation multiples.",
            "labor_market": "Labor market data continues to shape views on economic resilience and the path of policy.",
            "growth": "Growth indicators remain important because they frame the earnings outlook and recession debate.",
            "federal_reserve": "Federal Reserve policy expectations remain a dominant variable across stocks and bonds.",
        },
        "investor_implications": "For long-term investors, the key takeaway is that short-term cross-asset moves do not automatically require portfolio changes. The more important issue is whether the broader balance of growth, inflation, and policy is materially shifting. Advisors can add value by helping clients distinguish between noise and genuine changes in the investment backdrop.",
        "advisor_talking_points": [
            "Short-term market moves reflected changing rate expectations and incoming macro data.",
            "Headline index performance only tells part of the story; sector leadership also mattered.",
            "Treasury yields remain an important signal for both stock valuations and bond returns.",
            "Clients may ask whether volatility changes the long-term plan. In most cases, it does not.",
            "This is a good week to reinforce discipline and context rather than react to headlines.",
        ],
        "client_email": (
            f"What You Need To Know About the Markets\n\n"
            f"For the week ending {week_ending}, markets reflected a mix of economic data, rate expectations, and sector rotation. "
            f"The S&P 500 returned {eq.get('sp500_weekly_return_pct')}% while the Nasdaq returned {eq.get('nasdaq_weekly_return_pct')}% "
            f"and the Dow returned {eq.get('dow_weekly_return_pct')}%.\n\n"
            f"The 10-year Treasury yield ended the week at {rates.get('ten_year_yield')}%, a move of {rates.get('ten_year_change_bps')} basis points. "
            f"These changes matter because interest rates influence both stock valuations and bond performance.\n\n"
            f"As always, short-term market moves can feel significant in the moment, but they do not necessarily change the long-term investment picture. "
            f"Markets constantly adjust to new information. The more important issue is whether the broader outlook for inflation, growth, and policy is meaningfully changing.\n\n"
            f"For long-term investors, discipline remains critical. Staying focused on your goals and maintaining a thoughtful plan is usually more important than reacting to any single week of market activity."
        ),
        "linkedin_post": (
            f"What You Need To Know About the Markets for the week ending {week_ending}: "
            f"S&P 500 {eq.get('sp500_weekly_return_pct')}%, Nasdaq {eq.get('nasdaq_weekly_return_pct')}%, "
            f"Dow {eq.get('dow_weekly_return_pct')}%. Treasury yields, sector rotation, and macro data continued to shape "
            f"the market conversation. For advisors and clients alike, the bigger message is that short-term moves should "
            f"be viewed in the context of long-term goals and a disciplined investment process."
        ),
        "client_faq": [
            {
                "question": "Should I be worried about this week's market move?",
                "answer": "Short-term market moves are normal. The more important question is whether the long-term economic and policy backdrop has materially changed.",
            },
            {
                "question": "Why do interest rates matter so much?",
                "answer": "Interest rates affect borrowing costs, bond prices, and how investors value future corporate earnings.",
            },
            {
                "question": "Does this change our long-term plan?",
                "answer": "In most cases, no. Long-term plans are built to account for normal volatility and changing headlines.",
            },
            {
                "question": "Why are different sectors moving differently?",
                "answer": "Different sectors react differently to rates, earnings expectations, economic growth, and investor sentiment.",
            },
        ],
        "advisor_internal_notes": "This week is a reminder that advisors need language that connects market data to client concerns. Focus conversations on context, diversification, rates, and the difference between short-term volatility and long-term planning.",
        "risk_watch": "Continue monitoring rate sensitivity, labor market data, inflation trends, and any widening disconnect between headline index performance and underlying breadth or leadership.",
        "bottom_line": "The market backdrop continues to evolve, but the core message for investors remains the same: maintain perspective, stay diversified, and avoid overreacting to short-term moves.",
    }


def write_outputs(input_payload: Dict[str, Any], commentary: Dict[str, Any]) -> None:
    ensure_output_dir()

    with open(WEEKLY_DATA_FILENAME, "w", encoding="utf-8") as f:
        json.dump(input_payload, f, indent=2)

    week_ending = input_payload["week_ending"]
    commentary_path = os.path.join(OUTPUT_DIR, f"commentary_{week_ending}.json")

    with open(commentary_path, "w", encoding="utf-8") as f:
        json.dump(commentary, f, indent=2)

    print(f"Commentary written to {commentary_path}")
    print("Weekly market data and commentary built successfully.")


if __name__ == "__main__":
    payload = build_input_payload()

    try:
        commentary = call_openai_for_commentary(payload)
    except Exception as e:
        print(f"OpenAI generation failed, using fallback commentary. Reason: {e}")
        commentary = build_fallback_commentary(payload)

    write_outputs(payload, commentary)



