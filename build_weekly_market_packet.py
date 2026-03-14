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
ROOT_WEEKLY_DATA_FILENAME = "weekly_market_data.json"


def ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value in [None, "", "."]:
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


def get_weekly_return_pct_yf(symbol: str) -> Optional[float]:
    try:
        hist = fetch_history_with_retry(symbol, period="6mo", interval="1wk")

        if "Close" not in hist.columns:
            return None

        closes = hist["Close"].dropna()

        if len(closes) < 2:
            return None

        latest = float(closes.iloc[-1])
        previous = float(closes.iloc[-2])

        return pct_change(latest, previous)
    except Exception:
        return None


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

    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get("observations", [])
    except Exception:
        return []


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
        return {"latest": None, "change": None, "previous": None}

    latest = vals[0]
    previous = vals[1]

    if unit == "bps":
        change = round((latest - previous) * 100.0, 1)
    else:
        change = round(latest - previous, 2)

    return {
        "latest": latest,
        "previous": previous,
        "change": change,
    }


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
        return [a.get("title", "").strip() for a in articles if a.get("title")]
    except Exception:
        return []


def latest_headline_or_blank(query: str) -> str:
    items = fetch_news(query, page_size=1)
    return items[0] if items else ""


def rank_sector_moves(sectors: Dict[str, Optional[float]]) -> Dict[str, List[Dict[str, Any]]]:
    valid = [{"sector": k, "return_pct": v} for k, v in sectors.items() if v is not None]
    sorted_valid = sorted(valid, key=lambda x: x["return_pct"], reverse=True)
    return {
        "top_sectors": sorted_valid[:3],
        "bottom_sectors": sorted_valid[-3:],
    }


def build_market_breadth(sectors: Dict[str, Optional[float]]) -> Dict[str, Any]:
    valid = {k: v for k, v in sectors.items() if v is not None}
    positive = [k for k, v in valid.items() if v > 0]
    negative = [k for k, v in valid.items() if v < 0]
    flat = [k for k, v in valid.items() if v == 0]

    total = len(valid)
    positive_pct = round((len(positive) / total) * 100.0, 1) if total > 0 else None

    return {
        "positive_sector_count": len(positive),
        "negative_sector_count": len(negative),
        "flat_sector_count": len(flat),
        "tracked_sector_count": total,
        "positive_sector_pct": positive_pct,
        "equal_weight_sp500_weekly_return_pct": get_weekly_return_pct_yf("RSP"),
        "cap_weight_sp500_weekly_return_pct": get_weekly_return_pct_yf("SPY"),
        "positive_sectors": positive,
        "negative_sectors": negative,
    }


def build_credit_snapshot() -> Dict[str, Any]:
    ig = get_fred_latest_and_change("BAMLC0A0CM", unit="level")
    hy = get_fred_latest_and_change("BAMLH0A0HYM2", unit="level")

    spread_gap = None
    if ig["latest"] is not None and hy["latest"] is not None:
        spread_gap = round(hy["latest"] - ig["latest"], 2)

    return {
        "investment_grade_oas": ig,
        "high_yield_oas": hy,
        "hy_minus_ig_spread_gap": spread_gap,
    }


def build_yield_curve_snapshot(two_year: Dict[str, Optional[float]], ten_year: Dict[str, Optional[float]]) -> Dict[str, Any]:
    current_spread = None
    previous_spread = None
    spread_change_bps = None

    if two_year["latest"] is not None and ten_year["latest"] is not None:
        current_spread = round(ten_year["latest"] - two_year["latest"], 2)

    if two_year["previous"] is not None and ten_year["previous"] is not None:
        previous_spread = round(ten_year["previous"] - two_year["previous"], 2)

    if current_spread is not None and previous_spread is not None:
        spread_change_bps = round((current_spread - previous_spread) * 100.0, 1)

    return {
        "current_2s10s_spread_pct": current_spread,
        "previous_2s10s_spread_pct": previous_spread,
        "weekly_change_bps": spread_change_bps,
        "curve_state": (
            "inverted" if current_spread is not None and current_spread < 0
            else "steepening_or_positive" if current_spread is not None
            else None
        ),
    }


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

    sectors: Dict[str, Optional[float]] = {}
    for name, ticker in sector_map.items():
        sectors[name] = get_weekly_return_pct_yf(ticker)

    two_year = get_fred_latest_and_change("DGS2", unit="bps")
    ten_year = get_fred_latest_and_change("DGS10", unit="bps")

    breadth = build_market_breadth(sectors)
    credit = build_credit_snapshot()
    yield_curve = build_yield_curve_snapshot(two_year, ten_year)

    return {
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
        "fixed_income": {
            "agg_bond_return_pct": get_weekly_return_pct_yf("AGG"),
            "investment_grade_return_pct": get_weekly_return_pct_yf("LQD"),
            "high_yield_return_pct": get_weekly_return_pct_yf("HYG"),
            "tips_return_pct": get_weekly_return_pct_yf("TIP"),
        },
        "rates": {
            "two_year_yield": two_year["latest"],
            "two_year_change_bps": two_year["change"],
            "ten_year_yield": ten_year["latest"],
            "ten_year_change_bps": ten_year["change"],
        },
        "yield_curve": yield_curve,
        "credit": credit,
        "breadth": breadth,
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
        "sectors": sectors,
        "sector_rank": rank_sector_moves(sectors),
    }


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


def likely_client_questions(payload: Dict[str, Any]) -> List[str]:
    market = payload["market_snapshot"]
    eq = market["equities"]
    rates = market["rates"]
    breadth = market["breadth"]
    credit = market["credit"]
    ycurve = market["yield_curve"]
    volatility = market["volatility"]

    questions: List[str] = []

    spx = eq.get("sp500_weekly_return_pct")
    ten_year_change = rates.get("ten_year_change_bps")
    hy_change = credit.get("high_yield_oas", {}).get("change")
    positive_sector_pct = breadth.get("positive_sector_pct")
    vix_level = volatility.get("vix_level")
    curve_state = ycurve.get("curve_state")

    if spx is not None and abs(spx) >= 1.25:
        questions.append("Should I be worried about this week's market move?")

    if ten_year_change is not None and abs(ten_year_change) >= 10:
        questions.append("Why did Treasury yields move so much this week, and what does that mean for my portfolio?")

    if hy_change is not None and hy_change > 0.15:
        questions.append("Is the market showing signs of real credit stress?")

    if positive_sector_pct is not None and positive_sector_pct < 40:
        questions.append("Why does the market feel weaker than the headline index return suggests?")

    if vix_level is not None and vix_level >= 20:
        questions.append("Does higher volatility mean we should reduce risk right now?")

    if curve_state == "inverted":
        questions.append("What does an inverted yield curve actually mean for investors?")

    questions.append("Does any of this change our long-term plan?")
    questions.append("Should we be doing anything different right now?")

    deduped: List[str] = []
    for q in questions:
        if q not in deduped:
            deduped.append(q)

    return deduped[:6]


def build_input_payload() -> Dict[str, Any]:
    today = datetime.date.today()
    week_ending = today.strftime("%Y-%m-%d")

    market_snapshot = build_market_snapshot()
    macro_snapshot = build_macro_snapshot()

    payload = {
        "week_ending": week_ending,
        "generated_at_utc": datetime.datetime.utcnow().isoformat(),
        "market_snapshot": market_snapshot,
        "macro_snapshot": macro_snapshot,
        "flow_snapshot": {
            "top_etf_inflows": [],
            "top_etf_outflows": [],
            "flow_commentary": "ETF flow integration not yet connected."
        },
        "headlines": {
            "fed": fetch_news("Federal Reserve interest rates policy", page_size=3),
            "inflation": fetch_news("CPI inflation PPI prices United States", page_size=3),
            "jobs": fetch_news("US jobs report unemployment payrolls", page_size=3),
            "growth": fetch_news("US economy retail sales manufacturing services", page_size=3),
            "geopolitics": fetch_news("geopolitics markets stocks bonds oil", page_size=3),
            "corporate": fetch_news("corporate earnings stock market outlook", page_size=3),
        },
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

    payload["likely_client_questions"] = likely_client_questions(payload)
    return payload


def get_master_prompt() -> str:
    return """
You are a Chief Market Strategist writing a premium weekly market commentary product for financial advisors.

You are not writing a news recap.
You are writing an advisor-grade market intelligence brief.

Your output must feel analytical, useful, specific, and premium.

Your commentary must consistently use this framework:

1. Observation
What happened in markets.

2. Interpretation
Why it likely happened based on cross-asset relationships.

3. Implication
What it may mean for investors and advisor conversations.

Use inference aggressively but responsibly.

Allowed inference language:
- suggests
- appears consistent with
- likely reflects
- may indicate
- points to
- worth monitoring
- does not yet confirm

Do not:
- recommend trades
- make personalized investment advice
- guarantee outcomes
- make sensational forecasts
- sound generic

Do not write shallow phrases like:
- markets were mixed
- investors reacted to data
- uncertainty remains

Be specific.

You must interpret relationships between:
- equities and Treasury yields
- equities and sector leadership
- equities and volatility
- equities and credit spreads
- growth vs value
- market breadth
- oil, gold, and inflation expectations
- macro data and Fed expectations
- the 2s10s curve and growth/policy expectations

If signals conflict, explain the tension.

Examples of premium inference:
- Equity weakness alongside stable credit may suggest valuation pressure more than systemic stress.
- Growth underperformance with higher yields is consistent with duration sensitivity.
- Narrow breadth beneath a stable headline index may suggest weakening internal participation.
- A steepening or re-inverting curve can reflect changing expectations around growth and policy.

Return ONLY valid JSON with exactly this structure:

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
- market_dashboard.summary: 100 to 170 words
- market_summary: 180 to 320 words
- what_drove_markets: 180 to 320 words
- under_the_surface: 180 to 320 words
- each macro_update field: 90 to 160 words
- investor_implications: 180 to 300 words
- advisor_talking_points: 6 to 8 bullets
- client_email: 350 to 600 words
- linkedin_post: 120 to 180 words
- client_faq: 4 to 6 items
- advisor_internal_notes: 180 to 280 words
- risk_watch: 120 to 220 words
- bottom_line: 90 to 140 words

Make this feel like a premium strategist note that an advisor would genuinely value.
""".strip()


def extract_response_text(data: Dict[str, Any]) -> str:
    if isinstance(data.get("output_text"), str) and data.get("output_text").strip():
        return data["output_text"].strip()

    output = data.get("output", [])
    collected: List[str] = []

    for item in output:
        for content in item.get("content", []):
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                collected.append(text)

    return "\n".join(collected).strip()


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

    text = extract_response_text(data)

    if not text:
        raise RuntimeError("OpenAI returned no text output")

    try:
        return json.loads(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start:end + 1])
        raise RuntimeError(f"Could not parse OpenAI JSON. Raw output starts with: {text[:800]}")


def build_fallback_commentary(payload: Dict[str, Any]) -> Dict[str, Any]:
    week_ending = payload["week_ending"]
    market = payload["market_snapshot"]
    eq = market["equities"]
    rates = market["rates"]
    fixed_income = market["fixed_income"]
    credit = market["credit"]
    breadth = market["breadth"]
    ycurve = market["yield_curve"]
    commodities = market["commodities"]
    volatility = market["volatility"]
    top = market["sector_rank"]["top_sectors"]
    bottom = market["sector_rank"]["bottom_sectors"]

    sp500 = eq.get("sp500_weekly_return_pct")
    nasdaq = eq.get("nasdaq_weekly_return_pct")
    dow = eq.get("dow_weekly_return_pct")
    russell = eq.get("russell2000_weekly_return_pct")
    growth = market["style_factor"].get("large_growth_weekly_return_pct")
    value = market["style_factor"].get("large_value_weekly_return_pct")

    ten_year = rates.get("ten_year_yield")
    ten_year_change = rates.get("ten_year_change_bps")
    two_year = rates.get("two_year_yield")
    curve = ycurve.get("current_2s10s_spread_pct")
    curve_change = ycurve.get("weekly_change_bps")

    hy_oas = credit.get("high_yield_oas", {}).get("latest")
    hy_oas_change = credit.get("high_yield_oas", {}).get("change")
    ig_oas = credit.get("investment_grade_oas", {}).get("latest")

    breadth_pct = breadth.get("positive_sector_pct")
    vix = volatility.get("vix_level")
    oil = commodities.get("oil_return_pct")
    gold = commodities.get("gold_return_pct")

    return {
        "week_ending": week_ending,
        "market_dashboard": {
            "summary": (
                f"For the week ending {week_ending}, headline performance only told part of the story. "
                f"The S&P 500 moved {sp500}%, the Nasdaq moved {nasdaq}%, the Dow moved {dow}%, and the Russell 2000 moved {russell}%. "
                f"Growth and value also diverged, with large growth at {growth}% and large value at {value}%, offering a clue as to whether markets were reacting more to interest-rate pressure or to broader cyclical expectations. "
                f"The 10-year Treasury yield ended the week near {ten_year}%, while the 2-year stood near {two_year}%, leaving the 2s10s curve around {curve}%. "
                f"Credit and internal market participation added another layer, with high-yield spreads at {hy_oas}, investment-grade spreads at {ig_oas}, and roughly {breadth_pct}% of tracked sectors finishing positive."
            ),
            "top_sectors": top,
            "bottom_sectors": bottom,
        },
        "market_summary": (
            f"This week’s market action was shaped by a mix of rate sensitivity, leadership rotation, and macro interpretation rather than by any single headline. "
            f"The S&P 500 moved {sp500}% while the Nasdaq moved {nasdaq}%, the Dow moved {dow}%, and the Russell 2000 moved {russell}%. "
            f"That combination matters because it helps distinguish whether the week favored size, defensiveness, duration, or cyclicality. "
            f"When large-cap growth behaves differently from value, or when small caps diverge from the broader market, investors get a better read on what the market is actually repricing.\n\n"
            f"Rates remained central to the narrative. The 10-year Treasury yield ended near {ten_year}% after a weekly move of {ten_year_change} basis points, while the curve sat near {curve}%, changing by about {curve_change} basis points. "
            f"That matters because rate moves influence both equity valuations and fixed-income performance. "
            f"At the same time, fixed-income proxies such as AGG, investment-grade credit, and high yield provided additional context for whether the tone was simply valuation-related or more broadly risk-off.\n\n"
            f"Commodity and volatility signals also helped round out the picture. Oil moved {oil}% and gold moved {gold}%, while the VIX ended near {vix}. "
            f"Taken together, the week looked less like a one-dimensional market move and more like a repricing of growth, inflation, and policy expectations across multiple asset classes."
        ),
        "what_drove_markets": (
            "The most important drivers of market performance this week were likely changes in interest-rate expectations, ongoing interpretation of macro data, and the market’s evolving preference for certain sectors and styles over others. "
            "That is a more useful framework than simply saying investors reacted to headlines. Markets continuously reprice the outlook for growth, inflation, Fed policy, and earnings, and those repricings tend to show up first in the relationship between stocks, bonds, and sector leadership.\n\n"
            f"If yields moved higher while growth-oriented equities lagged, that would be more consistent with valuation pressure than with broad economic panic. "
            f"If credit spreads remained relatively contained, especially with high-yield OAS changing only {hy_oas_change}, that would suggest the market move may not have reflected a major deterioration in systemic risk. "
            f"On the other hand, if equities weakened and credit widened meaningfully at the same time, the message would become more cautionary.\n\n"
            "This is why cross-asset confirmation matters. The week’s moves should be read as a balance of signals, not a single verdict. "
            "The broader point is that market action continues to reflect how investors are updating expectations rather than reacting mechanically to one event."
        ),
        "under_the_surface": (
            f"The beneath-the-surface story was at least as important as the headline index returns. Sector leadership, market breadth, credit behavior, and the shape of the yield curve all offered clues about the week’s internal tone. "
            f"Roughly {breadth_pct}% of tracked sectors finished positive, which helps clarify whether the move had broad participation or whether leadership narrowed beneath the surface. "
            "A narrow market with weak breadth often deserves a different interpretation from a market in which most sectors move together.\n\n"
            "Sector leadership also matters because it can reveal whether investors leaned toward cyclicals, defensives, or inflation-sensitive areas. "
            "When leadership narrows into a handful of defensive groups while more cyclical or duration-sensitive areas lag, the market may be expressing caution without fully signaling panic. "
            "Similarly, if small caps underperform while larger, more liquid names hold up better, that can hint at a more selective risk appetite.\n\n"
            f"Credit and the curve add another layer. High-yield spreads near {hy_oas} and a 2s10s spread near {curve} can help distinguish between ordinary valuation pressure and a more meaningful deterioration in confidence. "
            "For advisors, this internal layer is often where the most useful explanation lives."
        ),
        "macro_update": {
            "inflation": (
                "Inflation remains one of the market’s most important anchor variables because it shapes both policy expectations and valuation multiples. "
                "Even when no single inflation release dominates the week, markets continue to interpret rate moves, commodity performance, and sector leadership through the lens of whether inflation is cooling, stabilizing, or proving sticky. "
                "If oil is firm and yields rise, the market may be more sensitive to inflation persistence. If gold rises while yields ease, that can suggest a different mix of caution and policy expectations. "
                "The practical takeaway is that inflation still matters well beyond the CPI headline itself."
            ),
            "labor_market": (
                "Labor-market conditions continue to sit at the center of the growth-and-policy debate. A firm labor market can support the earnings backdrop and reinforce economic resilience, but it can also make the disinflation process less straightforward if wage pressure remains sticky. "
                "That is why jobs data is rarely interpreted in a one-dimensional way. "
                "For markets, the question is not only whether hiring is strong or weak, but whether the labor backdrop supports a soft landing, delays easier policy, or begins to signal broader economic strain."
            ),
            "growth": (
                "Growth indicators such as retail sales and industrial production remain important because they help investors distinguish between a modest normalization and a more concerning slowdown. "
                "Markets tend to respond very differently to those two scenarios. "
                "A slowing but resilient economy can still support earnings and risk assets, particularly if inflation also moderates. "
                "A sharper growth slowdown would have broader implications for cyclicals, credit, and the overall balance of risk appetite. "
                "That makes growth data critical not only for the economy, but also for leadership within markets."
            ),
            "federal_reserve": (
                "Federal Reserve expectations remain one of the most powerful forces across both stocks and bonds. "
                "Even in weeks without an actual policy change, markets can move meaningfully based on incoming data and Fed communication because both affect the expected path of rates. "
                "That is why yield changes often provide a cleaner read on market thinking than the headlines themselves. "
                "For advisors, it is often useful to frame volatility as a repricing of future policy expectations rather than as evidence that long-term fundamentals have suddenly changed."
            ),
        },
        "investor_implications": (
            "For long-term investors, the key issue is not whether markets moved this week. It is what kind of move it was. "
            "Some weekly declines are primarily about discount rates and valuation pressure. Others reflect worsening growth expectations or more meaningful stress in credit and risk appetite. "
            "That distinction matters because it affects whether the week should change the planning conversation or simply reinforce the need for discipline.\n\n"
            "This is one reason diversified portfolios remain valuable. Different assets respond differently to rates, inflation, and growth. "
            "If equities struggle because yields rise, other portfolio building blocks may behave differently depending on duration, credit quality, or sector composition. "
            "When clients see volatility, the advisor’s value is often in explaining what the move likely reflects and what it does not yet confirm.\n\n"
            "The broader implication is that this was a week to emphasize context. Investors are usually better served by understanding the market’s message than by reacting to one week of price action."
        ),
        "advisor_talking_points": [
            "The headline index move only told part of the story. Rates, breadth, and sector leadership were also important.",
            "If growth underperformed while yields rose, that is more consistent with valuation pressure than with broad panic.",
            "Contained credit spreads would suggest the market move was not necessarily signaling systemic stress.",
            "Breadth helps explain whether the market was broadly healthy or narrow beneath the surface.",
            "The shape of the yield curve still matters because it reflects both growth expectations and policy expectations.",
            "One week of volatility rarely changes a long-term financial plan on its own.",
            "This is a good environment to emphasize context, discipline, and diversification.",
        ],
        "client_email": (
            f"What You Need To Know About the Markets\n\n"
            f"For the week ending {week_ending}, markets delivered a more nuanced message than the headline returns alone might suggest. "
            f"The S&P 500 moved {sp500}%, while the Nasdaq moved {nasdaq}%, the Dow moved {dow}%, and the Russell 2000 moved {russell}%. "
            f"Those differences matter because they show not only whether markets rose or fell, but also which parts of the market investors favored.\n\n"
            f"Interest rates remained an important part of the story. The 10-year Treasury yield ended the week near {ten_year}%, while the 2-year yield stood near {two_year}%. "
            f"Changes in Treasury yields matter because they influence borrowing costs, bond prices, and how investors value future corporate earnings. "
            f"In other words, rates often help explain why one part of the market outperforms another.\n\n"
            f"The market’s internal tone also mattered. Roughly {breadth_pct}% of tracked sectors finished the week positive, which gives a better sense of how broad the move really was. "
            f"Credit markets added context as well, with high-yield spreads near {hy_oas} and investment-grade spreads near {ig_oas}. "
            f"That helps investors distinguish between normal market repricing and deeper stress in risk markets.\n\n"
            f"Other signals helped round out the picture. Oil moved {oil}% and gold moved {gold}%, while the VIX ended near {vix}. "
            f"Taken together, these moves suggest a market still working through the balance between growth, inflation, and policy expectations.\n\n"
            f"For long-term investors, the most important point is that short-term moves do not automatically change the long-term investment picture. "
            f"Markets constantly adjust to new information. Staying disciplined and keeping market activity in context remains one of the most important advantages."
        ),
        "linkedin_post": (
            f"What You Need To Know About the Markets for the week ending {week_ending}: "
            f"headline returns only told part of the story. The S&P 500 moved {sp500}%, the Nasdaq moved {nasdaq}%, and the Russell 2000 moved {russell}%, while Treasury yields, sector breadth, and credit spreads added the deeper context. "
            f"When rates, leadership, and credit tell different stories, the right takeaway is usually interpretation, not reaction. "
            f"For advisors, this is where real value gets created: helping clients understand what the market move likely reflects and what it does not yet confirm."
        ),
        "client_faq": [
            {
                "question": "Should I be worried about this week's market move?",
                "answer": "Short-term market moves are normal. The more important question is whether the move reflects a lasting shift in the economic backdrop or simply a repricing of expectations."
            },
            {
                "question": "Why do interest rates matter so much?",
                "answer": "Interest rates affect both bond prices and how investors value future corporate earnings, which is why they often influence leadership across the stock market."
            },
            {
                "question": "Why does market breadth matter?",
                "answer": "Breadth helps show whether the market move was broad or narrow. A stable headline index can still hide weaker participation beneath the surface."
            },
            {
                "question": "What do credit spreads tell us?",
                "answer": "Credit spreads help investors gauge whether markets are simply repricing valuations or becoming more concerned about deeper financial stress."
            },
            {
                "question": "Does this change our long-term plan?",
                "answer": "In most cases, no. Long-term plans are built to navigate normal volatility and changing headlines without requiring constant adjustments."
            }
        ],
        "advisor_internal_notes": (
            "This week’s market message should be framed through cross-asset context rather than headline returns alone. "
            "Client conversations should focus on whether the move reflected valuation pressure, changing policy expectations, or a more meaningful shift in growth expectations. "
            "Breadth and credit provide especially useful context here. If breadth weakened but credit remained relatively contained, that is a different conversation from one in which both deteriorate together.\n\n"
            "The most useful advisor posture this week is likely context over prediction. Help clients understand what changed, what did not, and why discipline still matters."
        ),
        "risk_watch": (
            "Key risks worth monitoring include further repricing in Treasury yields, continued narrowing in market breadth, widening in high-yield spreads, and any renewed inflation sensitivity showing up through rates and commodities. "
            "If volatility, breadth deterioration, and credit widening begin to reinforce one another, the tone becomes more concerning. "
            "If those signals remain mixed or contained, the more likely interpretation is a normal repricing of expectations rather than a deeper breakdown."
        ),
        "bottom_line": (
            "The biggest takeaway this week is that the market’s message was more nuanced than a simple headline return. "
            "For long-term investors, the right response is usually perspective, diversification, and discipline rather than reacting to one week of market movement."
        ),
    }


def write_outputs(payload: Dict[str, Any], commentary: Dict[str, Any]) -> None:
    ensure_output_dir()

    week_ending = payload["week_ending"]

    root_data_path = ROOT_WEEKLY_DATA_FILENAME
    dated_data_path = os.path.join(OUTPUT_DIR, f"weekly_market_data_{week_ending}.json")
    commentary_path = os.path.join(OUTPUT_DIR, f"commentary_{week_ending}.json")

    with open(root_data_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    with open(dated_data_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    with open(commentary_path, "w", encoding="utf-8") as f:
        json.dump(commentary, f, indent=2)

    print(f"Weekly market data written to {dated_data_path}")
    print(f"Commentary written to {commentary_path}")
    print("Weekly market data and commentary built successfully.")


def main() -> None:
    payload = build_input_payload()

    try:
        commentary = call_openai_for_commentary(payload)
        print("OpenAI commentary generated successfully.")
    except Exception as e:
        print(f"OpenAI generation failed. Using fallback commentary. Reason: {e}")
        commentary = build_fallback_commentary(payload)

    write_outputs(payload, commentary)


if __name__ == "__main__":
    main()
