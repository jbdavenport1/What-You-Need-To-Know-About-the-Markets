import os
import json
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


def safe_float(value, default=None):
    try:
        if value in [None, "", "."]:
            return default
        return float(value)
    except Exception:
        return default


def get_weekly_return(symbol):
    hist = yf.Ticker(symbol).history(period="6mo", interval="1wk", auto_adjust=False)

    if hist is None or hist.empty or "Close" not in hist.columns:
        raise RuntimeError(f"Could not get weekly data for {symbol}")

    closes = hist["Close"].dropna()

    if len(closes) < 2:
        raise RuntimeError(f"Not enough weekly closes for {symbol}")

    latest = float(closes.iloc[-1])
    prev = float(closes.iloc[-2])

    return pct_change(latest, prev)


def get_latest_close(symbol):
    hist = yf.Ticker(symbol).history(period="1mo", interval="1d", auto_adjust=False)

    if hist is None or hist.empty or "Close" not in hist.columns:
        return None

    closes = hist["Close"].dropna()

    if len(closes) == 0:
        return None

    return round(float(closes.iloc[-1]), 2)


def fred(series_id):
    if not FRED_API_KEY:
        return {"latest": None, "change": None}

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 8,
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        obs = data.get("observations", [])

        values = []
        for row in obs:
            v = safe_float(row.get("value"))
            if v is not None:
                values.append(v)
            if len(values) >= 2:
                break

        if len(values) < 2:
            return {"latest": None, "change": None}

        latest = values[0]
        prev = values[1]

        return {
            "latest": latest,
            "change": round(latest - prev, 2)
        }
    except Exception:
        return {"latest": None, "change": None}


def latest_headlines(query, page_size=3):
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


def build_market_snapshot():
    sectors = {
        "technology": get_weekly_return("XLK"),
        "financials": get_weekly_return("XLF"),
        "health_care": get_weekly_return("XLV"),
        "energy": get_weekly_return("XLE"),
        "industrials": get_weekly_return("XLI"),
        "consumer_discretionary": get_weekly_return("XLY"),
        "utilities": get_weekly_return("XLU"),
        "real_estate": get_weekly_return("XLRE"),
        "consumer_staples": get_weekly_return("XLP"),
        "materials": get_weekly_return("XLB"),
    }

    sorted_sectors = sorted(sectors.items(), key=lambda x: x[1], reverse=True)

    return {
        "equities": {
            "sp500": get_weekly_return("SPY"),
            "nasdaq": get_weekly_return("QQQ"),
            "dow": get_weekly_return("DIA"),
            "russell2000": get_weekly_return("IWM"),
            "eafe": get_weekly_return("EFA"),
            "em": get_weekly_return("EEM"),
            "growth": get_weekly_return("IWF"),
            "value": get_weekly_return("IWD"),
        },
        "fixed_income": {
            "agg": get_weekly_return("AGG"),
            "investment_grade": get_weekly_return("LQD"),
            "high_yield": get_weekly_return("HYG"),
            "tips": get_weekly_return("TIP"),
        },
        "rates": {
            "two_year": fred("DGS2"),
            "ten_year": fred("DGS10"),
            "fed_funds": fred("FEDFUNDS"),
        },
        "volatility": {
            "vix_level": get_latest_close("^VIX"),
        },
        "commodities": {
            "oil": get_weekly_return("USO"),
            "gold": get_weekly_return("GLD"),
        },
        "currency": {
            "dollar_proxy": get_weekly_return("UUP"),
        },
        "sectors": sectors,
        "top_sectors": [{"sector": k, "return_pct": v} for k, v in sorted_sectors[:3]],
        "bottom_sectors": [{"sector": k, "return_pct": v} for k, v in sorted_sectors[-3:]],
    }


def build_macro_snapshot():
    return {
        "inflation": {
            "cpi": fred("CPIAUCSL"),
            "headlines": latest_headlines("US CPI inflation prices", 2),
        },
        "labor_market": {
            "unemployment": fred("UNRATE"),
            "payrolls": fred("PAYEMS"),
            "headlines": latest_headlines("US jobs report unemployment payrolls", 2),
        },
        "growth": {
            "retail_sales": fred("RSAFS"),
            "industrial_production": fred("INDPRO"),
            "headlines": latest_headlines("US retail sales economy growth manufacturing", 2),
        },
        "federal_reserve": {
            "fed_funds": fred("FEDFUNDS"),
            "headlines": latest_headlines("Federal Reserve policy interest rates", 2),
        },
    }


def build_input_payload():
    today = datetime.date.today().strftime("%Y-%m-%d")

    return {
        "week_ending": today,
        "market_snapshot": build_market_snapshot(),
        "macro_snapshot": build_macro_snapshot(),
        "major_headlines": {
            "geopolitics": latest_headlines("geopolitics markets oil stocks bonds", 3),
            "corporate": latest_headlines("corporate earnings stock market outlook", 3),
        },
        "client_question_candidates": [
            "Should I be worried about this week's volatility?",
            "Why did stocks and bonds move the way they did this week?",
            "What do higher or lower Treasury yields mean for my portfolio?",
            "Does this change our long-term investment plan?",
            "What is the market signaling about inflation and the economy?",
        ],
    }


def get_master_prompt():
    return """
You are a Chief Market Strategist writing a premium weekly market commentary product for financial advisors.

You are not writing a news recap.
You are writing an advisor-grade market intelligence brief.

Your output must feel thoughtful, analytical, and premium.

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
- growth vs value
- credit vs stocks
- oil, gold, and inflation expectations
- macro data and Fed expectations

If signals conflict, explain the tension.

Examples of premium inference:
- Equity weakness alongside stable credit may suggest valuation pressure more than systemic stress.
- Growth underperformance with higher yields is consistent with duration sensitivity.
- Defensive sector leadership with contained volatility may indicate caution, not panic.

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

Length requirements:
- market_dashboard.summary: 80 to 140 words
- market_summary: 180 to 300 words
- what_drove_markets: 180 to 300 words
- under_the_surface: 180 to 300 words
- each macro_update field: 90 to 160 words
- investor_implications: 180 to 280 words
- advisor_talking_points: 6 to 8 bullets
- client_email: 350 to 550 words
- linkedin_post: 120 to 180 words
- client_faq: 4 to 6 items
- advisor_internal_notes: 180 to 260 words
- risk_watch: 120 to 200 words
- bottom_line: 90 to 140 words

Make this feel like a premium strategist note that an advisor would genuinely value.
""".strip()


def extract_response_text(data):
    if isinstance(data.get("output_text"), str) and data.get("output_text").strip():
        return data["output_text"].strip()

    output = data.get("output", [])
    collected = []

    for item in output:
        for content in item.get("content", []):
            if content.get("type") in ["output_text", "text"]:
                text = content.get("text", "")
                if isinstance(text, str) and text.strip():
                    collected.append(text)

    return "\n".join(collected).strip()


def call_openai(payload):
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
                        "text": get_master_prompt()
                    }
                ]
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(payload, indent=2)
                    }
                ]
            }
        ]
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
        raise RuntimeError(f"Could not parse OpenAI JSON. Raw output: {text[:1000]}")


def build_fallback_commentary(payload):
    week_ending = payload["week_ending"]
    m = payload["market_snapshot"]
    e = m["equities"]
    r = m["rates"]
    f = m["fixed_income"]
    c = m["commodities"]
    vix = m["volatility"]["vix_level"]
    top = m["top_sectors"]
    bottom = m["bottom_sectors"]

    return {
        "week_ending": week_ending,
        "market_dashboard": {
            "summary": (
                f"For the week ending {week_ending}, markets offered a more nuanced picture than the headline index returns alone would suggest. "
                f"The S&P 500 moved {e['sp500']}%, the Nasdaq moved {e['nasdaq']}%, the Dow moved {e['dow']}%, and the Russell 2000 moved {e['russell2000']}%. "
                f"In fixed income, AGG returned {f['agg']}% and high yield returned {f['high_yield']}%. "
                f"Oil moved {c['oil']}% and gold moved {c['gold']}%, while the VIX finished near {vix}. "
                f"Leadership beneath the surface mattered, with top-performing sectors and lagging sectors offering clues about whether investors were leaning toward cyclicals, defensives, or rate-sensitive exposures."
            ),
            "top_sectors": top,
            "bottom_sectors": bottom,
        },
        "market_summary": (
            f"Market performance during the week ending {week_ending} reflected a combination of macro repricing, sector rotation, and shifting expectations around the path of interest rates. "
            f"The S&P 500 returned {e['sp500']}%, while the Nasdaq returned {e['nasdaq']}%, suggesting that growth-oriented areas of the market either led or lagged the broader tape depending on rate pressure. "
            f"The Dow moved {e['dow']}% and the Russell 2000 moved {e['russell2000']}%, helping frame whether leadership favored larger, more defensive companies or more economically sensitive segments.\n\n"
            f"At the same time, Treasury markets remained central to the weekly narrative. The 2-year yield stood at {r['two_year']['latest']} and the 10-year yield stood at {r['ten_year']['latest']}, reinforcing that the rate backdrop continues to shape both equity valuations and fixed income returns. "
            f"Commodity moves added another layer, with oil at {c['oil']}% and gold at {c['gold']}%, offering clues about inflation sensitivity, geopolitical caution, and broader investor positioning.\n\n"
            f"The overall takeaway is that this was not simply a week of up or down markets. It was a week in which cross-asset relationships provided the more important message."
        ),
        "what_drove_markets": (
            "The most important drivers of market action this week were likely the interaction between interest-rate expectations, incoming macro data, and shifting investor preferences across sectors and styles. "
            "When markets move, the headline index return rarely tells the whole story. The more useful question is whether the move was driven by growth concerns, inflation concerns, Fed repricing, valuation pressure, or some combination of all four.\n\n"
            "If higher yields coincided with weakness in growth-oriented equities, that would be more consistent with duration pressure than with broad economic panic. "
            "If defensive sectors outperformed while volatility stayed contained, that would suggest caution rather than outright stress. "
            "If credit held up better than equities, the signal would lean more toward valuation adjustment than systemic risk.\n\n"
            "That kind of cross-asset interpretation is what matters most. Investors are not just reacting to one headline. They are continuously repricing the balance between growth, inflation, policy, and earnings."
        ),
        "under_the_surface": (
            "The internal market tone often says more than the headline averages. Sector leadership, style leadership, and the relationship between equities, bonds, and volatility help explain what changed beneath the surface this week.\n\n"
            "Top-performing sectors can reveal whether investors favored cyclicals, inflation beneficiaries, or defensives. Bottom-performing sectors can help show whether markets were penalizing rate-sensitive growth exposure, economically sensitive areas, or defensive laggards. "
            "Similarly, the relationship between growth and value performance can reveal whether the week was shaped more by changing discount rates or by changing economic expectations.\n\n"
            "Volatility also matters here. A higher VIX does not automatically mean panic, but it can indicate rising demand for protection. When paired with narrowing leadership or weaker small-cap performance, it may suggest a more cautious internal tone even if headline indexes appear relatively stable.\n\n"
            "For advisors, this beneath-the-surface layer is often where the most useful explanation lives."
        ),
        "macro_update": {
            "inflation": (
                "Inflation remains one of the most important variables in the market because it influences both Fed expectations and valuation multiples. "
                "Even when no major inflation release dominates a given week, markets continue to interpret asset-price moves through the lens of whether inflation is cooling, stabilizing, or proving stickier than expected. "
                "Oil, gold, nominal yields, and inflation-sensitive sectors can all provide indirect clues. "
                "If inflation pressure appears to be easing, that can support duration-sensitive assets. If inflation concerns re-emerge, the market may put greater weight on restrictive policy staying in place for longer."
            ),
            "labor_market": (
                "Labor-market data remains central because it sits at the intersection of growth, inflation, and Fed policy. "
                "A labor market that remains firm can support the earnings outlook and broader growth expectations, but it can also complicate the inflation picture if wage pressure persists. "
                "That is why investors often interpret payroll and unemployment data through two lenses at once: economic resilience and policy restraint. "
                "For advisors, the key message is that strong labor data is not always a pure positive if it also reduces the odds of easier policy."
            ),
            "growth": (
                "Growth indicators such as retail sales and industrial production help the market determine whether the economy is slowing meaningfully or simply normalizing after a stronger period. "
                "Markets are highly sensitive to this distinction. A mild slowdown can be absorbed well. A sharper deterioration would carry broader implications for earnings, credit, and risk appetite. "
                "That makes the growth backdrop important not just for equities, but for the leadership profile within equities and the behavior of rates, spreads, and defensives."
            ),
            "federal_reserve": (
                "Federal Reserve expectations remain one of the most powerful market forces because they influence both the level of rates and how investors discount future earnings. "
                "Even without an actual policy change, the market can reprice rapidly based on speeches, meeting minutes, inflation data, or labor data. "
                "That is why weekly market moves often reflect shifting expectations about the path of policy rather than any actual Fed decision. "
                "For advisors, this means it is often more accurate to frame market volatility as repricing of expectations rather than a dramatic change in long-term fundamentals."
            )
        },
        "investor_implications": (
            "For long-term investors, the most important takeaway is that not every week of volatility carries the same message. "
            "Some weeks reflect genuine deterioration in the growth or credit backdrop. Others reflect changing expectations around rates, inflation, or valuation. "
            "That distinction matters because it affects whether a market move should be interpreted as a threat to long-term planning or as a normal repricing within a still-functioning market environment.\n\n"
            "This is where diversified portfolios continue to matter. Different parts of the market respond differently to rates, inflation, and growth expectations. "
            "When one segment is under pressure, another may be holding up for a different reason. That diversification benefit is often easiest to appreciate during weeks when leadership is narrow or cross-asset relationships are shifting.\n\n"
            "For advisors, the value-add is not predicting every move. It is helping clients separate short-term noise from signals that actually change the planning conversation."
        ),
        "advisor_talking_points": [
            "Headline index returns only tell part of the story. Sector leadership and rates often explain more than the broad averages.",
            "If growth stocks struggled while yields rose, that is more consistent with valuation pressure than with broad panic.",
            "If credit remained stable, the market message may be caution rather than systemic stress.",
            "This week is a reminder that volatility does not automatically require portfolio changes.",
            "Long-term plans are designed to absorb weeks like this without losing discipline.",
            "Clients are often best served by context, not reaction.",
            "Cross-asset relationships matter more than any single headline.",
        ],
        "client_email": (
            f"What You Need To Know About the Markets\n\n"
            f"For the week ending {week_ending}, markets delivered a more nuanced message than the headline returns alone might suggest. "
            f"The S&P 500 moved {e['sp500']}%, while the Nasdaq moved {e['nasdaq']}%, the Dow moved {e['dow']}%, and the Russell 2000 moved {e['russell2000']}%. "
            f"That mix of returns helps show not just whether markets rose or fell, but which parts of the market investors favored.\n\n"
            f"Interest rates remained an important part of the story. The 2-year Treasury yield stood at {r['two_year']['latest']}, while the 10-year yield stood at {r['ten_year']['latest']}. "
            f"Moves in Treasury yields matter because they influence borrowing costs, bond prices, and how investors value future corporate earnings. "
            f"In other words, rates often help explain why one segment of the market outperforms another.\n\n"
            f"Other asset classes also added context. Investment-grade bonds returned {f['investment_grade']}%, high yield returned {f['high_yield']}%, oil moved {c['oil']}%, and gold moved {c['gold']}%. "
            f"Together, these signals help investors understand whether the week was defined more by inflation concerns, growth concerns, policy repricing, or normal rotation beneath the surface.\n\n"
            f"The most important point is that short-term moves do not automatically change the long-term investment picture. "
            f"Markets constantly reprice expectations as new data arrives. For long-term investors, staying disciplined and keeping market activity in context remains one of the most important advantages."
        ),
        "linkedin_post": (
            f"What You Need To Know About the Markets for the week ending {week_ending}: "
            f"the headline returns only told part of the story. The S&P 500 moved {e['sp500']}%, the Nasdaq moved {e['nasdaq']}%, and the Russell 2000 moved {e['russell2000']}. "
            f"Treasury yields, sector leadership, and cross-asset behavior likely said more than the broad averages alone. "
            f"For advisors, this is where real value gets created: helping clients understand whether a move reflects valuation pressure, policy repricing, or something more meaningful for long-term planning."
        ),
        "client_faq": [
            {
                "question": "Should I be worried about this week's market move?",
                "answer": "Short-term market moves are normal. The more important question is whether the move reflects a lasting change in the economic backdrop or simply a repricing of expectations."
            },
            {
                "question": "Why do interest rates matter so much?",
                "answer": "Interest rates affect both bond prices and how investors value future corporate earnings. That is why they often influence leadership across stocks and bonds."
            },
            {
                "question": "Does this change our long-term plan?",
                "answer": "In most cases, no. Long-term plans are built to navigate normal volatility and changing headlines without requiring constant adjustments."
            },
            {
                "question": "Why can the market feel weak even when some areas hold up?",
                "answer": "Different sectors and asset classes respond differently to growth, inflation, and policy expectations. That is why looking beneath the surface matters."
            }
        ],
        "advisor_internal_notes": (
            "This week’s market message should be framed through cross-asset context rather than headline performance alone. "
            "Client conversations should focus on whether the move reflected growth concerns, inflation pressure, or changes in Fed expectations. "
            "If rates drove the move, explain duration sensitivity. If defensives led, explain caution rather than panic. If credit stayed relatively stable, remind clients that valuation adjustments are not the same thing as systemic stress.\n\n"
            "The highest-value advisor framing this week is likely context, not prediction. Help clients understand what changed, what did not, and why discipline still matters."
        ),
        "risk_watch": (
            "Key risks worth monitoring include further repricing in rate expectations, renewed inflation sensitivity, narrowing market leadership, and any signs that credit markets begin to confirm equity weakness. "
            "If volatility rises at the same time that breadth weakens and credit spreads widen, the market message becomes more concerning. "
            "If those signals remain contained, the more likely interpretation is normal repricing rather than a deeper breakdown."
        ),
        "bottom_line": (
            "The biggest takeaway this week is that the market’s message was more nuanced than a simple up-or-down headline. "
            "For long-term investors, the right response is usually perspective, diversification, and discipline, not reaction to a single week of movement."
        ),
    }


def write_outputs(payload, commentary):
    ensure_output_dir()

    path = os.path.join(OUTPUT_DIR, f"commentary_{payload['week_ending']}.json")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(commentary, f, indent=2)

    print(f"Commentary written to {path}")
    print("Weekly market data and commentary built successfully.")


def main():
    payload = build_input_payload()

    try:
        commentary = call_openai(payload)
        print("OpenAI commentary generated successfully.")
    except Exception as e:
        print(f"OpenAI generation failed. Using fallback commentary. Reason: {e}")
        commentary = build_fallback_commentary(payload)

    write_outputs(payload, commentary)


if __name__ == "__main__":
    main()

