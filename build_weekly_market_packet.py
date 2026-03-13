import os
import json
import requests
import yfinance as yf
from dateutil import parser as date_parser
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dateutil import parser as date_parser
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
TIMEZONE = ZoneInfo(os.getenv("OUTPUT_TIMEZONE", "America/New_York"))

ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY", "")
FRED_API_KEY = os.getenv("FRED_API_KEY", "")
NEWSAPI_API_KEY = os.getenv("NEWSAPI_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.4-mini")
EMAIL_TO = os.getenv("EMAIL_TO", "advisor@example.com")

INDEX_SYMBOLS = {
    "sp500": "SPY",   # proxy for S&P 500
    "nasdaq": "QQQ",  # proxy for Nasdaq-100
    "dow": "DIA",     # proxy for Dow Jones Industrial Average
    "oil": "USO",     # proxy for crude oil
    "gold": "GLD",    # proxy for gold
}

FRED_SERIES = {
    "us10y": "DGS10",  # 10-Year Treasury Constant Maturity Rate
}

NEWS_QUERIES = {
    "fed_headline": '("Federal Reserve" OR Fed) AND (rates OR policy OR Powell)',
    "inflation_headline": '(inflation OR CPI OR PCE) AND (US OR U.S.)',
    "jobs_headline": '(jobs report OR payrolls OR unemployment) AND (US OR U.S.)',
    "geopolitics_headline": '(tariffs OR sanctions OR conflict OR ceasefire OR trade policy) AND markets',
    "corporate_headline": '(earnings OR guidance OR AI spending OR mega cap) AND markets',
}

ETF_FLOW_PLACEHOLDER = {
    "top_etf_inflow_symbol": "",
    "top_etf_inflow_name": "",
    "top_etf_inflow_usd_mm": "",
    "top_etf_outflow_symbol": "",
    "top_etf_outflow_name": "",
    "top_etf_outflow_usd_mm": "",
}


def pct_change(start_value: float, end_value: float) -> float:
    if start_value == 0:
        raise ValueError("Start value cannot be zero when calculating percent change.")
    return ((end_value / start_value) - 1.0) * 100.0


def bps_change(start_pct: float, end_pct: float) -> float:
    return (end_pct - start_pct) * 100.0


def newest_complete_friday(now_et: datetime) -> datetime.date:
    date_et = now_et.date()
    offset = (date_et.weekday() - 4) % 7
    friday = date_et - timedelta(days=offset)
    if date_et.weekday() < 4:
        friday -= timedelta(days=7)
    return friday


def http_get(url: str, params: Dict[str, Any], timeout: int = 30) -> Dict[str, Any]:
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()

def get_weekly_return_pct_yf
sp500_weekly_return_pct = get_weekly_return_pct_yf("SPY")
nasdaq_weekly_return_pct = get_weekly_return_pct_yf("QQQ")
dow_weekly_return_pct = get_weekly_return_pct_yf("DIA")
oil_weekly_return_pct = get_weekly_return_pct_yf("USO")
gold_weekly_return_pct = get_weekly_return_pct_yf("GLD")

def latest_two_weeks_from_av(symbol: str, target_friday: datetime.date) -> Tuple[Tuple[str, Dict[str, str]], Tuple[str, Dict[str, str]]]:
    weekly = fetch_alpha_vantage_weekly_series(symbol)
    ordered = sorted(weekly.items(), key=lambda x: x[0], reverse=True)
    eligible = []
    for date_str, values in ordered:
        dt = date_parser.parse(date_str).date()
        if dt <= target_friday:
            eligible.append((date_str, values))
        if len(eligible) == 2:
            break
    if len(eligible) < 2:
        raise RuntimeError(f"Not enough weekly observations for {symbol}.")
    return eligible[0], eligible[1]


def fetch_weekly_return(symbol: str, target_friday: datetime.date) -> float:
    current_week, previous_week = latest_two_weeks_from_av(symbol, target_friday)
    current_close = float(current_week[1]["5. adjusted close"])
    previous_close = float(previous_week[1]["5. adjusted close"])
    return round(pct_change(previous_close, current_close), 2)


def fetch_fred_recent_values(series_id: str, target_friday: datetime.date) -> Tuple[float, float]:
    if not FRED_API_KEY:
        raise RuntimeError("Missing FRED_API_KEY.")
    start_date = (target_friday - timedelta(days=14)).isoformat()
    end_date = target_friday.isoformat()
    data = http_get(
        "https://api.stlouisfed.org/fred/series/observations",
        {
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": start_date,
            "observation_end": end_date,
            "sort_order": "desc",
        },
    )
    observations = data.get("observations", [])
    valid = []
    for obs in observations:
        value = obs.get("value")
        if value is None or value == ".":
            continue
        valid.append((obs["date"], float(value)))
    if len(valid) < 2:
        raise RuntimeError(f"Not enough valid FRED observations for {series_id}.")
    return valid[0][1], valid[1][1]


def fetch_treasury_data(target_friday: datetime.date) -> Tuple[float, float]:
    latest, previous = fetch_fred_recent_values(FRED_SERIES["us10y"], target_friday)
    return round(latest, 3), round(bps_change(previous, latest), 1)


def fetch_news_headline(query: str, target_friday: datetime.date) -> str:
    if not NEWSAPI_API_KEY:
        raise RuntimeError("Missing NEWSAPI_API_KEY.")
    from_date = (target_friday - timedelta(days=6)).isoformat()
    to_date = (target_friday + timedelta(days=1)).isoformat()
    data = http_get(
        "https://newsapi.org/v2/everything",
        {
            "q": query,
            "from": from_date,
            "to": to_date,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": 1,
            "apiKey": NEWSAPI_API_KEY,
        },
    )
    articles = data.get("articles", [])
    if not articles:
        return ""
    article = articles[0]
    title = article.get("title", "").strip()
    source = article.get("source", {}).get("name", "").strip()
    published_at = article.get("publishedAt", "").strip()
    if source and published_at:
        return f"{title} | {source} | {published_at}"
    return title


def build_market_regime_summary(row: Dict[str, Any]) -> str:
    equity_parts = []
    if row["sp500_weekly_return_pct"] > 0:
        equity_parts.append("U.S. equities finished higher")
    elif row["sp500_weekly_return_pct"] < 0:
        equity_parts.append("U.S. equities finished lower")
    else:
        equity_parts.append("U.S. equities were little changed")

    if row["us10y_yield_change_bps"] > 0:
        rate_text = "Treasury yields moved higher"
    elif row["us10y_yield_change_bps"] < 0:
        rate_text = "Treasury yields moved lower"
    else:
        rate_text = "Treasury yields were little changed"

    commodity_parts = []
    if abs(row["oil_weekly_return_pct"]) >= 1.0:
        commodity_parts.append(f"oil {('rose' if row['oil_weekly_return_pct'] > 0 else 'fell')} {abs(row['oil_weekly_return_pct']):.2f}%")
    if abs(row["gold_weekly_return_pct"]) >= 1.0:
        commodity_parts.append(f"gold {('rose' if row['gold_weekly_return_pct'] > 0 else 'fell')} {abs(row['gold_weekly_return_pct']):.2f}%")

    summary = f"{equity_parts[0]}; {rate_text}."
    if commodity_parts:
        summary += " Key commodity moves: " + "; ".join(commodity_parts) + "."
    return summary


def build_weekly_row() -> Dict[str, Any]:
    now_et = datetime.now(tz=TIMEZONE)
    week_ending = newest_complete_friday(now_et)
    row: Dict[str, Any] = {
        "week_ending": week_ending.isoformat(),
        "generated_at_et": now_et.isoformat(),
        "sp500_weekly_return_pct": fetch_weekly_return(INDEX_SYMBOLS["sp500"], week_ending),
        "nasdaq_weekly_return_pct": fetch_weekly_return(INDEX_SYMBOLS["nasdaq"], week_ending),
        "dow_weekly_return_pct": fetch_weekly_return(INDEX_SYMBOLS["dow"], week_ending),
        "oil_weekly_return_pct": fetch_weekly_return(INDEX_SYMBOLS["oil"], week_ending),
        "gold_weekly_return_pct": fetch_weekly_return(INDEX_SYMBOLS["gold"], week_ending),
    }
    us10y_end_pct, us10y_change_bps = fetch_treasury_data(week_ending)
    row["us10y_yield_end_pct"] = us10y_end_pct
    row["us10y_yield_change_bps"] = us10y_change_bps

    for key, query in NEWS_QUERIES.items():
        row[key] = fetch_news_headline(query, week_ending)

    row.update(ETF_FLOW_PLACEHOLDER)
    row["market_regime_summary"] = build_market_regime_summary(row)
    row["source_notes"] = (
        "Equity and ETF proxies: Alpha Vantage weekly adjusted series. "
        "Treasury yield: FRED DGS10. Headlines: NewsAPI /v2/everything. "
        "ETF flow fields are placeholders until a licensed flow source is attached."
    )
    return row


def save_weekly_csv(row: Dict[str, Any]) -> Path:
    output_path = OUTPUT_DIR / f"weekly_market_packet_{row['week_ending']}.csv"
    fieldnames = list(row.keys())
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(row)
    return output_path


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
            "gold_weekly_return_pct": row["gold_weekly_return_pct"],
        },
        "macro_headlines": {
            "fed": row["fed_headline"],
            "inflation": row["inflation_headline"],
            "jobs": row["jobs_headline"],
            "geopolitics": row["geopolitics_headline"],
            "corporate": row["corporate_headline"],
        },
        "etf_flows": {
            "top_inflow_symbol": row["top_etf_inflow_symbol"],
            "top_inflow_name": row["top_etf_inflow_name"],
            "top_inflow_usd_mm": row["top_etf_inflow_usd_mm"],
            "top_outflow_symbol": row["top_etf_outflow_symbol"],
            "top_outflow_name": row["top_etf_outflow_name"],
            "top_outflow_usd_mm": row["top_etf_outflow_usd_mm"],
        },
        "market_regime_summary": row["market_regime_summary"],
    }


def call_openai(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        raise RuntimeError("Missing OPENAI_API_KEY.")

    prompt = f"""
You are writing a weekly market commentary package for financial advisors.

Audience: retail investors.
Tone: clear, calm, professional, plain English.
Rules:
- Do not predict markets.
- Do not give individualized investment advice.
- Do not use hype, fear language, or jargon without explanation.
- Keep the writing concise.
- Include no markdown.

Return valid JSON with exactly these keys:
client_email_subject
client_email_body
newsletter_body
linkedin_post
advisor_talking_points

advisor_talking_points must be an array of 5 to 7 short strings.

Input payload:
{json.dumps(payload, indent=2)}
""".strip()

    response = requests.post(
        "https://api.openai.com/v1/responses",
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": OPENAI_MODEL,
            "input": prompt,
            "text": {"format": {"type": "json_object"}},
        },
        timeout=60,
    )
    response.raise_for_status()
    data = response.json()
    output_text = ""
    for item in data.get("output", []):
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                output_text += content.get("text", "")
    if not output_text:
        raise RuntimeError(f"No text returned from OpenAI API: {data}")
    return json.loads(output_text)


def save_outputs(row: Dict[str, Any], content: Dict[str, Any]) -> Dict[str, Path]:
    week = row["week_ending"]
    json_path = OUTPUT_DIR / f"commentary_{week}.json"
    txt_path = OUTPUT_DIR / f"commentary_{week}.txt"
    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(content, handle, indent=2)

    talking_points = content.get("advisor_talking_points", [])
    txt = []
    txt.append(content.get("client_email_subject", "What You Need To Know About the Markets"))
    txt.append("")
    txt.append("CLIENT EMAIL")
    txt.append(content.get("client_email_body", ""))
    txt.append("")
    txt.append("NEWSLETTER")
    txt.append(content.get("newsletter_body", ""))
    txt.append("")
    txt.append("LINKEDIN")
    txt.append(content.get("linkedin_post", ""))
    txt.append("")
    txt.append("ADVISOR TALKING POINTS")
    for point in talking_points:
        txt.append(f"- {point}")
    with txt_path.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(txt))
    return {"json": json_path, "txt": txt_path}


def build_email_html(row: Dict[str, Any], content: Dict[str, Any]) -> str:
    points = "".join(f"<li>{p}</li>" for p in content.get("advisor_talking_points", []))
    return f"""
    <html>
      <body style="font-family: Arial, sans-serif; line-height: 1.5; color: #111;">
        <h2>What You Need To Know About the Markets</h2>
        <p><strong>Week ending:</strong> {row['week_ending']}</p>
        <h3>Client Email</h3>
        <p>{content.get('client_email_body', '').replace(chr(10), '<br>')}</p>
        <h3>Newsletter</h3>
        <p>{content.get('newsletter_body', '').replace(chr(10), '<br>')}</p>
        <h3>LinkedIn</h3>
        <p>{content.get('linkedin_post', '').replace(chr(10), '<br>')}</p>
        <h3>Advisor Talking Points</h3>
        <ul>{points}</ul>
      </body>
    </html>
    """.strip()


def save_email_preview(row: Dict[str, Any], content: Dict[str, Any]) -> Path:
    html_path = OUTPUT_DIR / f"email_preview_{row['week_ending']}.html"
    html_path.write_text(build_email_html(row, content), encoding="utf-8")
    return html_path


def main() -> int:
    row = build_weekly_row()
    csv_path = save_weekly_csv(row)

    payload = build_prompt_payload(row)
    try:
        commentary = call_openai(payload)
        output_paths = save_outputs(row, commentary)
        email_preview = save_email_preview(row, commentary)
    except Exception as exc:
        print(f"Weekly data file created, but commentary generation failed: {exc}")
        print(f"CSV: {csv_path}")
        return 1

    print("Build complete.")
    print(f"CSV: {csv_path}")
    for name, path in output_paths.items():
        print(f"{name.upper()}: {path}")
    print(f"EMAIL_PREVIEW: {email_preview}")
    print(f"EMAIL_TO placeholder: {EMAIL_TO}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
