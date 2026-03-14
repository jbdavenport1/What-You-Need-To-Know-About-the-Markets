from __future__ import annotations

import json
import math
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd
import requests
import yfinance as yf
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv


load_dotenv()

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

PACKET_JSON_PATH = OUTPUT_DIR / "weekly_market_packet.json"
PACKET_TXT_PATH = OUTPUT_DIR / "weekly_market_packet.txt"

FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()

TICKERS = {
    "SPY": "SPY",
    "QQQ": "QQQ",
    "IWM": "IWM",
    "RSP": "RSP",
    "TLT": "TLT",
    "GLD": "GLD",
    "HYG": "HYG",
    "LQD": "LQD",
    "VIX": "^VIX",
    "VIX3M": "^VIX3M",
    "TNX": "^TNX",   # 10Y Treasury yield x10
    "IRX": "^IRX",   # 13-week Treasury bill x100
}

CHARTS = [
    {
        "path": "output/spx_trend.png",
        "caption": "Figure 1. S&P 500 Trend"
    },
    {
        "path": "output/yield_curve.png",
        "caption": "Figure 2. Treasury Yield Curve"
    },
    {
        "path": "output/credit_spreads.png",
        "caption": "Figure 3. Credit Spread Proxy"
    }
]


def fmt_pct(x: Optional[float], digits: int = 1) -> str:
    if x is None or pd.isna(x):
        return "N/A"
    return f"{x:.{digits}f}%"


def fmt_num(x: Optional[float], digits: int = 2) -> str:
    if x is None or pd.isna(x):
        return "N/A"
    return f"{x:.{digits}f}"


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def previous_business_day(date_obj: datetime) -> datetime:
    d = date_obj
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def get_history(
    ticker: str,
    period: str = "6mo",
    interval: str = "1d"
) -> pd.DataFrame:
    try:
        df = yf.download(
            ticker,
            period=period,
            interval=interval,
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        if df is None or df.empty:
            return pd.DataFrame()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df
    except Exception:
        return pd.DataFrame()


def get_close_series(ticker: str, period: str = "6mo") -> pd.Series:
    df = get_history(ticker, period=period)
    if df.empty or "Close" not in df.columns:
        return pd.Series(dtype=float)
    s = df["Close"].dropna()
    s.name = ticker
    return s


def calc_return(series: pd.Series, days: int) -> Optional[float]:
    if series.empty or len(series) <= days:
        return None
    latest = safe_float(series.iloc[-1])
    prior = safe_float(series.iloc[-(days + 1)])
    if latest is None or prior in (None, 0):
        return None
    return ((latest / prior) - 1.0) * 100.0


def calc_ma(series: pd.Series, window: int) -> Optional[float]:
    if series.empty or len(series) < window:
        return None
    return safe_float(series.rolling(window).mean().iloc[-1])


def calc_ytd_return(series: pd.Series) -> Optional[float]:
    if series.empty:
        return None
    current_year = datetime.now().year
    year_data = series[series.index.year == current_year]
    if year_data.empty:
        return None
    start_val = safe_float(year_data.iloc[0])
    end_val = safe_float(year_data.iloc[-1])
    if start_val in (None, 0) or end_val is None:
        return None
    return ((end_val / start_val) - 1.0) * 100.0


def get_fred_series(series_id: str) -> pd.DataFrame:
    if not FRED_API_KEY:
        return pd.DataFrame()

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "asc",
    }

    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        obs = data.get("observations", [])
        if not obs:
            return pd.DataFrame()
        df = pd.DataFrame(obs)
        df["date"] = pd.to_datetime(df["date"])
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df[["date", "value"]].dropna()
    except Exception:
        return pd.DataFrame()


def get_latest_fred_value(series_id: str) -> Optional[float]:
    df = get_fred_series(series_id)
    if df.empty:
        return None
    return safe_float(df["value"].iloc[-1])


def get_latest_values() -> Dict[str, Any]:
    data: Dict[str, Any] = {}

    close_cache: Dict[str, pd.Series] = {}
    for key, ticker in TICKERS.items():
        close_cache[key] = get_close_series(ticker, period="12mo")

    for key, series in close_cache.items():
        data[key] = {
            "last": safe_float(series.iloc[-1]) if not series.empty else None,
            "1w_return": calc_return(series, 5),
            "1m_return": calc_return(series, 21),
            "3m_return": calc_return(series, 63),
            "ytd_return": calc_ytd_return(series),
            "50dma": calc_ma(series, 50),
            "200dma": calc_ma(series, 200),
        }

    return data


def compute_market_snapshot(values: Dict[str, Any]) -> Dict[str, Any]:
    spy = values.get("SPY", {})
    qqq = values.get("QQQ", {})
    iwm = values.get("IWM", {})
    rsp = values.get("RSP", {})
    hyg = values.get("HYG", {})
    lqd = values.get("LQD", {})
    vix = values.get("VIX", {})
    vix3m = values.get("VIX3M", {})
    tnx = values.get("TNX", {})
    irx = values.get("IRX", {})

    spy_last = safe_float(spy.get("last"))
    spy_50 = safe_float(spy.get("50dma"))
    spy_200 = safe_float(spy.get("200dma"))

    breadth_ratio = None
    rsp_1m = safe_float(rsp.get("1m_return"))
    spy_1m = safe_float(spy.get("1m_return"))
    if rsp_1m is not None and spy_1m is not None:
        breadth_ratio = rsp_1m - spy_1m

    credit_ratio = None
    hyg_last = safe_float(hyg.get("last"))
    lqd_last = safe_float(lqd.get("last"))
    if hyg_last is not None and lqd_last not in (None, 0):
        credit_ratio = hyg_last / lqd_last

    vol_term_structure = None
    vix_last = safe_float(vix.get("last"))
    vix3m_last = safe_float(vix3m.get("last"))
    if vix_last is not None and vix3m_last not in (None, 0):
        vol_term_structure = vix_last / vix3m_last

    # yfinance treasury symbols are scaled oddly
    ten_year = None
    three_month = None

    tnx_last = safe_float(tnx.get("last"))
    irx_last = safe_float(irx.get("last"))

    if tnx_last is not None:
        ten_year = tnx_last / 10.0
    if irx_last is not None:
        three_month = irx_last / 100.0

    curve_slope = None
    if ten_year is not None and three_month is not None:
        curve_slope = ten_year - three_month

    fed_funds = get_latest_fred_value("FEDFUNDS")
    cpi_yoy = get_latest_fred_value("CPIAUCSL")
    unemployment = get_latest_fred_value("UNRATE")

    cpi_yoy_est = None
    if cpi_yoy is not None:
        cpi_df = get_fred_series("CPIAUCSL")
        if not cpi_df.empty and len(cpi_df) >= 13:
            last = safe_float(cpi_df["value"].iloc[-1])
            year_ago = safe_float(cpi_df["value"].iloc[-13])
            if last is not None and year_ago not in (None, 0):
                cpi_yoy_est = ((last / year_ago) - 1.0) * 100.0

    snapshot = {
        "spy_last": spy_last,
        "spy_1w": safe_float(spy.get("1w_return")),
        "spy_1m": safe_float(spy.get("1m_return")),
        "spy_ytd": safe_float(spy.get("ytd_return")),
        "qqq_1m": safe_float(qqq.get("1m_return")),
        "iwm_1m": safe_float(iwm.get("1m_return")),
        "rsp_1m": safe_float(rsp.get("1m_return")),
        "spy_above_50dma": (spy_last is not None and spy_50 is not None and spy_last > spy_50),
        "spy_above_200dma": (spy_last is not None and spy_200 is not None and spy_last > spy_200),
        "breadth_ratio": breadth_ratio,
        "credit_ratio": credit_ratio,
        "vol_term_structure": vol_term_structure,
        "vix_last": vix_last,
        "ten_year": ten_year,
        "three_month": three_month,
        "curve_slope": curve_slope,
        "fed_funds": fed_funds,
        "cpi_yoy": cpi_yoy_est,
        "unemployment": unemployment,
    }
    return snapshot


def market_regime_text(snapshot: Dict[str, Any]) -> str:
    spy_above_50 = snapshot.get("spy_above_50dma")
    spy_above_200 = snapshot.get("spy_above_200dma")
    breadth = safe_float(snapshot.get("breadth_ratio"))
    vol_ts = safe_float(snapshot.get("vol_term_structure"))
    vix = safe_float(snapshot.get("vix_last"))

    positives = 0
    negatives = 0

    if spy_above_50:
        positives += 1
    else:
        negatives += 1

    if spy_above_200:
        positives += 1
    else:
        negatives += 1

    if breadth is not None:
        if breadth >= 0:
            positives += 1
        else:
            negatives += 1

    if vol_ts is not None:
        if vol_ts < 1:
            positives += 1
        else:
            negatives += 1

    if vix is not None:
        if vix < 18:
            positives += 1
        elif vix > 24:
            negatives += 1

    if positives >= negatives + 2:
        return "constructive"
    if negatives >= positives + 2:
        return "defensive"
    return "mixed"


def build_executive_summary(snapshot: Dict[str, Any]) -> str:
    regime = market_regime_text(snapshot)

    spy_1w = fmt_pct(snapshot.get("spy_1w"))
    spy_1m = fmt_pct(snapshot.get("spy_1m"))
    spy_ytd = fmt_pct(snapshot.get("spy_ytd"))
    vix = fmt_num(snapshot.get("vix_last"), 1)
    ten_year = fmt_num(snapshot.get("ten_year"), 2)
    curve = fmt_num(snapshot.get("curve_slope"), 2)

    if regime == "constructive":
        tone = (
            "The tape remains constructive. Price trend, volatility structure, and broad risk appetite still support measured exposure."
        )
    elif regime == "defensive":
        tone = (
            "The backdrop is defensive. Trend, risk appetite, and internal participation suggest caution remains warranted."
        )
    else:
        tone = (
            "The backdrop is mixed. The market is offering opportunity, but the signals are not clean enough to argue for complacency."
        )

    return (
        f"{tone} Over the past week, SPY returned {spy_1w}. Over the past month, SPY returned {spy_1m}, and the year-to-date move stands at {spy_ytd}. "
        f"Implied volatility remains a key watchpoint, with VIX at {vix}. The 10-year Treasury yield is {ten_year}%, and the 10Y minus 3M curve stands at {curve}%."
    )


def build_market_overview(snapshot: Dict[str, Any]) -> str:
    spy_1w = fmt_pct(snapshot.get("spy_1w"))
    qqq_1m = fmt_pct(snapshot.get("qqq_1m"))
    iwm_1m = fmt_pct(snapshot.get("iwm_1m"))
    ten_year = fmt_num(snapshot.get("ten_year"), 2)
    fed_funds = fmt_num(snapshot.get("fed_funds"), 2)
    cpi = fmt_num(snapshot.get("cpi_yoy"), 2)
    unemployment = fmt_num(snapshot.get("unemployment"), 2)

    return (
        f"U.S. equities continue to digest shifting expectations around growth, inflation, and policy. "
        f"SPY returned {spy_1w} over the last week. Over the last month, QQQ returned {qqq_1m} while IWM returned {iwm_1m}, offering a quick read on large-cap growth leadership versus smaller-cap participation. "
        f"Rates remain central to the macro conversation. The 10-year Treasury yield is {ten_year}%, while the effective fed funds rate is {fed_funds}%. "
        f"On the macro side, estimated CPI year-over-year inflation is {cpi}% and unemployment is {unemployment}%."
    )


def build_equity_market_trends(snapshot: Dict[str, Any]) -> str:
    spy_ytd = fmt_pct(snapshot.get("spy_ytd"))
    qqq_1m = fmt_pct(snapshot.get("qqq_1m"))
    iwm_1m = fmt_pct(snapshot.get("iwm_1m"))
    breadth = safe_float(snapshot.get("breadth_ratio"))
    spy_above_50 = snapshot.get("spy_above_50dma")
    spy_above_200 = snapshot.get("spy_above_200dma")

    breadth_text = "Breadth is keeping up with cap-weighted benchmarks."
    if breadth is not None:
        if breadth < 0:
            breadth_text = (
                "Breadth remains narrower than ideal. Equal weight has lagged cap weight over the last month, which suggests leadership is concentrated."
            )
        else:
            breadth_text = (
                "Breadth has improved. Equal weight has kept pace with or exceeded cap weight over the last month, which points to healthier participation."
            )

    trend_text_parts: List[str] = []
    if spy_above_50:
        trend_text_parts.append("SPY remains above its 50-day moving average")
    else:
        trend_text_parts.append("SPY has slipped below its 50-day moving average")

    if spy_above_200:
        trend_text_parts.append("and it is still above its 200-day moving average")
    else:
        trend_text_parts.append("and it is below its 200-day moving average")

    trend_text = " ".join(trend_text_parts) + "."

    return (
        f"Equity leadership remains important. SPY is up {spy_ytd} year to date. QQQ returned {qqq_1m} over the past month, while IWM returned {iwm_1m}. "
        f"{trend_text} {breadth_text}"
    )


def build_rates_and_macro_backdrop(snapshot: Dict[str, Any]) -> str:
    ten_year = fmt_num(snapshot.get("ten_year"), 2)
    three_month = fmt_num(snapshot.get("three_month"), 2)
    curve = safe_float(snapshot.get("curve_slope"))
    fed_funds = fmt_num(snapshot.get("fed_funds"), 2)
    cpi = fmt_num(snapshot.get("cpi_yoy"), 2)
    unemployment = fmt_num(snapshot.get("unemployment"), 2)

    if curve is None:
        curve_text = "The shape of the front-end versus long-end Treasury curve remains an important watchpoint."
    elif curve < 0:
        curve_text = (
            f"The Treasury curve remains inverted, with 10Y minus 3M at {curve:.2f}%. That still argues for discipline around cyclical risk."
        )
    else:
        curve_text = (
            f"The Treasury curve is positively sloped, with 10Y minus 3M at {curve:.2f}%. That modestly reduces one of the market's longer-running macro stress signals."
        )

    return (
        f"The rates backdrop remains central to positioning. The 10-year Treasury yield is {ten_year}%, while the 3-month Treasury bill yield is {three_month}%. "
        f"{curve_text} The effective fed funds rate is {fed_funds}%. Estimated CPI inflation is {cpi}% year over year, and unemployment is {unemployment}%. "
        f"Together, these data points help frame whether the market is pricing a soft landing, renewed inflation pressure, or slower growth."
    )


def build_institutional_signals(snapshot: Dict[str, Any]) -> str:
    breadth = safe_float(snapshot.get("breadth_ratio"))
    credit_ratio = safe_float(snapshot.get("credit_ratio"))
    vol_ts = safe_float(snapshot.get("vol_term_structure"))
    vix = safe_float(snapshot.get("vix_last"))

    if breadth is None:
        breadth_text = "Breadth data are currently unavailable."
    elif breadth >= 0:
        breadth_text = (
            f"Market breadth has improved. RSP has outperformed or matched SPY over the last month by {breadth:.2f} percentage points. That is usually a healthier sign for internal participation."
        )
    else:
        breadth_text = (
            f"Market breadth is still weaker than ideal. RSP has lagged SPY over the last month by {abs(breadth):.2f} percentage points. That suggests narrower leadership."
        )

    if credit_ratio is None:
        credit_text = "Credit risk appetite data are currently unavailable."
    else:
        credit_text = (
            f"Credit appetite remains a useful cross-asset signal. The HYG-to-LQD price ratio is {credit_ratio:.3f}. A rising ratio typically supports a more constructive read on risk appetite, while a falling ratio argues for more caution."
        )

    if vol_ts is None:
        vol_text = "Volatility term structure data are currently unavailable."
    elif vol_ts < 1:
        vol_text = (
            f"Volatility term structure is constructive. VIX divided by VIX3M is {vol_ts:.2f}, which suggests near-term stress is lower than medium-term implied volatility. VIX itself is {vix:.1f}."
        )
    else:
        vol_text = (
            f"Volatility term structure has turned less friendly. VIX divided by VIX3M is {vol_ts:.2f}, which suggests elevated near-term stress. VIX itself is {vix:.1f}."
        )

    return "\n".join([breadth_text, credit_text, vol_text])


def build_top_risks(snapshot: Dict[str, Any]) -> str:
    risks: List[str] = []

    breadth = safe_float(snapshot.get("breadth_ratio"))
    vol_ts = safe_float(snapshot.get("vol_term_structure"))
    curve = safe_float(snapshot.get("curve_slope"))
    vix = safe_float(snapshot.get("vix_last"))

    if breadth is not None and breadth < 0:
        risks.append(
            "Leadership remains concentrated. Narrow breadth often leaves the market more vulnerable if leadership names lose momentum."
        )

    if vol_ts is not None and vol_ts >= 1:
        risks.append(
            "Volatility term structure is signaling stress. When near-term implied volatility rises above medium-term volatility, market conditions often become less forgiving."
        )

    if curve is not None and curve < 0:
        risks.append(
            "The Treasury curve remains inverted. That does not time a recession, but it continues to argue against complacency in cyclical exposures."
        )

    if vix is not None and vix > 22:
        risks.append(
            "Elevated implied volatility suggests the market is still pricing meaningful uncertainty around growth, policy, or earnings."
        )

    if not risks:
        risks = [
            "The main near-term risk is false comfort. Even when headline index performance looks stable, internals and cross-asset signals can deteriorate before price does.",
            "Policy repricing remains a risk. Shifts in inflation or labor data can move rate expectations quickly and tighten financial conditions."
        ]

    return "\n".join(risks)


def build_closing_takeaways(snapshot: Dict[str, Any]) -> str:
    regime = market_regime_text(snapshot)

    if regime == "constructive":
        return (
            "The primary takeaway is that the market still supports disciplined risk-taking, but position sizing should respect the fact that macro repricing can happen quickly. "
            "Stay with leadership, but keep watching breadth, credit, and volatility structure for early signs of deterioration."
        )
    if regime == "defensive":
        return (
            "The primary takeaway is that preservation matters more than forcing offense. "
            "Until trend, breadth, and volatility structure improve together, it makes sense to stay selective, upgrade quality, and avoid stretching for beta."
        )
    return (
        "The primary takeaway is balance. The market is not broken, but the signal set is mixed enough that investors should prefer selective exposure over broad complacency. "
        "Keep one eye on trend and another on the cross-asset confirmation signals."
    )


def build_appendix_notes() -> str:
    notes = [
        "Data sources include Yahoo Finance market data and optional Federal Reserve Economic Data series if a valid FRED_API_KEY is provided in the environment.",
        "Treasury yield symbols from Yahoo Finance use different scaling conventions. This script normalizes ^TNX and ^IRX for commentary purposes.",
        "Credit spread commentary uses the HYG-to-LQD ratio as a market-based proxy for credit risk appetite when direct spread data are not supplied.",
        "Breadth commentary uses RSP relative to SPY as a practical proxy for equal-weight versus cap-weight participation."
    ]
    return "\n".join(notes)


def build_text_version(packet: Dict[str, Any]) -> str:
    sections = [
        ("TITLE", packet.get("title", "")),
        ("SUBTITLE", packet.get("subtitle", "")),
        ("DATE", packet.get("date", "")),
        ("EXECUTIVE SUMMARY", packet.get("executive_summary", "")),
        ("MARKET OVERVIEW", packet.get("market_overview", "")),
        ("EQUITY MARKET TRENDS", packet.get("equity_market_trends", "")),
        ("RATES AND MACRO BACKDROP", packet.get("rates_and_macro_backdrop", "")),
        ("INSTITUTIONAL SIGNALS", packet.get("institutional_signals", "")),
        ("TOP RISKS", packet.get("top_risks", "")),
        ("CLOSING TAKEAWAYS", packet.get("closing_takeaways", "")),
        ("APPENDIX / NOTES", packet.get("appendix_notes", "")),
    ]

    parts: List[str] = []
    for heading, content in sections:
        parts.append(heading)
        parts.append("=" * len(heading))
        parts.append(str(content).strip())
        parts.append("")

    return "\n".join(parts).strip() + "\n"


def build_packet() -> Dict[str, Any]:
    values = get_latest_values()
    snapshot = compute_market_snapshot(values)

    today = previous_business_day(datetime.now())

    packet = {
        "title": "Weekly Market Packet",
        "subtitle": "Institutional Market Commentary",
        "date": today.strftime("%B %d, %Y"),
        "executive_summary": build_executive_summary(snapshot),
        "market_overview": build_market_overview(snapshot),
        "equity_market_trends": build_equity_market_trends(snapshot),
        "rates_and_macro_backdrop": build_rates_and_macro_backdrop(snapshot),
        "institutional_signals": build_institutional_signals(snapshot),
        "top_risks": build_top_risks(snapshot),
        "closing_takeaways": build_closing_takeaways(snapshot),
        "charts": CHARTS,
        "appendix_notes": build_appendix_notes(),
        "data_snapshot": snapshot,
        "raw_market_values": values,
    }
    return packet


def save_packet(packet: Dict[str, Any]) -> None:
    with open(PACKET_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(packet, f, indent=2, ensure_ascii=False)

    text_version = build_text_version(packet)
    with open(PACKET_TXT_PATH, "w", encoding="utf-8") as f:
        f.write(text_version)


def main() -> None:
    print("[INFO] Building weekly market packet...")
    packet = build_packet()
    save_packet(packet)
    print(f"[OK] JSON packet written to: {PACKET_JSON_PATH}")
    print(f"[OK] Text packet written to: {PACKET_TXT_PATH}")


if __name__ == "__main__":
    main()
