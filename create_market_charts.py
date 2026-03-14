import os
import json
from typing import Dict, Any, List, Tuple

import matplotlib.pyplot as plt

OUTPUT_DIR = "output"
ROOT_WEEKLY_DATA_FILENAME = "weekly_market_data.json"


def ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_weekly_data() -> Dict[str, Any]:
    if not os.path.exists(ROOT_WEEKLY_DATA_FILENAME):
        raise FileNotFoundError(f"Could not find {ROOT_WEEKLY_DATA_FILENAME}")

    with open(ROOT_WEEKLY_DATA_FILENAME, "r", encoding="utf-8") as f:
        return json.load(f)


def safe_num(x):
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def save_chart(fig, filename: str) -> str:
    ensure_output_dir()
    path = os.path.join(OUTPUT_DIR, filename)
    fig.tight_layout()
    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"Chart written to {path}")
    return path


def build_market_dashboard_chart(data: Dict[str, Any]) -> str:
    market = data["market_snapshot"]
    eq = market["equities"]
    fi = market["fixed_income"]
    commodities = market["commodities"]

    items: List[Tuple[str, float]] = [
        ("S&P 500", safe_num(eq.get("sp500_weekly_return_pct"))),
        ("Nasdaq", safe_num(eq.get("nasdaq_weekly_return_pct"))),
        ("Dow", safe_num(eq.get("dow_weekly_return_pct"))),
        ("Russell 2000", safe_num(eq.get("russell2000_weekly_return_pct"))),
        ("AGG", safe_num(fi.get("agg_bond_return_pct"))),
        ("LQD", safe_num(fi.get("investment_grade_return_pct"))),
        ("HYG", safe_num(fi.get("high_yield_return_pct"))),
        ("Oil", safe_num(commodities.get("oil_return_pct"))),
        ("Gold", safe_num(commodities.get("gold_return_pct"))),
    ]

    items = [(label, value) for label, value in items if value is not None]

    labels = [x[0] for x in items]
    values = [x[1] for x in items]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(labels, values)
    ax.axvline(0, linewidth=1)
    ax.set_title("Weekly Market Dashboard")
    ax.set_xlabel("Weekly Return (%)")
    ax.invert_yaxis()

    return save_chart(fig, "market_dashboard.png")


def build_sector_leadership_chart(data: Dict[str, Any]) -> str:
    sector_rank = data["market_snapshot"]["sector_rank"]
    top = sector_rank.get("top_sectors", [])
    bottom = sector_rank.get("bottom_sectors", [])

    combined = top + bottom
    labels = [x["sector"].replace("_", " ").title() for x in combined]
    values = [safe_num(x["return_pct"]) for x in combined]

    filtered = [(l, v) for l, v in zip(labels, values) if v is not None]
    labels = [x[0] for x in filtered]
    values = [x[1] for x in filtered]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(labels, values)
    ax.axvline(0, linewidth=1)
    ax.set_title("Sector Leadership")
    ax.set_xlabel("Weekly Return (%)")
    ax.invert_yaxis()

    return save_chart(fig, "sector_leadership.png")


def build_rates_curve_chart(data: Dict[str, Any]) -> str:
    rates = data["market_snapshot"]["rates"]
    ycurve = data["market_snapshot"]["yield_curve"]

    labels = ["2-Year", "10-Year", "2s10s Spread"]
    values = [
        safe_num(rates.get("two_year_yield")),
        safe_num(rates.get("ten_year_yield")),
        safe_num(ycurve.get("current_2s10s_spread_pct")),
    ]

    filtered = [(l, v) for l, v in zip(labels, values) if v is not None]
    labels = [x[0] for x in filtered]
    values = [x[1] for x in filtered]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(labels, values)
    ax.axhline(0, linewidth=1)
    ax.set_title("Treasury Rates and Yield Curve")
    ax.set_ylabel("Percent / Spread")

    return save_chart(fig, "rates_curve.png")


def build_breadth_credit_chart(data: Dict[str, Any]) -> str:
    breadth = data["market_snapshot"]["breadth"]
    credit = data["market_snapshot"]["credit"]

    labels = [
        "Positive Sector %",
        "HY OAS",
        "IG OAS",
    ]
    values = [
        safe_num(breadth.get("positive_sector_pct")),
        safe_num(credit.get("high_yield_oas", {}).get("latest")),
        safe_num(credit.get("investment_grade_oas", {}).get("latest")),
    ]

    filtered = [(l, v) for l, v in zip(labels, values) if v is not None]
    labels = [x[0] for x in filtered]
    values = [x[1] for x in filtered]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(labels, values)
    ax.set_title("Breadth and Credit")
    ax.set_ylabel("Level")

    return save_chart(fig, "breadth_credit.png")


def main() -> None:
    data = load_weekly_data()

    build_market_dashboard_chart(data)
    build_sector_leadership_chart(data)
    build_rates_curve_chart(data)
    build_breadth_credit_chart(data)

    print("All market charts created successfully.")


if __name__ == "__main__":
    main()

