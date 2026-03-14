from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import yfinance as yf


OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)


def get_close_series(ticker: str, period: str = "12mo") -> pd.Series:
    df = yf.download(
        ticker,
        period=period,
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    if df is None or df.empty:
        return pd.Series(dtype=float)

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    if "Close" not in df.columns:
        return pd.Series(dtype=float)

    return df["Close"].dropna()


def save_spx_trend_chart() -> None:
    spy = get_close_series("SPY", "12mo")
    if spy.empty:
        print("[WARN] Could not build SPY chart.")
        return

    ma50 = spy.rolling(50).mean()
    ma200 = spy.rolling(200).mean()

    plt.figure(figsize=(10, 6))
    plt.plot(spy.index, spy.values, label="SPY")
    plt.plot(ma50.index, ma50.values, label="50DMA")
    plt.plot(ma200.index, ma200.values, label="200DMA")
    plt.title("S&P 500 Trend")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "spx_trend.png", dpi=200, bbox_inches="tight")
    plt.close()


def save_yield_curve_chart() -> None:
    tnx = get_close_series("^TNX", "6mo")
    irx = get_close_series("^IRX", "6mo")

    if tnx.empty or irx.empty:
        print("[WARN] Could not build yield curve chart.")
        return

    tnx = tnx / 10.0
    irx = irx / 100.0

    combined = pd.concat(
        [
            irx.rename("3M T-Bill"),
            tnx.rename("10Y Treasury"),
        ],
        axis=1
    ).dropna()

    if combined.empty:
        print("[WARN] Combined yield data is empty.")
        return

    plt.figure(figsize=(10, 6))
    plt.plot(combined.index, combined["3M T-Bill"], label="3M T-Bill")
    plt.plot(combined.index, combined["10Y Treasury"], label="10Y Treasury")
    plt.title("Treasury Yield Comparison")
    plt.xlabel("Date")
    plt.ylabel("Yield (%)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "yield_curve.png", dpi=200, bbox_inches="tight")
    plt.close()


def save_credit_proxy_chart() -> None:
    hyg = get_close_series("HYG", "12mo")
    lqd = get_close_series("LQD", "12mo")

    if hyg.empty or lqd.empty:
        print("[WARN] Could not build credit proxy chart.")
        return

    ratio = (hyg / lqd).dropna()
    if ratio.empty:
        print("[WARN] Credit ratio series is empty.")
        return

    plt.figure(figsize=(10, 6))
    plt.plot(ratio.index, ratio.values, label="HYG / LQD")
    plt.title("Credit Risk Proxy")
    plt.xlabel("Date")
    plt.ylabel("Ratio")
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "credit_spreads.png", dpi=200, bbox_inches="tight")
    plt.close()


def main() -> None:
    print("[INFO] Creating market charts...")
    save_spx_trend_chart()
    save_yield_curve_chart()
    save_credit_proxy_chart()
    print("[OK] Charts created in output/")


if __name__ == "__main__":
    main()
