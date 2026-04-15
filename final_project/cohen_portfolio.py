"""
Calendar-time utilities: Yahoo panels, month-end returns, and quarterly EW
portfolios from a cross-sectional **score** (e.g., novelty / impact-adjusted novelty).

Quintile convention (default in this module):
  * **Higher score ⇒ Q5** (long leg)
  * **Lower score ⇒ Q1** (short leg)
  * Long–short return = **Q5 − Q1** (EW means within fiscal quarter).

To reproduce the original Cohen–Diether–Malloy “Lazy Prices” convention where
*lower novelty* is the long leg (“lazy” = Q5), call
``assign_lazy_novelty_quintiles(..., high_is_q5=False)``.

Typical usage::

    import cohen_portfolio as cp
    tickers = cp.top50_sp500_tickers()
    daily = cp.fetch_adj_close_panel_yahoo(tickers, start="2010-01-01")
    monthly = cp.daily_to_month_end_returns(daily)

    events = cp.assign_lazy_novelty_quintiles(events, "novelty_score", "fiscal_quarter")
    events = cp.add_cdm_holding_dates(events, entry_lag_days=4, holding_calendar_days=91)
    daily = cp.fetch_adj_close_panel_yahoo(events["ticker"].unique(), start=..., end=...)
    events = cp.compute_holding_returns_from_panel(events, daily)
    long_df = cp.aggregate_cdm_portfolio_returns(events)
    stats = cp.cdm_performance_stats(long_df)
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Literal, Optional, Sequence, Union

import numpy as np
import pandas as pd

# Same default as novelty notebooks (project root = parent of final_project)
_PROJECT_ROOT = Path(__file__).resolve().parent
_DEFAULT_CACHE_DIR = _PROJECT_ROOT / "pkl_cache"

# Hardcoded fallback (first 50 rows of Wikipedia table order) — matches notebooks
_FALLBACK_TOP50: tuple[str, ...] = (
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "META",
    "GOOGL",
    "GOOG",
    "BRK-B",
    "LLY",
    "AVGO",
    "JPM",
    "TSLA",
    "UNH",
    "V",
    "XOM",
    "MA",
    "JNJ",
    "PG",
    "HD",
    "MRK",
    "COST",
    "ABBV",
    "BAC",
    "CRM",
    "CVX",
    "WFC",
    "AMD",
    "NFLX",
    "KO",
    "PEP",
    "ORCL",
    "TMO",
    "ACN",
    "LIN",
    "MCD",
    "ABT",
    "PM",
    "GE",
    "IBM",
    "QCOM",
    "DHR",
    "AMGN",
    "CAT",
    "GS",
    "INTC",
    "ISRG",
    "SPGI",
    "VZ",
    "BLK",
    "SCHW",
)


def yahoo_ticker(symbol: str) -> str:
    """Wikipedia/CRSP-style symbol → Yahoo (dots to hyphens, e.g. BRK.B → BRK-B)."""
    return str(symbol).strip().upper().replace(".", "-")


def top50_sp500_tickers(*, use_web: bool = True) -> list[str]:
    """
    Tickers for the heavy-NLP “top 50” scope: first 50 symbols in the Wikipedia
    S&P 500 constituents table (same as ``novelty_driven_mean_reversion*.ipynb``).

    If ``use_web`` is False or the scrape fails, returns the hardcoded fallback tuple.
    """
    if not use_web:
        return list(_FALLBACK_TOP50)
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        sp500 = tables[0]
        col = "Symbol" if "Symbol" in sp500.columns else sp500.columns[0]
        return [yahoo_ticker(x) for x in sp500[col].head(50).tolist()]
    except Exception:
        return list(_FALLBACK_TOP50)


def fetch_adj_close_panel_yahoo(
    tickers: Sequence[str],
    start: Union[str, pd.Timestamp],
    end: Optional[Union[str, pd.Timestamp]] = None,
    *,
    threads: bool = True,
    progress: bool = False,
) -> pd.DataFrame:
    """
    Download split- and dividend-adjusted daily **close** prices (one column per ticker).

    Requires ``yfinance``. Rows are calendar days; missing days are NaN (thin trading
    / halts). Failed tickers may be absent from columns — check ``prices.columns``.
    """
    import yfinance as yf

    if not tickers:
        raise ValueError("tickers must be non-empty")
    ysym = [yahoo_ticker(t) for t in tickers]
    # yfinance accepts space-separated list for batch download
    raw = yf.download(
        " ".join(ysym),
        start=start,
        end=end,
        auto_adjust=True,
        progress=progress,
        threads=threads,
        group_by="column",
    )
    if raw.empty:
        return pd.DataFrame()

    if isinstance(raw.columns, pd.MultiIndex):
        # level 0 = field (Open, High, Low, Close, Volume), level 1 = ticker
        if "Close" in raw.columns.get_level_values(0):
            out = raw.xs("Close", axis=1, level=0).copy()
        else:
            raise ValueError(f"Unexpected MultiIndex columns: {raw.columns[:5]} …")
    else:
        # single ticker
        if "Close" in raw.columns:
            out = raw[["Close"]].copy()
            out.columns = [ysym[0]]
        else:
            raise ValueError(f"Expected 'Close' column; got {list(raw.columns)}")

    out.index = pd.to_datetime(out.index).normalize()
    out = out.sort_index()
    # Normalize column names to Yahoo form
    out.columns = [yahoo_ticker(c) for c in out.columns]
    return out


def load_or_fetch_adj_close_panel(
    tickers: Sequence[str],
    start: Union[str, pd.Timestamp],
    end: Union[str, pd.Timestamp],
    *,
    cache_path: Optional[Union[str, Path]] = None,
    refresh: bool = False,
    threads: bool = True,
    progress: bool = False,
) -> pd.DataFrame:
    """
    Load a cached daily **adjusted close** panel if it covers all ``tickers`` and
    ``[start, end]``; otherwise download via Yahoo (possibly widening the stored
    panel to a superset of tickers/dates) and write ``cache_path``.

    Cache file: CSV with DatetimeIndex and one column per Yahoo symbol.
    """
    start_ts = pd.Timestamp(start).normalize()
    end_ts = pd.Timestamp(end).normalize()
    ysym = sorted({yahoo_ticker(t) for t in tickers if pd.notna(t) and str(t).strip()})

    cached: Optional[pd.DataFrame] = None
    cpath = Path(cache_path) if cache_path else None
    if cpath is not None and cpath.is_file() and not refresh:
        cached = pd.read_csv(cpath, index_col=0, parse_dates=True)
        cached.index = pd.to_datetime(cached.index).normalize()
        cached.columns = [yahoo_ticker(c) for c in cached.columns]

    if cached is not None and not refresh:
        miss_syms = [t for t in ysym if t not in cached.columns]
        ext_left = cached.index.min() > start_ts
        ext_right = cached.index.max() < end_ts
        if not miss_syms and not ext_left and not ext_right:
            sub = cached.loc[(cached.index >= start_ts) & (cached.index <= end_ts)]
            return sub.reindex(columns=ysym)

    if cached is not None and not refresh:
        all_syms = sorted(set(ysym) | set(cached.columns))
        dl_start = min(start_ts, cached.index.min())
        dl_end = max(end_ts, cached.index.max())
    else:
        all_syms = ysym
        dl_start, dl_end = start_ts, end_ts

    out = fetch_adj_close_panel_yahoo(
        all_syms, start=dl_start, end=dl_end, threads=threads, progress=progress
    )
    if cpath is not None and not out.empty:
        cpath.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(cpath)
    if out.empty:
        return out
    sub = out.loc[(out.index >= start_ts) & (out.index <= end_ts)]
    return sub.reindex(columns=ysym)


def daily_to_month_end_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Month-end close → simple monthly return (decimals). First month per column is NaN.
    Index is month-end timestamps.
    """
    if prices.empty:
        return prices.copy()
    pe = prices.resample("ME").last()
    return pe.pct_change()


def save_price_panel(
    prices: pd.DataFrame,
    path: Union[str, Path],
    *,
    monthly_returns: Optional[pd.DataFrame] = None,
    monthly_path: Optional[Union[str, Path]] = None,
) -> None:
    """Write CSV(s) under ``pkl_cache`` or a path you choose."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    prices.to_csv(path)
    if monthly_returns is not None and monthly_path is not None:
        mp = Path(monthly_path)
        mp.parent.mkdir(parents=True, exist_ok=True)
        monthly_returns.to_csv(mp)


def load_or_fetch_top50_prices(
    *,
    start: Union[str, pd.Timestamp] = "2005-01-01",
    end: Optional[Union[str, pd.Timestamp]] = None,
    cache_dir: Optional[Path] = None,
    refresh: bool = False,
    use_web_tickers: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    """
    Load cached daily panel + monthly returns if present; else download via Yahoo.

    Returns ``(daily_adj_close, monthly_returns, tickers_used)``.
    Cache files: ``yahoo_top50_adj_close_daily.csv``,
    ``yahoo_top50_monthly_returns.csv``.
    """
    cache_dir = cache_dir or _DEFAULT_CACHE_DIR
    cache_dir.mkdir(parents=True, exist_ok=True)
    daily_p = cache_dir / "yahoo_top50_adj_close_daily.csv"
    monthly_p = cache_dir / "yahoo_top50_monthly_returns.csv"

    if not refresh and daily_p.is_file() and monthly_p.is_file():
        daily = pd.read_csv(daily_p, index_col=0, parse_dates=True)
        monthly = pd.read_csv(monthly_p, index_col=0, parse_dates=True)
        tickers = top50_sp500_tickers(use_web=use_web_tickers)
        return daily, monthly, tickers

    tickers = top50_sp500_tickers(use_web=use_web_tickers)
    daily = fetch_adj_close_panel_yahoo(tickers, start=start, end=end)
    monthly = daily_to_month_end_returns(daily)
    save_price_panel(daily, daily_p, monthly_returns=monthly, monthly_path=monthly_p)
    return daily, monthly, tickers


# ---------------------------------------------------------------------------
# Cohen–Diether–Malloy “Lazy Prices” — quarterly EW portfolios from novelty
# ---------------------------------------------------------------------------

# Quintile labels for pd.qcut: ranks low…high map to these labels.
_QUINTILE_LABELS_HIGH_IS_Q5 = [1, 2, 3, 4, 5]
_QUINTILE_LABELS_HIGH_IS_Q1 = [5, 4, 3, 2, 1]


def prepare_cdm_novelty_from_firm_quarter(
    firm_quarter_signal: pd.DataFrame,
    df_events: pd.DataFrame,
    *,
    score_col: str = "mean_adjusted_novelty",
    merge_keys: tuple[str, str] = ("ticker", "quarter_str"),
    date_cols: tuple[str, ...] = (
        "actual_call_date",
        "mostimportantdateutc",
        "call_date",
    ),
) -> pd.DataFrame:
    """
    Build one row per (ticker, quarter) for the CDM pipeline: ``ticker``,
    ``filing_date`` (call / event date from ``df_events``), ``fiscal_quarter``
    (same string as ``quarter_str``), ``novelty_score`` (e.g. mean adjusted novelty).

    Drops rows with missing dates or scores.
    """
    keys = list(merge_keys)
    for k in keys + [score_col]:
        if k not in firm_quarter_signal.columns:
            raise KeyError(f"firm_quarter_signal missing {k!r}")
    date_pick: Optional[str] = None
    for dc in date_cols:
        if dc in df_events.columns:
            date_pick = dc
            break
    if date_pick is None:
        raise KeyError(
            f"df_events has none of the call-date columns {date_cols!r} — check FINAL.csv load."
        )

    base = firm_quarter_signal[[*keys, score_col]].drop_duplicates(subset=keys, keep="last")
    ev = df_events[[*keys, date_pick]].drop_duplicates(subset=keys, keep="last")
    out = base.merge(ev, on=keys, how="left")
    out = out.rename(
        columns={
            date_pick: "filing_date",
            score_col: "novelty_score",
            merge_keys[1]: "fiscal_quarter",
        }
    )
    out["ticker"] = out["ticker"].astype(str).str.strip().str.upper()
    out["fiscal_quarter"] = out["fiscal_quarter"].astype(str)
    out["filing_date"] = pd.to_datetime(out["filing_date"], errors="coerce")
    out["novelty_score"] = pd.to_numeric(out["novelty_score"], errors="coerce")
    out = out.dropna(subset=["filing_date", "novelty_score"])
    return out[["ticker", "filing_date", "fiscal_quarter", "novelty_score"]].reset_index(
        drop=True
    )


def assign_lazy_novelty_quintiles(
    df: pd.DataFrame,
    score_col: str = "novelty_score",
    quarter_col: str = "fiscal_quarter",
    out_col: str = "novelty_quintile",
    *,
    high_is_q5: bool = True,
    duplicates: str = "drop",
    log_imbalance: bool = True,
) -> pd.DataFrame:
    """
    Cross-sectional quintiles **within** ``quarter_col``.

    - If ``high_is_q5=True`` (default): higher ``score_col`` ⇒ **Q5** (long leg).
    - If ``high_is_q5=False``: higher ``score_col`` ⇒ **Q1** (original CDM mapping).

    Uses ``pd.qcut`` on ``rank(method="first")`` for stable breaks under ties.
    """
    d = df.copy()
    if score_col not in d.columns:
        raise KeyError(score_col)
    if quarter_col not in d.columns:
        raise KeyError(quarter_col)

    labels: list[Literal[1, 2, 3, 4, 5]]
    if high_is_q5:
        labels = _QUINTILE_LABELS_HIGH_IS_Q5
    else:
        labels = _QUINTILE_LABELS_HIGH_IS_Q1

    out = pd.Series(np.nan, index=d.index, dtype=float)
    for qval, grp in d.groupby(quarter_col, sort=False):
        s = grp[score_col]
        r = s.rank(method="first")
        if r.nunique() < 5:
            warnings.warn(
                f"{quarter_col}={qval!r}: fewer than 5 distinct ranks; quintiles NaN for this group."
            )
            continue
        try:
            cat = pd.qcut(
                r,
                q=5,
                labels=labels,
                duplicates=duplicates,
            ).astype(float)
            out.loc[grp.index] = cat.values
        except ValueError as e:
            warnings.warn(f"qcut failed for {quarter_col}={qval!r}: {e}")

    d[out_col] = out

    if log_imbalance:
        for qval, grp in d.dropna(subset=[out_col]).groupby(quarter_col):
            vc = grp[out_col].value_counts().sort_index()
            if len(vc) < 5 or vc.min() < max(1, len(grp) // 12):
                warnings.warn(
                    f"Uneven or incomplete CDM quintile counts in {quarter_col}={qval!r}: {vc.to_dict()}"
                )
    return d


def add_cdm_holding_dates(
    df: pd.DataFrame,
    filing_col: str = "filing_date",
    *,
    entry_lag_days: int = 4,
    holding_calendar_days: int = 91,
    entry_col: str = "entry_date",
    exit_col: str = "exit_date",
) -> pd.DataFrame:
    """
    **4-day** lag after filing before entry; **~one quarter** calendar hold (default 91 days).

    For 10-K-style annual holds, pass ``holding_calendar_days=365`` (calendar) or
    implement trading-day counts in a follow-up.
    """
    d = df.copy()
    if filing_col not in d.columns:
        raise KeyError(filing_col)
    fd = pd.to_datetime(d[filing_col], utc=False).dt.normalize()
    d[entry_col] = fd + pd.Timedelta(days=entry_lag_days)
    d[exit_col] = d[entry_col] + pd.Timedelta(days=holding_calendar_days)
    return d


def _first_price_on_or_after(
    price_index: pd.DatetimeIndex, d: pd.Timestamp
) -> Optional[pd.Timestamp]:
    d = pd.Timestamp(d).normalize()
    hit = price_index[price_index >= d]
    return hit[0] if len(hit) else None


def _last_price_on_or_before(
    price_index: pd.DatetimeIndex, d: pd.Timestamp
) -> Optional[pd.Timestamp]:
    d = pd.Timestamp(d).normalize()
    hit = price_index[price_index <= d]
    return hit[-1] if len(hit) else None


def holding_return_from_price_series(
    prices: pd.Series,
    entry_date: pd.Timestamp,
    exit_date: pd.Timestamp,
) -> float:
    """
    First **trading** observation on/after ``entry_date`` vs last on/before ``exit_date``.
    Returns ``np.nan`` if fewer than two usable prices (no zero-fill).
    """
    if prices is None or prices.empty:
        return np.nan
    idx = pd.DatetimeIndex(pd.to_datetime(prices.index).normalize())
    s = pd.Series(prices.values, index=idx).sort_index()
    s = s.dropna()
    if s.empty:
        return np.nan
    t0 = _first_price_on_or_after(s.index, entry_date)
    t1 = _last_price_on_or_before(s.index, exit_date)
    if t0 is None or t1 is None or t0 >= t1:
        return np.nan
    p0 = float(s.loc[t0])
    p1 = float(s.loc[t1])
    if p0 <= 0:
        return np.nan
    return (p1 / p0) - 1.0


def compute_holding_returns_from_panel(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    *,
    ticker_col: str = "ticker",
    entry_col: str = "entry_date",
    exit_col: str = "exit_date",
    out_col: str = "holding_return",
    log_missing: bool = True,
) -> pd.DataFrame:
    """
    Vectorized-friendly row loop: one Yahoo batch ``prices`` panel for
    ``[min(entry), max(exit)]``; **no per-ticker download**.

    Missing tickers or insufficient prices → ``NaN`` in ``out_col`` (logged if
    ``log_missing``).
    """
    d = events.copy()
    prices = prices.copy()
    prices.index = pd.to_datetime(prices.index).normalize()
    prices.columns = [yahoo_ticker(c) for c in prices.columns]
    out: list[float] = []
    missing_reasons: list[str] = []

    for i, row in d.iterrows():
        sym = yahoo_ticker(row[ticker_col])
        if sym not in prices.columns:
            out.append(np.nan)
            missing_reasons.append(f"{sym}: not in price panel")
            continue
        ser = prices[sym]
        r = holding_return_from_price_series(
            ser,
            row[entry_col],
            row[exit_col],
        )
        out.append(r)
        if np.isnan(r) and log_missing:
            missing_reasons.append(
                f"{sym}: entry={row[entry_col]} exit={row[exit_col]} insufficient prices"
            )

    d[out_col] = out
    if log_missing and missing_reasons:
        n = len(missing_reasons)
        sample = missing_reasons[: min(15, n)]
        warnings.warn(
            f"CDM holding returns: {n} NaN rows (showing up to 15): {sample}"
        )
    return d


def aggregate_cdm_portfolio_returns(
    events: pd.DataFrame,
    *,
    quarter_col: str = "fiscal_quarter",
    quintile_col: str = "novelty_quintile",
    ret_col: str = "holding_return",
) -> pd.DataFrame:
    """
    Equal-weighted mean holding return per (fiscal quarter × quintile).

    Output columns: ``fiscal_quarter``, ``quintile``, ``portfolio_return``,
    ``n_stocks``, ``ls_return`` (Q5 − Q1 for that quarter; repeated on each row).
    """
    use = events.dropna(subset=[ret_col, quintile_col]).copy()
    g = use.groupby([quarter_col, quintile_col], as_index=False)
    long_df = g.agg(
        portfolio_return=(ret_col, "mean"),
        n_stocks=(ret_col, "count"),
    )
    long_df = long_df.rename(columns={quintile_col: "quintile"})

    wide = long_df.pivot_table(
        index=quarter_col,
        columns="quintile",
        values="portfolio_return",
    )
    wide.columns = [int(float(c)) for c in wide.columns]
    if 5 not in wide.columns or 1 not in wide.columns:
        warnings.warn("Cannot compute ls_return: missing quintile 5 and/or 1 in some periods.")
        ls = pd.Series(np.nan, index=wide.index)
    else:
        ls = wide[5].astype(float) - wide[1].astype(float)
    ls_df = ls.rename("ls_return").reset_index()
    long_df = long_df.merge(ls_df, on=quarter_col, how="left")
    return long_df


def cdm_portfolio_returns_wide(
    events: pd.DataFrame,
    *,
    quarter_col: str = "fiscal_quarter",
    quintile_col: str = "novelty_quintile",
    ret_col: str = "holding_return",
) -> pd.DataFrame:
    """Wide table: one row per fiscal quarter, columns Q1…Q5 and ``LS``."""
    use = events.dropna(subset=[ret_col, quintile_col]).copy()
    wide = (
        use.groupby([quarter_col, quintile_col])[ret_col]
        .mean()
        .unstack(quintile_col)
        .sort_index()
    )
    _cols = [int(float(c)) for c in wide.columns]
    wide.columns = _cols
    if 5 in wide.columns and 1 in wide.columns:
        wide["LS"] = wide[5] - wide[1]
    return wide


def cdm_performance_stats(
    wide: pd.DataFrame,
    *,
    periods_per_year: float = 4.0,
) -> pd.DataFrame:
    """
    Time-series summary on **quarterly** portfolio return columns (default annualization √4).

    Rows: ``mean``, ``std``, ``sharpe`` (mean/std × √periods_per_year), ``t_stat``
    (simple ``mean / sem`` per column — not Fama–MacBeth).
    """
    num = wide.select_dtypes(include=[np.number]).copy()
    mean = num.mean()
    std = num.std(ddof=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        sharpe = mean / std * (periods_per_year**0.5)
    def _t_stat(s: pd.Series) -> float:
        s = s.dropna()
        if len(s) < 2:
            return np.nan
        sem = s.sem()
        if sem is None or sem == 0 or (isinstance(sem, float) and np.isnan(sem)):
            return np.nan
        return float(s.mean() / sem)

    t_stat = num.apply(_t_stat)
    # Match common table layout: rows = statistic, columns = portfolio
    return pd.DataFrame(
        {"mean": mean, "std": std, "sharpe": sharpe, "t_stat": t_stat}
    ).T


def run_cdm_lazy_prices_pipeline(
    novelty_df: pd.DataFrame,
    *,
    score_col: str = "novelty_score",
    quarter_col: str = "fiscal_quarter",
    filing_col: str = "filing_date",
    ticker_col: str = "ticker",
    prices: Optional[pd.DataFrame] = None,
    fetch_prices: bool = True,
    price_buffer_days: int = 5,
    entry_lag_days: int = 4,
    holding_calendar_days: int = 91,
    duplicates: str = "drop",
    log_imbalance: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    End-to-end: quintiles → holding dates → (optional) Yahoo batch → returns → long output.

    If ``prices`` is None and ``fetch_prices`` is True, downloads one panel from
    ``min(entry)-buffer`` to ``max(exit)+buffer``.

    Returns ``(events_with_returns, long_portfolio_df, wide_with_LS)``.
    """
    d = assign_lazy_novelty_quintiles(
        novelty_df,
        score_col=score_col,
        quarter_col=quarter_col,
        duplicates=duplicates,
        log_imbalance=log_imbalance,
    )
    d = add_cdm_holding_dates(
        d,
        filing_col=filing_col,
        entry_lag_days=entry_lag_days,
        holding_calendar_days=holding_calendar_days,
    )

    if prices is None and fetch_prices:
        tickers = d[ticker_col].dropna().unique().tolist()
        start = pd.Timestamp(d["entry_date"].min()) - pd.Timedelta(days=price_buffer_days)
        end = pd.Timestamp(d["exit_date"].max()) + pd.Timedelta(days=price_buffer_days)
        prices = fetch_adj_close_panel_yahoo(
            [yahoo_ticker(t) for t in tickers],
            start=start,
            end=end,
        )
    elif prices is None:
        raise ValueError("Provide prices or set fetch_prices=True")

    d = compute_holding_returns_from_panel(d, prices, ticker_col=ticker_col)
    long_out = aggregate_cdm_portfolio_returns(
        d,
        quarter_col=quarter_col,
        quintile_col="novelty_quintile",
    )
    wide_out = cdm_portfolio_returns_wide(
        d,
        quarter_col=quarter_col,
        quintile_col="novelty_quintile",
    )
    return d, long_out, wide_out


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Download Yahoo adjusted closes for S&P top-50 list.")
    p.add_argument("--start", default="2005-01-01", help="YYYY-MM-DD")
    p.add_argument("--end", default=None, help="YYYY-MM-DD (optional)")
    p.add_argument("--refresh", action="store_true", help="Ignore cache and re-download")
    p.add_argument("--no-web-tickers", action="store_true", help="Use hardcoded ticker list")
    p.add_argument(
        "--cache-dir",
        type=Path,
        default=_DEFAULT_CACHE_DIR,
        help="Directory for CSV caches (default: final_project/pkl_cache)",
    )
    args = p.parse_args()
    d, m, t = load_or_fetch_top50_prices(
        start=args.start,
        end=args.end,
        cache_dir=args.cache_dir,
        refresh=args.refresh,
        use_web_tickers=not args.no_web_tickers,
    )
    print(f"Tickers ({len(t)}): {t[:5]} …")
    print(f"Daily panel {d.shape[0]}×{d.shape[1]} → {args.cache_dir / 'yahoo_top50_adj_close_daily.csv'}")
    print(f"Monthly returns {m.shape[0]}×{m.shape[1]} → {args.cache_dir / 'yahoo_top50_monthly_returns.csv'}")
