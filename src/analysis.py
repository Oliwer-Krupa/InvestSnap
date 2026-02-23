"""Logika analizy portfela â€” wzbogacanie pozycji, klasyfikacja, agregacja."""

from __future__ import annotations

import pandas as pd

from src.config import COUNTRY_CODE_MAP, COUNTRY_TO_CURRENCY_MAP, ETF_SYMBOL_HINTS
from src.models import CashFlowSummary
from src.parsers import (
    find_column,
    find_optional_column,
    normalize_column_name,
    normalize_text_value,
    to_numeric_series,
)

# =========================================================================
# Klasyfikacja typu aktywu
# =========================================================================


def _normalize_raw_asset_type(raw: str) -> str:
    norm = normalize_column_name(raw)
    if not norm:
        return ""

    if "etf" in norm or "fund" in norm:
        return "ETF"
    if any(t in norm for t in ("stock", "equity", "share", "akcj")):
        return "Akcje"
    if any(t in norm for t in ("forex", "fx", "walut")):
        return "Forex"
    if any(t in norm for t in ("crypto", "krypto", "coin")):
        return "Krypto"
    if any(t in norm for t in ("commodity", "surow")):
        return "Surowce"
    return ""


def _is_probably_etf_symbol(symbol: str) -> bool:
    base = symbol.split(".", 1)[0].upper().strip()
    if not base:
        return False
    if "ETF" in base or base.endswith("UCITS"):
        return True
    if base in ETF_SYMBOL_HINTS:
        return True
    if base.startswith(("SXR", "IUS", "EUN", "CSP", "SPY", "VWR", "VW")) and len(base) <= 5:
        return True
    return False


def _infer_asset_type(symbol: str) -> str:
    upper = symbol.upper()

    if "/" in upper and len(upper) <= 12:
        return "Forex"

    _COMMODITY_TOKENS = ("XAU", "XAG", "GOLD", "SILVER", "WTI", "BRENT", "OIL", "NGAS", "GAS")
    if any(t in upper for t in _COMMODITY_TOKENS):
        return "Surowce"

    _CRYPTO_TOKENS = ("BTC", "ETH", "LTC", "XRP", "DOGE", "SOL")
    if any(t in upper for t in _CRYPTO_TOKENS):
        return "Krypto"

    if _is_probably_etf_symbol(upper):
        return "ETF"
    if "." in upper:
        return "Akcje"
    return "Inne"


def _resolve_asset_type(raw: str, symbol: str) -> str:
    normalized = _normalize_raw_asset_type(raw)
    return normalized if normalized else _infer_asset_type(symbol)


# =========================================================================
# Geografia
# =========================================================================


def _infer_geography(symbol: str) -> str:
    if "." not in symbol:
        return "Nieznana"
    country_code = symbol.rsplit(".", 1)[1].upper().strip()
    if not country_code:
        return "Nieznana"
    return COUNTRY_CODE_MAP.get(country_code, country_code)


def _infer_currency(symbol: str) -> str:
    upper = symbol.upper().strip()

    if "." in upper:
        country_code = upper.rsplit(".", 1)[1]
        if country_code in COUNTRY_TO_CURRENCY_MAP:
            return COUNTRY_TO_CURRENCY_MAP[country_code]

    if len(upper) >= 6 and upper[:3].isalpha() and upper[3:6].isalpha():
        return upper[:3]

    return "Nieznana"


def _lookup_symbol_meta(
    symbol: str,
    symbol_metadata: dict[str, dict[str, str]] | None,
    field: str,
) -> str:
    if not symbol_metadata:
        return ""

    key = symbol.upper().strip()
    if not key:
        return ""

    raw = symbol_metadata.get(key, {}).get(field, "")
    return normalize_text_value(raw)


def _contains_any(text: str, needles: tuple[str, ...]) -> bool:
    lower = text.lower()
    return any(needle in lower for needle in needles)


# =========================================================================
# Wzbogacanie i podsumowanie pozycji
# =========================================================================

_ASSET_TYPE_ALIASES = ["Asset type", "Instrument type", "Security type", "Category", "Class"]
_GEOGRAPHY_ALIASES = ["Geography", "Country", "Market", "Exchange", "Region"]


def build_positions_enriched_df(
    open_positions_df: pd.DataFrame,
    *,
    symbol_metadata: dict[str, dict[str, str]] | None = None,
) -> pd.DataFrame:
    """Wzbogaca surowe pozycje o kolumny analityczne i klasyfikacje."""
    symbol_col = find_column(open_positions_df, "Symbol")
    purchase_col = find_column(open_positions_df, "Purchase value")
    gross_pl_col = find_column(open_positions_df, "Gross P/L")
    volume_col = find_optional_column(open_positions_df, ["Volume"])
    open_price_col = find_optional_column(open_positions_df, ["Open price"])

    raw_type_col = find_optional_column(open_positions_df, _ASSET_TYPE_ALIASES)
    raw_geo_col = find_optional_column(open_positions_df, _GEOGRAPHY_ALIASES)

    df = open_positions_df.copy()
    df["Symbol"] = df[symbol_col].apply(normalize_text_value)
    df["Purchase value"] = to_numeric_series(df[purchase_col])
    df["Gross P/L"] = to_numeric_series(df[gross_pl_col])
    df["Current Value"] = df["Purchase value"] + df["Gross P/L"]
    df["Volume"] = to_numeric_series(df[volume_col]) if volume_col else 0.0
    df["Open Price"] = to_numeric_series(df[open_price_col]) if open_price_col else 0.0
    df["Open Value"] = df["Open Price"] * df["Volume"]

    # Typ aktywu â€” z kolumny zrodlowej lub heurystyka
    if raw_type_col is not None:
        df["_raw_asset_type"] = df[raw_type_col].apply(normalize_text_value)
        unique_types = {v.lower() for v in df["_raw_asset_type"] if v}
        if unique_types and unique_types.issubset({"buy", "sell"}):
            df["_raw_asset_type"] = ""
    else:
        df["_raw_asset_type"] = ""

    # Geografia â€” z kolumny zrodlowej lub heurystyka
    df["_raw_geography"] = (
        df[raw_geo_col].apply(normalize_text_value) if raw_geo_col else ""
    )

    # Usun wiersze z pustym symbolem
    df = df[~df["Symbol"].str.lower().isin({"", "nan", "none"})]

    # Ręczne metadane per symbol (najwyższy priorytet)
    df["_meta_asset_type"] = df["Symbol"].apply(
        lambda s: _lookup_symbol_meta(s, symbol_metadata, "asset_type")
    )
    df["_meta_region"] = df["Symbol"].apply(
        lambda s: _lookup_symbol_meta(s, symbol_metadata, "region")
    )
    df["_meta_currency"] = df["Symbol"].apply(
        lambda s: _lookup_symbol_meta(s, symbol_metadata, "currency")
    )

    df["Asset Type"] = df.apply(
        lambda r: (
            r["_meta_asset_type"]
            if r["_meta_asset_type"]
            else _resolve_asset_type(r["_raw_asset_type"], r["Symbol"])
        ),
        axis=1,
    )
    df["Geography"] = df.apply(
        lambda r: (
            r["_meta_region"]
            if r["_meta_region"]
            else (
                r["_raw_geography"]
                if r["_raw_geography"]
                else _infer_geography(r["Symbol"])
            )
        ),
        axis=1,
    )
    df["Currency"] = df.apply(
        lambda r: r["_meta_currency"] if r["_meta_currency"] else _infer_currency(r["Symbol"]),
        axis=1,
    )

    return df[
        [
            "Symbol",
            "Purchase value",
            "Gross P/L",
            "Current Value",
            "Volume",
            "Open Price",
            "Open Value",
            "Asset Type",
            "Geography",
            "Currency",
        ]
    ].copy()


def summarize_portfolio(positions_df: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    """Grupuje pozycje, liczy udzialy procentowe. Zwraca (DataFrame, laczna wartosc)."""
    grouped = (
        positions_df.groupby("Symbol", dropna=False)[
            ["Purchase value", "Gross P/L", "Current Value", "Volume", "Open Value"]
        ]
        .sum()
        .reset_index()
    )
    grouped["Symbol"] = grouped["Symbol"].astype(str).str.strip()
    grouped = grouped[grouped["Current Value"] > 0]

    grouped["Avg Buy Price"] = grouped.apply(
        lambda r: (r["Open Value"] / r["Volume"]) if r["Volume"] > 0 and r["Open Value"] > 0
        else ((r["Purchase value"] / r["Volume"]) if r["Volume"] > 0 else 0.0),
        axis=1,
    )
    grouped["Cost (PLN)"] = grouped["Purchase value"]
    grouped["Unrealized P/L (PLN)"] = grouped["Gross P/L"]
    grouped["Unrealized P/L (%)"] = grouped.apply(
        lambda r: (r["Gross P/L"] / r["Purchase value"] * 100) if r["Purchase value"] else 0.0,
        axis=1,
    )
    grouped["Current Value (PLN)"] = grouped["Current Value"]

    total = grouped["Current Value"].sum()
    grouped["Weight (%)"] = (grouped["Current Value"] / total * 100) if total else 0.0

    grouped = grouped[
        [
            "Symbol",
            "Volume",
            "Avg Buy Price",
            "Cost (PLN)",
            "Unrealized P/L (PLN)",
            "Unrealized P/L (%)",
            "Current Value (PLN)",
            "Weight (%)",
        ]
    ]
    grouped = grouped.sort_values("Current Value (PLN)", ascending=False).reset_index(drop=True)
    return grouped, float(total)


def summarize_cash_operations(cash_df: pd.DataFrame) -> CashFlowSummary:
    """Podsumowuje cashflowy: wpłaty/wypłaty/dywidendy/opłaty i przeplywy pozostale."""
    id_col = find_optional_column(cash_df, ["ID"])
    type_col = find_column(cash_df, "Type")
    amount_col = find_column(cash_df, "Amount")
    comment_col = find_optional_column(cash_df, ["Comment", "Description", "Details"])

    df = cash_df.copy()
    df["_type"] = df[type_col].apply(normalize_text_value)
    df["_comment"] = df[comment_col].apply(normalize_text_value) if comment_col else ""
    df["_row_text"] = (df["_type"] + " " + df["_comment"]).str.lower()
    df["_amount"] = to_numeric_series(df[amount_col])

    if id_col is not None:
        id_text = df[id_col].apply(normalize_text_value).str.lower()
        df = df[~id_text.isin({"total", "subtotal"})]
    df = df[~df["_type"].str.lower().isin({"total", "subtotal"})]

    deposit_mask = df["_row_text"].apply(lambda t: _contains_any(t, ("deposit",)))
    withdrawal_mask = df["_row_text"].apply(lambda t: _contains_any(t, ("withdraw", "withdrawal")))
    dividend_mask = df["_row_text"].apply(lambda t: _contains_any(t, ("dividend", "divident")))
    withholding_mask = df["_row_text"].apply(lambda t: _contains_any(t, ("withholding tax", "wht")))
    fee_mask = df["_row_text"].apply(
        lambda t: _contains_any(
            t,
            ("commission", "fee", "swap", "rollover", "conversion", "fx", "interest tax"),
        )
    )
    realized_cash_mask = df["_row_text"].apply(lambda t: _contains_any(t, ("close trade",)))
    trade_cash_mask = df["_row_text"].apply(lambda t: _contains_any(t, ("stock purchase", "stock sell")))

    deposits = float(df.loc[deposit_mask, "_amount"].sum())
    withdrawals = float(df.loc[withdrawal_mask, "_amount"].sum())
    net_deposits = deposits - abs(withdrawals)

    dividends_gross = float(df.loc[dividend_mask, "_amount"].sum())
    withholding_tax = float(df.loc[withholding_mask, "_amount"].sum())
    dividends_net = float(dividends_gross + withholding_tax)

    fee_rows = df.loc[fee_mask, "_amount"]
    fees_total = float(fee_rows.sum()) if not fee_rows.empty else None

    excluded_mask = deposit_mask | withdrawal_mask | dividend_mask | withholding_mask | fee_mask | realized_cash_mask | trade_cash_mask
    other_cash_flows = float(df.loc[~excluded_mask, "_amount"].sum())

    return CashFlowSummary(
        deposits=deposits,
        withdrawals=withdrawals,
        net_deposits=net_deposits,
        dividends_gross=dividends_gross,
        withholding_tax=withholding_tax,
        dividends_net=dividends_net,
        fees_total=fees_total,
        other_cash_flows=other_cash_flows,
    )


def summarize_closed_positions(closed_df: pd.DataFrame) -> float:
    """Zwraca Realized P/L (PLN) jako sume Gross P/L z zamknietych pozycji."""
    gross_col = find_column(closed_df, "Gross P/L")
    position_col = find_optional_column(closed_df, ["Position"])

    df = closed_df.copy()
    if position_col is not None:
        pos_text = df[position_col].apply(normalize_text_value).str.lower()
        df = df[~pos_text.isin({"total", "subtotal"})]

    df["_gross"] = to_numeric_series(df[gross_col])
    return float(df["_gross"].sum())


# =========================================================================
# Agregacja danych do wykresow
# =========================================================================


def _top_n_with_other(series: pd.Series, top_n: int) -> pd.Series:
    """Obcina serie do top_n elementow, reszta laduje do kategorii 'Inne'."""
    series = series[series > 0]
    if series.empty or len(series) <= top_n:
        return series

    other_sum = series.iloc[top_n:].sum()
    result = series.iloc[:top_n].copy()
    if other_sum > 0:
        result.loc["Inne"] = other_sum
    return result


def aggregate_for_pie(positions_df: pd.DataFrame, group_column: str, *, top_n: int = 8) -> pd.Series:
    """Grupuje pozycje po danej kolumnie; zwraca serie do wykresu kolowego."""
    grouped = (
        positions_df.groupby(group_column)["Current Value"]
        .sum()
        .sort_values(ascending=False)
    )
    return _top_n_with_other(grouped, top_n)


def aggregate_geography_for_chart(
    positions_df: pd.DataFrame,
    etf_overrides: dict[str, dict[str, float]],
    *,
    top_n: int = 8,
) -> pd.Series:
    """Agreguje geograficznie, z opcjonalnym look-through dla ETF."""
    bucket: dict[str, float] = {}

    for _, row in positions_df.iterrows():
        symbol = str(row["Symbol"]).upper().strip()
        asset_type = str(row["Asset Type"]).strip().upper()
        geography = normalize_text_value(row["Geography"]) or "Nieznana"
        current_value = float(row["Current Value"])

        if current_value <= 0:
            continue

        if asset_type == "ETF" and symbol in etf_overrides:
            for region, ratio in etf_overrides[symbol].items():
                bucket[region] = bucket.get(region, 0.0) + current_value * ratio
        else:
            bucket[geography] = bucket.get(geography, 0.0) + current_value

    grouped = pd.Series(bucket, dtype=float).sort_values(ascending=False)
    return _top_n_with_other(grouped, top_n)


def build_symbol_share_series(portfolio_df: pd.DataFrame, *, top_n: int = 12) -> pd.Series:
    """Tworzy serie udzialow procentowych poszczegolnych spolek do wykresu."""
    if portfolio_df.empty:
        return pd.Series(dtype=float)

    weight_column = "Weight (%)" if "Weight (%)" in portfolio_df.columns else "Weight %"
    series = (
        portfolio_df[["Symbol", weight_column]]
        .dropna(subset=["Symbol", weight_column])
        .set_index("Symbol")[weight_column]
        .sort_values(ascending=False)
    )
    return _top_n_with_other(series, top_n)
