"""Punkt wejscia: ``python -m src``."""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime

from src.analysis import (
    aggregate_for_pie,
    build_positions_enriched_df,
    build_symbol_share_series,
    summarize_cash_operations,
    summarize_closed_positions,
    summarize_portfolio,
)
from src.charts import save_pie_chart
from src.config import (
    CASH_OPERATIONS_MARKER,
    CLOSED_POSITION_MARKER,
    DATA_DIR,
    ENABLE_FUND_TRACKING,
    FUNDS_CACHE_DIR,
    NOTES_FILE,
    OPEN_POSITION_MARKER,
    REPORTS_DIR,
    REQUIRED_CASH_COLUMNS,
    REQUIRED_CLOSED_COLUMNS,
    REQUIRED_OPEN_COLUMNS,
    SNAPSHOTS_DIR,
    SYMBOL_METADATA_FILE,
    TRACKED_FUNDS,
)
from src.parsers import (
    extract_account_snapshot,
    extract_date_range_from_path,
    find_table_source,
    load_source_table,
    load_symbol_metadata,
    read_and_clear_notes,
)
from src.report import generate_report
from src.snapshots import build_delta, load_latest_snapshot, save_snapshot
from src.funds import fetch_fund_reports, format_fund_reports_section

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Glowna procedura generowania raportu."""

    # 1. Wczytaj dane zrodlowe
    open_source = find_table_source(DATA_DIR, OPEN_POSITION_MARKER)
    cash_source = find_table_source(DATA_DIR, CASH_OPERATIONS_MARKER)

    open_positions_df = load_source_table(open_source, REQUIRED_OPEN_COLUMNS)
    cash_operations_df = load_source_table(cash_source, REQUIRED_CASH_COLUMNS)

    closed_positions_df = None
    try:
        closed_source = find_table_source(DATA_DIR, CLOSED_POSITION_MARKER)
        closed_positions_df = load_source_table(closed_source, REQUIRED_CLOSED_COLUMNS)
    except Exception as exc:
        logger.warning("Nie znaleziono danych CLOSED POSITION HISTORY: %s", exc)

    # 2. Analiza i metryki
    symbol_metadata = load_symbol_metadata(SYMBOL_METADATA_FILE)

    positions_df = build_positions_enriched_df(
        open_positions_df,
        symbol_metadata=symbol_metadata,
    )
    portfolio_df, positions_value = summarize_portfolio(positions_df)

    cash_summary = summarize_cash_operations(cash_operations_df)
    realized_pl = summarize_closed_positions(closed_positions_df) if closed_positions_df is not None else 0.0
    unrealized_pl = float(positions_df["Gross P/L"].sum()) if not positions_df.empty else 0.0

    account_snapshot = extract_account_snapshot(open_source)
    notes_text = read_and_clear_notes(NOTES_FILE)

    cash_value = (
        account_snapshot.balance
        if account_snapshot.balance is not None
        else (account_snapshot.free_margin if account_snapshot.free_margin is not None else 0.0)
    )
    equity_value = positions_value + cash_value

    net_deposits = cash_summary.net_deposits
    total_return_pln = equity_value - net_deposits
    total_return_pct = (total_return_pln / net_deposits * 100) if net_deposits else 0.0

    fees_for_components = cash_summary.fees_total if cash_summary.fees_total is not None else 0.0
    components_total = (
        unrealized_pl
        + realized_pl
        + cash_summary.dividends_net
        + fees_for_components
        + cash_summary.other_cash_flows
    )
    components_diff = total_return_pln - components_total

    # 3. Snapshot i delty
    current_metrics = {
        "equity": float(equity_value),
        "positions_value": float(positions_value),
        "cash": float(cash_value),
        "unrealized_pl": float(unrealized_pl),
        "realized_pl": float(realized_pl),
        "dividends_net": float(cash_summary.dividends_net),
    }
    current_positions = {
        str(row["Symbol"]): float(row["Current Value (PLN)"])
        for _, row in portfolio_df.iterrows()
    }

    previous_snapshot = load_latest_snapshot(SNAPSHOTS_DIR)
    delta_info = build_delta(
        current_metrics=current_metrics,
        current_positions=current_positions,
        previous_snapshot=previous_snapshot,
    )

    snapshot_payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "metrics": current_metrics,
        "positions": current_positions,
    }
    save_snapshot(SNAPSHOTS_DIR, snapshot_payload)

    # 4. Analiza decyzji duzych funduszy (13F)
    funds_section = ""
    if ENABLE_FUND_TRACKING and TRACKED_FUNDS:
        try:
            fund_reports = fetch_fund_reports(TRACKED_FUNDS, FUNDS_CACHE_DIR)
            funds_section = format_fund_reports_section(fund_reports)
            logger.info("Pobrano dane 13F dla %d funduszy.", len(fund_reports))
        except Exception as exc:
            logger.warning("Blad pobierania danych 13F: %s", exc)
            funds_section = f"_Blad pobierania danych z SEC EDGAR: {exc}_"

    # 5. Wykresy
    today = date.today()
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    symbols_series = build_symbol_share_series(portfolio_df, top_n=15)
    asset_type_series = aggregate_for_pie(positions_df, "Asset Type")
    region_series = aggregate_for_pie(positions_df, "Geography")
    currency_series = aggregate_for_pie(positions_df, "Currency")

    chart_specs: list[tuple[str, str]] = [
        ("Wykres_spolki", "Udzial spolek w portfelu"),
        ("Wykres_typ_aktywu", "Struktura portfela: typ aktywu"),
        ("Wykres_region", "Struktura portfela: region"),
        ("Wykres_waluta", "Struktura portfela: waluta"),
    ]
    series_list = [symbols_series, asset_type_series, region_series, currency_series]
    chart_names: list[str | None] = []

    for (prefix, title), data in zip(chart_specs, series_list):
        path = REPORTS_DIR / f"{prefix}_{today:%Y_%m}.png"
        saved = save_pie_chart(data, title, path)
        chart_names.append(path.name if saved else None)

    # 6. Okres danych (z nazwy pliku zrodlowego)
    data_start, data_end = extract_date_range_from_path(open_source.path)

    # 7. Raport Markdown
    report_md = generate_report(
        report_date=today,
        data_period_start=data_start,
        data_period_end=data_end,
        equity_value=equity_value,
        positions_value=positions_value,
        cash_value=cash_value,
        free_margin_value=account_snapshot.free_margin,
        net_deposits=net_deposits,
        total_return_pln=total_return_pln,
        total_return_pct=total_return_pct,
        unrealized_pl=unrealized_pl,
        realized_pl=realized_pl,
        dividends_gross=cash_summary.dividends_gross,
        withholding_tax=cash_summary.withholding_tax,
        dividends_net=cash_summary.dividends_net,
        fees_total=cash_summary.fees_total,
        other_cash_flows=cash_summary.other_cash_flows,
        components_total=components_total,
        components_diff=components_diff,
        portfolio_df=portfolio_df,
        notes_text=notes_text,
        symbols_chart_name=chart_names[0],
        asset_type_chart_name=chart_names[1],
        region_chart_name=chart_names[2],
        currency_chart_name=chart_names[3],
        uses_manual_symbol_map=bool(symbol_metadata),
        delta_info=delta_info,
        funds_section=funds_section,
    )

    output_path = REPORTS_DIR / f"Raport_{today:%Y_%m}.md"
    output_path.write_text(report_md, encoding="utf-8")
    logger.info("Raport zapisany: %s", output_path)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.error("Blad: %s", exc)
        sys.exit(1)
