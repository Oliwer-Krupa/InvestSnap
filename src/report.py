"""Generowanie raportu Markdown."""

from __future__ import annotations

from datetime import date

import pandas as pd

from src.charts import format_number


def _format_signed_currency(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{format_number(value)} PLN"


def _format_percent(value: float, *, signed: bool = False) -> str:
    text = f"{value:.2f}".replace(".", ",")
    if signed and value > 0:
        text = f"+{text}"
    return f"{text}%"


def _format_optional_currency(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{format_number(value)} PLN"


def _build_portfolio_table(portfolio_df: pd.DataFrame) -> str:
    if portfolio_df.empty:
        return "_Brak otwartych pozycji do zaprezentowania._"

    lines = [
        "| Symbol | Volume | Srednia cena zakupu | Cost (PLN) | Unrealized P/L (PLN) | Unrealized P/L (%) | Current Value (PLN) | Weight (%) |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    for _, row in portfolio_df.iterrows():
        symbol = str(row["Symbol"])
        volume = format_number(float(row["Volume"]))
        avg_buy = format_number(float(row["Avg Buy Price"]))
        cost = format_number(float(row["Cost (PLN)"]))
        unrealized_pl = _format_signed_currency(float(row["Unrealized P/L (PLN)"]))
        unrealized_pct = _format_percent(float(row["Unrealized P/L (%)"]), signed=True)
        current_value = format_number(float(row["Current Value (PLN)"]))
        weight = _format_percent(float(row["Weight (%)"]))

        lines.append(
            f"| {symbol} | {volume} | {avg_buy} | {cost} | {unrealized_pl} | {unrealized_pct} | {current_value} | {weight} |"
        )

    return "\n".join(lines)


def _chart_md(alt_text: str, filename: str | None, fallback: str) -> str:
    return f"![{alt_text}]({filename})" if filename else fallback


def _format_journal(notes_text: str, report_date: date) -> str:
    defaults = {
        "data": report_date.isoformat(),
        "decyzja": "-",
        "uzasadnienie": "-",
        "warunek zmiany zdania": "-",
        "nastepny krok": "-",
    }

    if notes_text:
        parsed_any = False
        for raw_line in notes_text.splitlines():
            line = raw_line.strip()
            if not line or ":" not in line:
                continue
            key_raw, value_raw = line.split(":", 1)
            key = key_raw.strip().lower()
            value = value_raw.strip()
            if key in defaults and value:
                defaults[key] = value
                parsed_any = True

        if not parsed_any:
            defaults["uzasadnienie"] = notes_text.strip()

    lines = [
        f"- Data: {defaults['data']}",
        f"- Decyzja: {defaults['decyzja']}",
        f"- Uzasadnienie: {defaults['uzasadnienie']}",
        f"- Warunek zmiany zdania: {defaults['warunek zmiany zdania']}",
        f"- Nastepny krok: {defaults['nastepny krok']}",
    ]
    return "\n".join(lines)


def _build_delta_section(delta: dict[str, object] | None) -> list[str]:
    if not delta:
        return ["- Brak poprzedniego zapisu porownawczego."]

    if bool(delta.get("missing_previous")):
        return ["- Brak poprzedniego zapisu porownawczego."]

    mover_symbol = str(delta.get("largest_mover_symbol", ""))
    mover_change = float(delta.get("largest_mover_change_pln", 0.0))

    lines = [
        f"- Zmiana wartosci konta (Equity): **{_format_signed_currency(float(delta.get('equity_delta_pln', 0.0)))}**",
        f"- Zmiana wartosci pozycji: **{_format_signed_currency(float(delta.get('positions_value_delta_pln', 0.0)))}**",
        f"- Zmiana gotowki: **{_format_signed_currency(float(delta.get('cash_delta_pln', 0.0)))}**",
        f"- Zmiana wyniku niezrealizowanego: **{_format_signed_currency(float(delta.get('unrealized_delta_pln', 0.0)))}**",
        f"- Zmiana wyniku zrealizowanego: **{_format_signed_currency(float(delta.get('realized_delta_pln', 0.0)))}**",
        f"- Zmiana dywidend netto: **{_format_signed_currency(float(delta.get('dividends_net_delta_pln', 0.0)))}**",
    ]

    if mover_symbol:
        lines.append(f"- Najwiekszy ruch pozycji: **{mover_symbol} ({_format_signed_currency(mover_change)})**")

    return lines


def generate_report(
    *,
    report_date: date,
    data_period_start: date | None = None,
    data_period_end: date | None = None,
    equity_value: float,
    positions_value: float,
    cash_value: float,
    free_margin_value: float | None,
    net_deposits: float,
    total_return_pln: float,
    total_return_pct: float,
    unrealized_pl: float,
    realized_pl: float,
    dividends_gross: float,
    withholding_tax: float,
    dividends_net: float,
    fees_total: float | None,
    other_cash_flows: float,
    components_total: float,
    components_diff: float,
    portfolio_df: pd.DataFrame,
    notes_text: str,
    symbols_chart_name: str | None,
    asset_type_chart_name: str | None,
    region_chart_name: str | None,
    currency_chart_name: str | None,
    uses_manual_symbol_map: bool,
    delta_info: dict[str, object] | None,
    funds_section: str = "",
) -> str:
    """Buduje pelny raport Markdown."""

    portfolio_table = _build_portfolio_table(portfolio_df)
    journal_section = _format_journal(notes_text, report_date)

    symbols_md = _chart_md("Udzial spolek w portfelu", symbols_chart_name, "_Brak danych do wykresu udzialu spolek._")
    asset_md = _chart_md("Udzial wg typu aktywu", asset_type_chart_name, "_Brak danych do wykresu typu aktywu._")
    region_md = _chart_md("Udzial wg regionu", region_chart_name, "_Brak danych do wykresu regionu._")
    currency_md = _chart_md("Udzial wg waluty", currency_chart_name, "_Brak danych do wykresu walut._")

    mapping_note = (
        "_Region/typ/waluta sa przypisane na podstawie recznej mapy symboli (`dane/mapa_symboli.json`)._"
        if uses_manual_symbol_map
        else "_Region/typ/waluta sa przypisane heurystycznie; dla ETF bez mapy stosowany jest fallback do rynku notowania (suffix symbolu). Dodaj `dane/mapa_symboli.json`, aby wymusic mapowanie reczne._"
    )

    fees_line = (
        f"- Fees/commissions/FX/swaps (PLN): **{format_number(fees_total)} PLN**"
        if fees_total is not None
        else "- Fees/commissions/FX/swaps (PLN): **N/A**"
    )

    delta_lines = _build_delta_section(delta_info)

    # Naglowek z okresem danych
    if data_period_start and data_period_end:
        _MONTHS_PL = {
            1: "styczen", 2: "luty", 3: "marzec", 4: "kwiecien",
            5: "maj", 6: "czerwiec", 7: "lipiec", 8: "sierpien",
            9: "wrzesien", 10: "pazdziernik", 11: "listopad", 12: "grudzien",
        }
        period_label = f"{_MONTHS_PL[data_period_end.month]} {data_period_end.year}"
        title = f"# Raport Inwestycyjny — {period_label}"
        subtitle = f"_Okres danych: {data_period_start.isoformat()} — {data_period_end.isoformat()} | Wygenerowano: {report_date.isoformat()}_"
    else:
        title = f"# Raport Inwestycyjny - {report_date.isoformat()}"
        subtitle = ""

    lines = [
        title,
        subtitle,
        "",
        "## 1. Stan konta (Equity / Cash / Positions Value)",
        f"- Calkowita wartosc portfela (Equity): **{format_number(equity_value)} PLN**",
        f"- Wartosc pozycji (Positions Value): **{format_number(positions_value)} PLN**",
        f"- Gotowka (Cash): **{format_number(cash_value)} PLN**",
        (
            f"- Gotowka gotowa do inwestycji (Free margin): **{format_number(free_margin_value)} PLN**"
            if free_margin_value is not None
            else "- Gotowka gotowa do inwestycji (Free margin): **N/A**"
        ),
        f"- Net deposits (deposits - withdrawals): **{format_number(net_deposits)} PLN**",
        f"- Calkowity wynik inwestycji: **{_format_signed_currency(total_return_pln)} ({_format_percent(total_return_pct, signed=True)})**",
        "",
        "## 2. Wynik: rozbicie skladnikow",
        f"- Niezrealizowany wynik (otwarte pozycje): **{_format_signed_currency(unrealized_pl)}**",
        f"- Zrealizowany wynik (zamkniete pozycje): **{_format_signed_currency(realized_pl)}**",
        f"- Dywidendy brutto: **{format_number(dividends_gross)} PLN**",
        f"- Podatek u zrodla (WHT): **{format_number(withholding_tax)} PLN**",
        f"- Dywidendy netto: **{format_number(dividends_net)} PLN**",
        fees_line.replace("Fees/commissions/FX/swaps (PLN)", "Oplaty/prowizje/FX/swap"),
        f"- Pozostale przeplywy gotowkowe: **{_format_signed_currency(other_cash_flows)}**",
        f"- Suma skladnikow wyniku: **{_format_signed_currency(components_total)}**",
        f"- Roznica kontrolna (wynik z Equity - suma skladnikow): **{_format_signed_currency(components_diff)}**",
        "",
        "## 3. Struktura portfela",
        portfolio_table,
        "",
        "### Udzial spolek",
        symbols_md,
        "",
        "## 4. Ekspozycje (AssetType / Region / Currency)",
        "### Typ aktywu",
        asset_md,
        "",
        "### Region",
        region_md,
        "",
        "### Waluta",
        currency_md,
        mapping_note,
        "",
        "## 5. Zmiana od poprzedniego raportu (roznica)",
    ]

    lines.extend(delta_lines)
    lines.extend(
        [
            "",
            "## 6. Dziennik inwestycyjny",
            journal_section,
            "",
            "## 7. Decyzje duzych funduszy (13F SEC EDGAR)",
            funds_section if funds_section else "_Brak danych o funduszach (brak polaczenia z SEC EDGAR lub yfinance)._",
            "",
        ]
    )

    return "\n".join(lines)
