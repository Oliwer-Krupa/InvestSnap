"""Modele danych (dataclassy) dla InvestSnap."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TableSource:
    """Opisuje zrodlo danych: plik tekstowy (CSV) lub arkusz Excel."""

    kind: str  # "text" | "excel"
    path: Path
    sheet_name: str | None = None


@dataclass(frozen=True)
class AccountSnapshot:
    """Podstawowe metryki konta odczytane z naglowka raportu XTB."""

    balance: float | None = None
    equity: float | None = None
    free_margin: float | None = None


@dataclass(frozen=True)
class CashFlowSummary:
    """Podsumowanie przeplywow pienieznych (cash operations)."""

    deposits: float
    withdrawals: float
    net_deposits: float
    dividends_gross: float
    withholding_tax: float
    dividends_net: float
    fees_total: float | None
    other_cash_flows: float


@dataclass(frozen=True)
class FundProfile:
    """Profil sledzonego funduszu inwestycyjnego (do raportow 13F)."""

    name: str
    manager: str
    cik: str  # Numer CIK w SEC EDGAR (bez zer wiodacych)
