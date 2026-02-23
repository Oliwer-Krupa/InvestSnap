"""Stale konfiguracyjne i sciezki projektu InvestSnap."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Wczytaj .env (jesli istnieje) — klucze API, sciezki, flagi
# ---------------------------------------------------------------------------
_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_ENV_PATH)

# ---------------------------------------------------------------------------
# Sciezki (nadpisywalne przez .env)
# ---------------------------------------------------------------------------
DATA_DIR = Path(os.getenv("DATA_DIR", "dane"))
REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "raporty"))
SNAPSHOTS_DIR = REPORTS_DIR / "_snapshots"
NOTES_FILE = DATA_DIR / "notatki.txt"
ETF_GEO_OVERRIDES_FILE = DATA_DIR / "etf_geografia.json"
SYMBOL_METADATA_FILE = DATA_DIR / "mapa_symboli.json"

# ---------------------------------------------------------------------------
# Markery zrodlowe â€” identyfikuja arkusze / pliki
# ---------------------------------------------------------------------------
OPEN_POSITION_MARKER = "OPEN POSITION"
CASH_OPERATIONS_MARKER = "CASH OPERATION HISTORY"
CLOSED_POSITION_MARKER = "CLOSED POSITION HISTORY"

# ---------------------------------------------------------------------------
# Wymagane kolumny
# ---------------------------------------------------------------------------
REQUIRED_OPEN_COLUMNS: list[str] = [
    "Position",
    "Symbol",
    "Purchase value",
    "Gross P/L",
]
REQUIRED_CASH_COLUMNS: list[str] = ["ID", "Type", "Amount"]
REQUIRED_CLOSED_COLUMNS: list[str] = ["Position", "Symbol", "Gross P/L"]

# ---------------------------------------------------------------------------
# Rozszerzenia plikow
# ---------------------------------------------------------------------------
TEXT_EXTENSIONS: frozenset[str] = frozenset({".csv", ".txt"})
EXCEL_EXTENSIONS: frozenset[str] = frozenset({".xlsx", ".xls", ".xlsm"})

# ---------------------------------------------------------------------------
# Maksymalna liczba wierszy skanowanych przy wykrywaniu naglowka
# ---------------------------------------------------------------------------
HEADER_SCAN_MAX_ROWS = 120

# ---------------------------------------------------------------------------
# Symbole ETF (heurystyka)
# ---------------------------------------------------------------------------
ETF_SYMBOL_HINTS: frozenset[str] = frozenset({
    "SPY", "IVV", "VOO", "QQQ", "IWM", "DIA", "VTI", "VEA", "VWO",
    "AGG", "BND", "GLD", "SLV", "TLT", "HYG", "LQD",
    "XLK", "XLF", "XLE", "XLY", "XLI", "XLP", "XLV", "XLU", "XLB", "XLRE",
    "CSPX", "SXR8", "VWCE", "IWDA", "EUNL", "VUSA", "VWRL", "VUAA",
    "IS3N", "EMIM", "IUSQ", "CNDX", "EQQQ", "QDVE", "SPYL", "LTAM", "AMEL",
})

# ---------------------------------------------------------------------------
# Paleta kolorow wykresow
# ---------------------------------------------------------------------------
DONUT_COLORS: list[str] = [
    "#0B3954", "#087E8B", "#FF5A5F", "#C81D25", "#F4D35E",
    "#7FB069", "#5B8E7D", "#5E6472", "#9A8C98", "#2B2D42",
]

# ---------------------------------------------------------------------------
# Mapa kodow krajow
# ---------------------------------------------------------------------------
COUNTRY_CODE_MAP: dict[str, str] = {
    "US": "USA",
    "PL": "Polska",
    "DE": "Niemcy",
    "NL": "Niderlandy",
    "GB": "Wielka Brytania",
    "FR": "Francja",
    "IT": "Wlochy",
    "ES": "Hiszpania",
    "SE": "Szwecja",
    "NO": "Norwegia",
    "DK": "Dania",
    "FI": "Finlandia",
    "CH": "Szwajcaria",
    "AT": "Austria",
    "BE": "Belgia",
    "IE": "Irlandia",
    "CA": "Kanada",
    "AU": "Australia",
    "JP": "Japonia",
    "HK": "Hong Kong",
    "SG": "Singapur",
    "KR": "Korea Poludniowa",
    "CN": "Chiny",
}

COUNTRY_TO_CURRENCY_MAP: dict[str, str] = {
    "US": "USD",
    "PL": "PLN",
    "DE": "EUR",
    "NL": "EUR",
    "FR": "EUR",
    "IT": "EUR",
    "ES": "EUR",
    "BE": "EUR",
    "IE": "EUR",
    "AT": "EUR",
    "FI": "EUR",
    "PT": "EUR",
    "LU": "EUR",
    "GB": "GBP",
    "CH": "CHF",
    "JP": "JPY",
    "HK": "HKD",
    "SG": "SGD",
    "CA": "CAD",
    "AU": "AUD",
    "NO": "NOK",
    "SE": "SEK",
    "DK": "DKK",
}

# ---------------------------------------------------------------------------
# SEC EDGAR / OpenFIGI (z .env)
# ---------------------------------------------------------------------------
SEC_USER_AGENT: str = os.getenv("SEC_USER_AGENT", "InvestSnap/1.0 (investsnap-report-bot)")
OPENFIGI_API_KEY: str = os.getenv("OPENFIGI_API_KEY", "")
ENABLE_FUND_TRACKING: bool = os.getenv("ENABLE_FUND_TRACKING", "true").lower() in ("true", "1", "yes")

# ---------------------------------------------------------------------------
# Cache 13F (fundusze)
# ---------------------------------------------------------------------------
FUNDS_CACHE_DIR = DATA_DIR / "_cache_13f"

# ---------------------------------------------------------------------------
# Sledzone fundusze inwestycyjne (CIK z SEC EDGAR)
# ---------------------------------------------------------------------------
# Mozna rozszerzac — wystarczy dodac FundProfile do listy.
# CIK mozna sprawdzic na: https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany
from src.models import FundProfile

TRACKED_FUNDS: list[FundProfile] = [
    FundProfile(name="Berkshire Hathaway", manager="Warren Buffett", cik="1067983"),
    FundProfile(name="Bridgewater Associates", manager="Ray Dalio", cik="1350694"),
    FundProfile(name="Pershing Square", manager="Bill Ackman", cik="1336528"),
    FundProfile(name="Scion Asset Management", manager="Michael Burry", cik="1649339"),
    FundProfile(name="Appaloosa Management", manager="David Tepper", cik="1656456"),
    FundProfile(name="Greenlight Capital", manager="David Einhorn", cik="1079114"),
]
