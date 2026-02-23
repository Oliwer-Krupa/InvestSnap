"""Pobieranie i analiza raportow 13F duzych funduszy inwestycyjnych z SEC EDGAR.

Modul pobiera najnowsze raporty 13F-HR z SEC EDGAR, porownuje je z poprzednimi
i zestawia zmiany z aktualnymi cenami rynkowymi (yfinance), zeby wylapac
potencjalne okazje inwestycyjne.
"""

from __future__ import annotations

import json
import logging
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests

from src.config import OPENFIGI_API_KEY, SEC_USER_AGENT
from src.models import FundProfile

logger = logging.getLogger(__name__)

# =========================================================================
# Konfiguracja SEC EDGAR
# =========================================================================

SEC_BASE = "https://data.sec.gov"
SEC_FULL_TEXT = "https://efts.sec.gov/LATEST"
SEC_ARCHIVES_WWW = "https://www.sec.gov/Archives/edgar/data"

# Opoznienie miedzy requestami zeby nie przekroczyc limitu SEC (10 req/s)
_SEC_DELAY = 0.12

_NS_13F = "http://www.sec.gov/xml/ns/13F/infotable"


@dataclass
class Holding:
    """Pojedyncza pozycja z raportu 13F."""

    issuer: str
    title_of_class: str
    cusip: str
    value_usd: float  # wartosc w pelnych USD
    shares: float
    put_call: str  # "PUT" / "CALL" / ""


@dataclass
class Filing13F:
    """Sparsowany raport 13F."""

    fund_name: str
    filed_date: str  # YYYY-MM-DD
    report_date: str  # YYYY-MM-DD (okres raportowy)
    accession: str
    holdings: list[Holding] = field(default_factory=list)


@dataclass
class HoldingChange:
    """Zmiana pozycji miedzy dwoma raportami 13F."""

    issuer: str
    title_of_class: str  # np. "COM", "MSCI STH KOR ETF", "6.375 09/01/28"
    cusip: str
    ticker: str  # zmapowany ticker (moze byc pusty)
    action: str  # "NEW" / "CLOSED" / "INCREASED" / "DECREASED"
    prev_shares: float
    curr_shares: float
    shares_delta: float
    prev_value_usd: float
    curr_value_usd: float
    current_price: float | None  # aktualna cena rynkowa z yfinance
    filing_price_est: float | None  # szacunkowa cena z raportu (value_usd / shares)
    price_change_pct: float | None  # zmiana ceny od szacunkowej ceny z raportu


@dataclass
class FundReport:
    """Pelny raport zmian dla jednego funduszu."""

    fund: FundProfile
    latest_filing: Filing13F | None
    previous_filing: Filing13F | None
    changes: list[HoldingChange] = field(default_factory=list)
    error: str | None = None


# =========================================================================
# Cache — zapis na dysk zeby nie odpytywac SEC za kazdym razem
# =========================================================================

_CACHE_TTL_DAYS = 7


def _cache_path(cache_dir: Path, fund_cik: str, label: str) -> Path:
    return cache_dir / f"13f_{fund_cik}_{label}.json"


def _read_cache(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        cached_at = datetime.fromisoformat(data.get("_cached_at", "2000-01-01"))
        if datetime.now() - cached_at > timedelta(days=_CACHE_TTL_DAYS):
            return None
        return data
    except Exception:
        return None


def _write_cache(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data["_cached_at"] = datetime.now().isoformat()
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# =========================================================================
# SEC EDGAR — komunikacja
# =========================================================================


def _sec_get(url: str) -> requests.Response:
    """GET z poprawnym User-Agent i opoznieniem."""
    time.sleep(_SEC_DELAY)
    resp = requests.get(url, headers={"User-Agent": SEC_USER_AGENT}, timeout=30)
    resp.raise_for_status()
    return resp


def _padded_cik(cik: str) -> str:
    return cik.zfill(10)


# =========================================================================
# Pobieranie listy filingow 13F
# =========================================================================


def _fetch_recent_13f_accessions(
    cik: str,
    cache_dir: Path,
    count: int = 2,
) -> list[dict[str, str]]:
    """Zwraca liste ``count`` najnowszych 13F-HR filingow (accession, filedDate, reportDate)."""
    cache = _cache_path(cache_dir, cik, "accessions")
    cached = _read_cache(cache)
    if cached and "accessions" in cached:
        return cached["accessions"][:count]

    url = f"{SEC_BASE}/submissions/CIK{_padded_cik(cik)}.json"
    data = _sec_get(url).json()

    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accessions = recent.get("accessionNumber", [])
    filed_dates = recent.get("filingDate", [])
    report_dates = recent.get("reportDate", [])
    primary_docs = recent.get("primaryDocument", [])

    results: list[dict[str, str]] = []
    for i, form in enumerate(forms):
        if form.strip() in ("13F-HR", "13F-HR/A") and i < len(accessions):
            results.append({
                "accession": accessions[i],
                "filed_date": filed_dates[i] if i < len(filed_dates) else "",
                "report_date": report_dates[i] if i < len(report_dates) else "",
                "primary_doc": primary_docs[i] if i < len(primary_docs) else "",
            })
            if len(results) >= count:
                break

    _write_cache(cache, {"accessions": results})
    return results[:count]


# =========================================================================
# Parsowanie 13F info table (XML)
# =========================================================================


def _fetch_and_parse_infotable(
    cik: str,
    accession_info: dict[str, str],
    cache_dir: Path,
) -> list[Holding]:
    """Pobiera i parsuje XML z info table 13F."""
    accession = accession_info["accession"]
    acc_no_dashes = accession.replace("-", "")

    cache = _cache_path(cache_dir, cik, f"holdings_{acc_no_dashes}")
    cached = _read_cache(cache)
    if cached and "holdings" in cached:
        return [Holding(**h) for h in cached["holdings"]]

    # Najpierw pobieramy indeks filing z www.sec.gov (data.sec.gov nie udostepnia index.json)
    index_url = f"{SEC_ARCHIVES_WWW}/{cik}/{acc_no_dashes}/index.json"
    try:
        index_data = _sec_get(index_url).json()
        items = index_data.get("directory", {}).get("item", [])
        xml_file = None

        # 1. Szukaj pliku z "infotable" w nazwie
        for item in items:
            name = item.get("name", "").lower()
            if "infotable" in name and name.endswith(".xml"):
                xml_file = item["name"]
                break

        # 2. Szukaj dowolnego XML ktory nie jest primary_doc ani index
        if not xml_file:
            for item in items:
                name = item.get("name", "").lower()
                if (
                    name.endswith(".xml")
                    and "primary" not in name
                    and "index" not in name
                    and not name.startswith(accession.replace('-', '') [:10])
                ):
                    xml_file = item["name"]
                    break
    except Exception:
        xml_file = None

    if not xml_file:
        logger.warning("Nie znaleziono XML info table dla accession %s", accession)
        return []

    xml_url = f"{SEC_ARCHIVES_WWW}/{cik}/{acc_no_dashes}/{xml_file}"
    try:
        resp = _sec_get(xml_url)
        holdings = _parse_infotable_xml(resp.text)
    except Exception as exc:
        logger.warning("Blad parsowania info table %s: %s", accession, exc)
        return []

    # Cache
    serializable = [
        {
            "issuer": h.issuer, "title_of_class": h.title_of_class,
            "cusip": h.cusip, "value_usd": h.value_usd,
            "shares": h.shares, "put_call": h.put_call,
        }
        for h in holdings
    ]
    _write_cache(cache, {"holdings": serializable})
    return holdings


def _parse_infotable_xml(xml_text: str) -> list[Holding]:
    """Parsuje XML info table do listy Holding."""
    root = ET.fromstring(xml_text)

    holdings: list[Holding] = []
    # Obsluga z namespace i bez
    for info_entry in root.iter():
        tag = info_entry.tag.split("}")[-1] if "}" in info_entry.tag else info_entry.tag
        if tag != "infoTable":
            continue

        def _text(parent: ET.Element, child_tag: str) -> str:
            for el in parent.iter():
                el_tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
                if el_tag == child_tag:
                    return (el.text or "").strip()
            return ""

        issuer = _text(info_entry, "nameOfIssuer")
        title = _text(info_entry, "titleOfClass")
        cusip = _text(info_entry, "cusip")
        value_str = _text(info_entry, "value")
        shares_str = _text(info_entry, "sshPrnamt")
        put_call = _text(info_entry, "putCall")

        try:
            value_usd = float(value_str) if value_str else 0.0
        except ValueError:
            value_usd = 0.0
        try:
            shares = float(shares_str.replace(",", "")) if shares_str else 0.0
        except ValueError:
            shares = 0.0

        if issuer:
            holdings.append(Holding(
                issuer=issuer, title_of_class=title, cusip=cusip,
                value_usd=value_usd, shares=shares, put_call=put_call,
            ))

    return holdings


# =========================================================================
# Mapowanie CUSIP -> ticker
# =========================================================================

# Podstawowa mapa popularnych spolek (CUSIP -> Yahoo Finance ticker)
# CUSIP to 9-znakowy identyfikator. SEC 13F uzywa 9-znakowego CUSIP.
_CUSIP_TO_TICKER: dict[str, str] = {
    # ---- Mega-cap tech ----
    "594918104": "MSFT",
    "037833100": "AAPL",
    "02079K107": "GOOG",
    "02079K305": "GOOGL",
    "023135106": "AMZN",
    "30303M102": "META",
    "67066G104": "NVDA",
    "88160R101": "TSLA",
    "11135F101": "AVGO",   # Broadcom
    "458140100": "INTC",   # Intel

    # ---- Large-cap tech / software ----
    "68389X105": "ORCL",
    "17275R102": "CSCO",
    "00724F101": "ABBV",
    "882508104": "TXN",
    "46120E602": "INTU",
    "79466L302": "CRM",    # Salesforce
    "00507V109": "ADBE",   # Adobe
    "64110L106": "NFLX",
    "70450Y103": "PYPL",
    "585055106": "MDLZ",
    "035420103": "ADP",

    # ---- Finance / Banks ----
    "084670702": "BRK-B",
    "46625H100": "JPM",
    "92826C839": "V",
    "571903202": "MA",
    "172967424": "C",
    "060505104": "BAC",
    "38141G104": "GS",
    "617446448": "MS",
    "949746101": "WFC",    # Wells Fargo
    "808513105": "SCHW",   # Charles Schwab
    "02005N100": "ALLY",   # Ally Financial
    "127055101": "CB",     # Chubb
    "00206R102": "T",
    "03027X100": "AIG",
    "053332102": "AXP",    # American Express

    # ---- Healthcare ----
    "91324P102": "UNH",
    "478160104": "JNJ",
    "58933Y105": "MRK",
    "718172109": "PFE",
    "00287Y109": "ABT",    # Abbott
    "872898104": "TMO",    # Thermo Fisher
    "444859102": "HUM",    # Humana
    "110122108": "BMY",    # Bristol-Myers
    "92556V106": "VTRS",   # Viatris
    "881624209": "TEVA",   # Teva Pharmaceutical
    "88033G407": "THC",    # Tenet Healthcare
    "45826H109": "NTLA",   # Intellia Therapeutics
    "517834107": "LLY",    # Eli Lilly
    "032511107": "AMGN",   # Amgen
    "49177J102": "KVUE",   # Kenvue

    # ---- Consumer / Retail ----
    "931142103": "WMT",
    "742718109": "PG",
    "191216100": "KO",
    "713448108": "PEP",
    "437076102": "HD",
    "548661107": "LOW",
    "855244109": "SBUX",
    "500754106": "KHC",
    "254687106": "DIS",
    "609207105": "MO",
    "125896100": "CAT",
    "49271V100": "KDP",
    "822582102": "SHW",
    "901167108": "UBER",
    "007903107": "ACN",    # Accenture
    "22160K105": "COST",   # Costco
    "92343V104": "VZ",     # Verizon
    "912909108": "UPS",    # UPS
    "20030N101": "CMCSA",  # Comcast
    "369604301": "GE",
    "98978V103": "ZTS",    # Zoetis
    "963320106": "WHR",    # Whirlpool

    # ---- Energy ----
    "30231G102": "XOM",
    "166764100": "CVX",
    "635405101": "NEE",    # NextEra Energy
    "29273V100": "ET",     # Energy Transfer
    "20825C104": "COP",    # ConocoPhillips
    "682680103": "OXY",    # Occidental Petroleum

    # ---- Industrials ----
    "74762E102": "QCOM",
    "46266C105": "IRM",    # Iron Mountain
    "530909108": "LRCX",   # Lam Research
    "74624M102": "PWR",    # Quanta Services
    "247361702": "DAL",    # Delta Air Lines
    "910047109": "UAL",    # United Airlines
    "382388106": "GT",     # Goodyear Tire
    "26441C204": "DHR",    # Danaher

    # ---- ETFs / Index ----
    "78462F103": "SPY",    # SPDR S&P 500
    "464287200": "IWM",    # iShares Russell 2000
    "46090E103": "QQQ",    # Invesco QQQ
    "78464A870": "XLE",    # Energy Select SPDR
    "78464A102": "XLF",    # Financial Select SPDR
    "78464A888": "XLK",    # Technology Select SPDR
    "78464A557": "XLV",    # Health Care Select SPDR
    "500767107": "KWEB",   # KraneShares CSI China Internet ETF
    "46137V621": "RSP",    # Invesco S&P 500 Equal Weight

    # ---- Chinese ADR ----
    "47215P106": "JD",     # JD.com
    "09857L108": "BKNG",   # Booking Holdings
    "01609W102": "BABA",   # Alibaba

    # ---- Other notable ----
    "531229854": "LLYVA",  # Liberty Live Holdings
    "64952D105": "NYT",    # NY Times
    "12504L109": "CBRE",   # CBRE Group
    "55087P104": "LYFT",   # Lyft
    "38268T103": "GPRO",   # GoPro
    "50155Q100": "KD",     # Kyndryl
    "388689101": "GPK",    # Graphic Packaging
    "64107A104": "NPWR",   # NET Power
    "88337F105": "ODP",    # The ODP Corp
    "236272100": "DNMR",   # Danimer Scientific
    "649445103": "NYCB",   # NY Community Bancorp
    "320517105": "FHN",    # First Horizon
    "22407B105": "COYA",   # Coya Therapeutics
    "69331C108": "PANW",   # Palo Alto Networks
    "01626W109": "ALIT",   # Alight Inc
    "88579Y101": "MMM",    # 3M
    "252131107": "DE",     # Deere
    "075887109": "BDX",    # Becton Dickinson
    "878742204": "TECK",   # Teck Resources

    # ---- Zagraniczne spolki (CUSIP zaczynajacy sie od G/N/Y) ----
    "H1467J104": "CB",     # Chubb Limited (Bermuda)
    "G0403H108": "AON",    # Aon plc (Ireland)
    "G9001E102": "LILAK",  # Liberty Latin America (Bermuda)
    "G2662B103": "CRML",   # Critical Metals Corp
    "G01767105": "ALKS",   # Alkermes plc (Ireland)
    "G21810109": "CLVT",   # Clarivate plc (Jersey)
    "N07059210": "ASML",   # ASML Holding (Netherlands)
    "N00985106": "AER",    # AerCap Holdings (Netherlands)
    "Y2065G121": "DHT",    # DHT Holdings (Bermuda)
    "G7997W102": "SDRL",   # Seadrill (Bermuda)
    "G4412G101": "HLI",    # Houlihan Lokey (alt)
    "N4578E413": "LIN",    # Linde plc (Ireland)
    "G5480U104": "LBTYA",  # Liberty Global A (UK)
    "G5480U120": "LBTYB",  # Liberty Global B (UK)
    "G5480U138": "LBTYK",  # Liberty Global C (UK)
    "G0171V109": "AMCR",   # Amcor plc (Australia/UK)
    "G1151C101": "BUD",    # Anheuser-Busch InBev (Belgium)
    "G59669103": "MHK",    # Medtronic (Ireland — alt)
    "H3698D120": "SAN",    # Santander (Spain)
    "N75886109": "RIO",    # Rio Tinto (UK)
}


# =========================================================================
# OpenFIGI — dynamiczne mapowanie CUSIP -> ticker
# =========================================================================

_OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
_OPENFIGI_BATCH_SIZE = 100 if OPENFIGI_API_KEY else 10
_OPENFIGI_DELAY = 0.5 if OPENFIGI_API_KEY else 2.5

# Plik z trwale zapisanymi wynikami (nie wygasa — CUSIP sie nie zmienia)
_CUSIP_CACHE_FILENAME = "cusip_tickers_cache.json"


def _load_cusip_cache(cache_dir: Path) -> dict[str, str]:
    """Wczytuje trwaly cache CUSIP -> ticker z dysku."""
    path = cache_dir / _CUSIP_CACHE_FILENAME
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_cusip_cache(cache_dir: Path, cache: dict[str, str]) -> None:
    """Zapisuje cache CUSIP -> ticker na dysk."""
    path = cache_dir / _CUSIP_CACHE_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def _resolve_cusips_openfigi(
    cusips: list[str],
    cache_dir: Path,
) -> dict[str, str]:
    """Rozwiazuje liste CUSIP -> ticker przez OpenFIGI API z cache'owaniem.

    Wynik jest permanentnie cache'owany, bo CUSIP -> ticker nie zmienia sie.
    Pierwsze uruchomienie moze byc wolniejsze (~2-4 min dla 500+ CUSIPow),
    ale kolejne sa natychmiastowe.
    """
    cached = _load_cusip_cache(cache_dir)

    # Odfiltruj juz znane (statyczny dict + cache dyskowy)
    unknown: list[str] = []
    for c in cusips:
        c_upper = c.strip().upper()
        if c_upper not in _CUSIP_TO_TICKER and c_upper not in cached:
            unknown.append(c_upper)

    unknown = list(set(unknown))  # deduplicate

    if not unknown:
        return cached

    logger.info(
        "Rozwiazywanie %d nieznanych CUSIPow przez OpenFIGI API...", len(unknown),
    )

    figi_headers: dict[str, str] = {"Content-Type": "application/json"}
    if OPENFIGI_API_KEY:
        figi_headers["X-OPENFIGI-APIKEY"] = OPENFIGI_API_KEY

    resolved_count = 0
    for i in range(0, len(unknown), _OPENFIGI_BATCH_SIZE):
        batch = unknown[i : i + _OPENFIGI_BATCH_SIZE]
        payload = [{"idType": "ID_CUSIP", "idValue": c} for c in batch]

        try:
            resp = requests.post(
                _OPENFIGI_URL,
                json=payload,
                headers=figi_headers,
                timeout=15,
            )
            if resp.status_code == 429:
                logger.debug("OpenFIGI rate limit — czekam 10s...")
                time.sleep(10)
                resp = requests.post(
                    _OPENFIGI_URL,
                    json=payload,
                    headers=figi_headers,
                    timeout=15,
                )

            if resp.status_code == 200:
                results = resp.json()
                for cusip_val, result in zip(batch, results):
                    if "data" in result and result["data"]:
                        # Preferuj US equity ticker
                        ticker = ""
                        for item in result["data"]:
                            t = item.get("ticker", "")
                            mkt = item.get("marketSector", "")
                            exch = item.get("exchCode", "")
                            if t and mkt == "Equity" and exch in ("US", "UW", "UN", "UA", "UP", "UR", ""):
                                ticker = t
                                break
                        if not ticker and result["data"]:
                            ticker = result["data"][0].get("ticker", "")
                        if ticker:
                            cached[cusip_val] = ticker
                            resolved_count += 1
                    else:
                        # Oznacz jako nierozwiazalny (pusty string)
                        cached[cusip_val] = ""
            else:
                logger.debug("OpenFIGI HTTP %d dla batcha %d", resp.status_code, i)

        except Exception as exc:
            logger.debug("OpenFIGI blad dla batcha %d: %s", i, exc)

        if i + _OPENFIGI_BATCH_SIZE < len(unknown):
            time.sleep(_OPENFIGI_DELAY)

    _save_cusip_cache(cache_dir, cached)
    logger.info("Rozwiazano %d / %d CUSIPow przez OpenFIGI.", resolved_count, len(unknown))

    return cached


# Runtime cache (zaladowany raz z dysku + OpenFIGI)
_runtime_cusip_cache: dict[str, str] = {}


def _cusip_to_ticker(cusip: str, issuer_name: str) -> str:
    """Mapuje CUSIP lub nazwe emitenta na ticker Yahoo Finance.

    Kolejnosc:
    1. Statyczny slownik _CUSIP_TO_TICKER
    2. Dynamiczny cache (OpenFIGI, dyskowy)
    3. Heurystyka na podstawie nazwy emitenta
    """
    cusip = cusip.strip().upper()

    # 1. Statyczny slownik
    if cusip in _CUSIP_TO_TICKER:
        return _CUSIP_TO_TICKER[cusip]

    # 2. Cache z OpenFIGI
    if cusip in _runtime_cusip_cache:
        return _runtime_cusip_cache[cusip]

    # 3. Heurystyka — lista par (fragment_nazwy, ticker)
    #    WAZNE: specyficzne wpisy PRZED ogolnymi
    name_upper = issuer_name.upper().strip()

    _NAME_HINTS: list[tuple[str, str]] = [
        # Specyficzne — musza byc PRZED ogolnymi
        ("INTELLIA", "NTLA"),
        ("INTEL CORP", "INTC"),
        ("INTEL CO", "INTC"),
        ("QUANTA SVCS", "PWR"),
        ("QUANTA SER", "PWR"),
        ("QUALCOMM", "QCOM"),
        ("IQVIA", "IQV"),
        ("IRON MOUNTAIN", "IRM"),
        ("META PLATFORMS", "META"),
        ("FACEBOOK", "META"),
        ("MORGAN STANLEY", "MS"),
        ("BANK OF AMERICA", "BAC"),
        ("WELLS FARGO", "WFC"),
        ("HOME DEPOT", "HD"),
        ("COCA-COLA", "KO"),
        ("COCA COLA", "KO"),
        ("LIBERTY LIVE", "LLYVA"),
        ("NEW YORK TIMES", "NYT"),
        ("AMERICAN EXPRESS", "AXP"),
        ("ENERGY TRANSFER", "ET"),
        ("DELTA AIR", "DAL"),
        ("UNITED AIR", "UAL"),
        ("SOUTHWEST AIR", "LUV"),
        ("LAM RESEARCH", "LRCX"),
        ("LAM RESH", "LRCX"),
        ("GOODYEAR", "GT"),
        ("WHIRLPOOL", "WHR"),
        ("TENET HEALTH", "THC"),

        # Ogolne
        ("APPLE", "AAPL"),
        ("MICROSOFT", "MSFT"),
        ("AMAZON", "AMZN"),
        ("ALPHABET", "GOOGL"),
        ("GOOGLE", "GOOGL"),
        ("NVIDIA", "NVDA"),
        ("TESLA", "TSLA"),
        ("BERKSHIRE", "BRK-B"),
        ("JPMORGAN", "JPM"),
        ("J P MORGAN", "JPM"),
        ("VISA INC", "V"),
        ("JOHNSON", "JNJ"),
        ("PROCTER", "PG"),
        ("WALMART", "WMT"),
        ("MASTERCARD", "MA"),
        ("PEPSICO", "PEP"),
        ("DISNEY", "DIS"),
        ("EXXON", "XOM"),
        ("CHEVRON", "CVX"),
        ("ORACLE", "ORCL"),
        ("CISCO", "CSCO"),
        ("PFIZER", "PFE"),
        ("MERCK", "MRK"),
        ("STARBUCKS", "SBUX"),
        ("CATERPILLAR", "CAT"),
        ("UNITEDHEALTH", "UNH"),
        ("BROADCOM", "AVGO"),
        ("COSTCO", "COST"),
        ("ADOBE", "ADBE"),
        ("SALESFORCE", "CRM"),
        ("NETFLIX", "NFLX"),
        ("PAYPAL", "PYPL"),
        ("UBER TECH", "UBER"),
        ("INTUIT", "INTU"),
        ("GOLDMAN", "GS"),
        ("CITIGROUP", "C"),
        ("ABBVIE", "ABBV"),
        ("ELI LILLY", "LLY"),
        ("AMGEN", "AMGN"),
        ("BRISTOL", "BMY"),
        ("NEXTERA", "NEE"),
        ("THERMO FISH", "TMO"),
        ("HUMANA", "HUM"),
        ("DANAHER", "DHR"),
        ("OCCIDENTAL", "OXY"),
        ("CONOCOPHILLIPS", "COP"),
        ("TEVA PHARM", "TEVA"),
        ("VIATRIS", "VTRS"),
        ("KENVUE", "KVUE"),
        ("ALIGHT", "ALIT"),
        ("AERCAP", "AER"),
        ("SEADRILL", "SDRL"),
        ("GOPRO", "GPRO"),
        ("KYNDRYL", "KD"),
        ("GRAPHIC PACK", "GPK"),
        ("NET POWER", "NPWR"),
        ("COYA THER", "COYA"),
        ("DANIMER", "DNMR"),
        ("FIRST HORIZON", "FHN"),
        ("CBRE GROUP", "CBRE"),
        ("JD.COM", "JD"),
        ("LYFT", "LYFT"),
        ("BOOKING", "BKNG"),
        ("ALIBABA", "BABA"),
        ("BAIDU", "BIDU"),
        ("PALO ALTO", "PANW"),
        ("CHARLES SCHWAB", "SCHW"),
        ("DEERE", "DE"),
        ("ALLY FINL", "ALLY"),
        ("ALLY FIN", "ALLY"),
    ]
    for hint, ticker in _NAME_HINTS:
        if hint in name_upper:
            return ticker

    return ""


# =========================================================================
# Pobieranie aktualnych cen (yfinance)
# =========================================================================


def _fetch_current_prices(tickers: list[str]) -> dict[str, float]:
    """Pobiera aktualne ceny zamkniecia dla listy tickerow."""
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance nie jest zainstalowany — pomijam pobieranie cen.")
        return {}

    if not tickers:
        return {}

    prices: dict[str, float] = {}
    unique_tickers = list(set(t for t in tickers if t))

    if not unique_tickers:
        return prices

    try:
        data = yf.download(
            " ".join(unique_tickers),
            period="5d",
            progress=False,
            auto_adjust=True,
        )
        if data.empty:
            return prices

        close = data["Close"]
        if isinstance(close, float):
            # Pojedynczy ticker
            prices[unique_tickers[0]] = float(close)
        else:
            for ticker in unique_tickers:
                try:
                    col = close[ticker] if ticker in close.columns else close
                    last_val = col.dropna().iloc[-1]
                    prices[ticker] = float(last_val)
                except (KeyError, IndexError):
                    continue
    except Exception as exc:
        logger.warning("Blad pobierania cen z yfinance: %s", exc)

    return prices


# =========================================================================
# Porownanie dwoch raportow 13F
# =========================================================================


def _compare_filings(
    prev: Filing13F,
    curr: Filing13F,
    current_prices: dict[str, float],
) -> list[HoldingChange]:
    """Porownuje dwa raporty i zwraca liste zmian."""
    prev_by_cusip: dict[str, Holding] = {}
    for h in prev.holdings:
        key = h.cusip.strip()
        if key in prev_by_cusip:
            # Sumuj jesli jest wiele wierszy z tym samym CUSIP (np. PUT/CALL)
            existing = prev_by_cusip[key]
            prev_by_cusip[key] = Holding(
                issuer=existing.issuer, title_of_class=existing.title_of_class,
                cusip=existing.cusip,
                value_usd=existing.value_usd + h.value_usd,
                shares=existing.shares + h.shares,
                put_call=existing.put_call or h.put_call,
            )
        else:
            prev_by_cusip[key] = h

    curr_by_cusip: dict[str, Holding] = {}
    for h in curr.holdings:
        key = h.cusip.strip()
        if key in curr_by_cusip:
            existing = curr_by_cusip[key]
            curr_by_cusip[key] = Holding(
                issuer=existing.issuer, title_of_class=existing.title_of_class,
                cusip=existing.cusip,
                value_usd=existing.value_usd + h.value_usd,
                shares=existing.shares + h.shares,
                put_call=existing.put_call or h.put_call,
            )
        else:
            curr_by_cusip[key] = h

    all_cusips = set(prev_by_cusip.keys()) | set(curr_by_cusip.keys())
    changes: list[HoldingChange] = []

    for cusip in all_cusips:
        prev_h = prev_by_cusip.get(cusip)
        curr_h = curr_by_cusip.get(cusip)

        issuer = (curr_h.issuer if curr_h else prev_h.issuer) if (curr_h or prev_h) else ""
        title_cls = (curr_h.title_of_class if curr_h else prev_h.title_of_class) if (curr_h or prev_h) else ""
        ticker = _cusip_to_ticker(cusip, issuer)

        prev_shares = prev_h.shares if prev_h else 0.0
        curr_shares = curr_h.shares if curr_h else 0.0
        prev_val = prev_h.value_usd if prev_h else 0.0
        curr_val = curr_h.value_usd if curr_h else 0.0
        delta = curr_shares - prev_shares

        if abs(delta) < 1:
            continue  # brak istotnej zmiany

        if prev_shares == 0 and curr_shares > 0:
            action = "NEW"
        elif curr_shares == 0 and prev_shares > 0:
            action = "CLOSED"
        elif delta > 0:
            action = "INCREASED"
        else:
            action = "DECREASED"

        # Szacunkowa cena z raportu (value_usd / shares)
        # Dla CLOSED uzywamy danych z poprzedniego raportu,
        # dla pozostalych — z najnowszego (z fallbackiem na poprzedni).
        if curr_shares > 0:
            filing_price = curr_val / curr_shares
        elif prev_shares > 0:
            filing_price = prev_val / prev_shares
        else:
            filing_price = None
        current_price = current_prices.get(ticker) if ticker else None

        price_change_pct: float | None = None
        if filing_price and current_price and filing_price > 0:
            price_change_pct = (current_price - filing_price) / filing_price * 100

        changes.append(HoldingChange(
            issuer=issuer, title_of_class=title_cls,
            cusip=cusip, ticker=ticker,
            action=action,
            prev_shares=prev_shares, curr_shares=curr_shares,
            shares_delta=delta,
            prev_value_usd=prev_val, curr_value_usd=curr_val,
            current_price=current_price,
            filing_price_est=filing_price,
            price_change_pct=price_change_pct,
        ))

    # Sortuj: NEW i INCREASED najpierw, potem DECREASED, potem CLOSED
    action_order = {"NEW": 0, "INCREASED": 1, "DECREASED": 2, "CLOSED": 3}
    changes.sort(key=lambda c: (action_order.get(c.action, 9), -abs(c.shares_delta)))

    return changes


# =========================================================================
# Glowna funkcja modulu
# =========================================================================


def fetch_fund_reports(
    funds: list[FundProfile],
    cache_dir: Path,
) -> list[FundReport]:
    """Pobiera raporty 13F dla listy funduszy i zwraca analize zmian.

    Caly proces:
    1. Pobiera 2 najnowsze 13F-HR z SEC EDGAR dla kazdego funduszu
    2. Parsuje XML info table
    3. Rozwiazuje CUSIP -> ticker (OpenFIGI + cache)
    4. Porownuje pozycje miedzy raportami
    5. Pobiera aktualne ceny z yfinance
    6. Zestawia zmiany z cenami rynkowymi
    """
    global _runtime_cusip_cache

    cache_dir.mkdir(parents=True, exist_ok=True)
    reports: list[FundReport] = []

    all_cusips: set[str] = set()
    fund_data: list[tuple[FundProfile, Filing13F | None, Filing13F | None]] = []

    for fund in funds:
        try:
            accessions = _fetch_recent_13f_accessions(fund.cik, cache_dir, count=2)
            if not accessions:
                reports.append(FundReport(
                    fund=fund, latest_filing=None, previous_filing=None,
                    error="Brak raportow 13F w SEC EDGAR.",
                ))
                continue

            # Najnowszy
            latest_acc = accessions[0]
            latest_holdings = _fetch_and_parse_infotable(fund.cik, latest_acc, cache_dir)
            latest_filing = Filing13F(
                fund_name=fund.name,
                filed_date=latest_acc["filed_date"],
                report_date=latest_acc["report_date"],
                accession=latest_acc["accession"],
                holdings=latest_holdings,
            )

            # Poprzedni (jesli jest)
            previous_filing: Filing13F | None = None
            if len(accessions) >= 2:
                prev_acc = accessions[1]
                prev_holdings = _fetch_and_parse_infotable(fund.cik, prev_acc, cache_dir)
                previous_filing = Filing13F(
                    fund_name=fund.name,
                    filed_date=prev_acc["filed_date"],
                    report_date=prev_acc["report_date"],
                    accession=prev_acc["accession"],
                    holdings=prev_holdings,
                )

            # Zbierz CUSIPy z obu filingow
            for h in latest_holdings:
                all_cusips.add(h.cusip.strip().upper())
            if previous_filing:
                for h in previous_filing.holdings:
                    all_cusips.add(h.cusip.strip().upper())

            fund_data.append((fund, latest_filing, previous_filing))

        except Exception as exc:
            logger.warning("Blad pobierania danych dla %s: %s", fund.name, exc)
            reports.append(FundReport(
                fund=fund, latest_filing=None, previous_filing=None,
                error=str(exc),
            ))

    # Rozwiaz CUSIPy przez OpenFIGI (z permanentnym cache)
    _runtime_cusip_cache = _resolve_cusips_openfigi(list(all_cusips), cache_dir)

    # Zbierz tickery do pobrania cen hurtem
    all_tickers: set[str] = set()
    for _fund, latest, previous in fund_data:
        if latest:
            for h in latest.holdings:
                t = _cusip_to_ticker(h.cusip, h.issuer)
                if t:
                    all_tickers.add(t)

    # Pobierz ceny hurtem
    prices = _fetch_current_prices(list(all_tickers)) if all_tickers else {}

    # Porownaj filingi
    for fund, latest, previous in fund_data:
        if latest and previous:
            changes = _compare_filings(previous, latest, prices)
        else:
            changes = []

        reports.append(FundReport(
            fund=fund,
            latest_filing=latest,
            previous_filing=previous,
            changes=changes,
        ))

    return reports


# =========================================================================
# Formatowanie raportu Markdown
# =========================================================================

_ACTION_PL: dict[str, str] = {
    "NEW": "NOWY ZAKUP",
    "INCREASED": "ZWIEKSZENIE",
    "DECREASED": "ZMNIEJSZENIE",
    "CLOSED": "ZAMKNIECIE",
}

_ACTION_EMOJI: dict[str, str] = {
    "NEW": "+",
    "INCREASED": "+",
    "DECREASED": "-",
    "CLOSED": "X",
}

# Nazwy prawne rodzin funduszy ETF — zbyt ogolne jako nazwa spolki
_GENERIC_FUND_ISSUERS = frozenset({
    "ISHARES INC", "ISHARES TR", "ISHARES SILVER TR", "ISHARES GOLD TR",
    "SPDR SER TR", "SPDR SERIES TRUST", "SPDR S&P 500 ETF TR", "SPDR GOLD TR",
    "INVESCO EXCHANGE TRADED FD T", "INVESCO ACTVELY MNGD ETC FD",
    "KRANESHARES TRUST",
    "VANGUARD INDEX FDS", "VANGUARD INTL EQUITY INDEX F",
    "WISDOMTREE TR",
    "PROSHARES TR",
})


def _display_name(ch: HoldingChange) -> str:
    """Tworzy czytelna nazwe spolki/funduszu do wyswietlania w raporcie.

    Dla generycznych emitentow ETF (np. ISHARES INC) dolacza title_of_class
    zeby odroznic konkretny ETF.
    """
    issuer = ch.issuer.strip()
    title = ch.title_of_class.strip()
    if issuer.upper() in _GENERIC_FUND_ISSUERS and title and title.upper() not in ("SHS", "COM", "CL A", "CL B"):
        return f"{issuer} — {title}"
    return issuer


def format_fund_reports_section(reports: list[FundReport]) -> str:
    """Generuje sekcje Markdown z analiza decyzji funduszy."""
    if not reports:
        return "_Brak danych o funduszach._"

    lines: list[str] = []

    for report in reports:
        lines.append(f"### {report.fund.name} ({report.fund.manager})")
        lines.append("")

        if report.error:
            lines.append(f"_Blad: {report.error}_")
            lines.append("")
            continue

        if not report.latest_filing:
            lines.append("_Brak dostepnych raportow 13F._")
            lines.append("")
            continue

        # Informacje o raportach
        lines.append(f"- **Ostatni raport 13F:** zlozony {report.latest_filing.filed_date}, "
                      f"okres: {report.latest_filing.report_date}")
        if report.previous_filing:
            lines.append(f"- **Poprzedni raport 13F:** zlozony {report.previous_filing.filed_date}, "
                          f"okres: {report.previous_filing.report_date}")
            # Oblicz przedział czasowy zmian
            try:
                from datetime import date as _date, timedelta
                prev_end = _date.fromisoformat(report.previous_filing.report_date)
                curr_end = _date.fromisoformat(report.latest_filing.report_date)
                changes_start = prev_end + timedelta(days=1)
                lines.append(f"- **Zmiany dotycza okresu:** {changes_start.isoformat()} — {curr_end.isoformat()}")
            except (ValueError, TypeError):
                pass
        lines.append(f"- **Liczba pozycji w portfelu:** {len(report.latest_filing.holdings)}")
        lines.append("")

        if not report.changes:
            if not report.previous_filing:
                lines.append("_Tylko jeden raport dostepny — brak porownania zmian._")
            else:
                lines.append("_Brak istotnych zmian miedzy raportami._")
            lines.append("")
            continue

        # Tabela zmian
        lines.append("| Akcja | Spolka | Ticker | Shares delta | Cena z raportu (USD) | Cena obecna (USD) | Zmiana ceny |")
        lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: |")

        # Ogranicz do 20 najistotniejszych zmian
        visible = report.changes[:20]
        for ch in visible:
            action_label = f"[{_ACTION_EMOJI.get(ch.action, '?')}] {_ACTION_PL.get(ch.action, ch.action)}"
            display_name = _display_name(ch)

            # Ticker: usun fragmenty obligacji (np. "BRKR 6.375 09/01/28" -> "BRKR")
            raw_ticker = ch.ticker if ch.ticker else ""
            ticker_display = raw_ticker.split()[0] if raw_ticker else f"({ch.cusip[:6]}...)"
            # Jesli ticker po oczyszczeniu wyglada jak obligacja, oznacz to
            if raw_ticker and " " in raw_ticker:
                display_name += f" (obligacja)"

            filing_price_str = f"~${ch.filing_price_est:,.2f}" if ch.filing_price_est else "N/A"
            current_price_str = f"${ch.current_price:,.2f}" if ch.current_price else "N/A"

            if ch.price_change_pct is not None:
                sign = "+" if ch.price_change_pct > 0 else ""
                pct_str = f"{sign}{ch.price_change_pct:.1f}%"
                # Podswietlenie okazji: fundusz kupil, a cena spadla
                if ch.action in ("NEW", "INCREASED") and ch.price_change_pct < -5:
                    pct_str = f"**{pct_str} !!**"
                elif ch.action in ("NEW", "INCREASED") and ch.price_change_pct < 0:
                    pct_str = f"**{pct_str}**"
            else:
                pct_str = "N/A"

            shares_delta_str = f"{ch.shares_delta:+,.0f}"

            lines.append(
                f"| {action_label} | {display_name} | {ticker_display} | "
                f"{shares_delta_str} | {filing_price_str} | {current_price_str} | {pct_str} |"
            )

        if len(report.changes) > 20:
            lines.append(f"| ... | _{len(report.changes) - 20} wiecej zmian pominieto_ | | | | | |")

        lines.append("")

        # Podsumowanie — potencjalne okazje
        opportunities = [
            ch for ch in report.changes
            if ch.action in ("NEW", "INCREASED")
            and ch.price_change_pct is not None
            and ch.price_change_pct < -3
            and ch.ticker
        ]
        if opportunities:
            lines.append("**Potencjalne okazje** (fundusz kupil/zwieksyl, cena od tego czasu spadla):")
            for opp in opportunities[:10]:
                sign = "+" if opp.price_change_pct > 0 else ""
                opp_name = _display_name(opp)
                lines.append(
                    f"- **{opp.ticker}** ({opp_name}): cena z raportu ~${opp.filing_price_est:,.2f} "
                    f"-> teraz ${opp.current_price:,.2f} ({sign}{opp.price_change_pct:.1f}%)"
                )
            lines.append("")

    return "\n".join(lines)
