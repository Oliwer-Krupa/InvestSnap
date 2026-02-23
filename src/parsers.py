"""Odczyt i parsowanie plikow danych (CSV / Excel) dla InvestSnap."""

from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path

import pandas as pd

from src.config import (
    EXCEL_EXTENSIONS,
    HEADER_SCAN_MAX_ROWS,
    TEXT_EXTENSIONS,
)
from datetime import date as _date

from src.models import AccountSnapshot, TableSource

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Ekstrakcja zakresu dat z nazwy pliku
# ---------------------------------------------------------------------------
_DATE_RANGE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})[_\s]+(\d{4}-\d{2}-\d{2})")


def extract_date_range_from_path(path: Path) -> tuple[_date | None, _date | None]:
    """Wyciaga zakres dat z nazwy pliku (np. ``..._2005-12-31_2026-02-23.xlsx``).

    Zwraca ``(start_date, end_date)`` lub ``(None, None)`` jesli brak dopasowania.
    """
    m = _DATE_RANGE_RE.search(path.stem)
    if not m:
        return None, None
    try:
        start = _date.fromisoformat(m.group(1))
        end = _date.fromisoformat(m.group(2))
        return start, end
    except ValueError:
        return None, None

# =========================================================================
# Niskopoziomowe pomocniki tekstowe
# =========================================================================


def normalize_column_name(value: str) -> str:
    """Sprowadza nazwe kolumny do postaci porownawczej (male litery, bez znakow specjalnych)."""
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def normalize_text_value(value: object) -> str:
    """Bezpiecznie konwertuje wartosc do `str`, zwraca pusty string dla NaN / None."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none"}:
        return ""
    return text


def parse_amount(value: object) -> float:
    """Parsuje wartosc liczbowa z roznych formatow (spacja/przecinek/kropka jako separator)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return 0.0

    text = text.replace("\xa0", "").replace(" ", "")
    text = re.sub(r"[^0-9,.\-+]", "", text)

    if not text or text in {"-", "+", "--"}:
        return 0.0

    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")

    try:
        return float(text)
    except ValueError:
        return 0.0


def to_numeric_series(series: pd.Series) -> pd.Series:
    """Konwertuje cala serie na wartosci liczbowe za pomoca `parse_amount`."""
    return series.apply(parse_amount)


# =========================================================================
# Detekcja naglowka i delimitera
# =========================================================================

_DELIMITER_CANDIDATES = [";", ",", "\t", "|"]


def _detect_delimiter(line: str) -> str:
    best = max(_DELIMITER_CANDIDATES, key=line.count)
    return best if line.count(best) > 0 else ";"


def _has_required_columns(df: pd.DataFrame, required_columns: list[str]) -> bool:
    current = {normalize_column_name(str(col).strip()) for col in df.columns}
    expected = {normalize_column_name(col) for col in required_columns}
    return expected.issubset(current)


def _read_lines_with_fallback(path: Path) -> tuple[list[str], str]:
    for encoding in ("utf-8-sig", "cp1250", "latin1"):
        try:
            with path.open("r", encoding=encoding) as handle:
                return handle.readlines(), encoding
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Nie mozna odczytac pliku: {path}")


def _detect_header_from_lines(
    lines: list[str],
    required_columns: list[str],
) -> tuple[int | None, str | None]:
    required = {normalize_column_name(col) for col in required_columns}

    for idx, line in enumerate(lines[:HEADER_SCAN_MAX_ROWS]):
        if not line.strip():
            continue
        delimiter = _detect_delimiter(line)
        values = [item.strip().strip('"').strip("'") for item in line.strip().split(delimiter)]
        normalized_values = {normalize_column_name(v) for v in values if v.strip()}
        if required.issubset(normalized_values):
            return idx, delimiter

    return None, None


def _detect_header_index_from_frame(
    df: pd.DataFrame,
    required_columns: list[str],
) -> int | None:
    required = {normalize_column_name(col) for col in required_columns}
    max_rows = min(HEADER_SCAN_MAX_ROWS, len(df))

    for idx in range(max_rows):
        row_values = ["" if pd.isna(v) else str(v).strip() for v in df.iloc[idx].tolist()]
        normalized_values = {normalize_column_name(v) for v in row_values if v}
        if required.issubset(normalized_values):
            return idx

    return None


def _build_header_candidates(detected_index: int | None, row_count: int) -> list[int]:
    candidates: list[int] = []
    if detected_index is not None:
        candidates.append(detected_index)
    if row_count > 10:
        candidates.append(10)
    candidates.append(0)

    seen: set[int] = set()
    unique: list[int] = []
    for idx in candidates:
        if idx not in seen and idx < row_count:
            seen.add(idx)
            unique.append(idx)
    return unique


# =========================================================================
# Ladowanie tabel (CSV / Excel)
# =========================================================================


def _load_text_table(path: Path, required_columns: list[str]) -> pd.DataFrame:
    lines, encoding = _read_lines_with_fallback(path)
    detected_skiprows, detected_delimiter = _detect_header_from_lines(lines, required_columns)

    attempts: list[tuple[int, str]] = []
    if detected_skiprows is not None and detected_delimiter is not None:
        attempts.append((detected_skiprows, detected_delimiter))

    if len(lines) > 10:
        attempts.append((10, _detect_delimiter(lines[10])))

    attempts.extend([(10, ";"), (10, ","), (0, ";"), (0, ",")])

    checked: set[tuple[int, str]] = set()
    for skiprows, delimiter in attempts:
        key = (skiprows, delimiter)
        if key in checked:
            continue
        checked.add(key)

        try:
            df = pd.read_csv(
                path,
                sep=delimiter,
                skiprows=skiprows,
                encoding=encoding,
                engine="python",
                dtype=str,
            )
            df.columns = [str(col).strip() for col in df.columns]
            df = df.dropna(how="all")
            if _has_required_columns(df, required_columns):
                return df
        except Exception:
            continue

    raise ValueError(f"Nie mozna sparsowac wymaganych kolumn z pliku: {path.name}")


def _load_excel_table(path: Path, sheet_name: str, required_columns: list[str]) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name=sheet_name, header=None, dtype=str)
    raw = raw.dropna(how="all")

    if raw.empty:
        raise ValueError(f"Arkusz jest pusty: {sheet_name}")

    detected_index = _detect_header_index_from_frame(raw, required_columns)
    for header_idx in _build_header_candidates(detected_index, len(raw)):
        header_values = ["" if pd.isna(v) else str(v).strip() for v in raw.iloc[header_idx].tolist()]
        data = raw.iloc[header_idx + 1:].copy()
        data.columns = header_values
        data.columns = [str(col).strip() for col in data.columns]
        data = data.loc[:, [col for col in data.columns if col]]
        data = data.dropna(how="all")

        if _has_required_columns(data, required_columns):
            return data

    raise ValueError(
        f"Nie mozna sparsowac wymaganych kolumn z arkusza: {path.name} / {sheet_name}"
    )


# =========================================================================
# Wyszukiwanie zrodla danych
# =========================================================================


def _pick_sheet_name(workbook_path: Path, marker: str, *, allow_fallback_first: bool) -> str | None:
    try:
        sheet_names = pd.ExcelFile(workbook_path).sheet_names
    except Exception:
        return None

    for name in sheet_names:
        sheet = str(name)
        if marker.upper() in sheet.upper():
            return sheet

    if allow_fallback_first and sheet_names:
        return str(sheet_names[0])

    return None


def find_table_source(data_dir: Path, marker: str) -> TableSource:
    """Znajduje plik zrodlowy danych na podstawie markera (np. 'OPEN POSITION')."""
    if not data_dir.exists():
        raise FileNotFoundError(f"Brak folderu danych: {data_dir}")

    files = [p for p in data_dir.iterdir() if p.is_file()]

    # Tryb preferowany: nazwa pliku zawiera marker
    marker_files = [p for p in files if marker.upper() in p.name.upper()]

    text_files = [p for p in marker_files if p.suffix.lower() in TEXT_EXTENSIONS]
    if text_files:
        selected = max(text_files, key=lambda p: p.stat().st_mtime)
        return TableSource(kind="text", path=selected)

    excel_by_name = [p for p in marker_files if p.suffix.lower() in EXCEL_EXTENSIONS]
    if excel_by_name:
        selected = max(excel_by_name, key=lambda p: p.stat().st_mtime)
        sheet = _pick_sheet_name(selected, marker, allow_fallback_first=True)
        if sheet is None:
            raise ValueError(f"Skoroszyt nie zawiera arkuszy: {selected}")
        return TableSource(kind="excel", path=selected, sheet_name=sheet)

    # Tryb awaryjny: szukaj markera w nazwach arkuszy
    excel_files = sorted(
        [p for p in files if p.suffix.lower() in EXCEL_EXTENSIONS],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for workbook in excel_files:
        sheet = _pick_sheet_name(workbook, marker, allow_fallback_first=False)
        if sheet:
            return TableSource(kind="excel", path=workbook, sheet_name=sheet)

    raise FileNotFoundError(f"Brak zrodla danych dla markera '{marker}' w {data_dir}")


def load_source_table(source: TableSource, required_columns: list[str]) -> pd.DataFrame:
    """Laduje DataFrame z danego zrodla (CSV lub Excel)."""
    if source.kind == "text":
        return _load_text_table(source.path, required_columns)
    if source.kind == "excel" and source.sheet_name is not None:
        return _load_excel_table(source.path, source.sheet_name, required_columns)
    raise ValueError(f"Nieobslugiwane zrodlo: {source}")


# =========================================================================
# Pomocniki wyszukiwania kolumn
# =========================================================================


def find_column(df: pd.DataFrame, expected_name: str) -> str:
    """Znajduje kolumne w DataFrame po znormalizowanej nazwie. Rzuca KeyError jesli brak."""
    normalized_expected = normalize_column_name(expected_name)
    columns = {str(col).strip(): normalize_column_name(str(col).strip()) for col in df.columns}

    for col, norm in columns.items():
        if norm == normalized_expected:
            return col

    for col, norm in columns.items():
        if normalized_expected in norm:
            return col

    raise KeyError(f"Brak kolumny: {expected_name}")


def find_optional_column(df: pd.DataFrame, expected_names: list[str]) -> str | None:
    """Szuka kolumny pod wieloma alternatywnymi nazwami. Zwraca ``None`` jesli nie znaleziono."""
    normalized_columns: dict[str, str] = {}
    for column in df.columns:
        col_text = str(column).strip()
        normalized_columns.setdefault(normalize_column_name(col_text), col_text)

    for name in expected_names:
        norm = normalize_column_name(name)
        if norm in normalized_columns:
            return normalized_columns[norm]

    return None


# =========================================================================
# Notatki (dziennik inwestycyjny)
# =========================================================================


def read_and_clear_notes(notes_file: Path) -> str:
    """Odczytuje notatki z pliku, a nastepnie czysci jego zawartosc."""
    notes_file.parent.mkdir(parents=True, exist_ok=True)

    if not notes_file.exists():
        notes_file.write_text("", encoding="utf-8")
        return ""

    text = ""
    for encoding in ("utf-8", "utf-8-sig", "cp1250", "latin1"):
        try:
            text = notes_file.read_text(encoding=encoding)
            break
        except UnicodeDecodeError:
            continue

    notes_file.write_text("", encoding="utf-8")
    return text.strip()


# =========================================================================
# Snapshot konta (Balance / Equity / Free margin)
# =========================================================================


def _load_raw_source_for_metrics(source: TableSource) -> pd.DataFrame:
    """Laduje surowe dane (bez detekcji naglowka tabeli) do skanowania metryk konta."""
    if source.kind == "excel" and source.sheet_name is not None:
        raw = pd.read_excel(source.path, sheet_name=source.sheet_name, header=None, dtype=str)
        return raw.fillna("")

    if source.kind == "text":
        lines, encoding = _read_lines_with_fallback(source.path)
        delimiters: list[str] = []
        for line in lines[:20]:
            if line.strip():
                delimiters.append(_detect_delimiter(line))
        delimiters.extend(_DELIMITER_CANDIDATES)

        seen: set[str] = set()
        for delimiter in delimiters:
            if delimiter in seen:
                continue
            seen.add(delimiter)
            try:
                raw = pd.read_csv(
                    source.path,
                    sep=delimiter,
                    header=None,
                    dtype=str,
                    encoding=encoding,
                    engine="python",
                )
                raw = raw.fillna("")
                if raw.shape[1] > 1:
                    return raw
            except Exception:
                continue

        # Awaryjnie: jedna kolumna z liniami tekstu.
        return pd.DataFrame({0: [line.rstrip("\n") for line in lines]}).fillna("")

    raise ValueError(f"Nieobslugiwane zrodlo metryk konta: {source}")


def _parse_optional_amount(value: object) -> float | None:
    text = normalize_text_value(value)
    if not text:
        return None
    return parse_amount(text)


def _extract_metric_from_raw(raw_df: pd.DataFrame, aliases: list[str]) -> float | None:
    if raw_df.empty:
        return None

    alias_norm = {normalize_column_name(alias) for alias in aliases}
    row_count, col_count = raw_df.shape
    max_rows = min(HEADER_SCAN_MAX_ROWS, row_count)

    for row_idx in range(max_rows):
        row_values = [normalize_text_value(v) for v in raw_df.iloc[row_idx].tolist()]
        row_norm = [normalize_column_name(v) for v in row_values]

        for col_idx, norm in enumerate(row_norm):
            if norm not in alias_norm:
                continue

            candidates: list[object] = []
            if row_idx + 1 < row_count:
                candidates.append(raw_df.iat[row_idx + 1, col_idx])
            if col_idx + 1 < col_count:
                candidates.append(raw_df.iat[row_idx, col_idx + 1])
            if row_idx + 1 < row_count and col_idx + 1 < col_count:
                candidates.append(raw_df.iat[row_idx + 1, col_idx + 1])

            for candidate in candidates:
                parsed = _parse_optional_amount(candidate)
                if parsed is not None:
                    return parsed

    return None


def extract_account_snapshot(source: TableSource) -> AccountSnapshot:
    """Zwraca Balance / Equity / Free margin odczytane z naglowka raportu XTB."""
    raw = _load_raw_source_for_metrics(source)

    return AccountSnapshot(
        balance=_extract_metric_from_raw(raw, ["Balance"]),
        equity=_extract_metric_from_raw(raw, ["Equity"]),
        free_margin=_extract_metric_from_raw(raw, ["Free margin", "Free margin:"]),
    )


# =========================================================================
# Ladowanie nadpisan ETF (geografia)
# =========================================================================


def load_etf_geography_overrides(path: Path) -> dict[str, dict[str, float]]:
    """Laduje plik ``etf_geografia.json`` z look-through dla ETF."""
    if not path.exists():
        return {}

    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        logger.warning("Nie mozna wczytac %s: %s", path.name, exc)
        return {}

    if not isinstance(payload, dict):
        logger.warning("%s musi zawierac obiekt JSON.", path.name)
        return {}

    overrides: dict[str, dict[str, float]] = {}
    for symbol, regions in payload.items():
        if not isinstance(regions, dict):
            continue

        cleaned: dict[str, float] = {}
        for region, raw_weight in regions.items():
            region_name = normalize_text_value(region)
            if not region_name:
                continue
            try:
                weight = float(raw_weight)
            except (TypeError, ValueError):
                continue
            if weight > 0:
                cleaned[region_name] = weight

        if not cleaned:
            continue

        total_weight = sum(cleaned.values())
        if total_weight <= 0:
            continue

        normalized = {name: value / total_weight for name, value in cleaned.items()}
        overrides[str(symbol).upper().strip()] = normalized

    return overrides


def load_symbol_metadata(path: Path) -> dict[str, dict[str, str]]:
    """
    Laduje reczna mape symboli:
    {
      "MSFT.US": {"asset_type": "Akcje", "region": "USA", "currency": "USD"},
      "LTAM.NL": {"asset_type": "ETF", "region": "Ameryka Poludniowa", "currency": "USD"}
    }
    """
    if not path.exists():
        return {}

    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        logger.warning("Nie mozna wczytac %s: %s", path.name, exc)
        return {}

    if not isinstance(payload, dict):
        logger.warning("%s musi zawierac obiekt JSON.", path.name)
        return {}

    normalized: dict[str, dict[str, str]] = {}
    for symbol, raw_meta in payload.items():
        if not isinstance(raw_meta, dict):
            continue

        symbol_key = normalize_text_value(symbol).upper()
        if not symbol_key:
            continue

        asset_type = normalize_text_value(raw_meta.get("asset_type", ""))
        region = normalize_text_value(raw_meta.get("region", ""))
        currency = normalize_text_value(raw_meta.get("currency", "")).upper()

        normalized[symbol_key] = {
            "asset_type": asset_type,
            "region": region,
            "currency": currency,
        }

    return normalized

