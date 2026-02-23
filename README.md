# InvestSnap

Generator miesięcznych raportów inwestycyjnych w formacie Markdown z wykresami donut.

## Funkcjonalności

- Automatyczne wczytywanie pozycji portfelowych z plików CSV / Excel
- Klasyfikacja aktywów (Akcje, ETF, Forex, Krypto, Surowce) — z kolumn źródłowych lub heurystyki
- Ekspozycja geograficzna z opcjonalnym look-through dla ETF (`dane/etf_geografia.json`)
- Analiza decyzji dużych funduszy (13F SEC EDGAR) — Buffett, Dalio, Ackman, Burry, Tepper, Einhorn
- Wykresy donut: udziały spółek, typ aktywu, geografia, waluta
- Dziennik inwestycyjny z pliku `dane/notatki.txt`

## Struktura projektu

```
src/                 # Pakiet Python (kod źródłowy)
  __init__.py        # Wersja
  __main__.py        # Punkt wejścia (main)
  config.py          # Stałe, ścieżki, konfiguracja
  models.py          # Dataclassy
  parsers.py         # Odczyt i parsowanie plików
  analysis.py        # Analiza portfela, agregacja
  charts.py          # Generowanie wykresów
  report.py          # Generowanie raportu Markdown
  funds.py           # Analiza 13F SEC EDGAR
  snapshots.py       # Porównanie snapshotów
dane/                # Pliki źródłowe (gitignored)
raporty/             # Wygenerowane raporty (gitignored)
.env                 # Konfiguracja lokalna (gitignored)
.env.example         # Szablon konfiguracji
```

## Wymagania

- Python 3.11+
- Zależności: `pip install -r requirements.txt`

## Konfiguracja

Skopiuj plik `.env.example` jako `.env` i uzupełnij wartości:

```bash
cp .env.example .env
```

| Zmienna | Opis | Domyślnie |
|---------|------|-----------|
| `SEC_USER_AGENT` | Email kontaktowy wymagany przez SEC EDGAR | `InvestSnap/1.0 (investsnap-report-bot)` |
| `OPENFIGI_API_KEY` | Klucz API OpenFIGI (opcjonalny — przyspiesza mapowanie CUSIP → ticker) | _(pusty)_ |
| `DATA_DIR` | Folder z danymi wejściowymi | `dane` |
| `REPORTS_DIR` | Folder na raporty | `raporty` |
| `ENABLE_FUND_TRACKING` | Włącz/wyłącz sekcję funduszy 13F | `true` |

## Uruchomienie

### Lokalnie

```bash
pip install -r requirements.txt
python -m src
```

### Docker

```bash
docker compose up --build
```

Docker automatycznie wczytuje zmienne z pliku `.env`.

## Dane wejściowe

Umieść w folderze `dane/`:

| Plik | Opis |
|------|------|
| Plik z `OPEN POSITION` w nazwie | Otwarte pozycje (kolumny: Position, Symbol, Purchase value, Gross P/L) |
| Plik z `CASH OPERATION HISTORY` w nazwie | Historia operacji gotówkowych (kolumny: ID, Type, Amount) |
| `notatki.txt` | Opcjonalny dziennik inwestycyjny (czyszczony po wygenerowaniu raportu) |
| `etf_geografia.json` | Opcjonalny plik look-through dla ETF |
| `mapa_symboli.json` | Opcjonalna ręczna mapa symboli (region, typ, waluta) |

### Okres raportu

Okres jest automatycznie wykrywany z nazwy pliku danych (wzorzec: `..._YYYY-MM-DD_YYYY-MM-DD.xlsx`).
Jeśli nazwa pliku zawiera zakres dat, raport wyświetli go w nagłówku.

## Wyjście

Raport Markdown + wykresy PNG trafiają do folderu `raporty/`.