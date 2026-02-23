"""Zapisywanie i porownanie snapshotow raportu."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


def _snapshot_filename(ts: datetime) -> str:
    return f"snapshot_{ts:%Y%m%d_%H%M%S}.json"


def load_latest_snapshot(snapshots_dir: Path) -> dict[str, object] | None:
    """Zwraca ostatni snapshot (po nazwie pliku), albo None."""
    if not snapshots_dir.exists():
        return None

    candidates = sorted(
        [p for p in snapshots_dir.glob("snapshot_*.json") if p.is_file()],
        key=lambda p: p.name,
    )
    if not candidates:
        return None

    latest = candidates[-1]
    try:
        return json.loads(latest.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_snapshot(snapshots_dir: Path, payload: dict[str, object], *, now: datetime | None = None) -> Path:
    """Zapisuje snapshot i zwraca sciezke do zapisanego pliku."""
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    timestamp = now or datetime.now()
    path = snapshots_dir / _snapshot_filename(timestamp)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def build_delta(
    *,
    current_metrics: dict[str, float],
    current_positions: dict[str, float],
    previous_snapshot: dict[str, object] | None,
) -> dict[str, object]:
    """Liczy delty kluczowych metryk i najwiekszy mover po symbolu."""
    if not previous_snapshot:
        return {"missing_previous": True}

    prev_metrics = previous_snapshot.get("metrics", {})
    prev_positions = previous_snapshot.get("positions", {})
    if not isinstance(prev_metrics, dict) or not isinstance(prev_positions, dict):
        return {"missing_previous": True}

    def metric(name: str) -> float:
        try:
            return float(prev_metrics.get(name, 0.0))
        except (TypeError, ValueError):
            return 0.0

    all_symbols = set(current_positions.keys()) | set(prev_positions.keys())
    movers: dict[str, float] = {}
    for symbol in all_symbols:
        current_value = float(current_positions.get(symbol, 0.0))
        prev_value = float(prev_positions.get(symbol, 0.0))
        movers[symbol] = current_value - prev_value

    largest_symbol = ""
    largest_change = 0.0
    if movers:
        largest_symbol, largest_change = max(
            movers.items(),
            key=lambda item: abs(item[1]),
        )

    return {
        "missing_previous": False,
        "equity_delta_pln": current_metrics.get("equity", 0.0) - metric("equity"),
        "positions_value_delta_pln": current_metrics.get("positions_value", 0.0) - metric("positions_value"),
        "cash_delta_pln": current_metrics.get("cash", 0.0) - metric("cash"),
        "unrealized_delta_pln": current_metrics.get("unrealized_pl", 0.0) - metric("unrealized_pl"),
        "realized_delta_pln": current_metrics.get("realized_pl", 0.0) - metric("realized_pl"),
        "dividends_net_delta_pln": current_metrics.get("dividends_net", 0.0) - metric("dividends_net"),
        "largest_mover_symbol": largest_symbol,
        "largest_mover_change_pln": largest_change,
    }
