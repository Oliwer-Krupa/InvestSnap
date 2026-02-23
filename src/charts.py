"""Generowanie wykresow kolowych (donut) dla raportow InvestSnap."""

from __future__ import annotations

from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
from matplotlib import pyplot as plt

from src.config import DONUT_COLORS

# =========================================================================
# Formatowanie
# =========================================================================


def format_number(value: float) -> str:
    """Formatuje liczbe do postaci ``1 234,56`` (separator tysiecy = spacja, dziesietny = przecinek)."""
    formatted = f"{value:,.2f}"
    return formatted.replace(",", " ").replace(".", ",")


# =========================================================================
# Wykres kolowy (donut)
# =========================================================================


def save_pie_chart(series: pd.Series, title: str, output_path: Path) -> bool:
    """Zapisuje wykres donut na dysk. Zwraca ``True`` jesli plik zostal zapisany."""
    if series.empty or series.sum() <= 0:
        return False

    labels = [str(item) for item in series.index]
    total = float(series.sum())
    percentages = [v / total * 100 for v in series.values]
    legend_labels = [
        f"{label}: {pct:.1f}% ({format_number(float(val))})"
        for label, pct, val in zip(labels, percentages, series.values)
    ]
    colors = [DONUT_COLORS[i % len(DONUT_COLORS)] for i in range(len(series))]

    fig, ax = plt.subplots(figsize=(11, 7), facecolor="#F5F7FA")
    wedges, _, autotexts = ax.pie(
        series.values,
        labels=None,
        colors=colors,
        autopct=lambda pct: f"{pct:.1f}%" if pct >= 3 else "",
        startangle=90,
        counterclock=False,
        pctdistance=0.8,
        wedgeprops={"width": 0.45, "edgecolor": "white", "linewidth": 1.5},
    )

    for txt in autotexts:
        txt.set_color("#1F2937")
        txt.set_fontsize(10)
        txt.set_fontweight("bold")

    ax.text(
        0, 0,
        f"Razem\n{format_number(total)}",
        ha="center", va="center",
        fontsize=12, fontweight="bold", color="#0F172A",
    )
    ax.set_title(title, fontsize=15, fontweight="bold", color="#0F172A", pad=16)
    ax.axis("equal")
    ax.legend(
        wedges, legend_labels,
        title="Udzial",
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        frameon=False,
        fontsize=10,
        title_fontsize=11,
    )

    fig.tight_layout()
    fig.savefig(str(output_path), dpi=220, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return True
