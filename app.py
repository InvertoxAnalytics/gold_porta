#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app_portafolioGold.py — MT5 Trading Lab (Portfolio + Deep Analyzer)
===================================================================
Fusión maestra de:
  • Portafolio (app.py)  → Análisis multi-activo, Risk Parity, semáforo, PDF
  • Gold Anal  (app_gold.py) → Micro-análisis, sesiones FX, gaps, Monte Carlo

Plan maestro tomado de lo mejor de 3 propuestas (Claude, Codex, Gemini):
  ✓ Claude:  Presets por símbolo, MC generalizado, todo Plotly, session_state robusto
  ✓ Codex:   Drawdowns consistentes (High/Low), ingesta unificada, riesgos/mitigaciones
  ✓ Gemini:  Drill-down macro→micro, detección de anomalías cruzada, reporte 360°

Flujo:
  1) Subir CSVs MT5 (multi-archivo, misma temporalidad)
  2) 📥 Procesar CSVs (carga / validación / merge por símbolo)
  3) Configurar rango y ▶️ Iniciar análisis
  4) Ver Resumen Ejecutivo (insights + semáforos + portafolio recomendado)
  5) Modo Analista: Drawdowns, Par Óptimo, Picos Vol, Micro-Análisis, Monte Carlo
  6) Exportar CSV / 📄 Reporte PDF / 📦 ZIP

Notas:
  - No resamplea. Se asume que los CSV ya vienen en la MISMA temporalidad.
  - MT5 suele traer Tick Volume (<TICKVOL>), no volumen real.
  - Monte Carlo generalizado: CONTRACT_SIZE configurable por símbolo.
  - Sesiones FX (Asia/Londres/NY/Post-NY) con ajuste DST automático.
"""

from __future__ import annotations

import io
import re
import math
import time
import hashlib
import zipfile
import tempfile
import calendar
import warnings
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Optional, List

import numpy as np
import pandas as pd
import pytz
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# ── Clustering (opcional) ────────────────────────────────────────────────────
try:
    from scipy.cluster.hierarchy import linkage, leaves_list, fcluster
    from scipy.spatial.distance import squareform
    SCIPY_OK = True
except Exception:
    SCIPY_OK = False

# ── PDF (opcional) ───────────────────────────────────────────────────────────
try:
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                    Table, TableStyle, PageBreak)
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.pagesizes import letter
    from reportlab.lib import colors
    REPORTLAB_OK = True
except Exception:
    REPORTLAB_OK = False

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")


# ╔══════════════════════════════════════════════════════════════════════════════
# ║  SECCIÓN 1: CONSTANTES Y PRESETS POR SÍMBOLO
# ║  (De Claude Plan: presets editables; de Gemini: parametrización contrato)
# ╚══════════════════════════════════════════════════════════════════════════════

TZ_CDMX = pytz.timezone("America/Mexico_City")
TZ_NY   = pytz.timezone("America/New_York")
TZ_LON  = pytz.timezone("Europe/London")

# ── Presets de instrumentos conocidos ────────────────────────────────────────
# Cada preset define: contract_size (onzas/unidades por lote estándar),
# sessions_enabled (si aplica el análisis por sesión FX), y defaults
# razonables para el Monte Carlo (distancia, TP, umbrales de vela, swaps).
SYMBOL_PRESETS = {
    "XAUUSD": {
        "contract_size": 100,        # 100 onzas por lote
        "sessions_enabled": True,    # Forex/commodity → sesiones activas
        "mc_distance": 0.25,         # USD entre niveles del grid
        "mc_tp_offset": 0.06,        # USD de TP sobre PMP
        "mc_stop_loss": -200_000,    # USD stop global
        "candle_thresholds": [5, 10, 15, 20, 25, 30],
        "streak_thresholds": [10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60],
        "swap_long": -4.0,           # USD/lote/noche
        "swap_short": 1.0,
    },
    "XAGUSD": {
        "contract_size": 5000,
        "sessions_enabled": True,
        "mc_distance": 0.005,
        "mc_tp_offset": 0.002,
        "mc_stop_loss": -100_000,
        "candle_thresholds": [0.05, 0.10, 0.15, 0.20, 0.25, 0.30],
        "streak_thresholds": [0.1, 0.15, 0.2, 0.25, 0.3],
        "swap_long": -3.0,
        "swap_short": 0.5,
    },
    "EURUSD": {
        "contract_size": 100_000,
        "sessions_enabled": True,
        "mc_distance": 0.0005,
        "mc_tp_offset": 0.0002,
        "mc_stop_loss": -50_000,
        "candle_thresholds": [0.0005, 0.001, 0.0015, 0.002, 0.003, 0.005],
        "streak_thresholds": [0.001, 0.002, 0.003, 0.005],
        "swap_long": -6.0,
        "swap_short": 1.5,
    },
    "US30": {
        "contract_size": 1,
        "sessions_enabled": False,   # Índice → sin sesiones FX
        "mc_distance": 50,
        "mc_tp_offset": 20,
        "mc_stop_loss": -100_000,
        "candle_thresholds": [50, 100, 200, 300, 500, 1000],
        "streak_thresholds": [100, 200, 300, 500],
        "swap_long": -15.0,
        "swap_short": 5.0,
    },
    "BTCUSD": {
        "contract_size": 1,
        "sessions_enabled": False,   # 24/7 → sin sesiones
        "mc_distance": 100,
        "mc_tp_offset": 50,
        "mc_stop_loss": -200_000,
        "candle_thresholds": [50, 100, 200, 500, 1000, 2000],
        "streak_thresholds": [100, 200, 500, 1000],
        "swap_long": -20.0,
        "swap_short": -20.0,
    },
}

# Preset genérico (fallback) para cualquier símbolo desconocido
DEFAULT_PRESET = {
    "contract_size": 100,
    "sessions_enabled": True,
    "mc_distance": 1.0,
    "mc_tp_offset": 0.5,
    "mc_stop_loss": -100_000,
    "candle_thresholds": [5, 10, 15, 20, 25, 30],
    "streak_thresholds": [10, 15, 20, 25, 30],
    "swap_long": -5.0,
    "swap_short": 1.0,
}


def get_preset(symbol: str) -> dict:
    """Busca preset por símbolo; si no existe, devuelve el default."""
    sym = symbol.upper().replace(".", "").replace("/", "")
    return SYMBOL_PRESETS.get(sym, DEFAULT_PRESET).copy()


# ╔══════════════════════════════════════════════════════════════════════════════
# ║  SECCIÓN 2: UI SETUP — PAGE CONFIG + CSS THEME
# ║  (Tema oscuro "broker" del Portafolio original, pulido)
# ╚══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="MT5 Trading Lab",
    page_icon="📊",
    layout="wide",
)
st.title("📊 MT5 Trading Lab")
st.caption("Portfolio + Deep Analyzer · Flujo guiado · Sin resampling")
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=Space+Grotesk:wght@500;600;700&display=swap');
    :root {
        --font-head: "Space Grotesk", "IBM Plex Sans", sans-serif;
        --font-body: "IBM Plex Sans", "Space Grotesk", sans-serif;
        --ink: #e5e7eb; --muted: #9aa5b1; --accent: #3b82f6;
        --accent-strong: #2563eb; --card: #0f172a; --card-2: #111827;
        --stroke: #1f2937; --shadow: 0 18px 36px rgba(2,6,23,0.45);
    }
    html, body, [class*="css"] { font-family: var(--font-body); color: var(--ink); }
    .stApp {
        background:
            radial-gradient(900px 520px at -10% -20%, rgba(59,130,246,0.18) 0%, transparent 60%),
            radial-gradient(900px 520px at 110% -10%, rgba(14,165,233,0.16) 0%, transparent 55%),
            linear-gradient(180deg, #0b1220 0%, #0f172a 55%, #0a1120 100%);
        position: relative;
        overflow: hidden;
        z-index: 0;
    }
    /* Asegura que el contenido quede por encima del fondo animado */
    .stApp > * { position: relative; z-index: 1; }
    /* Ondas suaves animadas */
    .stApp::before {
        content: "";
        position: absolute;
        inset: -20% -10%;
        background:
            repeating-conic-gradient(from 0deg, rgba(59,130,246,0.08) 0deg, rgba(59,130,246,0.0) 25deg, rgba(14,165,233,0.07) 35deg, rgba(14,165,233,0.0) 60deg);
        filter: blur(10px);
        opacity: 0.25;
        animation: drift 22s ease-in-out infinite alternate;
        pointer-events: none;
        z-index: 0;
    }
    @keyframes drift {
        0%   { transform: translate3d(-6%, -4%, 0) scale(1.02) rotate(0deg); }
        50%  { transform: translate3d(4%, 3%, 0) scale(1.05) rotate(2deg); }
        100% { transform: translate3d(-3%, 6%, 0) scale(1.0) rotate(-2deg); }
    }
    .block-container { padding-top: 2.25rem; max-width: 1400px; }
    h1,h2,h3,h4,h5,h6 { font-family: var(--font-head); letter-spacing: -0.02em; color: var(--ink); }
    h1 { font-size: 2.1rem; }
    p, li, span { color: var(--ink); }
    header[data-testid="stHeader"] { background: rgba(11,18,32,0.92); border-bottom: 1px solid var(--stroke); backdrop-filter: blur(6px); }
    header[data-testid="stHeader"] * { color: #cbd5e1; }
    header[data-testid="stHeader"] svg { fill: #cbd5e1; }
    div[data-testid="stToolbar"] { background: rgba(15,23,42,0.9); border: 1px solid var(--stroke); border-radius: 999px; padding: 0.2rem 0.6rem; box-shadow: 0 8px 18px rgba(2,6,23,0.35); }
    div[data-testid="stDecoration"] { background: linear-gradient(90deg, #3b82f6, #06b6d4); height: 3px; }
    section[data-testid="stSidebar"] { background: linear-gradient(180deg, #0b1220 0%, #0f172a 100%); border-right: 1px solid var(--stroke); }
    section[data-testid="stSidebar"] h1, section[data-testid="stSidebar"] h2, section[data-testid="stSidebar"] h3, section[data-testid="stSidebar"] p, section[data-testid="stSidebar"] span, section[data-testid="stSidebar"] label { color: #e5e7eb; }
    .stButton > button { background: linear-gradient(135deg, var(--accent), var(--accent-strong)); color: #fff; border: none; padding: 0.55rem 1.1rem; border-radius: 999px; box-shadow: 0 12px 22px rgba(37,99,235,0.28); font-weight: 600; }
    .stButton > button:hover { filter: brightness(1.05); transform: translateY(-1px); }
    .stButton > button:active { transform: translateY(0); }
    /* Botones full width en sidebar y columnas para tamaños consistentes */
    section[data-testid="stSidebar"] .stButton > button,
    .block-container .stButton > button {
        width: 100%;
        min-height: 38px;
        white-space: nowrap;
    }
    div[data-testid="stMetric"] { background: var(--card); border: 1px solid var(--stroke); border-radius: 16px; padding: 0.85rem 1rem; box-shadow: var(--shadow); }
    div[data-testid="stMetric"] label { color: var(--muted); font-weight: 600; }
    div[data-testid="stDataFrame"] { background: var(--card); border: 1px solid var(--stroke); border-radius: 14px; box-shadow: var(--shadow); padding: 0.2rem; }
    div[data-testid="stDataFrame"] [role="grid"] { background: var(--card); }
    div[data-testid="stDataFrame"] [role="row"] { background: var(--card-2); }
    div[data-testid="stDataFrame"] [role="row"]:nth-child(even) { background: #0c1424; }
    div[data-testid="stDataFrame"] [role="gridcell"], div[data-testid="stDataFrame"] [role="columnheader"] { color: var(--ink); }
    div[data-testid="stDataFrame"] [role="columnheader"] { background: #111b2d; font-weight: 600; }
    div[data-testid="stTabs"] button[role="tab"] { border-radius: 999px; padding: 0.35rem 0.9rem; margin-right: 0.4rem; background: #111b2d; color: var(--ink); font-weight: 600; }
    div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] { background: var(--accent); color: #fff; box-shadow: 0 8px 18px rgba(37,99,235,0.25); }
    div[data-testid="stMarkdownContainer"] > p { color: var(--ink); }
    .stCaption { color: var(--muted); }
    div[data-testid="stFileUploader"] { background: var(--card); border: 1px dashed #24324a; border-radius: 16px; padding: 0.75rem; }
    div[data-testid="stFileUploader"] button { background: #111b2d; color: var(--ink); border: 1px solid var(--stroke); border-radius: 999px; font-weight: 600; box-shadow: none; }
    div[data-testid="stExpander"] { background: var(--card); border: 1px solid var(--stroke); border-radius: 14px; box-shadow: var(--shadow); }
    hr { border: none; border-top: 1px solid var(--stroke); margin: 1.5rem 0; }
    /* Aviso neutro personalizado */
    .alert-neutral {
        background: linear-gradient(135deg, #111827 0%, #0b1220 100%);
        border: 1px solid #1f2937;
        color: #e5e7eb;
        padding: 0.85rem 1rem;
        border-radius: 14px;
        box-shadow: var(--shadow);
        font-weight: 600;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def rerun():
    """Wrapper de rerun compatible con versiones antiguas de Streamlit."""
    try:
        st.rerun()
    except Exception:
        st.experimental_rerun()


# ╔══════════════════════════════════════════════════════════════════════════════
# ║  SECCIÓN 3: SESIONES FX + GAPS (de gold_anal, generalizado)
# ║  Asia/Londres/NY/Post-NY con ajuste DST automático
# ╚══════════════════════════════════════════════════════════════════════════════

SESSION_ORDER = ["Asia", "Londres", "NY", "Post-NY"]


def session_label(ts: pd.Timestamp) -> str:
    """Etiqueta la sesión FX usando horas locales reales (CDMX/LON/NY) para evitar
    errores en los cambios de horario de verano."""
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
        ts = ts.tz_localize(TZ_CDMX)
    ts_cdmx = ts.tz_convert(TZ_CDMX)
    ts_lon = ts_cdmx.tz_convert(TZ_LON)
    ts_ny = ts_cdmx.tz_convert(TZ_NY)

    hr_cdmx = ts_cdmx.hour
    hr_lon = ts_lon.hour
    hr_ny = ts_ny.hour

    if 17 <= hr_cdmx or hr_cdmx < 1:
        return "Asia"
    if 8 <= hr_lon < 14:
        return "Londres"
    if 8 <= hr_ny < 16:
        return "NY"
    return "Post-NY"


def classify_gap(prev_ts: pd.Timestamp, curr_ts: pd.Timestamp,
                 daily_min: int = 45, weekend_min: int = 60) -> str:
    """Clasifica el tipo de gap entre dos velas consecutivas.
    - no_gap: delta < 45 min (continuidad normal)
    - weekend_gap: viernes→domingo/lunes con delta >= 60 min
    - daily_break: cierre diario (hora previa 15:00-18:00 CDMX)
    - gap: cualquier otro hueco >= 45 min"""
    delta = (curr_ts - prev_ts).total_seconds() / 60
    if delta < daily_min:
        return "no_gap"
    if prev_ts.weekday() == 4 and curr_ts.weekday() in {6, 0} and delta >= weekend_min:
        return "weekend_gap"
    if 15 <= prev_ts.hour <= 18:
        return "daily_break"
    return "gap"


def gap_priority(gt: str) -> int:
    """Prioridad numérica de cada tipo de gap (mayor = peor)."""
    return {"no_gap": 0, "gap": 1, "daily_break": 2, "weekend_gap": 3}[gt]


def scan_gap_ext(df: pd.DataFrame, a: int, b: int) -> Tuple[str, float, float]:
    """Escanea el tramo [a,b] del DataFrame para encontrar el peor gap.
    Retorna: (tipo_gap, duración_minutos, tamaño_puntos)."""
    ts = df["datetime_cdmx"] if "datetime_cdmx" in df.columns else df.index
    worst, worst_delta, worst_pts = "no_gap", 0.0, 0.0
    for i in range(a + 1, b + 1):
        if i >= len(df):
            break
        prev_t = ts.iat[i - 1] if hasattr(ts, "iat") else ts[i - 1]
        curr_t = ts.iat[i] if hasattr(ts, "iat") else ts[i]
        gtype = classify_gap(prev_t, curr_t)
        if gtype == "no_gap":
            continue
        delta = (curr_t - prev_t).total_seconds() / 60
        gap_pts = abs(df["Open"].iat[i] - df["Close"].iat[i - 1])
        if (gap_priority(gtype) > gap_priority(worst)) or (
            gtype == worst and gap_pts > worst_pts
        ):
            worst, worst_delta, worst_pts = gtype, delta, gap_pts
    return worst, worst_delta, worst_pts


def count_rollovers(start: pd.Timestamp, end: pd.Timestamp, rollover_hr: int) -> int:
    """Cuenta rollovers (cruces de hora de rollover) entre dos timestamps CDMX.
    Se usa para calcular costos de swap overnight."""
    if end <= start:
        return 0
    first = start.normalize() + pd.Timedelta(hours=rollover_hr)
    if start >= first:
        first += pd.Timedelta(days=1)
    if end < first:
        return 0
    return (end - first).days + 1


# ╔══════════════════════════════════════════════════════════════════════════════
# ║  SECCIÓN 4: LOADER UNIFICADO (lo mejor de ambos proyectos)
# ║  Soporta ambos formatos MT5, encoding auto, merge por símbolo
# ╚══════════════════════════════════════════════════════════════════════════════

USECOLS = ["<DATE>", "<TIME>", "<OPEN>", "<HIGH>", "<LOW>", "<CLOSE>", "<TICKVOL>"]
RENAME = {
    "<DATE>": "Date", "<TIME>": "Time", "<OPEN>": "Open",
    "<HIGH>": "High", "<LOW>": "Low", "<CLOSE>": "Close",
    "<TICKVOL>": "Volume",
}


def detect_encoding(b: bytes) -> str:
    """Detecta encoding del archivo: UTF-16, UTF-8-BOM, o UTF-8."""
    if b.startswith(b"\xff\xfe") or b.startswith(b"\xfe\xff"):
        return "utf-16"
    if b.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    if b"\x00" in b[:2000]:
        return "utf-16"
    return "utf-8-sig"


def first_line_text(b: bytes, enc: str) -> str:
    """Extrae la primera línea del archivo para detectar formato."""
    i = b.find(b"\n")
    head = b if i == -1 else b[:i]
    try:
        return head.decode(enc, errors="ignore").lstrip()
    except Exception:
        return head.decode("utf-8", errors="ignore").lstrip()


def infer_symbol_from_filename(name: str) -> str:
    """Extrae símbolo del nombre de archivo. Ej: 'XAUUSD_M1.csv' → 'XAUUSD'."""
    base = name.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    base = base.rsplit(".", 1)[0].strip()
    sym = base.split("_")[0].upper()
    sym = re.split(r"[,\s;()\-]+", sym)[0]
    return sym.upper()


def _safe_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Convierte columnas a numérico de forma segura (errores → NaN)."""
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


@st.cache_data(show_spinner=False)
def load_and_prepare_bytes(file_bytes: bytes) -> Tuple[pd.DataFrame, Dict]:
    """Carga un CSV MT5 desde bytes crudos.
    Devuelve DataFrame con columnas:
      datetime_cdmx (tz-naive pero en hora CDMX),
      Open, High, Low, Close, Volume, range_pts
    Y un dict con metadatos de parsing."""
    enc = detect_encoding(file_bytes)
    head = first_line_text(file_bytes, enc)
    is_csv = head.startswith("<DATE>")

    info = {"encoding": enc, "hdr": "<DATE>" if is_csv else "other",
            "sep": None, "note": ""}
    bio = io.BytesIO(file_bytes)

    if is_csv:
        # Formato MT5 estándar con headers <DATE> <TIME> etc.
        df = None
        # Intento 1: separador TAB
        try:
            bio.seek(0)
            df = pd.read_csv(bio, sep="\t", usecols=USECOLS,
                             encoding=enc).rename(columns=RENAME)
            info["sep"] = "\\t"
        except Exception:
            df = None
        # Intento 2: whitespace genérico
        if df is None or df.empty:
            bio.seek(0)
            df = pd.read_csv(bio, sep=r"\s+", engine="python",
                             usecols=USECOLS, encoding=enc).rename(columns=RENAME)
            info["sep"] = "\\s+"

        dt_utc = pd.to_datetime(
            df["Date"].astype(str) + " " + df["Time"].astype(str),
            format="%Y.%m.%d %H:%M:%S", utc=True, errors="coerce",
        )
        df = df.assign(datetime_utc=dt_utc).dropna(subset=["datetime_utc"])
    else:
        # Formato alternativo sin headers (símbolo embebido en la línea)
        cols = ["Symbol", "Date", "Time", "Open", "High", "Low", "Close", "Volume"]
        bio.seek(0)
        df = pd.read_csv(bio, names=cols, header=None,
                         delim_whitespace=True, encoding=enc)
        dt_utc = pd.to_datetime(
            df["Date"].astype(str) + df["Time"].astype(str),
            format="%Y%m%d%H%M%S", utc=True, errors="coerce",
        )
        df = df.assign(datetime_utc=dt_utc).dropna(subset=["datetime_utc"])

    # Asegurar columnas numéricas
    df = _safe_numeric(df, ["Open", "High", "Low", "Close", "Volume"])
    df = df.dropna(subset=["Open", "High", "Low", "Close"])

    # Convertir UTC → CDMX (tz-naive para evitar problemas con Streamlit)
    dt_cdmx_naive = df["datetime_utc"].dt.tz_convert(TZ_CDMX).dt.tz_localize(None)
    df = df.assign(datetime_cdmx=dt_cdmx_naive).sort_values("datetime_cdmx")

    df["range_pts"] = df["High"] - df["Low"]
    out = df[["datetime_cdmx", "Open", "High", "Low", "Close",
              "Volume", "range_pts"]].copy()
    out = out.drop_duplicates(subset=["datetime_cdmx"], keep="last")
    return out, info


def to_indexed_ohlcv(raw: pd.DataFrame) -> pd.DataFrame:
    """Convierte el DataFrame del loader a formato indexado por datetime."""
    x = raw.copy()
    x = x.set_index("datetime_cdmx").sort_index()
    x.index.name = "datetime"
    x = x[~x.index.duplicated(keep="last")]
    return x


# ╔══════════════════════════════════════════════════════════════════════════════
# ║  SECCIÓN 5: TEMPORALIDAD Y ANUALIZACIÓN
# ╚══════════════════════════════════════════════════════════════════════════════

def infer_dt(index: pd.DatetimeIndex) -> Optional[pd.Timedelta]:
    """Infiere la resolución temporal del índice via mediana de deltas."""
    if index is None or len(index) < 3:
        return None
    d = pd.Series(index).diff().dropna()
    return d.median() if not d.empty else None


def timeframe_label(dt: Optional[pd.Timedelta]) -> str:
    """Convierte un Timedelta en etiqueta legible (1min, 5min, 1H, 4H, 1D)."""
    if dt is None or dt <= pd.Timedelta(0):
        return "—"
    sec = dt.total_seconds()
    cand = [("1min", 60), ("5min", 300), ("15min", 900), ("30min", 1800),
            ("1H", 3600), ("4H", 14400), ("1D", 86400)]
    for name, s in cand:
        if abs(sec - s) / s < 0.05:
            return name
    if sec < 60:
        return f"{sec:.0f}s"
    if sec < 3600:
        return f"{sec/60:.2f}min"
    if sec < 86400:
        return f"{sec/3600:.2f}h"
    return f"{sec/86400:.3f}D"


def bars_per_day_from_dt(dt: Optional[pd.Timedelta]) -> Optional[float]:
    """Calcula cuántas barras caben en 24h según la resolución temporal."""
    if dt is None or dt <= pd.Timedelta(0):
        return None
    sec = dt.total_seconds()
    return float(86400.0 / sec) if sec > 0 else None


def ann_factor_from_index(index: pd.DatetimeIndex, trading_days: int = 252) -> float:
    """Factor de anualización: para intraday = trading_days × barras_por_día."""
    dt = infer_dt(index)
    bpd = bars_per_day_from_dt(dt)
    if bpd is None:
        return float(trading_days)
    if dt is not None and dt >= pd.Timedelta(days=1):
        return float(trading_days)
    return float(trading_days) * float(bpd)


# ╔══════════════════════════════════════════════════════════════════════════════
# ║  SECCIÓN 6: INDICADORES TÉCNICOS
# ║  ADX/ATR, R², underwater curve, drawdown events (High/Low institucional)
# ╚══════════════════════════════════════════════════════════════════════════════

def compute_adx_atr(df: pd.DataFrame, n: int = 14) -> Tuple[pd.Series, pd.Series]:
    """Calcula ADX(n) y ATR(n) usando EWM (exponential weighted moving average)."""
    if df.empty:
        return pd.Series(dtype=float), pd.Series(dtype=float)

    close = df["Close"].astype(float)
    high = df["High"].astype(float).fillna(close)
    low = df["Low"].astype(float).fillna(close)

    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    tr1 = (high - low).abs()
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = tr.ewm(alpha=1 / n, adjust=False).mean()
    plus_dm_s = pd.Series(plus_dm, index=df.index).ewm(alpha=1/n, adjust=False).mean()
    minus_dm_s = pd.Series(minus_dm, index=df.index).ewm(alpha=1/n, adjust=False).mean()

    plus_di = 100 * (plus_dm_s / atr.replace(0, np.nan))
    minus_di = 100 * (minus_dm_s / atr.replace(0, np.nan))
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.ewm(alpha=1 / n, adjust=False).mean()
    return adx, atr


def rolling_r2_from_close(close: pd.Series, win: int = 200) -> pd.Series:
    """R² rolling: correlación² entre log(close) y una tendencia lineal."""
    close = close.dropna()
    if len(close) < win + 5:
        return pd.Series(index=close.index, dtype=float)
    y = np.log(close.astype(float))
    x = pd.Series(np.arange(len(y)), index=y.index, dtype=float)
    r = y.rolling(win).corr(x)
    return (r ** 2).rename("R2")


def underwater_curve(close: pd.Series) -> pd.Series:
    """Curva underwater: (precio / máximo acumulado) - 1. Siempre ≤ 0."""
    close = close.dropna()
    if close.empty:
        return pd.Series(dtype=float)
    return (close / close.cummax() - 1).rename("DD")


def drawdown_events(
    price: pd.DataFrame | pd.Series,
    min_new_high: float = 0.0,
    min_dd: float = 0.0,
) -> pd.DataFrame:
    """Detecta drawdowns NO solapados según especificación institucional.
    Usa High/Low (no Close) para detección de peaks/troughs.
    - Inicio: máximo histórico (High)
    - Fin: mínimo posterior (Low) antes de superar ese máximo
    - Cierre: cuando High SUPERA el máximo previo (+histéresis min_new_high)
    - DD% = (Trough Low - Peak High) / Peak High"""
    if isinstance(price, pd.DataFrame):
        x = price.sort_index()
        if not {"High", "Low"}.issubset(x.columns):
            return pd.DataFrame()
        high = x["High"].astype(float)
        low = x["Low"].astype(float)
    else:
        s = pd.Series(price).dropna().sort_index().astype(float)
        high = s
        low = s

    if high.empty or low.empty:
        return pd.DataFrame()

    eps = 1e-12
    events = []
    idx = high.index
    peak_high = float(high.iloc[0])
    peak_date = idx[0]
    trough_low = float(low.iloc[0])
    trough_date = idx[0]
    in_dd = False

    def maybe_append(recovery_dt):
        nonlocal in_dd
        ddp = trough_low / peak_high - 1.0
        if ddp <= -float(min_dd):
            events.append({
                "Peak": peak_date, "Peak High": peak_high,
                "Trough": trough_date, "Trough Low": trough_low,
                "Recovery": recovery_dt, "DD%": ddp,
            })
        in_dd = False

    for dt, h, l in zip(idx[1:], high.iloc[1:], low.iloc[1:]):
        h, l = float(h), float(l)
        threshold = peak_high * (1.0 + float(min_new_high))
        is_new_peak = h > threshold + eps

        if is_new_peak:
            if in_dd:
                maybe_append(dt)
            peak_high = h
            peak_date = dt
            trough_low = l
            trough_date = dt
        else:
            if not in_dd:
                in_dd = True
                trough_low = l
                trough_date = dt
            elif l < trough_low:
                trough_low = l
                trough_date = dt

    if in_dd:
        maybe_append(pd.NaT)

    ev = pd.DataFrame(events)
    if ev.empty:
        return ev

    ev["Peak"] = pd.to_datetime(ev["Peak"])
    ev["Trough"] = pd.to_datetime(ev["Trough"])
    ev["Recovery"] = pd.to_datetime(ev["Recovery"])
    ev["Dur Peak->Trough"] = ev["Trough"] - ev["Peak"]
    ev["Dur Trough->Recovery"] = ev["Recovery"] - ev["Trough"]
    ev["Dur Peak->Recovery"] = ev["Recovery"] - ev["Peak"]
    return ev.sort_values("DD%").reset_index(drop=True)


# ╔══════════════════════════════════════════════════════════════════════════════
# ║  SECCIÓN 7: MÉTRICAS DE ACTIVO + SEMANAL + ANOMALÍAS
# ╚══════════════════════════════════════════════════════════════════════════════

def compute_metrics(df: pd.DataFrame, trading_days: int = 252,
                    trend_win: int = 200, trend_lookback_days: int = 180) -> Optional[dict]:
    """Calcula 21 métricas para un activo individual: precio, CAGR, vol, Sharpe,
    Calmar, MaxDD, ADX, R², clasificación tendencial/lateral, etc."""
    close = df["Close"].dropna()
    if len(close) < max(250, trend_win + 20):
        return None

    ann = ann_factor_from_index(close.index, trading_days=trading_days)
    rets = close.pct_change().dropna()
    if rets.empty:
        return None

    span_years = max((close.index[-1] - close.index[0]).total_seconds() / (365.25 * 86400), 1e-9)
    total_ret = float(close.iloc[-1] / close.iloc[0] - 1.0)
    cagr = float((close.iloc[-1] / close.iloc[0]) ** (1.0 / span_years) - 1.0)

    mean_ann = float(rets.mean() * ann)
    vol_ann = float(rets.std(ddof=0) * np.sqrt(ann))
    sharpe = float(mean_ann / vol_ann) if vol_ann > 0 else np.nan

    dd = underwater_curve(close)
    mdd = float(dd.min()) if not dd.empty else np.nan
    calmar = float(cagr / abs(mdd)) if pd.notna(mdd) and mdd < 0 else np.nan
    dd_current = float(dd.iloc[-1]) if not dd.empty else np.nan

    avg_range_pct = float(
        ((df["High"] - df["Low"]).abs() / df["Close"])
        .replace([np.inf, -np.inf], np.nan).dropna().mean()
    )

    adx, atr = compute_adx_atr(df, 14)
    adx_last = float(adx.dropna().iloc[-1]) if not adx.dropna().empty else np.nan
    atr_last = float(atr.dropna().iloc[-1]) if not atr.dropna().empty else np.nan
    atr_pct = float(atr_last / close.iloc[-1]) if close.iloc[-1] != 0 else np.nan

    r2_series = rolling_r2_from_close(close, win=trend_win)
    r2_last = float(r2_series.dropna().iloc[-1]) if not r2_series.dropna().empty else np.nan

    # Clasificación: Tendencial si ADX≥25 y R²≥0.20, Lateral si ADX≤20 y R²<0.20
    label = "Mixto"
    if pd.notna(adx_last) and pd.notna(r2_last):
        if adx_last >= 25 and r2_last >= 0.20:
            label = "Tendencial"
        elif adx_last <= 20 and r2_last < 0.20:
            label = "Lateral"

    dt = infer_dt(close.index)
    bpd = bars_per_day_from_dt(dt) or 1.0
    lookback_bars = int(max(100, trend_lookback_days * bpd))
    sl = df.iloc[-lookback_bars:] if len(df) > lookback_bars else df

    adx_lb, _ = compute_adx_atr(sl, 14)
    r2_lb = rolling_r2_from_close(sl["Close"], win=min(trend_win, max(50, int(0.5 * lookback_bars))))
    z = pd.DataFrame({"ADX": adx_lb, "R2": r2_lb}).dropna()
    if z.empty:
        tend_share = np.nan
        lat_share = np.nan
    else:
        tend = (z["ADX"] >= 25) & (z["R2"] >= 0.20)
        lat = (z["ADX"] <= 20) & (z["R2"] < 0.20)
        tend_share = float(tend.mean())
        lat_share = float(lat.mean())

    vol_mean = float(df["Volume"].replace(0, np.nan).dropna().mean()) if df["Volume"].notna().any() else np.nan

    return {
        "Precio": float(close.iloc[-1]),
        "Retorno total": total_ret, "CAGR": cagr,
        "Mean ann": mean_ann, "Vol anual": vol_ann, "Sharpe": sharpe,
        "MaxDD": mdd, "DD actual": dd_current, "Calmar": calmar,
        "Avg rango%": avg_range_pct, "ATR14%": atr_pct,
        "ADX14": adx_last, "R2": r2_last,
        "% Tend": tend_share, "% Lat": lat_share,
        "Vol prom": vol_mean, "Tipo": label,
        "Barras": int(len(close)),
        "Desde": close.index.min(), "Hasta": close.index.max(),
        "AnnFactor": float(ann), "TF": timeframe_label(dt),
    }


def weekly_aggregation(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega datos OHLCV a resolución semanal."""
    if df.empty:
        return pd.DataFrame()
    x = df.copy()
    idx = x.index
    week_id = (idx - pd.to_timedelta(idx.weekday, unit="D")).normalize()
    x = x.assign(_week=week_id)
    agg = x.groupby("_week").agg(
        Open=("Open", "first"), High=("High", "max"),
        Low=("Low", "min"), Close=("Close", "last"),
        Volume=("Volume", "sum"),
    ).dropna(subset=["Close"])
    agg.index.name = "Week"
    agg["Ret semana"] = agg["Close"].pct_change()
    agg["Rango semana %"] = (agg["High"] - agg["Low"]) / agg["Close"].replace(0, np.nan)
    return agg


def this_week_summary(df: pd.DataFrame, trading_days: int = 252) -> Optional[dict]:
    """Resumen de la semana más reciente."""
    if df.empty or df["Close"].dropna().shape[0] < 10:
        return None
    w = weekly_aggregation(df)
    if w.empty or w.shape[0] < 3:
        return None
    last = w.iloc[-1]
    ann = ann_factor_from_index(df.index, trading_days=trading_days)
    last_week = w.index[-1]
    mask = (df.index >= last_week) & (df.index < last_week + pd.Timedelta(days=7))
    intr = df.loc[mask, "Close"].dropna().pct_change().dropna()
    vol_week_ann = float(intr.std(ddof=0) * np.sqrt(ann)) if intr.shape[0] > 5 else np.nan
    return {
        "Precio fin semana": float(last["Close"]),
        "Retorno semana": float(last["Ret semana"]) if pd.notna(last["Ret semana"]) else np.nan,
        "Rango semana %": float(last["Rango semana %"]) if pd.notna(last["Rango semana %"]) else np.nan,
        "Vol semana (ann)": vol_week_ann,
        "Volumen semana (suma)": float(last["Volume"]) if pd.notna(last["Volume"]) else np.nan,
        "WeekStart": w.index[-1],
    }


def week_anomaly_scores(df: pd.DataFrame, lookback_weeks: int = 52) -> Optional[dict]:
    """Calcula Z-scores semanales para detectar movimientos anómalos."""
    w = weekly_aggregation(df)
    if w.empty or w.shape[0] < max(10, lookback_weeks // 2):
        return None
    hist = w.iloc[:-1].copy()
    cur = w.iloc[-1].copy()

    ret_hist = hist["Ret semana"].dropna()
    if ret_hist.shape[0] < 8:
        z_ret = np.nan
    else:
        mu = ret_hist.tail(lookback_weeks).mean()
        sd = ret_hist.tail(lookback_weeks).std(ddof=0)
        z_ret = float((cur["Ret semana"] - mu) / sd) if sd and pd.notna(cur["Ret semana"]) else np.nan

    vol_hist = hist["Volume"].replace(0, np.nan).dropna()
    vol_ma = float(vol_hist.tail(lookback_weeks).mean()) if vol_hist.shape[0] else np.nan
    vol_ratio = float(cur["Volume"] / vol_ma) if vol_ma and pd.notna(cur["Volume"]) else np.nan

    rng_hist = hist["Rango semana %"].replace([np.inf, -np.inf], np.nan).dropna()
    pct_rng = float((rng_hist < cur["Rango semana %"]).mean()) if rng_hist.shape[0] >= 10 and pd.notna(cur["Rango semana %"]) else np.nan

    return {"Z Ret semana": z_ret, "Vol ratio vs MA": vol_ratio, "Pct rango semana": pct_rng}


def rolling_vol_peaks(close: pd.Series, win: int, top_n: int, ann: float):
    """Detecta los top_n picos de volatilidad rolling."""
    ret = close.pct_change().dropna()
    if len(ret) < win + 50:
        return None, None, None
    roll = ret.rolling(win).std(ddof=0) * np.sqrt(ann)
    peaks = roll.dropna().nlargest(top_n)
    table = pd.DataFrame({
        "Fecha": peaks.index,
        "Vol rolling (ann)": peaks.values,
        "Ret 1": ret.reindex(peaks.index).values,
        "Ret 5": close.pct_change(5).reindex(peaks.index).values,
    })
    return roll, peaks, table


# ╔══════════════════════════════════════════════════════════════════════════════
# ║  SECCIÓN 8: ANÁLISIS DE VELAS (de gold_anal, todo cacheado)
# ║  Conteo por umbral, distribución por hora, rachas, detección de gaps
# ╚══════════════════════════════════════════════════════════════════════════════

@st.cache_data(show_spinner=False)
def count_candles(df_range: pd.Series, thr: List[float]) -> pd.DataFrame:
    """Cuenta velas con range_pts >= cada umbral."""
    tot = len(df_range)
    return pd.DataFrame(
        [{"threshold": t, "count": int((df_range >= t).sum())} for t in thr]
    ).assign(pct_total=lambda d: d["count"] / tot * 100)


@st.cache_data(show_spinner=False)
def count_by_hour(_hours: pd.Series, _ranges: pd.Series, thr: List[float]) -> pd.DataFrame:
    """Distribución de velas >= umbral por hora CDMX (0-23)."""
    # Cache-friendly: Streamlit no puede hashear Index; convertimos a array plano.
    hours = np.asarray(_hours)
    ranges = np.asarray(_ranges)
    df_tmp = pd.DataFrame({"hour": hours, "range_pts": ranges})
    base = df_tmp.groupby("hour").size()
    rec = []
    for t in thr:
        sel = df_tmp[df_tmp["range_pts"] >= t].groupby("hour").size()
        for h in range(24):
            n, tot = int(sel.get(h, 0)), int(base.get(h, 0))
            rec.append(dict(threshold=t, hour_cdmx=h, count=n,
                            pct_in_hour=round(n / tot * 100, 3) if tot else np.nan))
    return pd.DataFrame(rec)


@st.cache_data(show_spinner=False)
def find_streaks(ranges_arr: np.ndarray, timestamps: np.ndarray,
                 thr: List[float]) -> pd.DataFrame:
    """Detecta rachas consecutivas de velas >= umbral."""
    rec = []
    for t in thr:
        mask = ranges_arr >= t
        grp = np.cumsum(np.concatenate(([0], np.diff(mask.astype(int)) != 0)))
        for g in np.unique(grp[mask]):
            idx = np.where(grp == g)[0]
            rec.append(dict(threshold=t, start=timestamps[idx[0]],
                            end=timestamps[idx[-1]], length=len(idx)))
    return pd.DataFrame(rec)


@st.cache_data(show_spinner=False)
def detect_gaps_in_series(timestamps: np.ndarray, opens: np.ndarray,
                          closes: np.ndarray, mins: int = 45) -> pd.DataFrame:
    """Detecta y clasifica gaps entre velas consecutivas."""
    out = []
    for i in range(1, len(timestamps)):
        g = classify_gap(timestamps[i - 1], timestamps[i], mins)
        if g == "no_gap":
            continue
        delta = (timestamps[i] - timestamps[i - 1]).total_seconds() / 60
        out.append(dict(prev_cdmx=timestamps[i - 1], next_cdmx=timestamps[i],
                        delta_min=delta,
                        abs_gap=abs(opens[i] - closes[i - 1]),
                        gap_type=g))
    return pd.DataFrame(out)


# ╔══════════════════════════════════════════════════════════════════════════════
# ║  SECCIÓN 9: MONTE CARLO GENERALIZADO (de gold_anal → cualquier símbolo)
# ║  Clave: CONTRACT_SIZE es parámetro, no constante hardcoded
# ╚══════════════════════════════════════════════════════════════════════════════

def parse_plan(text: str) -> List[Tuple[int, float]]:
    """Parsea un plan de pasos escalonado. Ej: '10:1.1,5:1.2' → [(10,1.1),(5,1.2)]"""
    out = []
    for tk in text.split(","):
        if not tk.strip():
            continue
        n, fac = tk.split(":", 1)
        out.append((int(float(n)), float(fac)))
    return out


def total_levels(plan: List[Tuple[int, float]]) -> int:
    """Total de niveles en un plan de pasos."""
    return sum(n for n, _ in plan)


def compute_total_lots(lot0: float, q0: float,
                       step_plan: List[Tuple[int, float]], max_steps: int) -> float:
    """Calcula el tamaño total de lotes teórico con el escalamiento definido."""
    add_levels = total_levels(step_plan) if step_plan else max_steps - 1
    if add_levels <= 0:
        return lot0
    if step_plan:
        seg_idx = 0
        remaining, q = step_plan[0]
    else:
        seg_idx, remaining, q = None, add_levels, q0
    total = lot = lot0
    for _ in range(add_levels):
        lot *= q
        total += lot
        remaining -= 1
        if remaining == 0 and step_plan and seg_idx + 1 < len(step_plan):
            seg_idx += 1
            remaining, q = step_plan[seg_idx]
    return total


def sample_start(n_rows: int, max_lv: int, n: int, lookahead: int = 10, seed: Optional[int] = None) -> np.ndarray:
    """Genera índices de arranque aleatorios para las simulaciones MC.
    seed opcional para reproducibilidad."""
    rng = np.random.default_rng(seed)
    need = max_lv * lookahead
    if n_rows - need <= 0:
        lookahead = max(n_rows // max_lv - 1, 1)
        warnings.warn("Histórico corto: lookahead reducido.")
    lim = n_rows - max_lv * lookahead
    return rng.choice(max(lim, 1), size=n, replace=n > lim)


def calc_eq(price: float, entry: list, lots: list,
            side: str, contract_size: float) -> float:
    """Calcula equity de una posición grid. Generalizado con contract_size."""
    return sum(
        l * (price - e if side == "BUY" else e - price)
        for l, e in zip(lots, entry)
    ) * contract_size


def add_lv(low: float, high: float, side: str, last: float, d: float) -> int:
    """Determina cuántos nuevos niveles del grid se activan en esta vela."""
    if side == "BUY" and low <= last - d:
        return math.floor((last - low) / d)
    if side == "SELL" and high >= last + d:
        return math.floor((high - last) / d)
    return 0


def simulate(closes, lows, highs, opens, timestamps, idx0, *,
             side, distance, lot0, q0, tp_offset, stop_loss, max_lv,
             step_plan, dd_inter, swap_long, swap_short, rollover_hr,
             contract_size, sessions_enabled):
    """Simula una operación grid completa desde idx0.
    Generalizado: contract_size y sessions son parámetros."""
    segments = step_plan if step_plan else [(max_lv - 1, q0)]
    seg_idx = 0
    remaining_seg, current_q = segments[0] if segments else (0, q0)

    entry = [opens[idx0]]
    lots = [lot0]
    pmp = entry[0]
    last = entry[0]
    steps = 1
    idx = idx0

    start_ts = timestamps[idx0]
    start_session = session_label(start_ts) if sessions_enabled else "—"

    eq = calc_eq(entry[0], entry, lots, side, contract_size)
    peak, max_dd = 0.0, 0.0
    rec = {thr: None for thr in dd_inter}
    adding = True
    hard = total_levels(step_plan) if step_plan else max_lv - 1

    while idx < len(closes) - 1:
        idx += 1
        price, low, high = closes[idx], lows[idx], highs[idx]

        # Actualizar equity y drawdown
        eq = calc_eq(price, entry, lots, side, contract_size)
        peak = max(peak, eq)
        max_dd = min(max_dd, eq - peak)
        dd_usd = -max_dd
        for thr in dd_inter:
            if rec[thr] is None and dd_usd >= thr:
                rec[thr] = dd_usd

        # STOP global → quiebra
        if max_dd <= stop_loss:
            break_ts = timestamps[idx]
            nights = count_rollovers(start_ts, break_ts, rollover_hr)
            swap_rate = swap_long if side == "BUY" else swap_short
            swap_usd = nights * sum(lots) * swap_rate
            result = dict(
                broke=True, dd_pico=max_dd, steps_used=steps,
                dur_min=idx - idx0, side=side,
                start_ts=start_ts, break_ts=break_ts,
                start_session=start_session,
                end_session=session_label(break_ts) if sessions_enabled else "—",
                swap_usd=swap_usd,
                exit_pnl_usd=eq + swap_usd,
            )
            for thr in dd_inter:
                result[f"dd_at_{thr}"] = rec[thr]
            return result

        # Take-profit
        if (side == "BUY" and price >= pmp + tp_offset) or \
           (side == "SELL" and price <= pmp - tp_offset):
            break

        # Añadir niveles del grid
        if adding and hard > 0:
            n_new = add_lv(low, high, side, last, distance)
            n_new = max(0, min(n_new, hard - (steps - 1)))
            for _ in range(n_new):
                if remaining_seg == 0 and seg_idx + 1 < len(segments):
                    seg_idx += 1
                    remaining_seg, current_q = segments[seg_idx]
                last = last - distance if side == "BUY" else last + distance
                entry.append(last)
                lots.append(lots[-1] * current_q)
                steps += 1
                remaining_seg = max(remaining_seg - 1, 0)
                pmp = sum(l * e for l, e in zip(lots, entry)) / sum(lots)
                if steps - 1 >= hard:
                    adding = False

    # Cierre normal (TP o fin de data)
    end_ts = timestamps[idx]
    nights = count_rollovers(start_ts, end_ts, rollover_hr)
    swap_rate = swap_long if side == "BUY" else swap_short
    swap_usd = nights * sum(lots) * swap_rate
    result = dict(
        broke=False, dd_pico=max_dd, steps_used=steps,
        dur_min=idx - idx0, side=side,
        start_ts=start_ts, exit_ts=end_ts,
        start_session=start_session,
        end_session=session_label(end_ts) if sessions_enabled else "—",
        swap_usd=swap_usd,
        exit_pnl_usd=calc_eq(closes[idx], entry, lots, side, contract_size) + swap_usd,
    )
    for thr in dd_inter:
        result[f"dd_at_{thr}"] = rec[thr]
    return result


def run_mc(df_indexed: pd.DataFrame, *, distance, lot0, q0, tp_offset,
           stop_loss, max_steps, n_samples, step_plan, dd_inter,
           swap_long, swap_short, rollover_hr, contract_size,
           sessions_enabled, seed: Optional[int] = None) -> pd.DataFrame:
    """Ejecuta N simulaciones Monte Carlo (BUY + SELL cada una).
    Generalizado para cualquier símbolo vía contract_size."""
    levels_extra = total_levels(step_plan) if step_plan else max_steps - 1
    idxs = sample_start(len(df_indexed), levels_extra, n_samples, seed=seed)

    # Extraer arrays para velocidad
    closes = df_indexed["Close"].values
    lows = df_indexed["Low"].values
    highs = df_indexed["High"].values
    opens = df_indexed["Open"].values
    timestamps = df_indexed.index.to_numpy()
    # Convertir timestamps a pd.Timestamp para sesiones
    ts_list = [pd.Timestamp(t) for t in timestamps]

    out = []
    prog = st.progress(0.0)
    next_up = max(n_samples // 20, 1)
    for i, base in enumerate(idxs, 1):
        for side in ("BUY", "SELL"):
            out.append(simulate(
                closes, lows, highs, opens, ts_list, base,
                side=side, distance=distance, lot0=lot0, q0=q0,
                tp_offset=tp_offset, stop_loss=stop_loss, max_lv=max_steps,
                step_plan=step_plan, dd_inter=dd_inter,
                swap_long=swap_long, swap_short=swap_short,
                rollover_hr=rollover_hr, contract_size=contract_size,
                sessions_enabled=sessions_enabled,
            ))
        if i % next_up == 0 or i == len(idxs):
            prog.progress(i / len(idxs))
    prog.empty()
    return pd.DataFrame(out)


# ╔══════════════════════════════════════════════════════════════════════════════
# ║  SECCIÓN 10: PORTAFOLIO (Risk Parity, Inverse Vol, Min Var, Clustering)
# ║  Idéntico al Portafolio original — probado y robusto
# ╚══════════════════════════════════════════════════════════════════════════════

def shrink_cov(cov: pd.DataFrame, lam: float = 0.10, jitter: float = 1e-10) -> pd.DataFrame:
    """Covarianza con shrinkage tipo Ledoit-Wolf: reduce ruido en correlaciones."""
    cov = cov.copy()
    diag = np.diag(np.diag(cov.values))
    shr = (1.0 - lam) * cov.values + lam * diag
    shr = shr + np.eye(shr.shape[0]) * jitter
    return pd.DataFrame(shr, index=cov.index, columns=cov.columns)


def inverse_vol_weights(vol: pd.Series) -> pd.Series:
    """Pesos inversamente proporcionales a la volatilidad."""
    v = vol.replace(0, np.nan).dropna()
    if v.empty:
        return pd.Series(dtype=float)
    w = 1.0 / v
    return w / w.sum()


def min_var_weights_unconstrained(cov: pd.DataFrame) -> pd.Series:
    """Portafolio de mínima varianza (puede tener pesos negativos = cortos)."""
    cov_ = cov.values
    n = cov_.shape[0]
    ones = np.ones((n, 1))
    try:
        inv = np.linalg.inv(cov_)
    except np.linalg.LinAlgError:
        inv = np.linalg.pinv(cov_)
    w = inv @ ones
    w = w / float(ones.T @ inv @ ones)
    return pd.Series(w.flatten(), index=cov.index)


def risk_parity_weights(cov: pd.DataFrame, max_iter: int = 5000,
                        tol: float = 1e-10) -> pd.Series:
    """Risk Parity iterativo: cada activo contribuye el mismo riesgo.
    Solo pesos positivos (long-only). Hasta max_iter iteraciones."""
    C = cov.values
    n = C.shape[0]
    w = np.ones(n) / n
    for _ in range(max_iter):
        port_var = float(w @ C @ w)
        mrc = C @ w
        rc = w * mrc
        target = port_var / n
        diff = rc - target
        if np.max(np.abs(diff)) < tol:
            break
        w *= target / (rc + 1e-16)
        w = np.clip(w, 1e-12, None)
        w /= w.sum()
    return pd.Series(w, index=cov.index)


def cluster_order_from_corr(corr: pd.DataFrame) -> Optional[np.ndarray]:
    """Ordena la matriz de correlación por clusters jerárquicos."""
    if not SCIPY_OK or corr.shape[0] < 3:
        return None
    dist = np.sqrt(0.5 * (1.0 - corr.fillna(0.0)))
    dist_cond = squareform(dist.values, checks=False)
    Z = linkage(dist_cond, method="average")
    return leaves_list(Z)


def cluster_labels_from_corr(corr: pd.DataFrame, k: int = 4) -> Optional[pd.Series]:
    """Asigna labels de cluster a cada activo (1..k)."""
    if not SCIPY_OK or corr.shape[0] < 3:
        return None
    dist = np.sqrt(0.5 * (1.0 - corr.fillna(0.0)))
    dist_cond = squareform(dist.values, checks=False)
    Z = linkage(dist_cond, method="average")
    labels = fcluster(Z, t=k, criterion="maxclust")
    return pd.Series(labels, index=corr.index, name="Cluster")


# ╔══════════════════════════════════════════════════════════════════════════════
# ║  SECCIÓN 11: SEMÁFORO DE RIESGO + INSIGHTS EJECUTIVOS
# ╚══════════════════════════════════════════════════════════════════════════════

def safe_imshow(df: pd.DataFrame, title: str):
    """Heatmap con plotly express (maneja TypeError en text_auto)."""
    try:
        return px.imshow(df, text_auto=".2f", aspect="auto", title=title)
    except TypeError:
        return px.imshow(df, aspect="auto", title=title)


def build_risk_table(summary: pd.DataFrame) -> pd.DataFrame:
    """Clasifica cada activo en semáforo de riesgo: 🔴 Alto / 🟡 Medio / 🟢 Bajo."""
    if summary is None or summary.empty:
        return pd.DataFrame()
    s = summary.copy()
    s["Vol_pct"] = s["Vol anual"].rank(pct=True)

    def classify(row):
        vol_pct = row.get("Vol_pct", np.nan)
        dd = row.get("DD actual", np.nan)
        mdd = row.get("MaxDD", np.nan)
        red = yellow = False
        if pd.notna(mdd) and mdd <= -0.30:
            red = True
        if pd.notna(dd) and dd <= -0.10 and pd.notna(vol_pct) and vol_pct >= 0.70:
            red = True
        if pd.notna(vol_pct) and vol_pct >= 0.85 and pd.notna(dd) and dd <= -0.06:
            red = True
        if not red:
            if pd.notna(mdd) and mdd <= -0.20:
                yellow = True
            if pd.notna(dd) and dd <= -0.06:
                yellow = True
            if pd.notna(vol_pct) and vol_pct >= 0.70:
                yellow = True
        if red:
            return "🔴 Alto", "Reducir exposición / cobertura"
        if yellow:
            return "🟡 Medio", "Operar con tamaño moderado"
        return "🟢 Bajo", "Ok para operar"

    out = []
    for sym, row in s.iterrows():
        level, action = classify(row)
        out.append({
            "Símbolo": sym, "Riesgo": level, "Acción": action,
            "Tipo": row.get("Tipo", "—"),
            "Vol anual": row.get("Vol anual", np.nan),
            "DD actual": row.get("DD actual", np.nan),
            "MaxDD": row.get("MaxDD", np.nan),
            "CAGR": row.get("CAGR", np.nan),
            "Score": row.get("Score", np.nan),
            "TF": row.get("TF", "—"),
        })
    return pd.DataFrame(out).set_index("Símbolo")


def build_executive_insights(summary, week_sev, reco, start, end) -> List[str]:
    """Genera bullets del Executive Brief automáticamente."""
    bullets = [f"Rango analizado: **{start} → {end}**."]
    if summary is not None and not summary.empty:
        top_vol = summary["Vol anual"].sort_values(ascending=False).head(1)
        if not top_vol.empty:
            sym = top_vol.index[0]
            bullets.append(f"Activo más volátil: **{sym}** (Vol={top_vol.iloc[0]:.1%}).")
        if "Score" in summary.columns and summary["Score"].notna().any():
            top_score = summary["Score"].sort_values(ascending=False).head(1)
            sym = top_score.index[0]
            row = summary.loc[sym]
            bullets.append(
                f"Mejor perfil riesgo/retorno: **{sym}** "
                f"(Score={row['Score']:.2f}, CAGR={row['CAGR']:.1%}, "
                f"Sharpe={row['Sharpe']:.2f}, MaxDD={row['MaxDD']:.1%})."
            )
        if "Tipo" in summary.columns:
            tend = int((summary["Tipo"] == "Tendencial").sum())
            lat = int((summary["Tipo"] == "Lateral").sum())
            mix = int((summary["Tipo"] == "Mixto").sum())
            bullets.append(f"Clasificación: **{tend} tendenciales**, **{lat} laterales**, **{mix} mixtos**.")
    if week_sev is not None and not week_sev.empty:
        sym = week_sev.index[0]
        r = week_sev.iloc[0]
        ret = r.get("Retorno semana", np.nan)
        sev = r.get("Severidad", np.nan)
        bullets.append(
            f"Esta semana, movimiento más relevante: **{sym}** "
            f"(Ret={ret:.1%} | Severidad={sev:.2f})."
        )
    if reco and reco.get("selected"):
        bullets.append(f"Portafolio sugerido: **{', '.join(reco['selected'])}**.")
        if reco.get("note"):
            bullets.append(f"Método: {reco['note']}")
    bullets.append("Nota: el 'volumen' es **TickVol** (indicador relativo, no volumen real).")
    return bullets


def compute_portfolio_metrics_from_reco(rets_df, reco, trading_days) -> dict:
    """Calcula métricas del portafolio recomendado."""
    out = {"vol": np.nan, "sharpe": np.nan, "maxdd": np.nan}
    if rets_df is None or rets_df.empty:
        return out
    w = reco.get("weights")
    sel = reco.get("selected", [])
    if w is None or getattr(w, "empty", True) or not sel:
        return out
    common = [c for c in sel if c in rets_df.columns and c in w.index]
    if len(common) < 2:
        return out
    R = rets_df[common].dropna(how="any")
    if R.shape[0] < 200:
        return out
    ww = w.reindex(common).astype(float)
    ww = ww / ww.sum()
    port_lr = (R * ww).sum(axis=1)
    ann = ann_factor_from_index(port_lr.index, trading_days=trading_days)
    mean = float(port_lr.mean() * ann)
    vol = float(port_lr.std(ddof=0) * np.sqrt(ann))
    sharpe = mean / vol if vol > 0 else np.nan
    port_curve = np.exp(port_lr.cumsum())
    dd = underwater_curve(port_curve)
    maxdd = float(dd.min()) if not dd.empty else np.nan
    out.update({"vol": vol, "sharpe": sharpe, "maxdd": maxdd})
    return out


# ╔══════════════════════════════════════════════════════════════════════════════
# ║  SECCIÓN 12: REPORTE PDF (2 páginas + sección MC si disponible)
# ╚══════════════════════════════════════════════════════════════════════════════

def df_to_table_data(df, cols, max_rows=12):
    """Convierte DataFrame a lista de listas para tabla de reportlab."""
    if df is None or df.empty:
        return [["—"]]
    x = df.head(max_rows)
    data = [["Símbolo"] + cols]
    for sym, row in x.iterrows():
        r = [sym]
        for c in cols:
            v = row.get(c, "")
            if isinstance(v, (float, np.floating)):
                if any(k in c for k in ["Vol", "CAGR", "DD", "MaxDD", "%"]):
                    r.append(f"{v*100:.1f}%")
                else:
                    r.append(f"{v:.2f}")
            else:
                r.append(str(v))
        data.append(r)
    return data


def make_pdf_report(title, start, end, symbols, tf_labels, bullets,
                    summary, risk_df, week_sev, reco, port_metrics) -> bytes:
    """Genera reporte PDF ejecutivo de 2+ páginas con reportlab."""
    if not REPORTLAB_OK:
        return b""
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, rightMargin=36,
                            leftMargin=36, topMargin=36, bottomMargin=36)
    styles = getSampleStyleSheet()
    story = []

    # Página 1: Resumen
    story.append(Paragraph(title, styles["Title"]))
    gen = datetime.now(TZ_CDMX).strftime("%Y-%m-%d %H:%M CDMX")
    story.append(Paragraph(f"Generado: {gen}", styles["Normal"]))
    story.append(Paragraph(f"Rango: {start} → {end}", styles["Normal"]))
    story.append(Paragraph(f"Activos: {', '.join(symbols)}", styles["Normal"]))
    story.append(Paragraph(f"Temporalidad: {', '.join(tf_labels) if tf_labels else '—'}", styles["Normal"]))
    story.append(Spacer(1, 12))

    story.append(Paragraph("Executive Brief", styles["Heading2"]))
    for b in bullets[:12]:
        story.append(Paragraph(f"• {b}", styles["Normal"]))
    story.append(Spacer(1, 12))

    if summary is not None and not summary.empty:
        story.append(Paragraph("Top volatilidad", styles["Heading2"]))
        data = df_to_table_data(summary.sort_values("Vol anual", ascending=False),
                                ["Vol anual", "CAGR", "Sharpe", "MaxDD", "Tipo"])
        t = Table(data, hAlign="LEFT")
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
        ]))
        story.append(t)
        story.append(Spacer(1, 10))

    if risk_df is not None and not risk_df.empty:
        story.append(Paragraph("Semáforo de riesgo", styles["Heading2"]))
        data = df_to_table_data(risk_df, ["Riesgo", "Acción", "Tipo", "Vol anual", "DD actual"])
        t = Table(data, hAlign="LEFT")
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))

    # Página 2: Portafolio
    story.append(PageBreak())
    story.append(Paragraph("Portafolio", styles["Title"]))
    if reco and reco.get("selected"):
        story.append(Paragraph(f"Selección: {', '.join(reco['selected'])}", styles["Normal"]))
        if reco.get("note"):
            story.append(Paragraph(f"Método: {reco['note']}", styles["Normal"]))
        story.append(Spacer(1, 10))
        w = reco.get("weights")
        if w is not None and not getattr(w, "empty", True):
            data = [["Símbolo", "Peso"]] + [[i, f"{float(v)*100:.1f}%"] for i, v in w.items()]
            t = Table(data, hAlign="LEFT")
            t.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
            ]))
            story.append(t)
        if port_metrics:
            story.append(Spacer(1, 10))
            story.append(Paragraph(
                f"Vol anual: {port_metrics.get('vol', np.nan)*100:.1f}% | "
                f"Sharpe: {port_metrics.get('sharpe', np.nan):.2f} | "
                f"MaxDD: {port_metrics.get('maxdd', np.nan)*100:.1f}%",
                styles["Normal"]
            ))
    else:
        story.append(Paragraph("No se pudo generar portafolio recomendado.", styles["Normal"]))

    story.append(Spacer(1, 12))
    story.append(Paragraph("Notas", styles["Heading2"]))
    story.append(Paragraph("• El 'volumen' proviene de TICKVOL. Úsalo como señal relativa.", styles["Normal"]))
    story.append(Paragraph("• Las métricas son descriptivas; no constituyen recomendación financiera.", styles["Normal"]))

    doc.build(story)
    return buf.getvalue()


# ╔══════════════════════════════════════════════════════════════════════════════
# ║  SECCIÓN 13: FORMATTING HELPERS
# ╚══════════════════════════════════════════════════════════════════════════════

def fmt_pct(x, digits=1):
    if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))):
        return "—"
    return f"{x*100:.{digits}f}%"


def fmt_num(x, digits=2):
    if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))):
        return "—"
    return f"{x:.{digits}f}"


# ╔══════════════════════════════════════════════════════════════════════════════
# ║  SECCIÓN 14: SESSION STATE + SIDEBAR + FILE PROCESSING + ANALYSIS
# ║  Flujo guiado de 3 pasos (idéntico al Portafolio original)
# ╚══════════════════════════════════════════════════════════════════════════════

# ── Session State Init ───────────────────────────────────────────────────────
for key, default in [
    ("processed", False), ("analysis", None), ("analysis_hash", None),
    ("processed_file_names", []), ("series_raw", {}),
    ("meta_df", pd.DataFrame()), ("ranges", None),
    ("mc_results", None), ("mc_params", None),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ── Sidebar: Modo + Carga ───────────────────────────────────────────────────
st.sidebar.header("🎩 Modo de uso")
mode = st.sidebar.radio(
    "¿Para quién es esta vista?",
    ["🎩 simple", "🧠 Analista (detallado)"],
    index=0,
)
SIMPLE = mode.startswith("🎩")

st.sidebar.markdown("---")
st.sidebar.header("Paso 1 — Sube CSVs")
files = st.sidebar.file_uploader(
    "Sube CSV MT5 (misma temporalidad)", type=["csv", "txt"],
    accept_multiple_files=True,
)

if not files:
    st.info("1) Sube CSVs en la barra lateral.\n\n2) Presiona **📥 Procesar CSVs**.")
    st.stop()

current_names = sorted([f.name for f in files])

# Invalidar si cambian archivos
if st.session_state.processed and current_names != st.session_state.processed_file_names:
    for k in ["processed", "analysis", "analysis_hash", "mc_results", "mc_params"]:
        st.session_state[k] = False if k == "processed" else None
    st.session_state.series_raw = {}
    st.session_state.meta_df = pd.DataFrame()
    st.session_state.ranges = None
    st.sidebar.warning("Cambio en archivos. Vuelve a procesar.")

with st.sidebar.expander("Símbolo por archivo (opcional)", expanded=False):
    overrides = {}
    for f in files:
        guess = infer_symbol_from_filename(f.name)
        sym = st.text_input(f.name, value=guess, key=f"sym_{f.name}")
        overrides[f.name] = sym.strip().upper()

st.sidebar.markdown("---")
colb1, colb2 = st.sidebar.columns(2)
btn_process_side = colb1.button("📥 Procesar", type="primary")
btn_reset = colb2.button("🧹 Reset", type="primary")

if btn_reset:
    for k in ["processed", "analysis", "analysis_hash", "mc_results", "mc_params"]:
        st.session_state[k] = False if k == "processed" else None
    st.session_state.series_raw = {}
    st.session_state.meta_df = pd.DataFrame()
    st.session_state.ranges = None
    st.session_state.processed_file_names = []
    rerun()

# ── Barra de progreso principal ──────────────────────────────────────────────
st.markdown("### ✅ Flujo recomendado")
stage = 0 if not st.session_state.processed else (1 if st.session_state.analysis is None else 2)
st.progress({0: 0.33, 1: 0.66, 2: 1.0}[stage])
if stage == 0:
    st.info("**Paso 1/3:** Sube CSVs → presiona **📥 Procesar CSVs**.")
elif stage == 1:
    st.info("**Paso 2/3:** Ajusta rango → presiona **▶️ Iniciar análisis**.")
else:
    st.success("**Paso 3/3:** Resultados listos. Abre **📌 Resumen Ejecutivo**.")

col_a, col_b, col_c = st.columns([1, 1, 2])
btn_process_main = col_a.button("📥 Procesar CSVs", type="primary", key="process_main")
btn_analyze_main = col_b.button("▶️ Iniciar análisis", type="primary", key="analyze_main")
col_c.caption("Tip: si cambias archivos → Procesar. Si cambias parámetros → Iniciar.")

process_now = bool(btn_process_side or btn_process_main)


# ── Procesar CSVs ────────────────────────────────────────────────────────────
def process_files(files_list, overrides_map):
    series_raw = {}
    meta_rows = []
    for f in files_list:
        sym = overrides_map.get(f.name, infer_symbol_from_filename(f.name))
        raw, info = load_and_prepare_bytes(f.getvalue())
        x = to_indexed_ohlcv(raw)
        if not x.empty:
            if sym in series_raw:
                comb = pd.concat([series_raw[sym], x]).sort_index()
                comb = comb[~comb.index.duplicated(keep="last")]
                series_raw[sym] = comb
            else:
                series_raw[sym] = x
        dt = infer_dt(x.index) if not x.empty else None
        meta_rows.append({
            "Archivo": f.name, "Símbolo": sym,
            "Barras": int(len(x)),
            "Desde": x.index.min().strftime("%Y-%m-%d %H:%M") if not x.empty else "—",
            "Hasta": x.index.max().strftime("%Y-%m-%d %H:%M") if not x.empty else "—",
            "Temporalidad": timeframe_label(dt),
            "Enc": info["encoding"], "Sep": info["sep"],
        })
    meta_df = pd.DataFrame(meta_rows)
    if not series_raw:
        return series_raw, meta_df, {"gmin": None, "gmax": None, "common_start": None, "common_end": None}
    symbols_all = sorted(series_raw.keys())
    gmin = min(series_raw[s].index.min() for s in symbols_all)
    gmax = max(series_raw[s].index.max() for s in symbols_all)
    common_start = max(series_raw[s].index.min() for s in symbols_all)
    common_end = min(series_raw[s].index.max() for s in symbols_all)
    return series_raw, meta_df, {"gmin": gmin, "gmax": gmax, "common_start": common_start, "common_end": common_end}


if process_now:
    with st.spinner("Procesando CSVs..."):
        sraw, mdf, ranges = process_files(files, overrides)
        st.session_state.series_raw = sraw
        st.session_state.meta_df = mdf
        st.session_state.ranges = ranges
        st.session_state.processed = True
        st.session_state.analysis = None
        st.session_state.analysis_hash = None
        st.session_state.mc_results = None
        st.session_state.processed_file_names = current_names

# ── Mostrar estado de carga ──────────────────────────────────────────────────
st.subheader("📦 Estado de carga")
if not st.session_state.processed:
    st.markdown(
        '<div class="alert-neutral">Aún no se han procesado los CSVs. Presiona <b>📥 Procesar CSVs</b>.</div>',
        unsafe_allow_html=True,
    )
    st.stop()

meta_df = st.session_state.meta_df
st.dataframe(meta_df, use_container_width=True)

series_raw = st.session_state.series_raw
if not series_raw:
    st.error("No se pudo cargar ningún símbolo.")
    st.stop()

symbols_all = sorted(series_raw.keys())
ranges = st.session_state.ranges
gmin, gmax = ranges["gmin"], ranges["gmax"]
common_start, common_end = ranges["common_start"], ranges["common_end"]

dt_labels = []
for s in symbols_all:
    dt = infer_dt(series_raw[s].index)
    dt_labels.append(timeframe_label(dt))
unique_labels = sorted(set([x for x in dt_labels if x != "—"]))

# ── Sidebar: Parámetros ─────────────────────────────────────────────────────
st.sidebar.markdown("---")
st.sidebar.header("Paso 2 — Configura rango")

modo_estricto = st.sidebar.checkbox("Exigir misma temporalidad", value=True)
if modo_estricto and len(unique_labels) > 1:
    st.error(f"Temporalidades: {unique_labels}. En modo estricto deben ser iguales.")
    st.stop()

st.sidebar.caption(f"Rango global: {gmin:%Y-%m-%d} → {gmax:%Y-%m-%d}")
st.sidebar.caption(f"Rango común:  {common_start:%Y-%m-%d} → {common_end:%Y-%m-%d}")

if "start_date" not in st.session_state or st.session_state.get("start_date") is None:
    st.session_state.start_date = gmin.date()
if "end_date" not in st.session_state or st.session_state.get("end_date") is None:
    st.session_state.end_date = gmax.date()

if st.sidebar.button("📌 Usar rango común"):
    st.session_state.start_date = common_start.date()
    st.session_state.end_date = common_end.date()

start = st.sidebar.date_input("Inicio", value=st.session_state.start_date)
end = st.sidebar.date_input("Fin", value=st.session_state.end_date)
if start > end:
    st.sidebar.error("Inicio no puede ser después de Fin.")
    st.stop()

trading_days = st.sidebar.selectbox("Días/año (anualización)", [252, 365], index=0)

with st.sidebar.expander("⚙️ Ajustes avanzados", expanded=not SIMPLE):
    trend_win = st.slider("Ventana R² (barras)", 50, 600, 200)
    trend_lookback_days = st.slider("Lookback % Tend/% Lat (días)", 30, 365, 180)
    roll_vol_days = st.slider("Vol rolling (días)", 1, 180, 30)
    roll_corr_days = st.slider("Rolling corr (días)", 1, 365, 90)
    top_dd = st.selectbox("Top drawdowns", [3, 5, 10, 15], index=2)
    top_peaks = st.selectbox("Top picos vol", [5, 10, 20, 30], index=1)
    min_new_high = st.slider("Ignorar micro-peaks (%)", 0.0, 1.0, 0.20, 0.05) / 100.0
    min_dd_event = st.slider("Solo eventos DD >= (%)", 0.0, 20.0, 1.0, 0.5) / 100.0
    use_common_for_corr = st.checkbox("Correlación/portafolio: usar rango común", value=True)
    n_assets = len(symbols_all)
    if n_assets < 3:
        portfolio_k = 2
    else:
        k_max = min(10, n_assets)
        portfolio_k = st.slider("Clusters (k)", 2, k_max, min(4, k_max))

# Defaults modo simple
if SIMPLE:
    trend_win = 200
    trend_lookback_days = 180
    roll_vol_days = 30
    roll_corr_days = 90
    top_dd = 5
    top_peaks = 10
    min_new_high = 0.002
    min_dd_event = 0.01
    use_common_for_corr = True
    portfolio_k = min(4, max(2, n_assets))


# ── Análisis principal ───────────────────────────────────────────────────────
def run_analysis(series_raw_, params_):
    start_ts = pd.Timestamp(params_["start"])
    end_ts = pd.Timestamp(params_["end"]) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    data = {}
    for s, df in series_raw_.items():
        m = (df.index >= start_ts) & (df.index <= end_ts)
        data[s] = df.loc[m].copy()

    symbols = [s for s in sorted(data.keys()) if not data[s].empty]
    metrics_rows, rets, week_rows = [], {}, []

    for s in symbols:
        m = compute_metrics(data[s], trading_days=params_["trading_days"],
                            trend_win=params_["trend_win"],
                            trend_lookback_days=params_["trend_lookback_days"])
        if m:
            m["Símbolo"] = s
            metrics_rows.append(m)
        close = data[s]["Close"].dropna()
        if close.shape[0] >= 200:
            rets[s] = np.log(close).diff()
        wk = this_week_summary(data[s], trading_days=params_["trading_days"])
        if wk:
            an = week_anomaly_scores(data[s], lookback_weeks=52) or {}
            wk.update(an)
            wk["Símbolo"] = s
            week_rows.append(wk)

    summary = pd.DataFrame(metrics_rows).set_index("Símbolo") if metrics_rows else pd.DataFrame()
    weekdf = pd.DataFrame(week_rows).set_index("Símbolo") if week_rows else pd.DataFrame()
    rets_df = pd.DataFrame(rets) if rets else pd.DataFrame()

    if not summary.empty:
        def pct_rank(x, asc=True):
            return x.rank(pct=True, ascending=asc)
        summary["Score"] = (
            0.35 * pct_rank(summary["Sharpe"], True) +
            0.35 * pct_rank(summary["Calmar"], True) +
            0.20 * pct_rank(summary["CAGR"], True) -
            0.10 * pct_rank(summary["Vol anual"], True)
        )

    week_sev = pd.DataFrame()
    if not weekdf.empty:
        tmp = weekdf.copy()
        tmp["Severidad"] = 0.0
        if "Z Ret semana" in tmp.columns:
            tmp["Severidad"] += tmp["Z Ret semana"].abs().fillna(0.0)
        if "Pct rango semana" in tmp.columns:
            tmp["Severidad"] += (tmp["Pct rango semana"] - 0.5).abs().fillna(0.0)
        if "Vol ratio vs MA" in tmp.columns:
            tmp["Severidad"] += (np.log(tmp["Vol ratio vs MA"]).abs()).replace([np.inf, -np.inf], 0.0).fillna(0.0)
        week_sev = tmp.sort_values("Severidad", ascending=False)

    corr = rets_df.corr(min_periods=200) if (not rets_df.empty and rets_df.shape[1] >= 2) else pd.DataFrame()
    clusters = cluster_labels_from_corr(corr, k=params_["portfolio_k"]) if not corr.empty else None

    # Portafolio recomendado
    reco = {"selected": [], "weights": pd.Series(dtype=float), "note": ""}
    if not rets_df.empty and not summary.empty and rets_df.shape[1] >= 2:
        selected = []
        if clusters is not None and SCIPY_OK and corr.shape[0] >= 3:
            for cl in sorted(clusters.unique()):
                members = clusters[clusters == cl].index.tolist()
                cand = summary.loc[summary.index.intersection(members)].copy()
                if "Score" in cand.columns and not cand["Score"].dropna().empty:
                    selected.append(cand["Score"].sort_values(ascending=False).index[0])
                else:
                    selected.append(members[0])
        else:
            selected = summary["Score"].sort_values(ascending=False).index.tolist()[:min(5, len(summary))]
        selected = list(dict.fromkeys(selected))
        R = rets_df[selected].dropna(how="any")
        if R.shape[0] >= 200 and R.shape[1] >= 2:
            cov = shrink_cov(R.cov(), lam=0.10, jitter=1e-10)
            w = risk_parity_weights(cov).sort_values(ascending=False)
            reco = {"selected": selected, "weights": w, "note": "Risk Parity (long-only) sobre cov shrink."}
        else:
            reco = {"selected": selected, "weights": pd.Series(dtype=float), "note": "Poco traslape para pesos robustos."}

    risk_df = build_risk_table(summary)
    return {
        "data": data, "symbols": symbols, "summary": summary,
        "risk_df": risk_df, "weekdf": weekdf, "week_sev": week_sev,
        "rets_df": rets_df, "corr": corr, "clusters": clusters,
        "reco_portfolio": reco,
    }


params = {
    "start": str(start), "end": str(end),
    "trading_days": int(trading_days), "trend_win": int(trend_win),
    "trend_lookback_days": int(trend_lookback_days),
    "roll_vol_days": int(roll_vol_days), "roll_corr_days": int(roll_corr_days),
    "top_dd": int(top_dd), "top_peaks": int(top_peaks),
    "min_new_high": float(min_new_high), "min_dd_event": float(min_dd_event),
    "use_common_for_corr": bool(use_common_for_corr),
    "portfolio_k": int(portfolio_k),
    "symbols": tuple(symbols_all), "files": tuple(current_names),
    "simple_mode": bool(SIMPLE),
}

cur_hash = hashlib.md5(repr(sorted(params.items())).encode()).hexdigest()

st.sidebar.markdown("---")
btn_analyze_side = st.sidebar.button("▶️ Iniciar análisis", type="primary")
run_now = bool(btn_analyze_side or btn_analyze_main)

if run_now:
    with st.spinner("Analizando..."):
        st.session_state.analysis = run_analysis(series_raw, params)
        st.session_state.analysis_hash = cur_hash

if st.session_state.analysis is None:
    st.warning("Listo para analizar. Presiona **▶️ Iniciar análisis**.")
    st.stop()

if st.session_state.analysis_hash != cur_hash:
    st.warning("Parámetros cambiados. Presiona ▶️ para recalcular.")

res = st.session_state.analysis
data = res["data"]
symbols = res["symbols"]
summary = res["summary"]
risk_df = res["risk_df"]
weekdf = res["weekdf"]
week_sev = res["week_sev"]
rets_df = res["rets_df"]
reco = res.get("reco_portfolio", {"selected": [], "weights": pd.Series(dtype=float), "note": ""})

if not symbols:
    st.warning("No hay datos en el rango seleccionado.")
    st.stop()

if use_common_for_corr and not rets_df.empty:
    rets_df_corr = rets_df.loc[(rets_df.index >= common_start) & (rets_df.index <= common_end)].copy()
else:
    rets_df_corr = rets_df


# ╔══════════════════════════════════════════════════════════════════════════════
# ║  SECCIÓN 15: TABS DE RESULTADOS
# ║  Simple: 4 tabs | Analista: 9 tabs (incluye Micro-Análisis y Monte Carlo)
# ╚══════════════════════════════════════════════════════════════════════════════

if SIMPLE:
    tab_exec, tab_week, tab_port, tab_corr = st.tabs(
        ["📌 Resumen Ejecutivo", "🗓️ Semana", "🧩 Portafolio", "🔗 Correlación"])
else:
    tab_exec, tab_week, tab_port, tab_corr, tab_dd, tab_gs, tab_peaks, tab_micro, tab_mc = st.tabs(
        ["📌 Resumen Ejecutivo", "🗓️ Semana", "🧩 Portafolio", "🔗 Correlación",
         "📉 Drawdowns", "🪙 Par Óptimo", "🧨 Picos Vol",
         "🔬 Micro-Análisis", "🎲 Monte Carlo"])

# ── Tab: Resumen Ejecutivo ───────────────────────────────────────────────────
with tab_exec:
    st.subheader("📌 Resumen Ejecutivo")
    st.caption("Semáforo, ranking, semana y portafolio sugerido.")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Activos", str(len(symbols)))
    c2.metric("Temporalidad", ", ".join(unique_labels) if unique_labels else "—")
    c3.metric("Rango", f"{start} → {end}")
    c4.metric("Overlap común", f"{common_start.date()} → {common_end.date()}")

    bullets = build_executive_insights(summary, week_sev, reco, str(start), str(end))
    st.markdown("### 🧾 Executive Brief")
    st.markdown("\n".join([f"- {b}" for b in bullets]))

    st.markdown("---")
    st.markdown("### 🚦 Semáforo de riesgo")
    if risk_df is None or risk_df.empty:
        st.info("No hay métricas suficientes para semáforo.")
    else:
        colf1, _ = st.columns([1, 2])
        show_only = colf1.multiselect("Filtrar", ["🔴 Alto", "🟡 Medio", "🟢 Bajo"],
                                       default=["🔴 Alto", "🟡 Medio", "🟢 Bajo"])
        tmp = risk_df[risk_df["Riesgo"].isin(show_only)]
        st.dataframe(tmp[["Riesgo", "Acción", "Tipo", "Vol anual", "DD actual", "MaxDD", "CAGR", "Score", "TF"]]
                     .sort_values(["Riesgo", "Score"], ascending=[True, False]),
                     use_container_width=True)

    st.markdown("---")
    st.markdown("### 🔥 Ranking rápido")
    if summary.empty:
        st.warning("No hay suficientes barras para métricas.")
    else:
        colL, colR = st.columns(2)
        with colL:
            st.markdown("**Top volatilidad**")
            st.dataframe(summary.sort_values("Vol anual", ascending=False)
                         [["Vol anual", "CAGR", "Sharpe", "Calmar", "MaxDD", "Tipo"]].head(8),
                         use_container_width=True)
        with colR:
            st.markdown("**Top rentabilidad (Score)**")
            st.dataframe(summary.sort_values("Score", ascending=False)
                         [["Score", "CAGR", "Vol anual", "Sharpe", "Calmar", "MaxDD", "Tipo"]].head(8),
                         use_container_width=True)

        x = summary.replace([np.inf, -np.inf], np.nan).dropna(subset=["Vol anual", "CAGR"])
        if not x.empty:
            fig = px.scatter(x, x="Vol anual", y="CAGR", text=x.index,
                             hover_data=["Sharpe", "Calmar", "MaxDD", "Tipo", "Score"],
                             title="Riesgo vs Retorno")
            fig.update_traces(textposition="top center")
            fig.update_layout(height=420, margin=dict(l=20, r=20, t=60, b=20))
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.markdown("### 🧩 Portafolio recomendado")
    if not reco.get("selected"):
        st.info("No pude construir recomendado. Necesitas 2+ activos con datos suficientes.")
    else:
        st.success(f"Selección: **{', '.join(reco['selected'])}**")
        if reco.get("weights") is not None and not reco["weights"].empty:
            st.dataframe(reco["weights"].to_frame("Peso"), use_container_width=True)
        pm = compute_portfolio_metrics_from_reco(rets_df_corr, reco, trading_days)
        c1, c2, c3 = st.columns(3)
        c1.metric("Vol anual (port)", fmt_pct(pm["vol"]))
        c2.metric("Sharpe (port)", f"{pm['sharpe']:.2f}" if pd.notna(pm["sharpe"]) else "—")
        c3.metric("MaxDD (port)", fmt_pct(pm["maxdd"]))
        st.caption(reco.get("note", ""))

    st.markdown("---")
    st.markdown("### 📤 Exportar")
    col_d1, col_d2, col_d3, col_d4 = st.columns(4)
    if not summary.empty:
        col_d1.download_button("⬇️ Resumen (CSV)", data=summary.to_csv().encode(),
                               file_name="resumen_activos.csv", mime="text/csv")
    if reco.get("weights") is not None and not reco["weights"].empty:
        col_d2.download_button("⬇️ Pesos (CSV)", data=reco["weights"].to_csv().encode(),
                               file_name="pesos_portafolio.csv", mime="text/csv")
    if REPORTLAB_OK:
        pm = compute_portfolio_metrics_from_reco(rets_df_corr, reco, trading_days)
        pdf_bytes = make_pdf_report(
            "MT5 Trading Lab — Reporte Ejecutivo", str(start), str(end),
            symbols, unique_labels, bullets, summary, risk_df, week_sev, reco, pm)
        col_d3.download_button("📄 Reporte PDF", data=pdf_bytes,
                               file_name="reporte_ejecutivo.pdf", mime="application/pdf")

# ── Tab: Semana ──────────────────────────────────────────────────────────────
with tab_week:
    st.subheader("🗓️ Semana — detalle")
    if week_sev is None or week_sev.empty:
        st.info("No hay suficiente data semanal.")
    else:
        st.dataframe(week_sev, use_container_width=True)
        st.caption("Ordenado por Severidad (movimientos raros / volumen inusual).")

# ── Tab: Portafolio ──────────────────────────────────────────────────────────
with tab_port:
    st.subheader("🧩 Portafolio")
    if rets_df_corr.empty or rets_df_corr.shape[1] < 2:
        st.info("Necesitas 2+ activos con retornos suficientes.")
    else:
        corrp = rets_df_corr.corr(min_periods=200)
        cols = list(corrp.columns)

        if reco.get("selected"):
            st.markdown("### ✅ Recomendado")
            st.write("Selección:", ", ".join(reco["selected"]))
            if reco.get("weights") is not None and not reco["weights"].empty:
                st.dataframe(reco["weights"].to_frame("Peso"), use_container_width=True)

        with st.expander("🧪 Builder manual", expanded=not SIMPLE):
            default_sel = cols[:min(5, len(cols))]
            selected = st.multiselect("Activos", options=cols, default=default_sel)
            if len(selected) < 2:
                st.info("Selecciona al menos 2.")
            else:
                R = rets_df_corr[selected].dropna(how="any")
                if R.shape[0] < 200:
                    st.warning("Poco traslape. Usa rango común.")
                else:
                    cov = shrink_cov(R.cov(), lam=0.10, jitter=1e-10)
                    method = st.selectbox("Método", ["Risk Parity (long-only)",
                                                     "Inverse Vol (long-only)",
                                                     "Min Var (puede tener negativos)"])
                    if method.startswith("Risk Parity"):
                        w = risk_parity_weights(cov)
                    elif method.startswith("Inverse Vol"):
                        w = inverse_vol_weights(R.std(ddof=0))
                    else:
                        w = min_var_weights_unconstrained(cov)
                    st.markdown("#### Pesos")
                    st.dataframe(w.to_frame("Peso").sort_values("Peso", ascending=False),
                                 use_container_width=True)
                    port_lr = (R * w).sum(axis=1)
                    port = np.exp(port_lr.cumsum())
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=port.index, y=port.values, mode="lines", name="Portfolio"))
                    fig.update_layout(title="Curva del portafolio (base 1.0)", height=300,
                                      margin=dict(l=20, r=20, t=50, b=20))
                    st.plotly_chart(fig, use_container_width=True)

# ── Tab: Correlación ─────────────────────────────────────────────────────────
with tab_corr:
    st.subheader("🔗 Correlación")
    if rets_df_corr.empty or rets_df_corr.shape[1] < 2:
        st.info("Necesitas 2+ símbolos.")
    else:
        corr2 = rets_df_corr.corr(min_periods=200)
        st.plotly_chart(safe_imshow(corr2, "Matriz de correlación"), use_container_width=True)

        cols = list(rets_df_corr.columns)
        colA, colB = st.columns(2)
        a = colA.selectbox("A", options=cols, key="corrA_sel")
        b = colB.selectbox("B", options=cols, index=min(1, len(cols)-1), key="corrB_sel")
        if a != b:
            ab = rets_df_corr[[a, b]].dropna()
            if ab.shape[0] >= 50:
                dt = infer_dt(ab.index)
                bpd = bars_per_day_from_dt(dt) or 1.0
                roll_win = max(10, int(roll_corr_days * bpd))
                rc = ab[a].rolling(roll_win).corr(ab[b])
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=rc.index, y=rc.values, mode="lines"))
                fig.add_hline(y=0)
                fig.update_layout(title=f"Rolling Corr (~{roll_corr_days} días): {a} vs {b}",
                                  height=260, margin=dict(l=20, r=20, t=50, b=20))
                st.plotly_chart(fig, use_container_width=True)

        if SCIPY_OK and corr2.shape[0] >= 3 and not SIMPLE:
            st.markdown("### 🧩 Clusters")
            ord2 = cluster_order_from_corr(corr2)
            if ord2 is not None:
                st.plotly_chart(safe_imshow(corr2.iloc[ord2, ord2], "Correlación ordenada"),
                                use_container_width=True)
            lbl = cluster_labels_from_corr(corr2, k=portfolio_k)
            if lbl is not None:
                st.dataframe(lbl.to_frame(), use_container_width=True)


# ── Tabs extra (solo Analista) ───────────────────────────────────────────────
if not SIMPLE:
    # Tab: Drawdowns
    with tab_dd:
        st.subheader("📉 Drawdowns (detalle)")
        sym = st.selectbox("Símbolo", options=symbols, key="dd_sym_sel")
        df = data[sym]
        close = df["Close"].dropna()
        dd = underwater_curve(close)

        c1, c2, c3 = st.columns(3)
        c1.metric("Precio", fmt_num(float(close.iloc[-1])) if not close.empty else "—")
        c2.metric("MaxDD", fmt_pct(float(dd.min())) if not dd.empty else "—")
        c3.metric("DD actual", fmt_pct(float(dd.iloc[-1])) if not dd.empty else "—")

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=close.index, y=close.values, mode="lines"))
        fig.update_layout(title=f"{sym} — Precio", height=320, margin=dict(l=20, r=20, t=50, b=20))
        st.plotly_chart(fig, use_container_width=True)

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=dd.index, y=dd.values, mode="lines"))
        fig.update_layout(title=f"{sym} — Underwater", height=240, margin=dict(l=20, r=20, t=50, b=20))
        fig.update_yaxes(tickformat=".0%")
        st.plotly_chart(fig, use_container_width=True)

        ev = drawdown_events(df[["High", "Low"]], min_new_high=min_new_high, min_dd=min_dd_event)
        st.markdown(f"### Top {top_dd} drawdowns")
        cols_dd = ["Peak", "Trough", "Recovery", "Peak High", "Trough Low", "DD%",
                   "Dur Peak->Trough", "Dur Trough->Recovery", "Dur Peak->Recovery"]
        st.dataframe(ev[cols_dd].head(top_dd) if not ev.empty else pd.DataFrame(),
                     use_container_width=True)

    # Tab: Par Óptimo
    with tab_gs:
        st.subheader("🪙 Par Óptimo — portafolio para 2 activos")
        if len(symbols) < 2:
            st.info("Necesitas al menos 2 activos.")
        else:
            col1, col2 = st.columns(2)
            a_sym = col1.selectbox("Activo A", options=symbols, index=0, key="par_a")
            b_sym = col2.selectbox("Activo B", options=symbols,
                                   index=1 if len(symbols) > 1 else 0, key="par_b")
            dfA, dfB = data[a_sym].copy(), data[b_sym].copy()
            if dfA.empty or dfB.empty:
                st.warning("No hay data para alguno de los dos.")
            else:
                join = dfA[["Close"]].rename(columns={"Close": f"{a_sym}_Close"}).join(
                    dfB[["Close"]].rename(columns={"Close": f"{b_sym}_Close"}), how="inner").dropna()
                if join.shape[0] < 100:
                    st.info("Poco traslape.")
                else:
                    lr = np.log(join).diff().dropna()
                    cov = shrink_cov(lr.cov(), lam=0.10, jitter=1e-10)
                    w_pair = risk_parity_weights(cov)
                    st.markdown("#### Pesos (Risk Parity)")
                    st.dataframe(w_pair.to_frame("Peso"), use_container_width=True)

                    port_lr = (lr * w_pair).sum(axis=1)
                    port_curve = np.exp(port_lr.cumsum())
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=join.index, y=join[f"{a_sym}_Close"]/join[f"{a_sym}_Close"].iloc[0], name=a_sym))
                    fig.add_trace(go.Scatter(x=join.index, y=join[f"{b_sym}_Close"]/join[f"{b_sym}_Close"].iloc[0], name=b_sym))
                    fig.add_trace(go.Scatter(x=port_curve.index, y=port_curve.values/port_curve.iloc[0], name="Port RP", line=dict(width=3)))
                    fig.update_layout(title="Precio normalizado", height=300, margin=dict(l=20, r=20, t=50, b=20))
                    st.plotly_chart(fig, use_container_width=True)

                    ann = ann_factor_from_index(port_lr.index, trading_days=trading_days)
                    vol = float(port_lr.std(ddof=0) * np.sqrt(ann))
                    mean = float(port_lr.mean() * ann)
                    sharpe = mean / vol if vol > 0 else np.nan
                    dd_pair = underwater_curve(port_curve)
                    mdd_pair = float(dd_pair.min()) if not dd_pair.empty else np.nan
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Vol anual Port", fmt_pct(vol))
                    c2.metric("Sharpe Port", f"{sharpe:.2f}" if pd.notna(sharpe) else "—")
                    c3.metric("MaxDD Port", fmt_pct(mdd_pair))

    # Tab: Picos de Volatilidad
    with tab_peaks:
        st.subheader("🧨 Picos de volatilidad")
        sym = st.selectbox("Activo", options=symbols, key="pvol_sym_sel")
        df = data[sym]
        close = df["Close"].dropna()
        if close.shape[0] < 300:
            st.info("Poca historia. Amplía el rango.")
        else:
            ann = ann_factor_from_index(close.index, trading_days=trading_days)
            dt = infer_dt(close.index)
            bpd = bars_per_day_from_dt(dt) or 1.0
            win = int(max(10, roll_vol_days * bpd))
            roll, peaks, table = rolling_vol_peaks(close, win=win, top_n=int(top_peaks), ann=ann)
            if roll is not None:
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=roll.index, y=roll.values, mode="lines"))
                for d in peaks.index:
                    fig.add_vline(x=d, line_dash="dash", opacity=0.25)
                fig.update_layout(title=f"{sym} — Vol rolling (win={win})", height=280,
                                  margin=dict(l=20, r=20, t=50, b=20))
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(table, use_container_width=True)

    # ── Tab: Micro-Análisis (de gold_anal, generalizado, todo en Plotly) ─────
    with tab_micro:
        st.subheader("🔬 Micro-Análisis (velas, rachas, gaps, sesiones)")
        st.caption("Funcionalidad del Gold Analyzer, generalizada para cualquier símbolo.")

        sym_micro = st.selectbox("Símbolo para micro-análisis", options=symbols, key="micro_sym")
        df_m = data[sym_micro]
        preset = get_preset(sym_micro)

        if df_m.empty or df_m.shape[0] < 100:
            st.warning("Poca data para micro-análisis.")
        else:
            # Preparar datos auxiliares para el análisis de velas
            ranges_s = df_m["range_pts"]
            hours_s = df_m.index.hour
            thr_candles = preset["candle_thresholds"]
            thr_streaks = preset["streak_thresholds"]

            c1, c2, c3 = st.columns(3)
            c1.metric("Máx rango", f"{ranges_s.max():.4f}")
            c2.metric("Rango medio", f"{ranges_s.mean():.4f}")

            # Gaps
            timestamps_arr = df_m.index.to_numpy()
            ts_pd = [pd.Timestamp(t) for t in timestamps_arr]
            d_gaps = detect_gaps_in_series(
                np.array(ts_pd), df_m["Open"].values, df_m["Close"].values, mins=45)
            c3.metric("Gaps detectados", len(d_gaps))

            # Conteo de velas
            d_counts = count_candles(ranges_s, thr_candles)

            # Sub-tabs dentro del micro-análisis (todo Plotly)
            mtabs = st.tabs(["📊 Conteo", "🕐 Hora", "📈 Histograma",
                             "📦 Sesión", "🗓️ DOW×Hora", "⚡ Gaps", "🔥 Rachas"])

            with mtabs[0]:
                fig = px.bar(d_counts, x="threshold", y="count",
                             title="Conteo de velas ≥ umbral",
                             labels={"threshold": "Umbral (pts)", "count": "# velas"})
                st.plotly_chart(fig, use_container_width=True)

            with mtabs[1]:
                d_hour = count_by_hour(hours_s, ranges_s, thr_candles)
                piv = d_hour.pivot(index="threshold", columns="hour_cdmx", values="pct_in_hour").sort_index(ascending=False)
                fig = px.imshow(piv, aspect="auto", title="% velas ≥ umbral por hora CDMX",
                                labels=dict(x="Hora CDMX", y="Umbral", color="%"))
                st.plotly_chart(fig, use_container_width=True)

            with mtabs[2]:
                fig = px.histogram(df_m, x="range_pts", nbins=60, marginal="box",
                                   title="Histograma de rangos",
                                   labels={"range_pts": "Rango (pts)"})
                st.plotly_chart(fig, use_container_width=True)

            with mtabs[3]:
                if preset["sessions_enabled"]:
                    sessions = df_m.index.map(lambda t: session_label(t))
                    df_sess = df_m.assign(session=pd.Categorical(sessions, categories=SESSION_ORDER, ordered=True))
                    fig = px.box(df_sess, x="session", y="range_pts",
                                 title="Rangos por sesión FX",
                                 labels={"session": "Sesión", "range_pts": "Rango (pts)"})
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(f"{sym_micro} no tiene sesiones FX habilitadas (24/7 o índice).")

            with mtabs[4]:
                dow_hour = df_m.assign(dow=df_m.index.dayofweek, hour=df_m.index.hour)
                piv_dh = dow_hour.groupby(["dow", "hour"])["range_pts"].mean().unstack(fill_value=np.nan)
                piv_dh.index = [calendar.day_abbr[d] for d in piv_dh.index]
                fig = px.imshow(piv_dh, aspect="auto", title="Rango medio por día × hora",
                                labels=dict(x="Hora CDMX", y="Día", color="pts"))
                st.plotly_chart(fig, use_container_width=True)

            with mtabs[5]:
                if not d_gaps.empty:
                    fig = px.scatter(d_gaps, x="delta_min", y="abs_gap", color="gap_type",
                                     title="Gaps: tamaño vs duración",
                                     labels={"delta_min": "Duración (min)", "abs_gap": "Tamaño (pts)"})
                    st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(d_gaps.sort_values("abs_gap", ascending=False).head(10),
                                 use_container_width=True)
                else:
                    st.info("No se detectaron gaps.")

            with mtabs[6]:
                d_streaks = find_streaks(ranges_s.values, np.array(ts_pd), thr_streaks)
                if not d_streaks.empty:
                    st.dataframe(d_streaks.sort_values("length", ascending=False).head(15),
                                 use_container_width=True)
                else:
                    st.info("No se encontraron rachas significativas.")

            st.markdown("---")
            if st.button("📦 Exportar Micro (ZIP)", key="export_micro_zip"):
                counts_df = d_counts.copy()
                hour_df = count_by_hour(hours_s, ranges_s, thr_candles)
                streaks_df = d_streaks.copy()
                gaps_df = d_gaps.copy()
                mc_res = st.session_state.get("mc_results")

                mem = io.BytesIO()
                with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as z:
                    z.writestr(f"{sym_micro}_counts.csv", counts_df.to_csv(index=False))
                    z.writestr(f"{sym_micro}_counts_by_hour.csv", hour_df.to_csv(index=False))
                    if not gaps_df.empty:
                        z.writestr(f"{sym_micro}_gaps.csv", gaps_df.to_csv(index=False))
                    if not streaks_df.empty:
                        z.writestr(f"{sym_micro}_streaks.csv", streaks_df.to_csv(index=False))
                    if mc_res is not None and not mc_res.empty:
                        z.writestr(f"{sym_micro}_monte_carlo.csv", mc_res.to_csv(index=False))
                mem.seek(0)
                st.download_button(
                    "⬇️ Descargar ZIP",
                    data=mem.getvalue(),
                    file_name=f"{sym_micro}_micro.zip",
                    mime="application/zip",
                )

    # ── Tab: Monte Carlo (generalizado) ──────────────────────────────────────
    with tab_mc:
        st.subheader("🎲 Monte Carlo — Riesgo de Ruina")
        st.caption("Simulación generalizada: contract_size y sesiones se adaptan al símbolo.")

        sym_mc = st.selectbox("Símbolo para Monte Carlo", options=symbols, key="mc_sym")
        preset_mc = get_preset(sym_mc)

        with st.expander("⚙️ Parámetros Monte Carlo", expanded=True):
            mc_c1, mc_c2 = st.columns(2)
            mc_lot0 = mc_c1.number_input("LOT0", 0.01, 5.0, 0.01, 0.01, key="mc_lot0")
            mc_q = mc_c1.number_input("Factor q", 1.01, 2.0, 1.10, 0.01, key="mc_q")
            mc_dist = mc_c1.number_input("Distance", 0.0001, 1000.0,
                                          float(preset_mc["mc_distance"]), key="mc_dist")
            mc_tp = mc_c2.number_input("TP offset", 0.0001, 1000.0,
                                        float(preset_mc["mc_tp_offset"]), key="mc_tp")
            mc_sl = mc_c2.number_input("STOP-loss (USD<0)", -500_000.0, -1.0,
                                        float(preset_mc["mc_stop_loss"]), 1000.0, key="mc_sl")
            mc_max_steps = mc_c2.number_input("Max steps", 1, 10000, 100, key="mc_steps")

            mc_c3, mc_c4 = st.columns(2)
            mc_n = mc_c3.number_input("N samples", 100, 100_000, 5_000, 100, key="mc_n",
                                      help="Valores muy altos pueden tardar varios segundos.")
            mc_contract = mc_c3.number_input("Contract size", 1.0, 1_000_000.0,
                                              float(preset_mc["contract_size"]), key="mc_cs")
            mc_swap_l = mc_c4.number_input("Swap BUY (USD/lot/noche)", -50.0, 50.0,
                                            float(preset_mc["swap_long"]), 0.1, key="mc_swl")
            mc_swap_s = mc_c4.number_input("Swap SELL (USD/lot/noche)", -50.0, 50.0,
                                            float(preset_mc["swap_short"]), 0.1, key="mc_sws")
            mc_plan_str = st.text_input("Plan de pasos (n:factor)", "", key="mc_plan")
            mc_rollover = st.number_input("Hora rollover CDMX", 0, 23, 16, key="mc_roll")
            mc_seed_raw = st.text_input("Seed (opcional)", value="", key="mc_seed",
                                        help="Déjalo vacío para aleatorio; usa entero para reproducir resultados.")

        if mc_sl >= 0:
            st.warning("STOP-loss debe ser negativo.")
            mc_sl = -abs(mc_sl)

        try:
            mc_plan = parse_plan(mc_plan_str)
        except ValueError:
            mc_plan = []

        levels_eff = total_levels(mc_plan) if mc_plan else mc_max_steps
        lot_tot = compute_total_lots(mc_lot0, mc_q, mc_plan, mc_max_steps)
        mc_seed = int(mc_seed_raw) if str(mc_seed_raw).strip().isdigit() else None
        st.caption(f"Tamaño total teórico: **{lot_tot:,.2f} lots** | Niveles: {levels_eff}")

        if st.button("🎲 Ejecutar Monte Carlo", type="primary", key="run_mc"):
            df_mc_input = data[sym_mc]
            if df_mc_input.shape[0] < 500:
                st.error("Poca data para Monte Carlo (mínimo ~500 barras).")
            else:
                with st.spinner("Monte Carlo..."):
                    t0 = time.time()
                    df_mc = run_mc(
                        df_mc_input, distance=mc_dist, lot0=mc_lot0, q0=mc_q,
                        tp_offset=mc_tp, stop_loss=mc_sl, max_steps=mc_max_steps,
                        n_samples=mc_n, step_plan=mc_plan, dd_inter=[],
                        swap_long=mc_swap_l, swap_short=mc_swap_s,
                        rollover_hr=mc_rollover, contract_size=mc_contract,
                        sessions_enabled=preset_mc["sessions_enabled"],
                        seed=mc_seed,
                    )
                    exec_t = time.time() - t0
                st.session_state.mc_results = df_mc
                st.session_state.mc_params = {
                    "symbol": sym_mc, "seed": mc_seed, "n_samples": mc_n,
                    "distance": mc_dist, "lot0": mc_lot0, "q": mc_q,
                    "tp_offset": mc_tp, "stop_loss": mc_sl, "max_steps": mc_max_steps,
                    "plan": mc_plan_str, "rollover_hr": mc_rollover,
                    "contract_size": mc_contract,
                    "swap_long": mc_swap_l, "swap_short": mc_swap_s,
                }
                logging.info("Monte Carlo en %.2f s", exec_t)

        # Mostrar resultados MC si existen
        df_mc = st.session_state.get("mc_results")
        mc_params = st.session_state.get("mc_params") or {}
        if df_mc is not None and not df_mc.empty:
            st.caption(
                f"Seed: {mc_params.get('seed', '—')} | N: {mc_params.get('n_samples', '—')} | "
                f"dist: {mc_params.get('distance', '—')} | q: {mc_params.get('q', '—')} | "
                f"TP: {mc_params.get('tp_offset', '—')} | STOP: {mc_params.get('stop_loss', '—')} | "
                f"Max steps: {mc_params.get('max_steps', '—')}"
            )
            broke_buy = df_mc[df_mc.side == "BUY"].broke.mean() * 100
            broke_sell = df_mc[df_mc.side == "SELL"].broke.mean() * 100

            st.markdown("### Resultados Monte Carlo")
            c1, c2, c3 = st.columns(3)
            c1.metric("% quiebras BUY", f"{broke_buy:.2f}%")
            c2.metric("% quiebras SELL", f"{broke_sell:.2f}%")
            c3.metric("% quiebras global", f"{df_mc.broke.mean()*100:.2f}%")

            c4, c5 = st.columns(2)
            c4.metric("DD pico media", f"{df_mc.dd_pico.mean():,.2f} USD")
            c5.metric("Duración media", f"{df_mc.dur_min.mean():.1f} barras")

            st.plotly_chart(px.histogram(df_mc, x="dd_pico", nbins=60,
                                          title="Distribución dd_pico (USD)")
                            .update_layout(template="plotly_white"), use_container_width=True)
            st.plotly_chart(px.histogram(df_mc, x="steps_used", nbins=min(50, levels_eff),
                                          title="Distribución steps_used")
                            .update_layout(template="plotly_white"), use_container_width=True)

            broke_df = df_mc[df_mc.broke]
            if broke_df.empty:
                st.success("No hubo quiebras en esta corrida. 🎉")
            else:
                show_cols = [c for c in ["start_ts", "start_session", "end_session",
                                          "dd_pico", "steps_used", "dur_min", "side",
                                          "swap_usd", "exit_pnl_usd"] if c in broke_df.columns]
                st.markdown("### Top-50 quiebras")
                st.dataframe(broke_df.sort_values("dd_pico").head(50)[show_cols],
                             use_container_width=True)
