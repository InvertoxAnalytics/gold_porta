# MT5 Trading Lab (Portafolio + Deep Analyzer)

Proyecto en Streamlit que fusiona dos apps previas:
- **Portafolio**: laboratorio macro multi–activo (metricas, semaforo de riesgo, portafolio recomendado, PDF).
- **gold_anal**: analisis micro de velas y simulacion Monte Carlo para grid trading con sesiones FX y ajustes DST.

El resultado es una sola app que permite ver el bosque (portfolio) y luego hacer drill‑down al arbol (micro + Monte Carlo) usando los mismos datos cargados.

## Que hay en este repo
- `app.py`: aplicacion unificada con modos *Simple* y *Analista*, presets por simbolo y tabs macro+micro.
- `requirements.txt`: dependencias de tiempo de ejecucion.
- Carpetas de referencia (no necesarias para correr):
  - `/Users/danielgomez/Desktop/portafolio/Portafolio` (app original macro)
  - `/Users/danielgomez/Desktop/portafolio/gold_anal` (app original micro)
  - `/Users/danielgomez/Desktop/Fusion/Portafolio Gold Claude` y `.../Portafolio Gold gpt` (versiones intermedias)
  - Planes de fusion en `/Users/danielgomez/Desktop/Planes de fusion portafolio-gold` (Claude, Codex, Gemini)

## Caracteristicas clave
- **Ingesta MT5**: sube uno o varios CSV por simbolo, detecta encoding y separador, limpia duplicados, calcula `range_pts` y deja el indice en horario CDMX.
- **Presets por simbolo**: contract_size, sesiones FX on/off, distancia y TP para Monte Carlo, umbrales de velas y swaps; editable desde la UI.
- **Flujo guiado**: subir archivos -> procesar -> elegir rango comun -> lanzar analisis.
- **Tabs macro (Simple)**: Resumen Ejecutivo, Semana, Portafolio, Correlacion.
- **Tabs extra (Analista)**: Drawdowns no solapados (High/Low), Par Optimo, Picos de Volatilidad, Micro‑Analisis, Monte Carlo.
- **Micro‑Analisis (todo Plotly)**: conteo por umbral, heatmap por hora, histograma, rangos por sesion FX, DOW x hora, gaps, rachas. Export ZIP con CSVs y ultimo resultado de MC.
- **Monte Carlo generalizado**: grid trading BUY/SELL con contract_size parametrico, sesiones FX opcionales, seed reproducible, n_samples hasta 100k, swaps y rollover configurables. Guarda parametros y resultados en `st.session_state`.
- **Exportes**: CSV de resumen/pesos, PDF ejecutivo (si reportlab disponible), ZIP de micro + MC.

## Requisitos
- Python 3.10+ recomendado.
- Dependencias en `requirements.txt`:
  `streamlit, pandas, numpy, plotly, scipy, yfinance, reportlab, pytz`.

## Instalacion rapida
```bash
cd /Users/danielgomez/Desktop/portafolio_gold
python -m venv .venv && source .venv/bin/activate  # opcional
pip install -r requirements.txt
streamlit run app.py
```

## Flujo de uso
1) En la barra lateral elige modo **Simple** o **Analista**.
2) Sube uno o varios CSV MT5 (misma temporalidad). Puedes sobrescribir el simbolo por archivo si el nombre no coincide.
3) Pulsa **Procesar**. Veras metadata (barras, fechas, TF) y el rango comun detectado.
4) Ajusta rango, lookbacks y parametros avanzados (R², clusters, top drawdowns, etc.).
5) Pulsa **Iniciar analisis** y navega los tabs:
   - Resumen: semaforo de riesgo, rankings, portafolio recomendado (Risk Parity) y descargas.
   - Semana: tabla de severidad semanal (Z-scores).
   - Portafolio: builder manual con inverse-vol / min-var, curva resultante y clusters (si SciPy disponible).
   - Correlacion: matriz y rolling corr de un par.
   - Drawdowns / Par Optimo / Picos (modo Analista): detalle de eventos, portafolio de dos activos, picos de volatilidad.
   - Micro‑Analisis: estadisticas de velas, gaps, rachas, sesiones FX (si aplica) y descarga ZIP.
   - Monte Carlo: parametriza LOT0, q, distancia, TP, STOP, max steps, swaps, contract_size, sesiones, seed; ejecuta y revisa % quiebra, DD pico, histos y descarga CSV (incluido en el ZIP de micro).

## Formato de datos esperado
- CSV MT5 con columnas `<DATE> <TIME> <OPEN> <HIGH> <LOW> <CLOSE> <TICKVOL>` separados por tab o espacios.
- Sin resampling: todos los archivos deben compartir la misma temporalidad (M1, H1, etc.).
- Timezone: se convierten a CDMX y se usan sesiones FX con ajuste DST para NY y Londres cuando `sessions_enabled=True`.

## Notas y pendientes
- Arquitectura sigue en un unico `app.py`; los planes (Claude/Codex/Gemini) proponen modularizar en `core/`, `charts/`, `ui/` y añadir ingestiones extra (Yahoo, multipagina). El codigo actual ya incluye presets, Monte Carlo generalizado y micro Plotly, pero falta esa refactorizacion.
- Matplotlib ya no se usa; todas las graficas son Plotly.
- Si SciPy o reportlab no estan instalados, se ocultan las partes que los requieren.

## Creditos
- Codigo base de las apps **Portafolio** y **gold_anal** (carpeta `portafolio`).
- Ideas y mejoras tomadas de los planes **Claude**, **Codex** y **Gemini** (carpeta `Planes de fusion portafolio-gold`).
