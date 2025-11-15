"""Paso del pipeline que calcula variaciones semanales de rendimiento SEO y escribe los resultados en Google Sheets.

Este módulo requiere un archivo hermano llamado ``sheets_manager.py`` que exponga una clase ``SheetsManager``
con al menos la siguiente interfaz mínima:

    manager = SheetsManager(spreadsheet_name="SEO_Master_Data")
    df = manager.read_worksheet("gsc_data_daily")  # devuelve un pandas.DataFrame
    manager.write_dataframe("analysis_raw", dataframe, replace=True)

Si tu manager ofrece nombres de método distintos, ajusta las funciones auxiliares
``fetch_dataframe`` y ``push_dataframe`` para que utilicen tu API.

Para ejecutar el pipeline cada siete días, programa este script en un scheduler. Ejemplo de entrada cron:

    0 3 * * 1 /usr/bin/python /path/to/analysis_variaciones.py

o un workflow de GitHub Actions con un disparador ``schedule`` y cron ``0 3 * * 1``.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from sheets_manager import SheetsManager

GOOGLE_SHEET_NAME = "SEO_Master_Data"
GSC_WORKSHEET = "gsc_data_daily"
GA4_WORKSHEET = "ga4_data_daily"
OUTPUT_WORKSHEET = "analysis_raw"
SUMMARY_COLUMN = "Resumen_IA"
MIN_BASELINE = 1.0
MAX_VARIATION_ABS = 1000.0

# Bloque de configuración ----------------------------------------------------

# Mapea cada métrica al nombre de columna esperado y a la función de agregación por periodo.
GSC_METRICS = {
    "CTR": {
        "column": "ctr",
        "agg": "mean",
        "change": "difference",
        "min_baseline": 0.0025,
        "multiplier": 100.0,
        "label": "CTR Δ (p.p.)",
    },
    "Impresiones": {
        "column": "impressions",
        "agg": "sum",
        "change": "percentage",
        "min_baseline": 10.0,
        "max_abs_variation": 1000.0,
        "label": "Impresiones Variacion (%)",
    },
    "Clics": {
        "column": "clicks",
        "agg": "sum",
        "change": "percentage",
        "min_baseline": 5.0,
        "max_abs_variation": 1000.0,
        "label": "Clics Variacion (%)",
    },
    "Posicion": {
        "column": "position",
        "agg": "mean",
        "change": "difference",
        "min_baseline": 0.25,
        "label": "Posicion Δ",
    },
}

GA4_METRICS = {
    "Sesiones": {
        "column": "sessions",
        "agg": "sum",
        "change": "percentage",
        "min_baseline": 5.0,
        "max_abs_variation": 1000.0,
        "label": "Sesiones Variacion (%)",
    },
    "Duracion": {
        "column": "avg_session_duration",
        "agg": "mean",
        "change": "difference",
        "min_baseline": 1.0,
        "label": "Duracion Δ",
    },
    "Rebote": {
        "column": "bounce_rate",
        "agg": "mean",
        "change": "difference",
        "min_baseline": 0.01,
        "multiplier": 100.0,
        "label": "Rebote Δ (p.p.)",
    },
}

METRIC_SEQUENCE = [
    ("CTR", GSC_METRICS),
    ("Impresiones", GSC_METRICS),
    ("Clics", GSC_METRICS),
    ("Posicion", GSC_METRICS),
    ("Sesiones", GA4_METRICS),
    ("Duracion", GA4_METRICS),
    ("Rebote", GA4_METRICS),
]

DATE_COLUMN_CANDIDATES = ("date", "fecha", "Date", "Fecha")
URL_COLUMN_CANDIDATES = ("page", "url", "URL", "Page")

OUTPUT_COLUMNS = [
    "Periodo Analizado",
    "URL",
    GSC_METRICS["CTR"]["label"],
    GSC_METRICS["Impresiones"]["label"],
    GSC_METRICS["Clics"]["label"],
    GSC_METRICS["Posicion"]["label"],
    GA4_METRICS["Sesiones"]["label"],
    GA4_METRICS["Duracion"]["label"],
    GA4_METRICS["Rebote"]["label"],
    SUMMARY_COLUMN,
]

# Funciones auxiliares -------------------------------------------------------


def fetch_dataframe(manager: SheetsManager, worksheet: str) -> pd.DataFrame:
    """Obtiene una pestaña como pandas DataFrame usando la API disponible del manager."""

    if hasattr(manager, "read_worksheet"):
        return manager.read_worksheet(worksheet)
    if hasattr(manager, "get_worksheet_df"):
        return manager.get_worksheet_df(worksheet)
    if hasattr(manager, "to_dataframe"):
        return manager.to_dataframe(worksheet)
    raise AttributeError(
        "SheetsManager no expone un método de lectura compatible. "
        "Implementa 'read_worksheet', 'get_worksheet_df' o 'to_dataframe'."
    )


def push_dataframe(manager: SheetsManager, worksheet: str, df: pd.DataFrame) -> None:
    """Escribe un pandas DataFrame en Google Sheets, reemplazando el contenido previo."""

    if hasattr(manager, "write_dataframe"):
        manager.write_dataframe(worksheet, df, replace=True)
        return
    if hasattr(manager, "update_worksheet"):
        manager.update_worksheet(worksheet, df, replace=True)
        return
    if hasattr(manager, "write_df"):
        manager.write_df(worksheet, df, replace=True)
        return
    raise AttributeError(
        "SheetsManager no expone un método de escritura compatible. "
        "Implementa 'write_dataframe', 'update_worksheet' o 'write_df'."
    )


def locate_column(df: pd.DataFrame, candidates: Tuple[str, ...], kind: str) -> str:
    """Devuelve la primera columna que coincide con los candidatos (sin sensibilidad a mayúsculas)."""

    lowered = {col.lower(): col for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    raise KeyError(f"No se encontró una columna de tipo {kind}. Revisado: {candidates}")


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Estandariza nombres de columnas y convierte fechas."""

    df = df.copy()
    date_col = locate_column(df, DATE_COLUMN_CANDIDATES, "date")
    url_col = locate_column(df, URL_COLUMN_CANDIDATES, "URL")

    df.rename(columns={date_col: "date", url_col: "url"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "url"]).reset_index(drop=True)
    df["url"] = df["url"].astype(str)
    return df


def compute_period_bounds(reference_date: datetime) -> Tuple[datetime, datetime, datetime, datetime]:
    """Devuelve las fechas de inicio y fin para las ventanas recientes y previas de 7 días."""

    recent_end = reference_date
    recent_start = recent_end - timedelta(days=6)
    previous_end = recent_start - timedelta(days=1)
    previous_start = previous_end - timedelta(days=6)
    return recent_start, recent_end, previous_start, previous_end


def aggregate_period(
    df: pd.DataFrame,
    metric_config: Dict[str, Dict[str, str]],
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """Agrega las métricas del periodo usando la función configurada para cada métrica."""

    mask = (df["date"] >= start) & (df["date"] <= end)
    period_df = df.loc[mask].copy()
    group = period_df.groupby("url", dropna=False)

    data = {}
    for metric_name, config in metric_config.items():
        column = config["column"]
        if column not in period_df.columns:
            continue
        # Fuerza las columnas métricas a ser numéricas para evitar fallos por strings.
        if not pd.api.types.is_numeric_dtype(period_df[column]):
            period_df[column] = pd.to_numeric(period_df[column], errors="coerce")
        agg_func = config.get("agg", "sum")
        data[metric_name] = group[column].agg(agg_func)

    if not data:
        return pd.DataFrame(columns=metric_config.keys())

    return pd.DataFrame(data)


def percentage_change(
    current: pd.Series,
    previous: pd.Series,
    *,
    min_baseline: float = MIN_BASELINE,
    max_abs_variation: float = MAX_VARIATION_ABS,
) -> pd.Series:
    """Calcula la variación porcentual evitando resultados extremos cuando la base es muy pequeña."""

    aligned_current, aligned_previous = current.align(previous, join="outer")
    denominator = aligned_previous.replace({0: np.nan})
    denominator = denominator.where(denominator.abs() >= min_baseline)
    variation = (aligned_current - aligned_previous) / denominator * 100
    variation = variation.replace([np.inf, -np.inf], np.nan)
    variation = variation.where(~denominator.isna())
    if max_abs_variation is not None:
        variation = variation.where(variation.abs() <= max_abs_variation)
    return variation


def difference_change(
    current: pd.Series,
    previous: pd.Series,
    *,
    min_baseline: float = MIN_BASELINE,
    multiplier: float = 1.0,
) -> pd.Series:
    """Calcula la diferencia directa entre periodos, opcionalmente escalada."""

    aligned_current, aligned_previous = current.align(previous, join="outer")
    baseline = aligned_previous.abs().combine(aligned_current.abs(), func=max)
    baseline = baseline.where(baseline >= min_baseline)
    diff = (aligned_current - aligned_previous) * multiplier
    diff = diff.where(~baseline.isna())
    return diff


def build_variation_table(
    gsc_df: pd.DataFrame,
    ga4_df: pd.DataFrame,
    reference_date: datetime,
) -> pd.DataFrame:
    """Genera el DataFrame final con las variaciones porcentuales para todas las métricas."""

    recent_start, recent_end, previous_start, previous_end = compute_period_bounds(reference_date)

    gsc_recent = aggregate_period(gsc_df, GSC_METRICS, recent_start, recent_end)
    gsc_previous = aggregate_period(gsc_df, GSC_METRICS, previous_start, previous_end)

    ga4_recent = aggregate_period(ga4_df, GA4_METRICS, recent_start, recent_end)
    ga4_previous = aggregate_period(ga4_df, GA4_METRICS, previous_start, previous_end)

    urls = pd.Index([]).union(gsc_recent.index).union(gsc_previous.index)
    urls = urls.union(ga4_recent.index).union(ga4_previous.index)

    result = pd.DataFrame({"URL": urls})
    result.set_index("URL", inplace=True)

    for metric_key, source in METRIC_SEQUENCE:
        config = source[metric_key]
        column_name = config["label"]
        if source is GSC_METRICS:
            current_series = gsc_recent.get(metric_key, pd.Series(dtype=float))
            previous_series = gsc_previous.get(metric_key, pd.Series(dtype=float))
        else:
            current_series = ga4_recent.get(metric_key, pd.Series(dtype=float))
            previous_series = ga4_previous.get(metric_key, pd.Series(dtype=float))

        change_mode = config.get("change", "percentage")
        min_baseline = config.get("min_baseline", MIN_BASELINE)

        if change_mode == "difference":
            multiplier = config.get("multiplier", 1.0)
            result[column_name] = difference_change(
                current_series,
                previous_series,
                min_baseline=min_baseline,
                multiplier=multiplier,
            )
        else:
            max_abs_variation = config.get("max_abs_variation", MAX_VARIATION_ABS)
            result[column_name] = percentage_change(
                current_series,
                previous_series,
                min_baseline=min_baseline,
                max_abs_variation=max_abs_variation,
            )

    recent_label = f"{recent_start:%Y-%m-%d} a {recent_end:%Y-%m-%d}"
    previous_label = f"{previous_start:%Y-%m-%d} a {previous_end:%Y-%m-%d}"
    result["Periodo Analizado"] = f"{recent_label} (vs {previous_label})"
    result[SUMMARY_COLUMN] = ""

    result = result.reset_index()
    result = result[OUTPUT_COLUMNS]
    return result


def determine_reference_date(gsc_df: pd.DataFrame, ga4_df: pd.DataFrame) -> datetime:
    """Selecciona la fecha más reciente disponible entre ambos datasets."""

    latest_dates = []
    if not gsc_df.empty:
        latest_dates.append(gsc_df["date"].max())
    if not ga4_df.empty:
        latest_dates.append(ga4_df["date"].max())
    if not latest_dates:
        raise ValueError("No se encontraron fechas en las pestañas de origen.")
    return max(latest_dates)


def run_pipeline(
    spreadsheet_name: str = GOOGLE_SHEET_NAME,
    *,
    manager: Optional[SheetsManager] = None,
    write_output: bool = True,
    verbose: bool = False,
) -> pd.DataFrame:
    """Ejecuta el flujo leer -> calcular -> escribir opcional y devuelve el DataFrame resultado."""

    if verbose:
        print(f"Iniciando pipeline contra la hoja: {spreadsheet_name}", flush=True)

    manager = manager or SheetsManager(spreadsheet_name=spreadsheet_name)

    if verbose:
        print(f"Cliente listo; leyendo pestaña {GSC_WORKSHEET}", flush=True)

    gsc_df_raw = fetch_dataframe(manager, GSC_WORKSHEET)
    ga4_df_raw = fetch_dataframe(manager, GA4_WORKSHEET)

    if verbose:
        print(
            f"GSC filas: {len(gsc_df_raw)} | columnas: {list(gsc_df_raw.columns)}",
            flush=True,
        )
        print(
            f"GA4 filas: {len(ga4_df_raw)} | columnas: {list(ga4_df_raw.columns)}",
            flush=True,
        )

    gsc_df = normalize_dataframe(gsc_df_raw)
    ga4_df = normalize_dataframe(ga4_df_raw)

    reference_date = determine_reference_date(gsc_df, ga4_df)
    variation_df = build_variation_table(gsc_df, ga4_df, reference_date)

    if verbose:
        print(f"Fecha de referencia detectada: {reference_date:%Y-%m-%d}", flush=True)
        print(f"Total de URLs analizadas: {len(variation_df)}", flush=True)
        for column in [col for col in variation_df.columns if col.endswith("Variacion (%)")]:
            blanks = variation_df[column].isna().sum()
            if blanks:
                print(
                    f"{column}: {blanks} URLs sin variación calculable (periodo previo casi cero o sin datos)",
                    flush=True,
                )

    if write_output:
        if verbose:
            print(f"Escribiendo resultados en la pestaña {OUTPUT_WORKSHEET}", flush=True)
        push_dataframe(manager, OUTPUT_WORKSHEET, variation_df)

    if verbose:
        print("Pipeline completado", flush=True)
    return variation_df


def main(args: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Calcula variaciones SEO semanales y las envía a Google Sheets.")
    parser.add_argument(
        "--spreadsheet-name",
        default=GOOGLE_SHEET_NAME,
        help="Nombre o ID de la hoja de Google que contiene los datos SEO.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Calcula las variaciones sin escribir en Google Sheets (imprime el CSV en stdout).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Muestra mensajes de progreso detallados durante la ejecución.",
    )

    parsed = parser.parse_args(args)

    variation_df = run_pipeline(
        spreadsheet_name=parsed.spreadsheet_name,
        write_output=not parsed.dry_run,
        verbose=parsed.verbose,
    )

    if parsed.dry_run:
        variation_df.to_csv(index=False)


if __name__ == "__main__":
    main()
