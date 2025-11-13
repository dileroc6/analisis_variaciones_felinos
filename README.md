# Pipeline de Análisis de Variaciones SEO

Este proyecto automatiza el cálculo semanal de variaciones en métricas SEO y de analítica web a partir de datos almacenados en la hoja de cálculo **SEO_Master_Data**. El resultado se publica en la pestaña `analysis_raw`, que actúa como insumo para el Asistente SEO basado en OpenAI.

## Componentes principales

- `pipeline/assistant_analysis.py`: script que lee las pestañas `gsc_data_daily` y `ga4_data_daily`, normaliza columnas, agrega métricas por periodos de siete días, calcula variaciones porcentuales por URL y escribe la tabla final en `analysis_raw`. También deja preparada la columna `Resumen_IA` para recomendaciones posteriores.
- `pipeline/sheets_manager.py`: módulo que autentica y comunica con Google Sheets mediante cuentas de servicio, exponiendo la clase `SheetsManager` usada por el pipeline.
- `.github/workflows/assistant-analysis.yml`: workflow de GitHub Actions programado para ejecutar el pipeline cada lunes a las 03:00 UTC o bajo demanda mediante `workflow_dispatch`.

## Flujo de trabajo

1. Se establece conexión con la hoja `SEO_Master_Data` mediante `SheetsManager`.
2. Se leen las pestañas `gsc_data_daily` y `ga4_data_daily` en `pandas.DataFrame`.
3. Se estandarizan nombres de columnas y formatos de fecha.
4. Se calculan agregados de los últimos 7 días y del periodo de 7 días inmediatamente anterior para las métricas:
   - CTR, impresiones, clics y posición media (GSC).
   - Sesiones, duración media y tasa de rebote (GA4).
5. Se calcula la variación porcentual por URL entre ambos periodos.
6. Se escribe el resultado en `analysis_raw`, reemplazando datos previos si existen.
7. Se agrega la columna vacía `Resumen_IA` para que otras tareas completen las recomendaciones.

## Dependencias y configuración

- Python 3.11 (configurado en GitHub Actions).
- Paquetes: `pandas`, `numpy`, `gspread`, `google-auth` (y dependencias adicionales si extiendes `sheets_manager.py`).
- Credenciales de Google Cloud Service Account con acceso de lectura y escritura a `SEO_Master_Data`.
- Variables y secretos recomendados:
  - `GOOGLE_SERVICE_ACCOUNT_JSON` (GitHub Secret) con el JSON del servicio.
  - `SEO_SPREADSHEET_NAME` (GitHub Secret opcional) para sobreescribir el nombre o ID de la hoja.

### Ejecución local

```bash
python -m pip install -r requirements.txt  # o instala pandas, numpy, gspread y google-auth manualmente
python pipeline/assistant_analysis.py --spreadsheet-name "SEO_Master_Data"
```

Utiliza `--dry-run` para imprimir el CSV en consola sin escribir en la hoja:

```bash
python pipeline/assistant_analysis.py --dry-run
```

## Automatización

El workflow `assistant-analysis.yml`:

1. Se ejecuta cada lunes a las 03:00 UTC (cron `0 3 * * 1`) o manualmente.
2. Instala dependencias desde `requirements.txt` si existe; de lo contrario instala `pandas` y `numpy`.
3. Guarda las credenciales de Google en `service_account.json` si el secreto está configurado.
4. Ejecuta el script `pipeline/assistant_analysis.py`, pasando el nombre de la hoja si existe el secreto `SEO_SPREADSHEET_NAME`.

Si deseas programar una ejecución en un servidor propio, puedes usar un cron equivalente:

```
0 3 * * 1 /usr/bin/python /ruta/al/proyecto/pipeline/assistant_analysis.py
```

## Buenas prácticas para futuras modificaciones

- Documenta siempre los cambios relevantes tanto en el código como en este README.
- Mantén `sheets_manager.py` alineado con la interfaz esperada por `assistant_analysis.py`.
- Si agregas nuevas métricas o pestañas, actualiza la sección **Flujo de trabajo** y el listado de dependencias.
- Verifica que el workflow de GitHub Actions refleje cualquier cambio en la configuración del entorno o credenciales.
# analisis_variaciones_felinos
