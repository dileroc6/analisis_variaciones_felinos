# Pipeline de Análisis de Variaciones SEO

 Este proyecto automatiza el cálculo quincenal de variaciones en métricas SEO y de analítica web a partir de datos almacenados en la hoja de cálculo **SEO_Master_Data**. El resultado se publica en la pestaña `analysis_raw`, que actúa como insumo para el Asistente SEO basado en OpenAI.

## Componentes principales

- `pipeline/analysis_variaciones.py`: script que lee las pestañas `gsc_data_daily` y `ga4_data_daily`, normaliza columnas, agrega métricas en ventanas consecutivas de catorce días, calcula variaciones o diferencias por URL y escribe la tabla final en `analysis_raw`. También deja preparada la columna `Resumen_IA` para recomendaciones posteriores y ofrece un modo `--verbose` para seguir la ejecución paso a paso.
- `pipeline/sheets_manager.py`: módulo que autentica y comunica con Google Sheets mediante cuentas de servicio, exponiendo la clase `SheetsManager` usada por el pipeline.
- `pipeline/telegram_notifier.py`: utilitario que arma un resumen ejecutivo de la corrida y lo envía por Telegram cuando los secretos del bot están configurados.
- `.github/workflows/analysis-variaciones.yml`: workflow de GitHub Actions programado para ejecutar el pipeline cada lunes a las 08:15 UTC (03:15 hora de Bogotá) o bajo demanda mediante `workflow_dispatch`, mostrando el detalle de cada paso y notificando el resultado por Telegram.

## Estructura de salida

La tabla escrita en `analysis_raw` contiene las siguientes columnas:

- `Periodo Analizado`: intervalo reciente y su comparación, por ejemplo `2025-11-01 a 2025-11-14 (vs 2025-10-18 a 2025-10-31)`.
- `URL`: página evaluada.
- `CTR (puntos porcentuales)`: diferencia en puntos porcentuales del CTR medio (acotada a ±50 p.p.).
- `Impresiones (variacion %)` y `Clics (variacion %)`: cambios porcentuales sobre totales.
- `Posicion promedio (variacion)`: diferencia absoluta en la posición promedio (negativo indica mejora si la posición baja, acotada a ±20 posiciones).
- `Sesiones (variacion %)`: cambio porcentual sobre sesiones totales.
- `Duracion promedio (variacion)`: diferencia en duración promedio (mismas unidades que la fuente, acotada a ±3600 segundos).
- `Rebote (puntos porcentuales)`: diferencia en puntos porcentuales de la tasa de rebote (acotada a ±50 p.p.).
- Las celdas se dejan en blanco cuando los valores de referencia son muy bajos o el cambio supera los umbrales definidos (+/-1000 % en cambios relativos o los límites de diferencia señalados), evitando cifras irreales.
- `Resumen_IA`: campo vacío listo para que un asistente genere recomendaciones.
- `Recomendacion` y `Ejecutar_Accion`: columnas adicionales para detallar la acción sugerida y si debe ejecutarse.

## Flujo de trabajo

1. Se establece conexión con la hoja `SEO_Master_Data` mediante `SheetsManager`.
2. Se leen las pestañas `gsc_data_daily` y `ga4_data_daily` en `pandas.DataFrame`.
3. Se estandarizan nombres de columnas y formatos de fecha.
4. Se convierten las métricas en numéricas (cuando llegan como texto) y se calculan agregados de los últimos 14 días y del periodo de 14 días inmediatamente anterior para las métricas:
   - CTR, impresiones, clics y posición media (GSC).
   - Sesiones, duración media y tasa de rebote (GA4).
5. Se calcula, por métrica, el cambio correspondiente (porcentaje para valores acumulados, diferencia para promedios) aplicando umbrales que descartan divisores diminutos y recortan valores atípicos. Después se compone la etiqueta de `Periodo Analizado` para documentar el intervalo comparado.
6. Se escribe el resultado en `analysis_raw`, reemplazando datos previos si existen.
7. Se agregan las columnas vacías `Resumen_IA`, `Recomendacion` y `Ejecutar_Accion` para que otras tareas completen recomendaciones y definan el siguiente paso.

### Parámetros del script

```text
--spreadsheet-name  Nombre o ID de la hoja (opcional; por defecto `SEO_Master_Data`).
--dry-run           Imprime el CSV en consola sin escribir en Google Sheets.
--verbose           Muestra mensajes detallados de progreso.
```

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
python pipeline/analysis_variaciones.py --spreadsheet-name "SEO_Master_Data" --verbose
```

Utiliza `--dry-run` para imprimir el CSV en consola sin escribir en la hoja:

```bash
python pipeline/analysis_variaciones.py --dry-run --verbose
```

## Automatización

El workflow `analysis-variaciones.yml`:

1. Se ejecuta cada lunes a las 08:15 UTC (03:15 hora de Bogotá, cron `15 8 * * 1`) o manualmente mediante la opción *Run workflow*.
2. Instala dependencias desde `requirements.txt` si existe; de lo contrario instala `pandas` y `numpy`.
3. Guarda las credenciales de Google en `service_account.json` si el secreto está configurado.
4. Ejecuta el script `pipeline/analysis_variaciones.py` en modo detallado (`--verbose`), pasando el nombre de la hoja si existe el secreto `SEO_SPREADSHEET_NAME`.
5. Envía un resumen por Telegram con el estado de la ejecución, la hora local de Bogotá y el total de URLs evaluadas, siempre que los secretos `TELEGRAM_BOT_TOKEN` y `TELEGRAM_CHAT_ID` estén definidos.

Si deseas programar una ejecución en un servidor propio, puedes usar un cron equivalente:

```
0 3 * * 1 /usr/bin/python /ruta/al/proyecto/pipeline/analysis_variaciones.py --verbose
```

## Buenas prácticas para futuras modificaciones

- Documenta siempre los cambios relevantes tanto en el código como en este README.
- Mantén `sheets_manager.py` alineado con la interfaz esperada por `analysis_variaciones.py`.
- Si agregas nuevas métricas o pestañas, actualiza la sección **Flujo de trabajo** y el listado de dependencias.
- Verifica que el workflow de GitHub Actions refleje cualquier cambio en la configuración del entorno o credenciales.
# analisis_variaciones_felinos
