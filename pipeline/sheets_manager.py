"""Manejador sencillo para interactuar con Google Sheets usando cuentas de servicio.

El flujo espera credenciales definidas en la variable de entorno
``GOOGLE_APPLICATION_CREDENTIALS`` (ruta a un archivo JSON de cuenta de servicio)
como lo hace por defecto la libreria oficial de Google.
"""

from __future__ import annotations

import os
from typing import Any, Iterable, List, Optional

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

DEFAULT_SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
)


class SheetsManager:
    """Encapsula operaciones de lectura y escritura sobre una hoja de calculo."""

    def __init__(
        self,
        spreadsheet_name: str,
        *,
        credentials_path: Optional[str] = None,
        scopes: Iterable[str] = DEFAULT_SCOPES,
    ) -> None:
        self._credentials_path = credentials_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not self._credentials_path:
            raise EnvironmentError(
                "Debes definir GOOGLE_APPLICATION_CREDENTIALS apuntando al JSON de la cuenta de servicio."
            )
        self._client = self._build_client(scopes)
        self._spreadsheet = self._open_spreadsheet(spreadsheet_name)

    def _build_client(self, scopes: Iterable[str]) -> gspread.Client:
        creds = Credentials.from_service_account_file(self._credentials_path, scopes=list(scopes))
        return gspread.authorize(creds)

    def _open_spreadsheet(self, spreadsheet_name: str) -> gspread.Spreadsheet:
        try:
            return self._client.open(spreadsheet_name)
        except (gspread.SpreadsheetNotFound, gspread.exceptions.APIError):
            return self._client.open_by_key(spreadsheet_name)

    def read_worksheet(self, worksheet_title: str) -> pd.DataFrame:
        """Lee una pestaña y la devuelve como DataFrame."""

        worksheet = self._spreadsheet.worksheet(worksheet_title)
        values = worksheet.get_all_values()
        if not values:
            return pd.DataFrame()
        header = values[0]
        rows = values[1:]
        return pd.DataFrame(rows, columns=header)

    def write_dataframe(self, worksheet_title: str, dataframe: pd.DataFrame, replace: bool = True) -> None:
        """Escribe un DataFrame en la pestaña objetivo; crea la pestaña si no existe."""

        worksheet = self._get_or_create_worksheet(worksheet_title, dataframe)
        if replace:
            worksheet.clear()
        payload = self._dataframe_to_rows(dataframe)
        worksheet.update("A1", payload, value_input_option="USER_ENTERED")

    def _get_or_create_worksheet(self, worksheet_title: str, dataframe: pd.DataFrame) -> gspread.Worksheet:
        try:
            return self._spreadsheet.worksheet(worksheet_title)
        except gspread.WorksheetNotFound:
            rows, cols = max(len(dataframe.index) + 1, 1), max(len(dataframe.columns), 1)
            return self._spreadsheet.add_worksheet(title=worksheet_title, rows=str(rows), cols=str(cols))

    @staticmethod
    def _dataframe_to_rows(dataframe: pd.DataFrame) -> List[List[Any]]:
        if dataframe.empty:
            return [dataframe.columns.tolist()]
        safe_df = dataframe.copy()
        safe_df = safe_df.fillna("")
        return [safe_df.columns.tolist()] + safe_df.astype(str).values.tolist()
