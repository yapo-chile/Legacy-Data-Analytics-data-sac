from __future__ import annotations
import pandas as pd


class GoogleSheets:

    def __init__(self, sheet_id: str, sheet_name: str) -> None:

        self.id = sheet_id
        self.sheet_name = sheet_name

    def get_data(self) -> type[DataFrame]:

        url = f'https://docs.google.com/spreadsheets/d/{self.id}/gviz/tq?tqx=out:csv&sheet={self.sheet_name}'
        return pd.read_csv(url, error_bad_lines=False)
