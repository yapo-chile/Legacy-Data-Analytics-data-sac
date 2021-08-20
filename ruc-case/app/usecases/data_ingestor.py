from __future__ import annotations
from infraestructure.gsheets import GoogleSheets


class DataIngestor:

    def __init__(self, config) -> None:
        self.config = config

    def get_data(self) -> type[DataFrame]:

        gs = GoogleSheets(self.config.gs)
        return gs.get_data()

    def generate(self, sheet_cols: dict[str, str]) -> type[DataFrame]:

        df = self.get_data()
        df.fillna('N/A', inplace=True)
        df = df.astype({sheet_cols['phones']: 'object',
                        sheet_cols['emails']: 'object'})
        return df
