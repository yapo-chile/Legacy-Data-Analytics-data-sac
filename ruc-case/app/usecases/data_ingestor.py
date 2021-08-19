from __future__ import annotations
from infraestructure.gsheets import GoogleSheets


class DataIngestor:

    def __init__(self, config) -> None:
        self.config = config

    def get_data(self) -> type[DataFrame]:

        gs = GoogleSheets(self.config.GoogleSheetsConfig())
        return gs.get_data()
