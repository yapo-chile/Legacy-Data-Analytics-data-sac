from __future__ import annotations
import pandas as pd


class GoogleSheets:

    """
    Class that allow to operate with google sheets files
    """

    def __init__(self, conf) -> None:

        self.conf = conf

    def get_data(self) -> type[DataFrame]:

        url = f'https://docs.google.com/spreadsheets/d/{self.conf.sheet_id}/gviz/tq?tqx=out:csv&sheet={self.conf.sheet_name}'
        return pd.read_csv(url, error_bad_lines=False)
