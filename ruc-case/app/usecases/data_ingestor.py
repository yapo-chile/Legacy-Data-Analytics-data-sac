from __future__ import annotations
import pandas as pd


class GoogleSheets:

    def __init__(self, sheet_id, sheet_name):

    self.id =
    self.sheet_name =


class DataIngestion:

    def from_google_sheet(self) -> tuple[str, str]:

        sheet_id = '1bdwa0t4WIkikb_Bd5_A9qGTX5pD4F5O_wY_Rv7GHPi8'
        sheet_name = 'responses'
        url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'
        df = pd.read_csv(url, error_bad_lines=False)


        input_emails = ''
        input_phones = ''

        return input_emails, input_phones