from __future__ import annotations
from infraestructure.psql import DataBase
from utils.query import ImplioReportQuery


class ImplioReport(ImplioReportQuery):

    def __init__(self, logger, config, params) -> None:
        super().__init__(params)
        self.logger = logger
        self.config = config

    def get_data(self) -> type[DataFrame]:

        db_source = DataBase(conf=self.config.blocket)
        df_data = db_source.select_to_dict(query=self.query_implio_report())
        db_source.close_connection()

        return df_data

    def generate(self) -> type[DataFrame]:

        df_data = self.get_data()
        self.logger(f'Data retrieved successfully. Number of rows: {df_data.shape[0]}')
        df_data.loc[:, 'month_id'] = df_data['action_timestamp'].map(lambda x: str(x)[0:7])
        year_month = self.params.get_last_month()

        return df_data[df_data['month_id'] == f'{year_month}']





