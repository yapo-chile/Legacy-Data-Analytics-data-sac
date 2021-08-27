# pylint: disable=no-member
# utf-8
import pandas as pd  # FIXME: TEST
from __future__ import annotations
from usecases.implio_report import ImplioReport
from usecases.output_handler import OutputHandler


class Process:

    def __init__(self,
                 logger,
                 config,
                 params: type[ReadParams]) -> None:

        self.logger = logger
        self.config = config
        self.params = params

    def generate(self) -> None:

        self.logger.info('The process begins')
        #df_report = ImplioReport(logger=self.logger,
        #                         config=self.config,
        #                         params=self.params)\
        #    .generate() # FIXME: TEST
        df_report = pd.read_csv('implio_null_revisions_202107.csv')  # FIXME: TEST
        self.logger.info(f'Report generated successfully, found {df_report.shape[0]} records')
        OutputHandler(logger=self.logger,
                      params=self.params)\
            .generate(data=df_report)
        self.logger.info('Process end successfully')
