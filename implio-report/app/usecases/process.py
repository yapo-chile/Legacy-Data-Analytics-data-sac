# pylint: disable=no-member
# utf-8
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
        df_report = ImplioReport(logger=self.logger,
                                 config=self.config,
                                 params=self.params)\
            .generate()
        self.logger.info(f'Report generated successfully, found {df_report.shape[0]} records')
        OutputHandler(logger=self.logger,
                      params=self.params)\
            .send_email(data=df_report)
        self.logger.info('Process end successfully')
