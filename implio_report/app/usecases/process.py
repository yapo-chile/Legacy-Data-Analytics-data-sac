# pylint: disable=no-member
# utf-8
from __future__ import annotations
from implio_report import ImplioReport
from output_handler import OutputHandler


class Process:

    def __init__(self,
                 logger,
                 config,
                 params: type[ReadParams]) -> None:

        self.logger = logger
        self.config = config
        self.params = params

    def generate(self):

        self.logger.info('')
        df_report = ImplioReport(logger=self.logger,
                                 config=self.config,
                                 params=self.params)\
            .generate()
        self.logger.info('')
        OutputHandler(config=self.config,
                      params=self.params)\
            .send_email(data=df_report)
        self.logger.info('')
