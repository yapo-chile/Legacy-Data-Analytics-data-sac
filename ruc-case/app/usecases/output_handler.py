from __future__ import annotations
from pandas import ExcelWriter
from infraestructure.email import Email


class OutputHandler:

    def __init__(self, logger, params):
        self.logger = logger
        self.params = params

    def create_excel(self,
                     ads_info: type[DataFrame],
                     ad_reply: type[DataFrame],
                     ruc_id: str) -> None:  #FIXME: What to do?

        today = self.params.get_current_date()
        with ExcelWriter(f'{today} RUC {ruc_id}.xlsx') as writer:
            ads_info.to_excel(writer, sheet_name='ads', index=False)
            ad_reply.to_excel(writer, sheet_name='ad_reply', index=False)

        self.logger.info(f'Ruc Case {ruc_id} exported to sheets')

    def send_email(self) -> None:

        email = Email()

    def save_to_gdrive(self) -> None:
        pass

    def generate(self,
                 ads_info: type[DataFrame],
                 ad_reply: type[DataFrame],
                 ruc_id: str) -> None:

        self.create_excel(ads_info, ad_reply, ruc_id)
        self.send_email()
        self.logger.info('Email sent successfully')
        self.save_to_gdrive()
        self.logger.info('Excel file loaded successfully in google drive')
