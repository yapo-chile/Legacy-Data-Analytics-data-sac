from __future__ import annotations
import base64
import os
from pandas import ExcelWriter
from infraestructure.email import Email


class OutputHandler:

    def __init__(self, logger, params):
        self.logger = logger
        self.params = params

    def create_excel(self,
                     filename: str,
                     ads_info: type[DataFrame],
                     ad_reply: type[DataFrame]
                     ) -> None:

        with ExcelWriter(filename) as writer:
            ads_info.to_excel(writer, sheet_name='ads', index=False)
            ad_reply.to_excel(writer, sheet_name='ad_reply', index=False)

    def send_email(self, filename: str, requester_email: str) -> None:

        data = open(filename, 'rb').read()
        encoded = base64.b64encode(data).decode('UTF-8')
        email = Email(to=self.params.deliver_to + requester_email,
                      subject=f"Inserting Fee Sellers with Yapesos info",
                      message="""<h3>Buen dia, se adjunta lo solicitado.</h3>
                        <h6><i>Este mensaje fue generado de forma automatica,
                        por favor no responder</i></h6>""",
                      )
        email.attach(
            "reporte.xlsx",
            encoded,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        email.send()
        # Removing file
        os.remove("output.xlsx")

    def generate(self,
                 ads_info: type[DataFrame],
                 ad_reply: type[DataFrame],
                 ruc_id: str,
                 requester_email: str) -> None:

        today = self.params.get_current_date()
        filename = f'{today} RUC {ruc_id}.xlsx'

        self.create_excel(filename, ads_info, ad_reply)
        self.logger.info(f'Ruc Case {ruc_id} exported to sheets')
        self.send_email(filename, requester_email)
        self.logger.info('Email sent successfully')
