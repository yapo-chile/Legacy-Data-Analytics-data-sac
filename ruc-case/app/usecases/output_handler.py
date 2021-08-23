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

    def send_email(self, filename: str, requester_email: str, ruc_id: str) -> None:

        data = open(filename, 'rb').read()
        encoded = base64.b64encode(data).decode('UTF-8')
        body = f""" <h3>Estimad@s, 

        Se adjunta archivo excel con los avisos y conversaciones encontrados en nuestro sitio asociados al RUC {ruc_id}.

        Saludos,
        D&A Team
        </h3>
        <h6><i>Este mensaje fue generado de forma automatica,
        por favor no responder</i></h6>"""

        email = Email(to=self.params.deliver_to + requester_email,
                      subject=f"Inserting Fee Sellers with Yapesos info",
                      message=body
                      )
        email.attach(
            filename,
            encoded,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        email.send()
        # Removing file
        os.remove(filename)

    def generate(self,
                 ads_info: type[DataFrame],
                 ad_reply: type[DataFrame],
                 ruc_id: str,
                 requester_email: str) -> None:

        today = self.params.get_date_to()
        filename = f'{today} RUC {ruc_id}.xlsx'

        self.create_excel(filename, ads_info, ad_reply)
        self.logger.info(f'Ruc Case {ruc_id} exported to sheets')
        self.send_email(filename, requester_email, ruc_id)
        self.logger.info('Email sent successfully')
