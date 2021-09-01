from __future__ import annotations
import os
import base64
from pandas import ExcelWriter
from infraestructure.email import Email


class OutputHandler:

    def __init__(self, logger, config, params):
        self.logger = logger
        self.config = config
        self.params = params

    def create_excel(self,
                     filename: str,
                     ads_info: type[DataFrame],
                     ad_reply: type[DataFrame]
                     ) -> None:

        with ExcelWriter(filename) as writer:
            ads_info.to_excel(writer, sheet_name='ads', index=False)
            ad_reply.to_excel(writer, sheet_name='ad_reply', index=False)

    def send_email(self, filename: str, ruc_id: str, requester_email: str) -> None:

        self.logger.info(f'param email_to: {self.params.email_to}')
        email_to = ['gp_data_analytics@yapo.cl',
                    'customer.care@yapo.cl'] + [requester_email]
        self.logger.info(f'final email_to: {email_to}')
        email_from = 'noreply@yapo.cl'

        subject = f"Información Caso RUC: {ruc_id}"
        body = f""" <h3>Estimad@s,

        Se adjunta archivo excel con los avisos y conversaciones encontrados en nuestro sitio asociados al RUC {ruc_id}.

        Saludos,
        D&A Team
        </h3>
        <h6><i>Este mensaje fue generado de forma automatica,
        por favor no responder</i></h6>"""

        # Sending email
        data = open(filename, 'rb').read()
        encoded = base64.b64encode(data).decode('UTF-8')
        email = Email(email_from=email_from,
                      to=email_to,
                      subject=subject,
                      message=body)
        email.attach(filename=filename,
                     binary=encoded,
                     file_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
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

        # Create Excel file
        self.create_excel(filename, ads_info, ad_reply)
        self.logger.info(f'Ruc Case {ruc_id} exported to sheets')
        # Send email
        self.send_email(filename, ruc_id, requester_email)
        self.logger.info('Email sent successfully')
