from __future__ import annotations
import os
import base64
import calendar
from pandas import ExcelWriter
from infraestructure.email import Email


class OutputHandler:

    def __init__(self, logger, params) -> None:
        self.logger = logger
        self.params = params

    def create_csv(self, filename: str, data: type[DataFrame]) -> None:

        with ExcelWriter(filename) as writer:
            data.to_excel(writer, sheet_name='implio', index=False)

    def send_email(self, filename: str, row_count: int) -> None:

        year_month = self.params.get_last_month()
        year_lm = year_month.split('-')[0]
        last_month = int(year_month.split('-')[1])
        month_name = calendar.month_name[last_month]

        # Define Email parameters
        email_to = ['gp_data_analytics@yapo.cl', 'customer.care@yapo.cl']
        subject = f'Implio null revisions {month_name} {year_lm}'
        body = f""" <h3>Estimad@s, 
        
        Se adjunta la base correspondiente al pasado mes, donde se obtienen {row_count} registros.
        
        Saludos,
        D&A Team
        </h3>
        <h6><i>Este mensaje fue generado de forma automatica,
        por favor no responder</i></h6>"""

        # Sending email
        data = open(filename, 'rb').read()
        encoded = base64.b64encode(data).decode('UTF-8')
        email = Email(to=email_to,
                      subject=subject,
                      message=body)
        email.attach(filename=filename,
                     binary=encoded,
                     file_type="ext/csv")
        email.send()
        # Removing file
        os.remove(filename)

    def generate(self, data: type[DataFrame]) -> None:

        year_month = self.params.get_last_month()
        filename = f'implio_null_revisions_{year_month}.csv'
        row_count = data.shape[0]

        # Create Excel file
        self.create_csv(filename, data)
        self.logger.info(f'Report exported to csv')
        # Send email
        self.send_email(filename, row_count)
        self.logger.info('Email sent successfully')
