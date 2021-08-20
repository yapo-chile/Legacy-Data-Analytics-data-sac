from __future__ import annotations
from infraestructure.email import Email
import calendar


class OutputHandler:

    def __init__(self, config, params) -> None:
        self.config = config
        self.params = params

    def send_email(self, data: type[DataFrame]):

        year_month = self.params.get_last_month()
        year_lm = year_month.split('-')[0]
        last_month = int(year_month.split('-')[1])
        month_name = calendar.month_name[last_month]

        subject = f'Implio null revisions {month_name} {year_lm}'
        body = f""" <h3>Estimad@s, 
        
        Se adjunta la base correspondiente al pasado mes, donde se obtienen {data.shape[0]} registros.
        
        Saludos,
        D&A Team
        </h3>
        <h6><i>Este mensaje fue generado de forma automatica,
        por favor no responder</i></h6>"""

        email = Email(self.params,
                      self.config,
                      subject=subject,
                      body=body)

        file_name = f'implio_null_revisions_{year_month}.csv'
        email.send_email_with_csv(data_to_send={file_name, data})
