# pylint: disable=no-member
# utf-8
from usecases.attention_ticket import Ticket
from usecases.satisfaction_survey import Surveypal


class Process(Ticket, Surveypal):
    def __init__(self,
                 config,
                 params,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # pylint: disable=W0201
    def generate(self):
        self.data_api_zendesk_tables = self.config.zendesk_api
        self.save_to_zendesk_tickets()
        self.data_api_surveypal_csat_answers = self.config.surveypal_api
        self.save_to_surveypal_answers()
