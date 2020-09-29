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

    def generate(self):
        self.save_to_zendesk_tickets()
        self.save_to_surveypal_answers()
