import logging
import pandas as pd
import requests


class SurveypalApi:
    """
    Class that extract data from Surveypal Api
    """
    def __init__(self, conf) -> None:
        self.log = logging.getLogger('surveypal_api')
        date_format = """%(asctime)s,%(msecs)d %(levelname)-2s """
        info_format = """[%(filename)s:%(lineno)d] %(message)s"""
        log_format = date_format + info_format
        logging.basicConfig(format=log_format, level=logging.INFO)
        self.conf = conf
        self.session = None
        self.get_session()

    def api_conf(self) -> dict:
        """
        Method that return dict with database credentials.
        """
        return {"token": self.conf.token,
                "urlprefix": self.conf.url}

    def get_session(self) -> None:
        """
        Method that get connection to surveypal api.
        """
        self.log.info('start session from surveypal api %s', self.conf.url)
        headers = {"X-Auth-Token": self.conf.token,
                   "Accept": "application/json"}
        self.session = requests.Session()
        self.session.headers.update(headers)

    def get_data(self, survey_id: str, date_from: str) -> pd.DataFrame:
        """
        Method that returns json response from query to surveypal api
        """
        endpoint = '/survey/{0}/answers?from={1}&to=now' \
            .format(survey_id, date_from)
        url_dir = self.conf.url + endpoint
        self.log.info('Endpoint : %s', url_dir)
        response = self.session.get(url_dir)
        if response.status_code != 200:
            self.log.info('Status: %s. Problem with the request \
                            to Surveypal Answers Source - \
                            Surveypal API. Exiting.',
                          str(response.status_code))
            raise SystemExit('Error : Data retrived from api failed \
                with response {0}, {1}'.format(response.status_code,
                                               response.reason))

        json_response = response.json()

        return json_response
