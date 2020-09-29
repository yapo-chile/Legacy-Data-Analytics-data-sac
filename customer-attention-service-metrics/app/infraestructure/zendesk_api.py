import sys
import logging
import time
import pandas as pd
import requests


class ZendeskApi:
    """
    Class that extract data from Zendesk Api
    """
    def __init__(self, conf) -> None:
        self.log = logging.getLogger('zendesk_api')
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
        return {"user": self.conf.user,
                "password": self.conf.password,
                "urlprefix": self.conf.url}

    def get_session(self) -> None:
        """
        Method that get connection to zendesk api.
        """
        self.log.info('start session from zendesk api %s', self.conf.url)
        credentials = self.conf.user, self.conf.password
        self.session = requests.Session()
        self.session.auth = credentials

    def get_data(self, endpoint: str) -> pd.DataFrame:
        """
        Method that returns json response from query to zendesk api
        """
        url_dir = self.conf.url + endpoint
        self.log.info('Endpoint : %s', url_dir)
        response = self.session.get(url_dir)
        if response.status_code == 429:
            self.log.info('Rate limited! Please wait.')
            time.sleep(int(response.headers['retry-after']))
            response = self.session.get(url_dir)
            if response.status_code != 200:
                self.log.info('Status: %s. Problem with the request \
                                to Ticket Fields Source \
                                - Zendesk API. Exiting.',
                              str(response.status_code))
                sys.exit()
        elif response.status_code != 200:
            self.log.info('Status: %s. Problem with the request \
                            to Ticket Fields Source \
                            - Zendesk API. Exiting.',
                          str(response.status_code))
            sys.exit()

        json_response = response.json()

        return json_response
