from __future__ import annotations
from usecases.data_handler import DataHandler
from usecases.ruc_case import RucCase
from usecases.user import User
from usecases.output_handler import OutputHandler


class RequestFactory:

    def __init__(self, logger, config, params):
        self.logger = logger
        self.config = config
        self.params = params

    def generate(self,
                 row: type[Series],
                 cols_dict: dict[str, str],
                 generic_phones: list[str],
                 blocket_schemas: list[str]) -> None:

        input_emails = row[cols_dict['emails']]
        input_phones = row[cols_dict['phones']]
        ruc_id = row[cols_dict['ruc_id']]
        req_email = row[cols_dict['requester_email']]

        self.logger.info(f'Starting RUC Case {ruc_id}')
        self.logger.info(f'input emails: {input_emails}, input phones: {input_phones}')
        self.logger.ingo(f'requester email: {req_email}')

        emails, phones = DataHandler(logger=self.logger) \
            .clean_and_parse(input_emails, input_phones)
        self.logger.info(f"Data handler output: user_ids: {emails}, phones: {phones}")
        users_found = User(logger=self.logger,
                           config=self.config) \
            .search(emails, phones, generic_phones, blocket_schemas,
                    early_stop=5)
        self.logger.info(f'Users found after iterative process: {users_found}')
        df_ads, df_adreply = RucCase(config=self.config) \
            .generate(users_found, blocket_schemas)
        self.logger.info(f'RUC Case: found {df_ads.shape[0]} ads and {df_adreply.shape[0]} ad replies')
        OutputHandler(logger=self.logger,
                      params=self.params) \
            .generate(df_ads, df_adreply, ruc_id, req_email)
        self.logger(f'RUC Case {ruc_id} finished')
