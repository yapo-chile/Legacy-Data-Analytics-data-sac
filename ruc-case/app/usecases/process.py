# pylint: disable=no-member
# utf-8
from __future__ import annotations
from data_ingestion import DataIngestion
from data_handler import DataHandler
from ruc_case import RucCase
from generic_phones import GenericPhones
from user import User
from blocket_schemas import BlocketSchemas
from output_handler import OutputHandler


class Process:

    def __init__(self,
                 logger,
                 config,
                 params: type[ReadParams]) -> None:
        self.logger = logger
        self.config = config
        self.params = params

    def generate(self) -> None:  # FIXME: INPUT FROM GFORM

        self.logger.info(f'Process start')
        input_emails, input_phones, ruc_id = DataIngestion\
            .from_google_sheet()

        blocket_schemas = BlocketSchemas(params=self.params)\
            .define(years_back=6)
        self.logger.info(f"Blocket schemas to query: {blocket_schemas}")
        generic_phones = GenericPhones(logger=self.logger,
                                       config=self.config,
                                       params=self.params)\
            .retrieve(years_back=2)
        self.logger.info(f"Generic phones found (sample): {generic_phones[:5]}...")
        emails, phones = DataHandler(logger=self.logger)\
            .clean(input_emails, input_phones)
        self.logger.info(f"Data handler output: user_ids: {emails}, phones: {phones}")
        users_found = User(logger=self.logger,
                           config=self.config)\
            .search(emails, phones, generic_phones, blocket_schemas,
                    early_stop=5)
        self.logger.info(f'Users found after iterative process: {users_found}')
        df_ads, df_adreply = RucCase(config=self.config)\
            .generate(users_found, blocket_schemas)
        self.logger.info(f'RUC Case: found {df_ads.shape[0]} ads and {df_adreply.shape[0]} ad replies' )
        OutputHandler(logger=self.logger,
                      params=self.params)\
            .generate(df_ads, df_adreply, ruc_id)

        self.logger.info(f'RucCase process succeed')
