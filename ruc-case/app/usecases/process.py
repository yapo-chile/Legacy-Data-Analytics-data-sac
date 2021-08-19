# pylint: disable=no-member
# utf-8
from __future__ import annotations
from usecases.data_ingestor import DataIngestor
from usecases.req_manager import RequestManager
from usecases.generic_phones import GenericPhones
from usecases.blocket_schemas import BlocketSchemas
from usecases.req_factory import RequestFactory


class Process:

    def __init__(self,
                 logger,
                 config,
                 params: type[ReadParams]) -> None:
        self.logger = logger
        self.config = config
        self.params = params

    def generate(self) -> None:

        self.logger.info(f'Process start')
        df_req = DataIngestor(config=self.config).get_data()
        self.logger.info(f'Request table shape: {df_req.shape}')
        new_req = RequestManager(params=self.params)\
            .get_new_requests(df_req)
        self.logger.info(f'Number of new requests: {new_req.shape[0]}')
        if new_req.empty:
            self.logger.info('No new RUC case requested')
        else:
            blocket_schemas = BlocketSchemas(params=self.params) \
                .define(years_back=6)
            self.logger.info(f"Blocket schemas to query: {blocket_schemas}")
            generic_phones = GenericPhones(logger=self.logger,
                                           config=self.config,
                                           params=self.params) \
                .retrieve(years_back=2)
            self.logger.info(f"Generic phones found (sample): {generic_phones[:5]}...")
            # Google Sheets columns name mapping
            sheet_cols = {
                'emails': 'Email(s) asociados al RUC',
                'phones': 'Teléfono(s) asociados al RUC',
                'ruc_id': 'RUC (Rol Único de Causa)',
                'requester_email': 'Email Address'
            }
            for ix, row in new_req.iterrows():

                RequestFactory(logger=self.logger,
                               config=self.config,
                               params=self.params).\
                    generate(row, sheet_cols, generic_phones, blocket_schemas)

        self.logger.info(f'RucCase process Finished')
