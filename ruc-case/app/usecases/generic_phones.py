from utils.query import GenericPhonesQuery
from infraestructure.psql import Database


class GenericPhones(GenericPhonesQuery):

    def __init__(self, logger, config, params) -> None:
        super().__init__(params)
        self.logger = logger
        self.config = config

    def retrieve(self, years_back: int = 3) -> list:
        """
        Get the generic phones numbers (ex: 11111111) used in yapo.cl

        """
        db_source = Database(conf=self.config.db)
        generic_df = db_source.select_to_dict(query=self.query_generic_phones(years_back))

        return generic_df['phone'].to_list()
