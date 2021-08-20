from __future__ import annotations
from infraestructure.psql import Database
from utils.query import RucCaseQuery


class RucCase(RucCaseQuery):

    def __init__(self, config):
        self.config = config

    def get_ads(self, user_ids: list, blocket_schemas: list) -> type[DataFrame]:

        db_source = Database(conf=self.config.db)
        df_ads = db_source.select_to_dict(query=self.query_ads(user_ids, blocket_schemas))
        db_source.close_connection()

        return df_ads

    def get_ad_reply(self, user_ids: list, blocket_schemas: list) -> type[DataFrame]:

        db_source = Database(conf=self.config.db)
        df_adreply = db_source.select_to_dict(query=self.query_adreply(user_ids, blocket_schemas))
        db_source.close_connection()

        return df_adreply

    def generate(self, user_ids: list, blocket_schemas: list) -> tuple[type[DataFrame], type[DataFrame]]:

        ads_info = self.get_ads(user_ids, blocket_schemas)
        ad_reply = self.get_ad_reply(user_ids, blocket_schemas)

        return ads_info, ad_reply
