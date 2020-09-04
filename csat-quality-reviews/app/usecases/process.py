# pylint: disable=no-member
# utf-8
from infraestructure.psql import Database
from utils.query import Query


class Process():
    def __init__(self,
                 config,
                 params,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data warehouse
    @property
    def data_dwh_reviews(self):
        return self.__data_dwh_reviews

    @data_dwh_reviews.setter
    def data_dwh_reviews(self, config):
        query = Query(config, self.params)
        db_source = Database(conf=config)
        output_df = db_source.select_to_dict(query \
            .get_reviews())
        db_source.close_connection()

        output_df['ad_id'] = \
            output_df['ad_id'].fillna(0).astype(int)
        output_df['tpo_creation_exit_min_real'] = \
            output_df['tpo_creation_exit_min_real'].fillna(0).astype(int)

        self.__data_dwh_reviews = output_df

    # Query data from data warehouse
    @property
    def data_dwh_pro_reviews(self):
        return self.__data_dwh_pro_reviews

    @data_dwh_pro_reviews.setter
    def data_dwh_pro_reviews(self, config):
        query = Query(config, self.params)
        db_source = Database(conf=config)
        output_df = db_source.select_to_dict(query \
            .get_pro_reviews())
        db_source.close_connection()
        self.__data_dwh_pro_reviews = output_df

    # Query data from data warehouse
    @property
    def data_dwh_agg_reviews(self):
        return self.__data_dwh_agg_reviews

    @data_dwh_agg_reviews.setter
    def data_dwh_agg_reviews(self, config):
        query = Query(config, self.params)
        db_source = Database(conf=config)
        output_df = db_source.select_to_dict(query \
            .get_agg_reviews())
        db_source.close_connection()
        self.__data_dwh_agg_reviews = output_df

    # Write data to data warehouse
    def save_to_temp_reviews(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dwh)
        db.execute_command(query.delete_temp_reviews_table())
        self.data_dwh_reviews = self.config.dwh
        self.logger.info('Executing dm_content_sac.temp_reviews inserts cycle')
        for row in self.data_dwh_reviews.itertuples():
            data_row = [(row.ad_id, row.category_name,
                         row.action_type, row.review_time,
                         row.pri_pro, row.tpo_creation_exit_min_real,
                         row.queue, row.action)]
            db.insert_data(query.insert_to_temp_reviews_table(), data_row)
        self.logger.info('INSERT dm_content_sac.temp_reviews COMMIT.')
        self.logger.info('Executed data persistence cycle')
        db.close_connection()

    # Write data to data warehouse
    def save_to_pro_reviews(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dwh)
        db.execute_command(query.delete_pro_reviews_table())
        self.data_dwh_pro_reviews = self.config.dwh
        self.logger.info('Executing dm_content_sac.pro_reviews inserts cycle')
        for row in self.data_dwh_pro_reviews.itertuples():
            data_row = [(row.review_time, row.pro_reviews)]
            db.insert_data(query.insert_to_pro_reviews_table(), data_row)
        self.logger.info('INSERT dm_content_sac.pro_reviews COMMIT.')
        self.logger.info('Executed data persistence cycle')
        db.close_connection()

    # Write data to data warehouse
    def save_to_agg_reviews(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dwh)
        db.execute_command(query.delete_agg_reviews_table())
        self.data_dwh_agg_reviews = self.config.dwh
        self.logger.info('Executing dm_content_sac.agg_reviews inserts cycle')
        for row in self.data_dwh_agg_reviews.itertuples():
            data_row = [(row.review_time, row.category_name,
                         row.queue, row.pri_pro, row.action_type,
                         row.action, row.less_than_30min,
                         row.less_than_60min, row.between_60_120min,
                         row.reviews)]
            db.insert_data(query.insert_to_agg_reviews_table(), data_row)
        self.logger.info('INSERT dm_content_sac.agg_reviews COMMIT.')
        self.logger.info('Executed data persistence cycle')
        db.close_connection()

    def generate(self):
        self.save_to_temp_reviews()
        self.save_to_pro_reviews()
        self.save_to_agg_reviews()
