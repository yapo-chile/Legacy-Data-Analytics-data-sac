# pylint: disable=no-member
# utf-8
import pandas as pd
from infraestructure.psql import Database
from infraestructure.surveypal_api import SurveypalApi
from utils.query import Query


class Surveypal():

    # Query data from data warehouse
    @property
    def data_surveypal_answers(self):
        return self.__data_surveypal_answers

    @data_surveypal_answers.setter
    def data_surveypal_answers(self, config):
        """
        Method return str with max update (timestamp) which
        will be the start date for the extraction of
        CSAT Answers from Surveypal API
        """
        query = Query(config, self.params)
        db_source = Database(conf=config)
        if self.params.get_reprocess_flag() == 'yes':
            db_source.execute_command(query \
                .delete_surveypal_answers_with_reprocess())
        data_surveypal_answers = db_source.select_to_dict(query \
            .get_surveypal_answers_max_update())
        db_source.close_connection()
        max_csat_update = str(data_surveypal_answers.iloc[0]['greatest'])

        self.__data_surveypal_answers = max_csat_update

    # Get data from API
    @property
    def data_api_surveypal_csat_answers(self):
        return self.__data_api_surveypal_csat_answers

    @data_api_surveypal_csat_answers.setter
    def data_api_surveypal_csat_answers(self, config):
        """
        Method return dataframe with CSAT Answers from Surveypal API
        """
        def getParam(listParams, key):
            returned_param = False
            for param in listParams:
                if param['key'] == key:
                    returned_param = param['value']
                    break
            return returned_param

        # pylint: disable=R1705
        def validate_columns(value: object,
                             position: int,
                             return_type: str):
            try:
                if list(value)[position]['values'][0]['value'] is not None:
                    return int(list(value)[position]['values'][0]['value']) \
                        if return_type == 'int' \
                            else list(value)[position]['values'][0]['value']
                else:
                    return 0 if return_type == 'int' \
                        else list(value)[position]['values'][0]['value']
            except Exception:
                pass

        # Take data from DWH to define from date variable
        self.data_surveypal_answers = self.config.dwh

        self.logger.info('Getting extraction CSAT Answers from Surveypal API')
        # Hit to endpoint and retrieve data
        survey_id = '865783919'
        endpoint = '/survey/{0}/answers?from={1}&to=now' \
            .format(survey_id, self.data_surveypal_answers)
        data_source = SurveypalApi(conf=config)

        answers = data_source.get_data(endpoint)
        answers = dict(answers[0])
        answers_df = pd.DataFrame(answers['answers'])
        answers_df_min = answers_df[['email', 'startDate', 'endDate',
                                     'meta', 'elements']]

        # Transform the elements field extracted from this endpoint
        answers_df_min['disposicion'] = answers_df_min['elements'] \
            .map(lambda x: validate_columns(x, 0, 'int'))
        answers_df_min['claridad'] = answers_df_min['elements'] \
            .map(lambda x: validate_columns(x, 1, 'int'))
        answers_df_min['comentario'] = answers_df_min['elements'] \
            .map(lambda x: validate_columns(x, 4, 'str'))
        answers_df_min['sac_sat'] = answers_df_min['elements'] \
            .map(lambda x: validate_columns(x, 3, 'int'))
        answers_df_min['rapidez'] = answers_df_min['elements'] \
            .map(lambda x: validate_columns(x, 4, 'int'))
        answers_df_min['zendeksadid'] = answers_df_min['meta'] \
            .map(lambda x: getParam(x, 'zendeksadid'))
        answers_df_min['satisfaccion'] = answers_df_min['elements'] \
            .map(lambda x: int(list(x)[5]['values'][0]['value'])
                 if len(list(x)) > 5 else 6)

        # Transform email field into ticket_id field
        answers_df_min = answers_df_min[answers_df_min['email'] \
            .str.isnumeric()]
        answers_df_min['email'] = answers_df_min['email'] \
            .map(lambda x: int(x) if x.isdigit() else x)
        answers_df_min = answers_df_min.drop('elements', 1)
        del answers_df_min['meta']
        answers_df_min.rename(columns={'email': 'ticket_id'},
                              inplace=True)
        self.logger.info('Ready extract and transform data from Surveypal API')

        self.__data_api_surveypal_csat_answers = answers_df_min

    # Write data to data warehouse
    def save_to_surveypal_answers(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dwh)
        db.execute_command(query.delete_temp_surveypal_answers_table())
        self.data_api_surveypal_csat_answers = self.config.surveypal_api
        self.logger.info('Inserting data into DWH Surveypal output table')
        for row in self.data_api_surveypal_csat_answers.itertuples():
            data_row = [(row.ticket_id, row.startDate, row.endDate,
                         row.disposicion, row.claridad, row.comentario,
                         row.sac_sat, row.rapidez, row.zendeksadid,
                         row.satisfaccion)]
            db.insert_data(query.insert_into_temp_surveypal_answers_table(),
                           data_row)
        db.execute_command(query.delete_surveypal_answers_table())
        db.execute_command(query.insert_into_surveypal_answers_table())
        self.logger.info('Ready data inserts into DWH table')
        db.close_connection()
