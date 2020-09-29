# pylint: disable=no-member
# utf-8
import pandas as pd
from unidecode import unidecode
from infraestructure.psql import Database
from infraestructure.surveypal_api import SurveypalApi
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
    def data_nps_max_update(self):
        return self.__data_nps_max_update

    @data_nps_max_update.setter
    def data_nps_max_update(self, config):
        """
        Method return str with max update (timestamp) which
        will be the start date for the extraction of
        NPS from Surveypal API
        """
        query = Query(config, self.params)
        db_source = Database(conf=config)
        if self.params.get_reprocess_flag() == 'yes':
            db_source.execute_command(query \
                .delete_surveypal_nps_answers_with_reprocess())
        data_nps_max_update = db_source.select_to_dict(query \
            .get_nps_max_update())
        db_source.close_connection()
        max_nps_update = str(data_nps_max_update.iloc[0]['greatest'])

        self.__data_nps_max_update = max_nps_update

    # Get data from API
    # pylint: disable=R0912
    @property
    def data_nps_api_surveypal(self):
        return self.__data_nps_api_surveypal

    @data_nps_api_surveypal.setter
    def data_nps_api_surveypal(self, config):
        """
        Method return dataframe with NPS from Surveypal API
        """
        # Take data from DWH to define from date variable
        self.data_nps_max_update = self.config.dwh

        # Hit to endpoint and retrieve data
        survey_id = '863343463'
        endpoint = '/survey/{0}/answers?from={1}&to=now' \
            .format(survey_id, self.data_nps_max_update)
        data_source = SurveypalApi(conf=config)
        self.logger.info('Getting extraction NPS from Surveypal API')
        nps_answers = data_source.get_data(endpoint)
        answers = []

        # Format data retrieved
        self.logger.info('Begining transformation NPS answers extracted \
            from Surveypal API')
        for j in range(len(nps_answers[0]['answers'])):
            temp = {}
            try:
                for i in nps_answers[0]['answers'][j]['elements']:
                    temp[i['name']] = i['values'][0]['value']
                temp['channel'] = nps_answers[0]['answers'][j]['channel']
                temp['email'] = nps_answers[0]['answers'][j]['email']
                temp['state'] = nps_answers[0]['answers'][j]['state']
                temp['startDate'] = nps_answers[0]['answers'][j]['startDate']
                temp['endDate'] = nps_answers[0]['answers'][j]['endDate']
                temp['duration'] = nps_answers[0]['answers'][j]['duration']
                temp['answerId'] = nps_answers[0]['answers'][j]['answerId']
                answers.append(temp)
            except Exception:
                pass
        ans_df = pd.DataFrame.from_dict(answers)
        ans_df['Por favor, déjanos tus comentarios o sugerencias aquí'] = \
            ans_df['Por favor, déjanos tus comentarios o sugerencias aquí'] \
                .map(lambda x: (unidecode(x.lower())).strip())

        # Order and split data retrieved in a new columns
        new_columns = list()
        for i in ans_df.columns:
            if 'probable' in i:
                new_columns.append('recomendarias')
            elif 'comentario' in i:
                new_columns.append('comentarios')
            elif 'rápido' in i:
                new_columns.append('rapidez')
            elif 'fácil' in i:
                new_columns.append('facil')
            elif 'answer' in i:
                new_columns.append('answerid')
            else:
                new_columns.append(i)
        ans_df.columns = new_columns

        self.__data_nps_api_surveypal = ans_df

    # Write data to data warehouse
    def save(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dwh)
        db.execute_command(query.delete_temp_nps_answers_table())
        self.data_nps_api_surveypal = self.config.surveypal_api
        self.logger.info('Inserting data into DWH output table')
        for row in self.data_nps_api_surveypal.itertuples():
            data_row = [(row.recomendarias, row.comentarios,
                         row.rapidez, row.facil, row.channel,
                         row.email, row.state, row.startDate,
                         row.endDate, row.duration, row.answerid)]
            db.insert_data(query.insert_into_temp_nps_answers_table(),
                           data_row)
        db.execute_command(query.delete_surveypal_nps_answers_table())
        db.execute_command(query.insert_into_nps_answers_table())
        self.logger.info('Ready data inserts into DWH table')
        db.close_connection()


    def generate(self):
        self.save()
