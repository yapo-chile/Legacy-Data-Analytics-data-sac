from infraestructure.conf import getConf
from utils.read_params import ReadParams


class Query:
    """
    Class that store all querys
    """
    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def get_nps_max_update(self) -> str:
        """
        Method return str with query
        """
        queryDwh = """
                select GREATEST(max("startDate"), max("endDate"))::text from dm_content_sac.nps_answers sca  
            """
        return queryDwh

    def insert_into_temp_nps_answers_table(self) -> str:
        """
        Method return str with query
        """
        queryDwh = """
                INSERT INTO dm_content_sac.temp_nps_answers
                    (recomendarias,
                     comentarios,
                     rapidez,
                     facil,
                     channel,
                     email,
                     state,
                     "startDate",
                     "endDate",
                     duration,
                     answerid)
                VALUES %s;
            """
        return queryDwh

    def insert_into_nps_answers_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    insert into dm_content_sac.nps_answers (
                    select * from dm_content_sac.temp_nps_answers) 
                """

        return command

    def delete_temp_nps_answers_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    truncate table dm_content_sac.temp_nps_answers 
                """

        return command

    def delete_surveypal_nps_answers_table(self) -> str:
        """
        Method that returns events of the  day
        """
        command = """
                    delete from dm_content_sac.nps_answers
                    where answerid in (
                            select z.answerid
                            from dm_content_sac.nps_answers z
                            left join
                            dm_content_sac.temp_nps_answers t
                            using (answerid)
                            where t.answerid is not null) """

        return command

    def delete_surveypal_nps_answers_with_reprocess(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_content_sac.nps_answers where
                    "startDate"::date >= '""" \
                        + self.params.get_date_from() + """'::date
                """

        return command
