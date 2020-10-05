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

    def get_zendesk_tickets_max_update(self) -> str:
        """
        Method return str with query
        """
        queryDwh = """
                select max(updated_at)::date as max_update from dm_content_sac.zendesk_tickets   
            """
        return queryDwh

    def get_surveypal_answers_max_update(self) -> str:
        """
        Method return str with query
        """
        queryDwh = """
                select GREATEST(max("startDate"), max("endDate"))::text from dm_content_sac.surveypal_csat_answers sca   
            """
        return queryDwh

    def insert_into_temp_zendesk_tickets_table(self) -> str:
        """
        Method return str with query
        """
        queryDwh = """
                INSERT INTO dm_content_sac.temp_zendesk_tickets
                    (ticket_id,
                    created_at,
                    updated_at,
                    via,
                    requester_email,
                    phone,
                    assignee_email,
                    reopens,
                    replies,
                    reply_time_in_minutes,
                    full_resolution_time_in_minutes,
                    solved_at,
                    group_stations,
                    assignee_stations,
                    main_contact_reason,
                    "2nd_order_contact_reason",
                    "3rd_order_contact_reason",
                    brand_name)
                VALUES %s;
            """
        return queryDwh

    def insert_into_zendesk_tickets_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    insert into dm_content_sac.zendesk_tickets (
                    select * from dm_content_sac.temp_zendesk_tickets) 
                """

        return command

    def insert_into_temp_surveypal_answers_table(self) -> str:
        """
        Method return str with query
        """
        queryDwh = """
                INSERT INTO dm_content_sac.temp_surveypal_csat_answers
                    (ticket_id,
                     "startDate",
                     "endDate",
                     disposicion,
                     claridad,
                     comentario,
                     sac_sat,
                     rapidez,
                     zendeksadid,
                     satisfaccion)
                VALUES %s;
            """
        return queryDwh

    def insert_into_surveypal_answers_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    insert into dm_content_sac.surveypal_csat_answers (
                    select * from dm_content_sac.temp_surveypal_csat_answers) 
                """

        return command

    def delete_temp_zendesk_tickets_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    truncate table dm_content_sac.temp_zendesk_tickets 
                """

        return command

    def delete_temp_surveypal_answers_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    truncate table dm_content_sac.temp_surveypal_csat_answers 
                """

        return command

    def delete_zendesk_tickets_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_content_sac.zendesk_tickets
                    where ticket_id in (
                    select z.ticket_id
                    from dm_content_sac.zendesk_tickets z
                    left join
                    dm_content_sac.temp_zendesk_tickets t
                    using (ticket_id)
                    where t.ticket_id is not null) 
                """

        return command

    def delete_zendesk_tickets_with_reprocess(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_content_sac.zendesk_tickets where
                    updated_at::date >= '""" \
                        + self.params.get_date_from() + """'::date
                """

        return command

    def delete_surveypal_answers_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_content_sac.surveypal_csat_answers
                    where ticket_id in (
                            select z.ticket_id
                            from dm_content_sac.surveypal_csat_answers z
                            left join
                            dm_content_sac.temp_surveypal_csat_answers t
                            using (ticket_id)
                            where t.ticket_id is not null) 
                """

        return command

    def delete_surveypal_answers_with_reprocess(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_content_sac.surveypal_csat_answers where
                    "startDate"::date >= '""" \
                        + self.params.get_date_from() + """'::date
                """

        return command
