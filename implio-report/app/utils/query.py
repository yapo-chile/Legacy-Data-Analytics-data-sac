from __future__ import annotations


class ImplioReportQuery:

    def __init__(self, params: type[ReadParams]) -> None:
        self.params = params

    def query_implio_report(self):

        last_month = int(self.params.get_last_month().split('-')[1])
        year_lm = self.params.get_last_month().split('-')[0]

        query = f"""
        SELECT 
            ad_id,
            action_id,
            MIN("timestamp") AS action_timestamp
        FROM 
            (SELECT * FROM  public.action_params UNION ALL SELECT * FROM blocket_{year_lm}.action_params) AS ap 
            LEFT JOIN 
                (SELECT * FROM public.action_states UNION ALL SELECT * FROM blocket_{year_lm}.action_states) AS ast 
                USING(ad_id, action_id)
            LEFT JOIN 
                review_log AS rl 
                USING(ad_id, action_id)
        WHERE
            "name" IN ('ad_evaluation_result')
            AND EXTRACT(YEAR FROM "timestamp") = {year_lm}
            AND EXTRACT(MONTH FROM "timestamp") = {last_month}
            AND "value" = 'no_decision'
            AND admin_id IS NULL
        GROUP BY 
            ad_id, action_id
        """
