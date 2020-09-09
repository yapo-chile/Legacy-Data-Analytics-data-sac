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

    def get_reviews(self) -> str:
        """
        Method return str with query
        """
        queryDW = """
                select
                    a.ad_id,
                    a.category_name,
                    a.action_type,
                    a.review_time,
                    a.pri_pro,
                    a.tpo_creation_exit_min_real,
                    a.queue,
                    a.action
                from
                    (select
                        rt.ad_id,
                        c.category_name,
                        c.category_id_pk,
                        rt.real_action_type::text as "action_type",
                        rt.review_time::date,
                        case 
                            when spd.user_id is null then 'Pri' else 'Pro'
                        end as pri_pro,
                        rt.grupo_revision,
                        rt.tpo_creation_exit_min_real,
                        rt.queue,
                        rt.action
                    from
                        stg.ads_reviews_time rt
                        left join ods.ad a on rt.ad_id = a.ad_id_nk
                        left join ods.seller_pro_details spd using(seller_id_fk, category_id_fk)
                        left join ods.category c on a.category_id_fk = c.category_id_pk
                    where
                        queue not in ('whitelist', 'autorefuse', 'autoaccept', 'pro_refuse_changes', 'double_pro_refuse', 'pack')
                        and review_time::date >= '{0}'
                        and review_time::date <= '{1}'
                        and c.category_id_pk  in (32, 47,48,7,8)
                    union all
                    select
                        rp.ad_id,
                        c.category_name,
                        c.category_id_pk,
                        rp.action_type,
                        rp.review_date as "review_time",
                        case 
                            when spd.user_id is null then 'Pri' else 'Pro'
                        end as pri_pro,
                        'auto' as grupo_revision,
                        30 as tpo_creation_exit_min_real,
                        rp.queue,
                        case
                            when rp.action = 'accept_w_chngs' then 'accept_ch'
                            when rp.action = 'accept' then 'accepted'
                            when rp.action = 'refuse' then 'refused'
                        end as "action"
                    from
                        stg.review_params rp
                        left join ods.ad a on rp.ad_id = a.ad_id_nk
                        left join ods.seller_pro_details spd using(seller_id_fk, category_id_fk)
                        left join ods.category c on a.category_id_fk = c.category_id_pk
                    where
                        queue in ('whitelist', 'autorefuse', 'autoaccept', 'pro_refuse_changes', 'double_pro_refuse', 'pack')
                        and review_date::date >= '{0}'
                        and review_date::date <= '{1}'
                        and c.category_id_pk  in (32, 47,48,7,8)) a 
            """.format(self.params.get_date_from(),
                       self.params.get_date_to())
        return queryDW

    def get_pro_reviews(self) -> str:
        """
        Method return str with query
        """
        queryDW = """
                select
                    review_time,
                    count(*) as pro_reviews
                from dm_content_sac.temp_reviews
                where
                    pri_pro = 'Pro'
                group by 1
                order by 1
            """
        return queryDW

    def get_agg_reviews(self) -> str:
        """
        Method return str with query
        """
        queryDW = """
                select
                    review_time,
                    category_name,
                    queue,
                    pri_pro,
                    action_type,
                    action,
                    sum(case when tpo_creation_exit_min_real <= 30 then 1 else 0 end) as less_than_30min,
                    sum(case when tpo_creation_exit_min_real <= 60 then 1 else 0 end) as less_than_60min,
                    sum(case when tpo_creation_exit_min_real > 60  and tpo_creation_exit_min_real <= 120 then 1 else 0 end) as between_60_120min,
                    count(*) as reviews
                from dm_content_sac.temp_reviews
                group by 1,2,3,4,5,6
                order by 1,2,3,4,5,6
            """
        return queryDW

    def insert_to_temp_reviews_table(self) -> str:
        """
        Method return str with query
        """
        query = """
                INSERT INTO dm_content_sac.temp_reviews
                            (ad_id,
                             category_name,
                             action_type,
                             review_time,
                             pri_pro,
                             tpo_creation_exit_min_real,
                             queue,
                             action)
                VALUES %s;"""
        return query

    def insert_to_pro_reviews_table(self) -> str:
        """
        Method return str with query
        """
        query = """
                INSERT INTO dm_content_sac.pro_reviews
                            (review_time,
                            pro_reviews)
                VALUES %s;"""
        return query

    def insert_to_agg_reviews_table(self) -> str:
        """
        Method return str with query
        dm_content_sac.agg_reviews
        """
        query = """
                INSERT INTO dm_content_sac.agg_reviews
                            (review_time,
                            category_name,
                            queue,
                            pri_pro,
                            action_type,
                            action,
                            less_than_30min,
                            less_than_60min,
                            between_60_120min,
                            reviews)
                VALUES %s;"""
        return query

    def delete_temp_reviews_table(self) -> str:
        """
        Method that returns events
        of the day
        """
        command = """
                    truncate table dm_content_sac.temp_reviews 
                """
        return command

    def delete_pro_reviews_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_content_sac.pro_reviews where
                    review_time::date >= '""" \
                        + self.params.get_date_from() + """'::date
                    and review_time::date <= '""" \
                        + self.params.get_date_to() + """'::date
                """

        return command

    def delete_agg_reviews_table(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_content_sac.agg_reviews where
                    review_time::date >= '""" \
                        + self.params.get_date_from() + """'::date
                    and review_time::date <= '""" \
                        + self.params.get_date_to() + """'::date
                """

        return command
