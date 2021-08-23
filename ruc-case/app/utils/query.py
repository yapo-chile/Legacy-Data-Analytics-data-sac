from __future__ import annotations


class Query:

    @staticmethod
    def list_to_str(input_list: list) -> str:

        if any(type(element) == str for element in input_list):
            return "'" + "','".join(str(x) for x in input_list) + "'"
        else:
            return ', '.join([str(x) for x in input_list])


class GenericPhonesQuery(Query):

    def __init__(self, params: type[ReadParams]) -> None:
        self.params = params

    def query_generic_phones(self, years_back: int = 3) -> str:

        current_year = int(self.params.get_current_year())
        start_year = current_year - years_back

        query = f"""
        select 
            RIGHT(phone, 8) AS phone,
            count(distinct seller_id_fk) AS distinct_sellers
        from 
            ods.ad
        where
            insert_date::date between '{start_year}-01-01' and current_date
        group by
            1
        order by
            2 desc
        limit 300
        """

        return query


class UserQuery(Query):

    def query_users_by_emails(self, emails):

        emails_str = self.list_to_str(emails)
        query = f"""
        SELECT
            user_id
        FROM
            public.users
        WHERE 
            email IN ({emails_str})
        """

        return query

    def query_phones(self, user_ids: list, schema: str) -> str:

        users_str = self.list_to_str(user_ids)
        query = f"""
        SELECT
            DISTINCT RIGHT(phone, 8) AS phone
        FROM 
            {schema}.ads 
        WHERE 
            user_id IN ({users_str})
        """

        return query

    def query_users(self, phones: list, schema: str) -> str:

        phones_str = "'" + "','".join(str(x) for x in phones) + "'"
        query = f"""
        SELECT 
            DISTINCT user_id 
        FROM 
            {schema}.ads 
        WHERE 
            RIGHT(phone, 8) IN ({phones_str})
        """

        return query


class RucCaseQuery(Query):

    def query_adreply(self, user_ids: list, blocket_schemas: list) -> str:

        mail_subquery = [f"""SELECT * FROM {schema}.mail_queue
            """ for schema in blocket_schemas]
        mail_subquery = 'UNION ALL '.join(mail_subquery)

        users_str = self.list_to_str(user_ids)
        list_ids_subquery = [f"""SELECT list_id FROM {schema}.ads WHERE user_id in ({users_str})
                """ for schema in blocket_schemas]
        list_ids_subquery = 'UNION ALL '.join(list_ids_subquery)

        adreply_query = f"""
        SELECT
            added_at, sender_email, sender_name, sender_phone, remote_addr, 
            subject, body, list_id, receipient_email
        FROM
            ({mail_subquery}) aa
        WHERE
            list_id in (
                {list_ids_subquery})
        """
        return adreply_query

    def query_ads(self, user_ids: list, blocket_schemas: list) -> str:

        users_str = self.list_to_str(user_ids)
        actions_subquery = [f"""
                    select
                        rank() over(partition by ad_id,action_id order by state_id asc) as inicio,
                        rank() over(partition by ad_id,action_id order by state_id desc) as fin,
                        acs.ad_id,
                        acs.action_id,
                        aa.action_type,
                        acs.state_id,
                        acs.state,
                        acs.transition,
                        acs."timestamp",
                        acs.remote_addr,
                        acs.token_id,
                        aa.queue
                    from
                        {schema}.action_states acs
                    left join
                        {schema}.ad_actions aa
                        using(ad_id,action_id)
                    where
                        acs.ad_id in (select ad_id from {schema}.ads where user_id in ({users_str}))
                """ for schema in blocket_schemas]
        actions_subquery = 'UNION ALL '.join(actions_subquery)

        ads_subquery = [f"""
                select ad_id, subject, body, status, phone, "name", user_id, '{schema}' as schema
                from {schema}.ads where user_id in ({users_str})
                """ for schema in blocket_schemas]
        ads_subquery = 'UNION ALL '.join(ads_subquery)

        link_subquery = [f"""
                select array_agg('https://img.yapo.cl/images/' || substring(
                    ad_media_id::text, 1, 2) || '/' || ad_media_id || '.jpg') as image_url, ad_id
                from {schema}.ad_media where ad_id in (
                    select ad_id from {schema}.ads where user_id in ({users_str})) 
                group by 2
                """ for schema in blocket_schemas]
        link_subquery = 'UNION ALL '.join(link_subquery)

        ads_query = f"""
        select
            ee.ad_id,
            ee.subject,
            ee.body,
            ee.status,
            ee.phone,
            ee."name",
            ee.email,
            ee.action_type,
            ee.insert_date,
            ee.remote_addr as user_ip,
            ee.accept_refuse_date
            ,ee.image_url
        from
            (--ee
            select
                bb.ad_id,
                bb.action_id,
                bb.state_id,
                bb.action_type,
                bb.insert_date,
                bb.remote_addr,
                dd.subject,
                dd.body,
                dd.status,
                dd.phone,
                dd."name",
                u.email,
                cc."timestamp" as accept_refuse_date
                ,ff.image_url
            from
                (--bb
                select
                    aa.ad_id,
                    aa.action_id,
                    aa.state_id,
                    aa.action_type,
                    aa."timestamp" as insert_date,
                    aa.remote_addr
                from
                    (--aa{actions_subquery}) aa
                where
                    (action_type = 'activate' and state = 'activated' and transition = 'user_activated' 
                        and remote_addr <> '1.1.1.1')
                    or (action_type = 'at_combo1' and state = 'reg' and transition = 'initial')
                    or (action_type = 'at_combo2' and state = 'reg' and transition = 'initial')
                    or (action_type = 'at_combo3' and state = 'reg' and transition = 'initial')
                    or (action_type = 'autobump' and state = 'unpaid' and transition = 'pending_pay')
                    or (action_type = 'autofact' and state = 'reg' and transition = 'initial')
                    or (action_type = 'bump' and state = 'unpaid' and transition = 'pending_pay')
                    or (action_type = 'daily_bump' and state = 'reg' and transition = 'initial')
                    or (action_type = 'deactivate' and state = 'deactivated' and transition = 'user_deactivated' 
                        and remote_addr <> '0.0.0.0')
                    or (action_type = 'delete' and state = 'deleted' and transition = 'auto_deleted' 
                        and remote_addr <> '0.0.0.0')
                    or (action_type = 'delete' and state = 'deleted' and transition = 'user_deleted' 
                        and remote_addr <> '0.0.0.0' and remote_addr is not null)
                    or (action_type = 'disable' and state = 'disabled' and transition = 'user_disabled')
                    or (action_type = 'edit' and state = 'reg' and transition = 'initial')
                    or (action_type = 'editrefused' and state = 'reg' and transition = 'initial')
                    or (action_type = 'gallery' and state = 'reg' and transition = 'initial')
                    or (action_type = 'gallery_1' and state = 'reg' and transition = 'initial')
                    or (action_type = 'gallery_30' and state = 'reg' and transition = 'initial')
                    or (action_type = 'if_pack_car' and state = 'reg' and transition = 'initial')
                    or (action_type = 'if_pack_inmo' and state = 'reg' and transition = 'initial')
                    or (action_type = 'inserting_fee' and state = 'reg' and transition = 'initial')
                    or (action_type = 'label' and state = 'reg' and transition = 'initial')
                    or (action_type = 'new' and state = 'reg' and transition = 'initial')
                    or (action_type = 'renew' and state = 'reg' and transition = 'initial')
                    or (action_type = 'status_change' and state = 'reg' and transition = 'initial' 
                        and remote_addr <> '0.0.0.0' and token_id is null)
                    or (action_type = 'upselling_advanced_cars' and state = 'accepted' 
                        and transition = 'accept_w_chngs')
                    or (action_type = 'upselling_advanced_inmo' and state = 'reg' and transition = 'initial')
                    or (action_type = 'upselling_advanced_others' and state = 'reg' and transition = 'initial')
                    or (action_type = 'upselling_daily_bump' and state = 'reg' and transition = 'initial')
                    or (action_type = 'upselling_gallery' and state = 'reg' and transition = 'initial')
                    or (action_type = 'upselling_label' and state = 'reg' and transition = 'initial')
                    or (action_type = 'upselling_premium_cars' and state = 'reg' and transition = 'initial')
                    or (action_type = 'upselling_premium_inmo' and state = 'reg' and transition = 'initial')
                    or (action_type = 'upselling_premium_others' and state = 'reg' and transition = 'initial')
                    or (action_type = 'upselling_standard_cars' and state = 'reg' and transition = 'initial')
                    or (action_type = 'upselling_standard_inmo' and state = 'reg' and transition = 'initial')
                    or (action_type = 'upselling_standard_others' and state = 'reg' and transition = 'initial')
                    or (action_type = 'upselling_weekly_bump' and state = 'reg' and transition = 'initial')
                    or (action_type = 'weekly_bump' and state = 'reg' and transition = 'initial')
                )bb
            left join
                (--cc{actions_subquery})cc
                on bb.ad_id = cc.ad_id 
                    and bb.action_id = cc.action_id 
                    and fin = 1
            left join
                (--dd{ads_subquery}
                )dd
                on bb.ad_id = dd.ad_id
            left join
                users u
                on dd.user_id = u.user_id
            left join
                (--ff{link_subquery}
                )ff
                on bb.ad_id = ff.ad_id
            order by
                bb.ad_id,
                bb.action_id asc,
                bb.state_id asc
            )ee
        """

        return ads_query
