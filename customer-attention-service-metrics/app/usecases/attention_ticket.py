# pylint: disable=no-member
# utf-8
import datetime
import time
from calendar import timegm
import pandas as pd
from infraestructure.psql import Database
from infraestructure.zendesk_api import ZendeskApi
from utils.query import Query


class Ticket():

    # Query data from data warehouse
    @property
    def data_zendesk_tickets(self):
        return self.__data_zendesk_tickets

    @data_zendesk_tickets.setter
    def data_zendesk_tickets(self, config):
        """
        Method return str with max update (timestamp) which
        will be the start date for the extraction of
        service tickets from the Zendesk API
        """
        query = Query(config, self.params)
        db_source = Database(conf=config)
        if self.params.get_reprocess_flag() == 'yes':
            db_source.execute_command(query \
                .delete_zendesk_tickets_with_reprocess())
        data_zendesk_tickets = db_source.select_to_dict(query \
            .get_zendesk_tickets_max_update())
        db_source.close_connection()
        max_update_date = str(data_zendesk_tickets.iloc[0]['max_update'])
        max_update_date = time.strptime(max_update_date, '%Y-%m-%d')
        max_update_date_epoch = timegm(max_update_date)

        self.__data_zendesk_tickets = max_update_date_epoch

    # Get data from API
    @property
    def data_api_zendesk_ticket_fields(self):
        return self.__data_api_zendesk_ticket_fields

    @data_api_zendesk_ticket_fields.setter
    def data_api_zendesk_ticket_fields(self, config):
        """
        Method return dataframe with tickets fields from Zendesk API
        """
        self.logger.info('Getting tickets fields from Zendesk API')
        # Hit to endpoint and retrieve data
        endpoint = '/api/v2/ticket_fields.json'
        articles_json = []
        data_source = ZendeskApi(conf=config)
        prefix_url = data_source.api_conf().get('urlprefix')

        while endpoint:
            json_response = data_source.get_data(endpoint)
            articles_json.extend(json_response['ticket_fields'])
            next_page_url = json_response['next_page']
            if next_page_url is not None:
                endpoint = next_page_url.replace(prefix_url, '')
            else:
                endpoint = next_page_url

        ticket_fields_df = pd.DataFrame(articles_json)
        ticket_fields_min = ticket_fields_df[['id', 'title']]

        ticket_fields_min['contact_reason_field'] = \
            ticket_fields_min['title'] \
            .map(lambda x: bool("RC" and ("Detalle" or "Descripci") in x))
        ticket_fields_min['contact_reason_field_2nd_order'] = \
            ticket_fields_min['title'] \
            .map(lambda x: bool("RC" and "Detalle" in x))
        ticket_fields_min['contact_reason_field_3rd_order'] = \
            ticket_fields_min['title'] \
            .map(lambda x: bool("RC" and "Descripci" in x))

        self.__data_api_zendesk_ticket_fields = ticket_fields_min

    # Get data from API
    @property
    def data_api_zendesk_ticket_brands(self):
        return self.__data_api_zendesk_ticket_brands

    @data_api_zendesk_ticket_brands.setter
    def data_api_zendesk_ticket_brands(self, config):
        """
        Method return dictionary with tickets brands from Zendesk API
        """
        self.logger.info('Getting tickets brands from Zendesk API')
        # Hit to endpoint and retrieve data
        endpoint = '/api/v2/brands.json'
        articles_json = []
        data_source = ZendeskApi(conf=config)
        prefix_url = data_source.api_conf().get('urlprefix')

        while endpoint:
            json_response = data_source.get_data(endpoint)
            articles_json.extend(json_response['brands'])
            next_page_url = json_response['next_page']
            if next_page_url is not None:
                endpoint = next_page_url.replace(prefix_url, '')
            else:
                endpoint = next_page_url

        ticket_brands_df = pd.DataFrame(articles_json)
        del ticket_brands_df['ticket_form_ids']
        del ticket_brands_df['logo']
        del ticket_brands_df['signature_template']

        brand_dict = dict(zip(ticket_brands_df.id, ticket_brands_df.name))

        self.__data_api_zendesk_ticket_brands = brand_dict

    # Get data from API
    @property
    def data_api_zendesk_tables(self):
        return self.__data_api_zendesk_tables

    @data_api_zendesk_tables.setter
    def data_api_zendesk_tables(self, config):
        """
        Method return dataframe with tickets, metrics and
        users data from Zendesk API
        """
        # pylint: disable=R0915
        def get_main_contact_value(field_id, fields_dict):
            temp_value = None
            for field in fields_dict:
                if field['id'] == field_id and field['value'] != '':
                    temp_value = field['value']
            return temp_value

        def get_n_ord_contact_value(fields_ids, fields_dict):
            temp_value = None
            for field_id in fields_ids:
                for field in fields_dict:
                    if field['value'] is not None \
                        and field['value'] != '' \
                        and field['id'] == field_id:
                        temp_value = field['value']
            return temp_value

        # Take data from DWH to define start_time variable
        self.data_zendesk_tickets = self.config.dwh

        self.logger.info('Getting tickets/metrics/users data from Zendesk API')
        # Hit to endpoint and retrieve data
        endpoint = '/api/v2/incremental/tickets.json?start_time=' \
            + str(self.__data_zendesk_tickets) + '&include=users,metric_sets'
        articles_json = {'tickets': [],
                         'users': [],
                         'metric_sets': [],
                         'comment_events': []}
        data_source = ZendeskApi(conf=config)
        prefix_url = data_source.api_conf().get('urlprefix')
        main_contact_reason_id = 360017110171

        while endpoint:
            json_response = data_source.get_data(endpoint)
            if json_response['count'] < 1000:
                endpoint = None
            else:
                next_page_url = json_response['next_page']
                if next_page_url is not None:
                    endpoint = next_page_url.replace(prefix_url, '')
                else:
                    endpoint = next_page_url

            for value in json_response['tickets']:
                temp_ticket = {
                    'id': value['id'],
                    'created_at': value['created_at'],
                    'updated_at': value['updated_at'],
                    'brand_id': value['brand_id'],
                    'requester_id': value['requester_id'],
                    'assignee_id': value['assignee_id'],
                    'via': value['via'],
                    'custom_fields': value['custom_fields'],
                    'subject': value['subject'],
                    'description': value['description']
                }
                articles_json['tickets'].append(temp_ticket)

            for value in json_response['users']:
                temp_user = {
                    'id': value['id'],
                    'email': value['email'],
                    'phone': value['phone']
                }
                articles_json['users'].append(temp_user)

            for value in json_response['metric_sets']:
                temp_metric = {
                    'ticket_id': value['ticket_id'],
                    'reopens': value['reopens'],
                    'replies': value['replies'],
                    'reply_time_in_minutes': value['reply_time_in_minutes'],
                    'full_resolution_time_in_minutes': \
                        value['full_resolution_time_in_minutes'],
                    'solved_at': value['solved_at'],
                    'group_stations': value['group_stations'],
                    'assignee_stations': value['assignee_stations']
                }
                articles_json['metric_sets'].append(temp_metric)

        tickets_df = pd.DataFrame.from_dict(articles_json['tickets'],
                                            orient='columns')
        users_df = pd.DataFrame.from_dict(articles_json['users'],
                                          orient='columns')
        metrics_df = pd.DataFrame.from_dict(articles_json['metric_sets'],
                                            orient='columns')

        # Take data from ticket fields endpoint
        # to transform contact reason fields
        # pylint: disable=C0121
        self.data_api_zendesk_ticket_fields = self.config.zendesk_api
        second_order_contact_reason_ids = list(
            self.data_api_zendesk_ticket_fields[ \
                self.data_api_zendesk_ticket_fields[ \
                    'contact_reason_field_2nd_order'] == True]['id'])
        third_order_contact_reason_ids = list(
            self.data_api_zendesk_ticket_fields[ \
                self.data_api_zendesk_ticket_fields[ \
                    'contact_reason_field_3rd_order'] == True]['id'])

        # Take data from tickets brands endpoint
        # to transform brand_id field
        self.data_api_zendesk_ticket_brands = self.config.zendesk_api
        tickets_df['brand_name'] = tickets_df['brand_id'] \
            .map(self.data_api_zendesk_ticket_brands)

        # Transform the rest of the fields extracted from this endpoint
        tickets_df['main_contact_reason'] = tickets_df \
            .apply((lambda x: \
                get_main_contact_value(main_contact_reason_id,
                                       x['custom_fields'])), axis=1)
        tickets_df['2nd_order_contact_reason'] = tickets_df \
            .apply((lambda x: \
                get_n_ord_contact_value(second_order_contact_reason_ids,
                                        x['custom_fields'])), axis=1)
        tickets_df['3rd_order_contact_reason'] = tickets_df \
            .apply((lambda x: \
                get_n_ord_contact_value(third_order_contact_reason_ids,
                                        x['custom_fields'])), axis=1)
        tickets_df['to_address'] = tickets_df['via'] \
            .map(lambda x: x['source']['to']['address']
                 if x['channel'] == 'email' else None)
        tickets_df['via'] = tickets_df['via']\
            .map(lambda x: x['channel'])
        metrics_df['full_resolution_time_in_minutes'] = \
            metrics_df['full_resolution_time_in_minutes'] \
                .map(lambda x: x['calendar'])
        metrics_df['reply_time_in_minutes'] = \
            metrics_df['reply_time_in_minutes'].map(lambda x: x['calendar'])

        # Drop duplicates from resulting dataframes
        tickets_df = tickets_df[['id', 'created_at', 'updated_at',
                                 'requester_id', 'assignee_id',
                                 'to_address',
                                 'via', 'subject', 'description',
                                 'main_contact_reason',
                                 '2nd_order_contact_reason',
                                 '3rd_order_contact_reason',
                                 'brand_name']].drop_duplicates()

        users_df = users_df[['id', 'email', 'phone']].drop_duplicates()

        metrics_df = metrics_df[['ticket_id', 'reopens', 'replies',
                                 'reply_time_in_minutes',
                                 'full_resolution_time_in_minutes',
                                 'solved_at', 'group_stations',
                                 'assignee_stations']].drop_duplicates()
        # Merge all dataframes
        merged_df = pd.merge(tickets_df,
                             users_df[['id', 'email', 'phone']],
                             how='inner', left_on='requester_id',
                             right_on='id')
        merged_df = pd.merge(merged_df,
                             users_df[['id', 'email']],
                             how='left',
                             left_on='assignee_id',
                             right_on='id')
        merged_df = pd.merge(merged_df,
                             metrics_df,
                             how='inner',
                             left_on='id_x',
                             right_on='ticket_id')

        merged_df.rename(columns={'email_x': 'requester_email',
                                  'email_y': 'assignee_email',},
                         inplace=True)

        merged_df_reduced = merged_df[['ticket_id', 'created_at',
                                       'updated_at', 'via',
                                       'requester_email', 'phone',
                                       'assignee_email', 'reopens',
                                       'replies', 'reply_time_in_minutes',
                                       'full_resolution_time_in_minutes',
                                       'solved_at', 'group_stations',
                                       'assignee_stations',
                                       'main_contact_reason',
                                       '2nd_order_contact_reason',
                                       '3rd_order_contact_reason',
                                       'brand_name']]
        # Format fields to persist them
        merged_df_reduced['ticket_id'] = merged_df_reduced['ticket_id'] \
            .fillna(0).astype(int)
        merged_df_reduced['reopens'] = merged_df_reduced['reopens'] \
            .fillna(0).astype(int)
        merged_df_reduced['replies'] = merged_df_reduced['replies'] \
            .fillna(0).astype(int)
        merged_df_reduced['reply_time_in_minutes'] = \
            merged_df_reduced['reply_time_in_minutes'].fillna(0).astype(int)
        merged_df_reduced['full_resolution_time_in_minutes'] = \
            merged_df_reduced['full_resolution_time_in_minutes'] \
                .fillna(0).astype(int)
        merged_df_reduced['group_stations'] = \
            merged_df_reduced['group_stations'].fillna(0).astype(int)
        merged_df_reduced['assignee_stations'] = \
            merged_df_reduced['assignee_stations'].fillna(0).astype(int)

        merged_df_reduced["updated_at"] = merged_df_reduced["updated_at"] \
            .fillna(datetime.datetime(2199, 12, 31, 0, 0, 0)).astype(str)
        merged_df_reduced["solved_at"] = merged_df_reduced["solved_at"] \
            .fillna(datetime.datetime(2199, 12, 31, 0, 0, 0)).astype(str)
        merged_df_reduced["created_at"] = merged_df_reduced["created_at"] \
            .fillna(datetime.datetime(2199, 12, 31, 0, 0, 0)).astype(str)
        self.logger.info('Ready extract and transform data from Zendesk API')

        self.__data_api_zendesk_tables = merged_df_reduced

    # Write data to data warehouse
    def save_to_zendesk_tickets(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.dwh)
        db.execute_command(query.delete_temp_zendesk_tickets_table())
        self.data_api_zendesk_tables = self.config.zendesk_api
        self.logger.info('Inserting data into Zendesk DWH output table')
        for row in self.data_api_zendesk_tables.itertuples():
            data_row = [(row.ticket_id, row.created_at, row.updated_at,
                         row.via, row.requester_email, row.phone,
                         row.assignee_email, row.reopens, row.replies,
                         row.reply_time_in_minutes,
                         row.full_resolution_time_in_minutes, row.solved_at,
                         row.group_stations, row.assignee_stations,
                         row.main_contact_reason,
                         row[15],
                         row[16],
                         row.brand_name)]
            db.insert_data(query.insert_into_temp_zendesk_tickets_table(),
                           data_row)
        db.execute_command(query.delete_zendesk_tickets_table())
        db.execute_command(query.insert_into_zendesk_tickets_table())
        self.logger.info('Ready data inserts into DWH table')
        db.close_connection()
