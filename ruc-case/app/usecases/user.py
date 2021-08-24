from __future__ import annotations
from infraestructure.psql import Database
from utils.query import UserQuery


class User(UserQuery):

    def __init__(self, logger, config):
        self.logger = logger
        self.config = config

    def get_users_by_emails(self, emails: list) -> list[int]:

        db_source = Database(conf=self.config.blocket)
        users_df = db_source.select_to_dict(query=self.query_users_by_emails(emails))
        db_source.close_connection()

        users_list = users_df['user_id'].tolist()
        if not users_list:
            self.logger.warn(f"No user_id associated with the email(s) {emails}")

        return users_list

    def get_phones_by_users(self, user_ids: list, blocket_schemas: list) -> list[str]:
        """
        Get the phone numbers associated with the user ids provided

        """
        db_source = Database(conf=self.config.blocket)
        phones = []
        for schema in blocket_schemas:
            phones_df = db_source.select_to_dict(query=self.query_phones(user_ids, schema))
            if not phones_df.empty:
                phones += phones_df['phone'].tolist()

        db_source.close_connection()
        # Remove duplicated results
        phones = list(set(phones))

        return phones

    def get_users_by_phones(self, phones: list, blocket_schemas: list) -> list[int]:

        # Search the user ids by the results the known phone numbers
        # Get the user ids
        db_source = Database(conf=self.config.blocket)
        users = []
        for schema in blocket_schemas:
            users_df = db_source.select_to_dict(query=self.query_users(phones, schema))
            if not users_df.empty:
                users += users_df['user_id'].tolist()

        db_source.close_connection()
        # Remove duplicated results
        users = list(set(users))

        return users

    def iterative_user_search(self, users: list, phones: list, generic_phones: list,
                              blocket_schemas: list, early_stop) -> list[int]:

        iteration = 1
        phones_found = phones
        users_found = users
        new_users = users

        while iteration < early_stop:

            self.logger.info(f'Starting iteration number {iteration}')
            # Extract user_ids from phone numbers and add to the found list
            new_phones = self.get_phones_by_users(new_users, blocket_schemas)
            # Remove generic phones and phones found before
            new_phones = [phone for phone in new_phones
                          if phone not in generic_phones + phones_found]
            self.logger.info(f'New phone numbers found: {new_phones}')
            # Add new phones to the found phones found variable
            phones_found += new_phones
            # Search at the first iteration only for every phone found yet
            if iteration == 1:
                new_phones = phones_found
            if not new_phones:
                break
            # Extract user ids from phones number
            new_users = self.get_users_by_phones(new_phones, blocket_schemas)
            # Remove and users found before
            new_users = [user for user in new_users
                         if user not in users_found]
            if not new_users:
                break
            else:
                users_found += new_users
                iteration += 1
                self.logger.info(f'New users found: {new_users}')
        self.logger.info('Loop finished')

        return users_found

    def search(self, emails: list, phones: list, generic_phones: list,
               blocket_schemas: list, early_stop: int = 5) -> list[int]:

        # Remove the generic phone from the input list
        filtered_phones = [phone for phone in phones if phone not in generic_phones]
        self.logger.info(f'Phone numbers after filter the generic ones: {filtered_phones}')

        users = self.get_users_by_emails(emails)
        self.logger.info(f'Users from input emails: {users}')

        users_found = self.iterative_user_search(users, filtered_phones, generic_phones,
                                                 blocket_schemas, early_stop)
        return users_found

