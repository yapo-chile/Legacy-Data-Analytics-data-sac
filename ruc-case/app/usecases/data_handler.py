from __future__ import annotations


class DataHandler:

    def __init__(self, logger) -> None:
        self.logger = logger

    @property
    def input_emails(self) -> list[str]:
        return self.__input_emails

    @input_emails.setter
    def input_emails(self, input_emails: str) -> None:

        # Validate the input emails string format
        self.validate_array_like_string(input_emails)
        # Get the users ids corresponding to the supplied emails
        email_list = [str(x) for x in input_emails.split(', ')]
        self.__input_emails = email_list

    @property
    def input_phones(self):
        return self.__input_phones

    @input_phones.setter
    def input_phones(self, input_phones: str) -> None:

        # Validate the input phones string format
        self.validate_array_like_string(input_phones)
        # Parse the string
        phone_list = input_phones.split(', ')
        # Validate the corresponding number of digits in the phone numbers
        if any(len(phone) != 9 for phone in phone_list):
            raise ValueError(f"A phone number in '{phone_list}' is not valid."
                             "Phone numbers should only have 9 digits (ex: 912345678)")
        # Remove the first character
        phone_list = [phone[1:] for phone in phone_list]
        self.__input_phones = phone_list

    def clean_and_parse(self, input_emails, input_phones) -> tuple[list[str], list[str]]:

        self.input_emails = input_emails
        self.input_phones = input_phones

        return self.input_emails, self.input_phones

    @staticmethod
    def validate_array_like_string(array_like_string: str) -> None:
        if any(element in [',', ' ', ';'] for element in array_like_string.split(', ')):
            raise ValueError(
                f"Input string '{array_like_string}' format not valid."
                "if the string it's array-like, split the elements by a comma and a space ', '")

