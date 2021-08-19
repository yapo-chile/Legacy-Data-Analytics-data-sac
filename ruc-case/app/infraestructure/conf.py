import environ


INI_PULSE = environ.secrets.INISecrets.from_path_in_env("APP_PULSE_SECRET")
INI_DB = environ.secrets.INISecrets.from_path_in_env("APP_DB_SECRET")


@environ.config(prefix="APP")
class AppConfig:
    """
    AppConfig Class representing the configuration of the application
    """

    @environ.config(prefix="GSheets")
    class GoogleSheetsConfig:
        """
        Class representing the configuration to access the Google Sheets for the request's form responses
        """
        sheet_id: str = '1bdwa0t4WIkikb_Bd5_A9qGTX5pD4F5O_wY_Rv7GHPi8'
        sheet_name: str = 'responses'

    @environ.config(prefix="DB")
    class DBConfig:
        """
        DBConfig Class representing the configuration to access the database
        """
        host: str = INI_DB.secret(name="host", default=environ.var())
        port: int = INI_DB.secret(name="port", default=environ.var())
        name: str = INI_DB.secret(name="dbname", default=environ.var())
        user: str = INI_DB.secret(name="user", default=environ.var())
        password: str = INI_DB.secret(name="password", default=environ.var())

    gsheets = environ.group(GoogleSheetsConfig)
    db = environ.group(DBConfig)


def getConf():
    return environ.to_config(AppConfig)


