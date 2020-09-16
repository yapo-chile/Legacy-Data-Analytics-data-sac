import environ


INI_DW = environ.secrets.INISecrets.from_path_in_env("APP_DW_SECRET")
INI_SURVEYPAL_API = environ.secrets.INISecrets \
    .from_path_in_env("APP_SURVEYPAL_API_SECRET")


@environ.config(prefix="APP")
class AppConfig:
    """
    AppConfig Class representing the configuration of the application
    """

    @environ.config(prefix="DW")
    class DB_DWConfig:
        """
        DBConfig Class representing the configuration to access the database
        """
        host: str = INI_DW.secret(name="host", default=environ.var())
        port: int = INI_DW.secret(name="port", default=environ.var())
        name: str = INI_DW.secret(name="dbname", default=environ.var())
        user: str = INI_DW.secret(name="user", default=environ.var())
        password: str = INI_DW.secret(name="password", default=environ.var())

    @environ.config(prefix="SURVEYPAL")
    class SURVEYPAL_APIConfig:
        """
        SURVEYPAL_APIConfig Class representing the configuration
        to access the surveypal api
        """
        url: str = INI_SURVEYPAL_API.secret(name="urlprefix",
                                            default=environ.var())
        token: str = INI_SURVEYPAL_API.secret(name="token",
                                              default=environ.var())
    dwh = environ.group(DB_DWConfig)
    surveypal_api = environ.group(SURVEYPAL_APIConfig)


def getConf():
    return environ.to_config(AppConfig)
