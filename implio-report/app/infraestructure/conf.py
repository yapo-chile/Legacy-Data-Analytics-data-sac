import environ

INI_BLOCKET = environ.secrets.INISecrets.from_path_in_env("APP_BLOCKET_SECRET")


@environ.config(prefix="APP")
class AppConfig:
    """
    AppConfig Class representing the configuration of the application
    """

    @environ.config(prefix="BLOCKET")
    class BlocketConfig:
        """
        DBConfig Class representing the configuration to access the Blocket database
        """
        host: str = INI_BLOCKET.secret(name="host", default=environ.var())
        port: int = INI_BLOCKET.secret(name="port", default=environ.var())
        name: str = INI_BLOCKET.secret(name="dbname", default=environ.var())
        user: str = INI_BLOCKET.secret(name="user", default=environ.var())
        password: str = INI_BLOCKET.secret(name="password", default=environ.var())

    blocket = environ.group(BlocketConfig)


def getConf():
    return environ.to_config(AppConfig)
