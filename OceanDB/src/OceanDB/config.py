import os
from dotenv import load_dotenv
from pathlib import Path


class Config:
    def __init__(self, env_file: str | None = None):
        """
        Initialize configuration.

        Either pass in a file path to an env_file, or maybe support a dictionary

        Parameters
        ----------
        env_file : str | None
            Optional path to a .env file. If not provided, will try:
            1. ENV_PATH env var
            2. cwd/.env
            3. HOME/.env
        """

        # Determine env path
        if env_file:
            env_path = Path(env_file)
        else:
            env_path = (
                Path(os.getenv("ENV_PATH", ""))
                if os.getenv("ENV_PATH") else None
            )

            if env_path is None or not env_path.exists():
                cwd_env = Path.cwd() / ".env"
                home_env = Path.home() / ".env"

                if cwd_env.exists():
                    env_path = cwd_env
                elif home_env.exists():
                    env_path = home_env
                else:
                    raise FileNotFoundError(
                        "No .env file found. Provide env_file or set ENV_PATH."
                    )

        # Load environment variables
        load_dotenv(env_path, override=False)

        # Store config values
        self.POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
        self.POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
        self.POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME", "postgres")
        self.POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
        self.POSTGRES_DATABASE = os.getenv("DB_NAME", "ocean")

        self.COPERNICUS_USERNAME = os.getenv("COPERNICUS_USERNAME")
        self.COPERNICUS_PASSWORD = os.getenv("COPERNICUS_PASSWORD")
        self.ALONG_TRACK_DATA_DIRECTORY = os.getenv("ALONG_TRACK_DATA_DIRECTORY")
        self.EDDIES_DATA_DIRECTORY = os.getenv("EDDIES_DATA_DIRECTORY")

    def connect_string(self):
        """
        Postgres connection string
        """
        conn_str = (
            f"host={self.POSTGRES_HOST} "
            f"dbname={self.POSTGRES_DATABASE} "
            f"port={self.POSTGRES_PORT} "
            f"user={self.POSTGRES_USERNAME} "
            f"password={self.POSTGRES_PASSWORD}"
        )
        return conn_str
