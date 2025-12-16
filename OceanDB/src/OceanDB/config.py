from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):

    postgres_host: str = Field(default="localhost")
    postgres_port: int = Field(default=5432)
    postgres_username: str
    postgres_password: str
    postgres_database: str = Field(default="ocean")


    along_track_data_directory: str
    eddy_data_directory: str
    copernicus_password: str
    copernicus_username: str

    model_config = SettingsConfigDict(
        env_prefix="",                # no prefix (POSTGRES_HOST, etc.)
        env_file=".env",               # default fallback
        env_file_encoding="utf-8",
        extra="ignore",                # ignore unrelated env vars
        case_sensitive=False,
    )

    @property
    def postgres_dsn_admin(self) -> str:
        return (
            f"host={self.postgres_host} "
            # f"dbname={self.postgres_database} "
            f"port={self.postgres_port} "
            f"user={self.postgres_username} "
            f"password={self.postgres_password}"
        )

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} "
            f"dbname={self.postgres_database} "
            f"port={self.postgres_port} "
            f"user={self.postgres_username} "
            f"password={self.postgres_password}"
        )
