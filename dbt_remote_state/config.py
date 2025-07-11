from enum import Enum
from pathlib import Path
import re
from typing import List, Union
from urllib.parse import ParseResult, urlparse

from pydantic import BaseModel, Field, validator

from dbt_remote_state.clients.az_blob import AzureReferenceConfig
from dbt_remote_state.clients.dbt_cloud import DbtCloudReferenceConfig
from dbt_remote_state.clients.gcs import GCSReferenceConfig
from dbt_remote_state.clients.paradime import ParadimeReferenceConfig
from dbt_remote_state.clients.s3 import S3ReferenceConfig
from dbt_remote_state.clients.snowflake_stage import SnowflakeReferenceConfig
from dbt_remote_state.clients.dbx import DatabricksReferenceConfig


class ManifestReferenceType(str, Enum):
    """Type of ManifestReference"""

    file = "file"
    dbt_cloud = "dbt_cloud"
    paradime = "paradime"
    gcs = "gcs"
    s3 = "s3"
    azure = "azure"
    snowflake = "snowflake"
    databricks = "databricks"


class FileReferenceConfig(BaseModel):
    """Configuration for a file reference"""

    path: ParseResult

    @validator("path", pre=True, always=True)
    def default_path(cls, v, values) -> ParseResult:
        """
        Check if the provided path is a valid URL. If not, convert it into an
        absolute file path.
        """

        if isinstance(v, ParseResult):
            return v

        if bool(re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", v)):
            return urlparse(v)

        return urlparse(Path(v).absolute().as_uri())


class ManifestReference(BaseModel):
    """Reference information for a manifest to be loaded into dbt-remote-state."""

    name: str
    type: ManifestReferenceType
    config: Union[
        FileReferenceConfig,
        DbtCloudReferenceConfig,
        ParadimeReferenceConfig,
        GCSReferenceConfig,
        S3ReferenceConfig,
        AzureReferenceConfig,
        SnowflakeReferenceConfig,
        DatabricksReferenceConfig,
    ]
    excluded_packages: List[str] = Field(default_factory=list)
    optional: bool = False


class dbtRemoteStateConfig(BaseModel):
    """Configuration for dbt remote state"""

    manifests: List[ManifestReference]
    enable_telemetry: bool = False


class RemoteStateConfigurationError(BaseException):
    """Error raised when dbt-remote-state has been misconfigured."""
