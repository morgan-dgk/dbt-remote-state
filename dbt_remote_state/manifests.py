from io import BytesIO
import json
import gzip
import os
from pathlib import Path
from typing import Dict
from urllib.parse import unquote, urlunparse

import requests

from dbt_remote_state.clients.snowflake_stage import (
    SnowflakeReferenceConfig,
    SnowflakeClient,
)

try:
    from dbt.artifacts.resources.types import NodeType
except ModuleNotFoundError:
    pass  # type: ignore

from dbt_remote_state.clients.az_blob import AzureClient, AzureReferenceConfig
from dbt_remote_state.clients.dbt_cloud import DbtCloud, DbtCloudReferenceConfig
from dbt_remote_state.clients.paradime import ParadimeClient, ParadimeReferenceConfig
from dbt_remote_state.clients.gcs import GCSClient, GCSReferenceConfig
from dbt_remote_state.clients.s3 import S3Client, S3ReferenceConfig
from dbt_remote_state.clients.dbx import DatabricksClient, DatabricksReferenceConfig
from dbt_remote_state.config import (
    FileReferenceConfig,
    RemoteStateConfigurationError,
    ManifestReference,
    ManifestReferenceType,
)


class UnknownManifestPathType(Exception):
    """Raised when the ManifestLoader receives a FileReferenceConfig with a path that does not have a known URL scheme."""


class InvalidManifestPath(Exception):
    """Raised when the ManifestLoader receives a FileReferenceConfig with an invalid path."""


class ManifestLoader:
    def __init__(self):
        self.loading_functions = {
            ManifestReferenceType.file: self.load_from_path,
            ManifestReferenceType.dbt_cloud: self.load_from_dbt_cloud,
            ManifestReferenceType.gcs: self.load_from_gcs,
            ManifestReferenceType.s3: self.load_from_s3,
            ManifestReferenceType.azure: self.load_from_azure,
            ManifestReferenceType.snowflake: self.load_from_snowflake,
            ManifestReferenceType.paradime: self.load_from_paradime,
            ManifestReferenceType.databricks: self.load_from_databricks,
        }

    @staticmethod
    def load_from_path(config: FileReferenceConfig) -> Dict:
        """
        Load a manifest dictionary based on a FileReferenceConfig. This config's
        path can point to either a local file or a URL to a remote location.
        """

        if config.path.scheme in ("http", "https"):
            return ManifestLoader.load_from_http(config)

        if config.path.scheme in ("file"):
            return ManifestLoader.load_from_local_filesystem(config)

        raise UnknownManifestPathType()

    @staticmethod
    def load_from_local_filesystem(config: FileReferenceConfig) -> Dict:
        """Load a manifest dictionary from a local file"""

        if not config.path.path:
            raise InvalidManifestPath()

        if config.path.netloc:
            file_path = Path(f"//{config.path.netloc}{config.path.path}")
        else:
            file_path = Path(
                unquote(
                    config.path.path.lstrip("/")
                    if os.name == "nt"
                    else config.path.path
                )
            )

        if not file_path.exists():
            raise RemoteStateConfigurationError(
                f"The path `{file_path}` does not exist."
            )

        if file_path.suffix == ".gz":
            with gzip.open(file_path, "rt") as file:
                return json.load(file)

        return json.load(open(file_path))

    @staticmethod
    def load_from_http(config: FileReferenceConfig) -> Dict:
        """Load a manifest dictionary from a local file"""

        if not config.path.path:
            raise InvalidManifestPath()

        response = requests.get(urlunparse(config.path), stream=True)
        response.raise_for_status()  # Check for request errors

        # Check for compression on the file. If compressed, store it in a buffer
        # and decompress it.
        if (
            config.path.path.endswith(".gz")
            or response.headers.get("Content-Encoding") == "gzip"
        ):
            with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz_file:
                return json.load(gz_file)

        return response.json()

    @staticmethod
    def load_from_dbt_cloud(config: DbtCloudReferenceConfig) -> Dict:
        """Load a manifest dictionary from dbt Cloud."""
        client = DbtCloud(
            account_id=config.account_id, api_endpoint=config.api_endpoint
        )

        return client.get_models(config.job_id, step=config.step)

    @staticmethod
    def load_from_gcs(config: GCSReferenceConfig) -> Dict:
        """Load a manifest dictionary from a GCS bucket."""
        gcs_client = GCSClient(
            project_id=config.project_id,
            bucket_name=config.bucket_name,
            object_name=config.object_name,
            credentials=config.credentials,
        )

        return gcs_client.load_manifest()

    @staticmethod
    def load_from_s3(config: S3ReferenceConfig) -> Dict:
        """Load a manifest dictionary from an S3-compatible bucket."""
        gcs_client = S3Client(
            bucket_name=config.bucket_name,
            object_name=config.object_name,
        )

        return gcs_client.load_manifest()

    @staticmethod
    def load_from_azure(config: AzureReferenceConfig) -> Dict:
        """Load a manifest dictionary from Azure storage."""
        azure_client = AzureClient(
            container_name=config.container_name,
            object_name=config.object_name,
            account_name=config.account_name,
        )

        return azure_client.load_manifest()

    @staticmethod
    def load_from_snowflake(config: SnowflakeReferenceConfig) -> Dict:
        """Load a manifest dictionary from Snowflake stage."""
        snowflake_client = SnowflakeClient(
            stage=config.stage, stage_path=config.stage_path
        )

        return snowflake_client.load_manifest()

    @staticmethod
    def load_from_paradime(config: ParadimeReferenceConfig) -> Dict:
        """Load a manifest dictionary from Paradime."""
        paradime_client = ParadimeClient(
            schedule_name=config.schedule_name,
            api_key=config.api_key,
            api_secret=config.api_secret,
            api_endpoint=config.api_endpoint,
            command_index=config.command_index,
        )
        return paradime_client.load_manifest()

    @staticmethod
    def load_from_databricks(config: DatabricksReferenceConfig) -> Dict:
        """Load a manifest dictionary from Databricks."""
        databricks_client = DatabricksClient(path=config.path)
        return databricks_client.load_manifest()

    def load(self, manifest_reference: ManifestReference) -> Dict:
        """Load a manifest dictionary based on a ManifestReference input."""

        if manifest_reference.type not in self.loading_functions:
            raise RemoteStateConfigurationError(
                f"The manifest reference provided for {manifest_reference.name} does "
                "not have a valid type."
            )

        try:
            manifest = self.loading_functions[manifest_reference.type](
                manifest_reference.config
            )
        except RemoteStateConfigurationError:
            if getattr(manifest_reference, "optional", False):
                return None  # type: ignore
            raise

        return manifest
