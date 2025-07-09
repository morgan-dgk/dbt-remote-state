from dataclasses import dataclass
import os
import re
from pathlib import Path
from typing import Callable, Optional

from dbt.contracts.graph.manifest import Manifest
from dbt.plugins.contracts import PluginArtifacts
import yaml

from dbt.plugins.manager import dbt_hook, dbtPlugin

try:
    from dbt.artifacts.resources.types import NodeType
except ModuleNotFoundError:
    from dbt.node_types import NodeType  # type: ignore


from dbt_remote_state.config import dbtRemoteStateConfig
from dbt_remote_state.logging import fire_event
from dbt_remote_state.manifests import ManifestLoader

import importlib.metadata


class dbtRemoteState(dbtPlugin):
    """
    dbtLoom is a dbt plugin that loads manifest files, parses a DAG from the manifest,
    and injects public nodes from imported manifest.
    """

    def __init__(self, project_name: str):
        # Log the version of dbt-loom being initialized
        fire_event(
            msg=f'Initializing dbt-loom={importlib.metadata.version("dbt-remote-state")}'
        )

        configuration_path = Path(
            os.environ.get("DBT_REMOTE_STATE_CONFIG", "dbt_remote_state.config.yml")
        )

        self._manifest_loader = ManifestLoader()

        self.config: Optional[dbtRemoteStateConfig] = self.read_config(
            configuration_path
        )

        if not self.config or (self.config and not self.config.enable_telemetry):
            self._patch_plugin_telemetry()

        super().__init__(project_name)

    def _patch_plugin_telemetry(self) -> None:
        """Patch the plugin telemetry function to prevent tracking of dbt plugins."""
        import dbt.tracking

        dbt.tracking.track = self.tracking_wrapper(dbt.tracking.track)

    def tracking_wrapper(self, function) -> Callable:
        """Wrap the telemetry `track` function and return early if we're tracking plugin actions."""

        def outer_function(*args, **kwargs):
            """Check the context of the snowplow tracker message for references to loom. Return if present."""

            if any(
                [
                    self.__class__.__name__ in str(context_item.__dict__)
                    or "dbt-remote-state" in str(context_item.__dict__)
                    or "dbt_remote_state" in str(context_item.__dict__)
                    for context_item in kwargs.get("context", [])
                ]
            ):
                return

            return function(*args, **kwargs)

        return outer_function

    def read_config(self, path: Path) -> Optional[dbtRemoteStateConfig]:
        """Read the dbt-loom configuration file."""
        if not path.exists():
            fire_event(msg=f"dbt-loom: Config file `{path}` does not exist")
            return None

        with open(path) as file:
            config_content = file.read()

        config_content = self.replace_env_variables(config_content)

        return dbtRemoteStateConfig(**yaml.load(config_content, yaml.SafeLoader))

    @staticmethod
    def replace_env_variables(config_str: str) -> str:
        """Replace environment variable placeholders in the configuration string."""
        pattern = r"\$(\w+)|\$\{([^}]+)\}"
        return re.sub(
            pattern,
            lambda match: os.environ.get(
                match.group(1) if match.group(1) is not None else match.group(2), ""
            ),
            config_str,
        )

    def initialize(self) -> None:
        """Initialize the plugin"""

        if not self.config:
            return

        ...

    @dbt_hook
    def get_manifest_artifacts(self, manifest: Manifest) -> PluginArtifacts:
        """
        Inject PluginNodes to dbt for injection into dbt's DAG.
        """
        fire_event(msg="dbt-loom: Injecting nodes")
        return {"./path_to_artifacts": {}}  # type: ignore


plugins = [dbtRemoteState]
