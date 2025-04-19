from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, Optional, TypeVar
from databricks.sdk.core import Config
from utils.__about__ import __version__
from databricks.sdk import WorkspaceClient


@dataclass
class ConnectConfig:
    # Keep all the fields in sync with databricks.sdk.core.Config
    host: Optional[str] = None
    account_id: Optional[str] = None
    token: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    azure_client_id: Optional[str] = None
    azure_tenant_id: Optional[str] = None
    azure_client_secret: Optional[str] = None
    azure_environment: Optional[str] = None
    cluster_id: Optional[str] = None
    profile: Optional[str] = None
    debug_headers: bool = False
    rate_limit: int = None
    max_connections_per_pool: int = None
    max_connection_pools: int = None

    @staticmethod
    def from_databricks_config(cfg: Config) -> "ConnectConfig":
        return ConnectConfig(
            host=cfg.host,
            token=cfg.token,
            client_id=cfg.client_id,
            client_secret=cfg.client_secret,
            azure_client_id=cfg.azure_client_id,
            azure_tenant_id=cfg.azure_tenant_id,
            azure_client_secret=cfg.azure_client_secret,
            azure_environment=cfg.azure_environment,
            cluster_id=cfg.cluster_id,
            profile=cfg.profile,
            debug_headers=cfg.debug_headers,
            rate_limit=cfg.rate_limit,
            max_connection_pools=cfg.max_connection_pools,
            max_connections_per_pool=cfg.max_connections_per_pool,
        )

    def to_databricks_config(self):
        return Config(
            host=self.host,
            account_id=self.account_id,
            token=self.token,
            client_id=self.client_id,
            client_secret=self.client_secret,
            azure_client_id=self.azure_client_id,
            azure_tenant_id=self.azure_tenant_id,
            azure_client_secret=self.azure_client_secret,
            azure_environment=self.azure_environment,
            cluster_id=self.cluster_id,
            profile=self.profile,
            debug_headers=self.debug_headers,
            rate_limit=self.rate_limit,
            max_connection_pools=self.max_connection_pools,
            max_connections_per_pool=self.max_connections_per_pool,
            product="dlt-meta",
            product_version=__version__,
        )

    @classmethod
    def from_dict(cls, raw: dict):
        return cls(**raw)


# Used to set the right expectation about configuration file schema
_CONFIG_VERSION = 1

T = TypeVar("T")


class _Config(Generic[T]):
    connect: Optional[ConnectConfig] = None

    @classmethod
    @abstractmethod
    def from_dict(cls, raw: dict) -> T:
        ...

    @classmethod
    def from_bytes(cls, raw: str) -> T:
        import json
        raw = json.loads(raw)
        return cls.from_dict({} if not raw else raw)

    @classmethod
    def from_file(cls, config_file: Path) -> T:
        return cls.from_bytes(config_file.read_text())

    @classmethod
    def _verify_version(cls, raw):
        stored_version = raw.pop("version", None)
        if stored_version != _CONFIG_VERSION:
            msg = (
                f"Unsupported config version: {stored_version}. "
                f"v{__version__} expects config version to be {_CONFIG_VERSION}"
            )
            raise ValueError(msg)

    def __post_init__(self):
        if self.connect is None:
            self.connect = ConnectConfig()

    def to_databricks_config(self) -> Config:
        connect = self.connect
        if connect is None:
            # default empty config
            connect = ConnectConfig()
        return connect.to_databricks_config()

    def as_dict(self) -> dict[str, Any]:
        from dataclasses import fields, is_dataclass

        def inner(x):
            if is_dataclass(x):
                result = []
                for f in fields(x):
                    value = inner(getattr(x, f.name))
                    if not value:
                        continue
                    result.append((f.name, value))
                return dict(result)
            return x

        serialized = inner(self)
        serialized["version"] = _CONFIG_VERSION
        return serialized


@dataclass
class WorkspaceConfig(_Config["WorkspaceConfig"]):
    dbr_version: str
    cloud_provider_name: str
    dbfs_path: str
    dlt_meta_operation: str
    onboarding_file_path: str
    uc_enabled: bool
    uc_catalog_name: str
    dlt_meta_schema: str
    bronze_dataflow_spec_table: str
    bronze_dataflow_spec_path: str
    silver_dataflow_spec_table: str
    silver_dataflow_spec_path: str
    overwrite_dataflow_spec: bool
    dataflow_spec_version: str
    bronze_schema: str
    silver_schema: str
    dlt_meta_layer: str
    dlt_meta_onboard_group: str
    serverless: bool
    num_workers: int

    @classmethod
    def from_dict(cls, raw: dict):
        cls._verify_version(raw)
        connect = ConnectConfig.from_dict(raw.pop("connect", {}))
        return cls(connect=connect, **raw)

    def to_workspace_client(self) -> WorkspaceClient:
        return WorkspaceClient(config=self.to_databricks_config())
