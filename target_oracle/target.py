"""Oracle target class."""

from __future__ import annotations

from singer_sdk.target_base import SQLTarget
from singer_sdk import typing as th

from target_oracle.sinks import OracleSink


class TargetOracle(SQLTarget):
    """Oracle target with wallet authentication support."""

    name = "target-oracle"
    config_jsonschema = th.PropertiesList(
        # Primary connection methods (choose one)
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            secret=True,
            description="Complete SQLAlchemy connection string (overrides other connection settings)",
        ),
        th.Property(
            "proxy_user",
            th.StringType,
            description="Oracle proxy user/TNS name from tnsnames.ora (recommended for wallet auth)",
        ),
        th.Property(
            "dsn",
            th.StringType,
            description="Oracle DSN connection string",
        ),
        
        # Wallet authentication
        th.Property(
            "tns_admin",
            th.StringType,
            description="Path to Oracle TNS_ADMIN directory (optional if already set in environment)",
        ),
        
        # Traditional connection (for non-wallet setups)
        th.Property(
            "username",
            th.StringType,
            secret=True,
            description="Oracle username (not needed for wallet auth)",
        ),
        th.Property(
            "password",
            th.StringType,
            secret=True,
            description="Oracle password (not needed for wallet auth)",
        ),
        th.Property(
            "host",
            th.StringType,
            description="Oracle host (not needed for wallet auth)",
        ),
        th.Property(
            "port",
            th.StringType,
            description="Oracle port (not needed for wallet auth)",
        ),
        th.Property(
            "database",
            th.StringType,
            description="Oracle database/service name (not needed for wallet auth)",
        ),
        
        # Target configuration
        th.Property(
            "target_schema",
            th.StringType,
            description="Target schema for tables (if not specified, uses default schema)",
        ),
        
        # Data type preferences
        th.Property(
            "prefer_float_over_numeric",
            th.BooleanType,
            description="Use float data type for numbers (otherwise numeric type is used)",
            default=False
        ),
        th.Property(
            "freeze_schema",
            th.BooleanType,
            description="Do not alter types of existing columns",
            default=False
        ),
        
    ).to_dict()

    default_sink_class = OracleSink


if __name__ == "__main__":
    TargetOracle.cli()