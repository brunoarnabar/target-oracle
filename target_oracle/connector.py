from __future__ import annotations
from typing import Any, Dict
import sqlalchemy as sa
from singer_sdk.connectors.sql import SQLConnector

class OracleConnector(SQLConnector):
    """Connector that injects externalauth for passwordless wallet auth."""
    def create_engine(self) -> sa.Engine:
        url = self.get_sqlalchemy_url()
        kwargs: Dict[str, Any] = self.get_engine_kwargs()

        if self.config.get("externalauth", False):
            ce = kwargs.setdefault("connect_args", {})
            ce.setdefault("externalauth", True)

        return sa.create_engine(url, **kwargs)