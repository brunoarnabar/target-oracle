import pytest

from target_oracle.sinks import OracleConnector


def test_wallet_url_construction():
    """Ensure wallet parameters are included in generated URL."""
    connector = OracleConnector({})
    config = {
        "host": "dbhost",
        "port": "1521",
        "database": "XE",
        "wallet_location": "/path/to/wallet",
        "config_dir": "/path/to/wallet",
    }
    url = connector.get_sqlalchemy_url(config)
    assert url.startswith("oracle+oracledb://")
    assert "wallet_location=%2Fpath%2Fto%2Fwallet" in url
    assert "config_dir=%2Fpath%2Fto%2Fwallet" in url
