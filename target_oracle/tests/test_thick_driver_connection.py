import os

import pytest

# Read environment variables for Oracle thick driver connection
ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_DSN = os.getenv("ORACLE_DSN")
ORACLE_SQLALCHEMY_URL = os.getenv("ORACLE_SQLALCHEMY_URL")
TNS_ADMIN = os.getenv("TNS_ADMIN")
ORACLE_LIB_DIR = os.getenv("ORACLE_LIB_DIR")

BASIC_CREDS = all([ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN])
WALLET_CREDS = all([ORACLE_SQLALCHEMY_URL, TNS_ADMIN])

pytestmark = pytest.mark.skipif(
    not (BASIC_CREDS or WALLET_CREDS),
    reason="Oracle thick driver settings not provided",
)


def test_thick_driver_connection():
    """Connect to Oracle using the thick driver and run a simple query."""
    oracledb = pytest.importorskip("oracledb", reason="oracledb package not installed")
    pytest.importorskip("sqlalchemy", reason="sqlalchemy package not installed")
    pytest.importorskip("singer_sdk", reason="singer_sdk package not installed")
    from sqlalchemy import create_engine, text
    from target_oracle.sinks import OracleConnector

    # Initialize Oracle client for thick mode
    client_kwargs = {}
    if ORACLE_LIB_DIR:
        client_kwargs["lib_dir"] = ORACLE_LIB_DIR
    if TNS_ADMIN:
        client_kwargs["config_dir"] = TNS_ADMIN

    try:
        oracledb.init_oracle_client(**client_kwargs)
    except Exception:
        pytest.skip("Oracle client libraries not available")

    if oracledb.is_thin_mode():
        pytest.skip("Oracle thick driver not enabled")

    # Build SQLAlchemy URL using the connector
    connector = OracleConnector({})
    if ORACLE_SQLALCHEMY_URL:
        config = {"sqlalchemy_url": ORACLE_SQLALCHEMY_URL}
    else:
        config = {
            "sqlalchemy_url": f"oracle+oracledb://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_DSN}"
        }

    url = connector.get_sqlalchemy_url(config)
    assert url.startswith("oracle+oracledb://")

    engine = create_engine(url)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 FROM dual")).scalar()

    assert result == 1


@pytest.mark.skipif(not WALLET_CREDS, reason="Oracle wallet settings not provided")
def test_wallet_connection():
    """Establish a thick-driver connection using an Oracle wallet."""
    oracledb = pytest.importorskip(
        "oracledb", reason="oracledb package not installed"
    )
    pytest.importorskip("sqlalchemy", reason="sqlalchemy package not installed")
    pytest.importorskip("singer_sdk", reason="singer_sdk package not installed")
    from sqlalchemy import create_engine, text
    from target_oracle.sinks import OracleConnector

    client_kwargs = {}
    if ORACLE_LIB_DIR:
        client_kwargs["lib_dir"] = ORACLE_LIB_DIR
    if TNS_ADMIN:
        client_kwargs["config_dir"] = TNS_ADMIN
    try:
        oracledb.init_oracle_client(**client_kwargs)
    except Exception:
        pytest.skip("Oracle client libraries not available")

    connector = OracleConnector({})
    wallet_path = connector._resolve_wallet_dir({"sqlalchemy_url": ORACLE_SQLALCHEMY_URL})
    assert wallet_path

    engine = create_engine(ORACLE_SQLALCHEMY_URL)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 FROM dual")).scalar()

    assert result == 1
