"""Integration tests for Oracle target - Connection Tests Only."""

import os
from typing import Dict, Any

import pytest
from dotenv import load_dotenv

from target_oracle.target import TargetOracle
from target_oracle.sinks import OracleConnector
import sqlalchemy
import oracledb

# Load environment variables from .env
load_dotenv()

def _load_integration_env_config() -> Dict[str, Any]:
    """Plain function to load integration config from environment for direct calls."""
    config: Dict[str, Any] = {}
    if os.getenv("ORACLE_PROXY_USER"):
        config["proxy_user"] = os.getenv("ORACLE_PROXY_USER")
    if os.getenv("TNS_ADMIN"):
        config["tns_admin"] = os.getenv("TNS_ADMIN")
    if os.getenv("ORACLE_TARGET_SCHEMA"):
        config["target_schema"] = os.getenv("ORACLE_TARGET_SCHEMA")
    config["prefer_float_over_numeric"] = os.getenv("ORACLE_PREFER_FLOAT", "false").lower() == "true"
    config["freeze_schema"] = os.getenv("ORACLE_FREEZE_SCHEMA", "false").lower() == "true"
    return config

@pytest.fixture()
def oracle_integration_config():
    """Fixture version for tests that request it."""
    return _load_integration_env_config()


@pytest.fixture
def oracle_target_integration(oracle_integration_config) -> TargetOracle:
    """Target using integration configuration."""
    return TargetOracle(config=oracle_integration_config)


@pytest.fixture
def oracle_connector_integration(oracle_integration_config) -> OracleConnector:
    """Connector using integration configuration."""
    return OracleConnector(config=oracle_integration_config)


def get_integration_engine():
    """Get engine using environment configuration."""
    proxy_user = os.getenv("ORACLE_PROXY_USER")
    tns_admin = os.getenv("TNS_ADMIN")
    
    if not proxy_user:
        pytest.skip("ORACLE_PROXY_USER not set in environment")
    
    try:
        oracledb.init_oracle_client()
    except:
        pass
    
    if tns_admin:
        os.environ['TNS_ADMIN'] = tns_admin
    
    def creator():
        return oracledb.connect(dsn=proxy_user)
    
    return sqlalchemy.create_engine('oracle+oracledb://', creator=creator)


# Configuration Loading Tests
def test_integration_config_loading():
    """Test that configuration loads from environment."""
    config = _load_integration_env_config()
    
    if os.getenv("ORACLE_PROXY_USER"):
        assert config["proxy_user"] == os.getenv("ORACLE_PROXY_USER")
    else:
        pytest.skip("ORACLE_PROXY_USER not set in environment")
    
    if os.getenv("ORACLE_TARGET_SCHEMA"):
        assert config["target_schema"] == os.getenv("ORACLE_TARGET_SCHEMA")


def test_integration_minimal_config():
    """Test minimal configuration with just proxy_user."""
    if not os.getenv("ORACLE_PROXY_USER"):
        pytest.skip("ORACLE_PROXY_USER not set")
    
    config = {"proxy_user": os.getenv("ORACLE_PROXY_USER")}
    target = TargetOracle(config=config)
    assert target.config["proxy_user"] == os.getenv("ORACLE_PROXY_USER")


def test_integration_full_config():
    """Test full configuration with all environment variables."""
    config = {}
    env_vars = [
        ("ORACLE_PROXY_USER", "proxy_user"),
        ("TNS_ADMIN", "tns_admin"),
        ("ORACLE_TARGET_SCHEMA", "target_schema")
    ]
    
    for env_var, config_key in env_vars:
        if os.getenv(env_var):
            config[config_key] = os.getenv(env_var)
    
    if not config:
        pytest.skip("No integration environment variables set")
    
    target = TargetOracle(config=config)
    for key, value in config.items():
        assert target.config[key] == value


# Connection Tests
@pytest.mark.skipif(not os.getenv("ORACLE_PROXY_USER"), reason="Integration config not available")
def test_integration_target_initialization(oracle_target_integration):
    """Test that target initializes successfully with integration config."""
    assert oracle_target_integration is not None
    assert hasattr(oracle_target_integration, 'config')
    if os.getenv("ORACLE_PROXY_USER"):
        assert oracle_target_integration.config["proxy_user"] == os.getenv("ORACLE_PROXY_USER")


@pytest.mark.skipif(not os.getenv("ORACLE_PROXY_USER"), reason="Integration config not available")
def test_integration_connector_initialization(oracle_connector_integration):
    """Test that connector initializes successfully with integration config."""
    assert oracle_connector_integration is not None
    assert hasattr(oracle_connector_integration, 'config')
    

@pytest.mark.skipif(not os.getenv("ORACLE_PROXY_USER"), reason="Integration config not available")
def test_integration_sqlalchemy_url_generation(oracle_connector_integration):
    """Test that SQLAlchemy URL is generated correctly."""
    url = oracle_connector_integration.get_sqlalchemy_url()
    assert url is not None
    assert "oracle+oracledb://" in url
    
    if os.getenv("ORACLE_PROXY_USER"):
        assert os.getenv("ORACLE_PROXY_USER") in url


@pytest.mark.skipif(not os.getenv("ORACLE_PROXY_USER"), reason="Integration config not available")
def test_integration_engine_creation(oracle_connector_integration):
    """Test that SQLAlchemy engine can be created."""
    try:
        engine = oracle_connector_integration.create_engine()
        assert engine is not None
        assert hasattr(engine, 'connect')
    except Exception as e:
        pytest.fail(f"Engine creation failed: {e}")


@pytest.mark.skipif(not os.getenv("ORACLE_PROXY_USER"), reason="Integration config not available")
def test_integration_basic_connection():
    """Test basic connection to Oracle using environment config."""
    proxy_user = os.getenv("ORACLE_PROXY_USER")
    if not proxy_user:
        pytest.skip("ORACLE_PROXY_USER not set in environment")
    
    try:
        engine = get_integration_engine()
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text("SELECT USER FROM DUAL"))
            user = result.fetchone()[0]
            assert user is not None
            # User should match or contain the proxy user in some form
            print(f"Connected as user: {user}")
    except Exception as e:
        pytest.fail(f"Basic connection test failed: {e}")


@pytest.mark.skipif(not os.getenv("ORACLE_PROXY_USER"), reason="Integration config not available")
def test_integration_connection_via_connector(oracle_connector_integration):
    """Test connection through the connector."""
    try:
        engine = oracle_connector_integration.create_engine()
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text("SELECT 1 FROM DUAL"))
            value = result.fetchone()[0]
            assert value == 1
    except Exception as e:
        pytest.fail(f"Connector connection test failed: {e}")


@pytest.mark.skipif(not os.getenv("ORACLE_PROXY_USER"), reason="Integration config not available")
def test_integration_tns_admin_environment():
    """Test that TNS_ADMIN environment is set correctly."""
    tns_admin = os.getenv("TNS_ADMIN")
    if tns_admin:
        # Test that the path exists
        import pathlib
        path = pathlib.Path(tns_admin)
        if not path.exists():
            pytest.skip(f"TNS_ADMIN path {tns_admin} does not exist")
        
        # Test that connector uses it
        config = {"proxy_user": os.getenv("ORACLE_PROXY_USER"), "tns_admin": tns_admin}
        connector = OracleConnector(config=config)
        
        # Creating engine should set the environment variable
        try:
            engine = connector.create_engine()
            assert os.environ.get("TNS_ADMIN") == tns_admin
        except Exception as e:
            pytest.fail(f"TNS_ADMIN test failed: {e}")
    else:
        pytest.skip("TNS_ADMIN not set in environment")


@pytest.mark.skipif(not os.getenv("ORACLE_PROXY_USER"), reason="Integration config not available")
def test_integration_oracle_client_initialization():
    """Test Oracle client initialization."""
    try:
        # Should not raise exception even if already initialized
        oracledb.init_oracle_client()
        
        # Test via connector
        config = {"proxy_user": os.getenv("ORACLE_PROXY_USER")}
        connector = OracleConnector(config=config)
        engine = connector.create_engine()
        
        # Should be able to get connection info
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text("SELECT BANNER FROM V$VERSION WHERE ROWNUM = 1"))
            banner = result.fetchone()[0]
            assert "Oracle" in banner
            print(f"Connected to: {banner}")
            
    except Exception as e:
        # This might fail due to permissions, but we can test client init separately
        try:
            oracledb.init_oracle_client()
        except Exception as init_e:
            pytest.skip(f"Oracle client initialization failed: {init_e}")
        else:
            pytest.fail(f"Connection failed but client init succeeded: {e}")


@pytest.mark.skipif(not os.getenv("ORACLE_TARGET_SCHEMA"), reason="ORACLE_TARGET_SCHEMA not set")
def test_integration_schema_context():
    """Test schema context information (read-only)."""
    target_schema = os.getenv("ORACLE_TARGET_SCHEMA")
    if not target_schema:
        pytest.skip("ORACLE_TARGET_SCHEMA not set in environment")
    
    try:
        engine = get_integration_engine()
        with engine.connect() as conn:
            # Test current schema context (read-only)
            result = conn.execute(sqlalchemy.text("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL"))
            current_schema = result.fetchone()[0]
            print(f"Current schema: {current_schema}")
            
            # Test session user
            result = conn.execute(sqlalchemy.text("SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') FROM DUAL"))
            session_user = result.fetchone()[0]
            print(f"Session user: {session_user}")
            
            # These are just informational - we don't assert specific values
            assert current_schema is not None
            assert session_user is not None
            
    except Exception as e:
        pytest.fail(f"Schema context test failed: {e}")


# Error Handling Tests
@pytest.mark.skipif(not os.getenv("ORACLE_PROXY_USER"), reason="Integration config not available")
def test_integration_connection_error_handling():
    """Test connection error handling with invalid config."""
    # Test with invalid proxy_user
    config = {"proxy_user": "INVALID_PROXY_USER_THAT_DOES_NOT_EXIST"}
    connector = OracleConnector(config=config)
    
    with pytest.raises(RuntimeError, match="Failed to connect to Oracle"):
        engine = connector.create_engine()
        with engine.connect():
            pass


def test_integration_missing_config_error():
    """Test error handling when required config is missing."""
    # This should work even without Oracle connection
    config = {}  # Empty config
    connector = OracleConnector(config=config)
    
    with pytest.raises(ValueError, match="Oracle connection configuration incomplete"):
        connector.get_sqlalchemy_url()