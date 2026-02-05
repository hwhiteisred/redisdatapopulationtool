"""
Pytest configuration and shared fixtures for the test suite.
"""

import pytest
import tempfile
import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

# Try to import fakeredis for mocking
try:
    import fakeredis
    FAKEREDIS_AVAILABLE = True
except ImportError:
    FAKEREDIS_AVAILABLE = False


@pytest.fixture
def temp_config_file():
    """Create a temporary config file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        config = {
            "profiles": {
                "test-local": {
                    "name": "Test Local",
                    "host": "localhost",
                    "port": 6379,
                    "password": "",
                    "ssl": False
                },
                "test-remote": {
                    "name": "Test Remote",
                    "host": "redis.example.com",
                    "port": 10000,
                    "password": "secret",
                    "ssl": True
                }
            },
            "active_profile": "test-local"
        }
        json.dump(config, f)
        f.flush()
        yield f.name
    
    # Cleanup
    os.unlink(f.name)


@pytest.fixture
def empty_config_file():
    """Create an empty/new config file path for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        pass  # Just create the file path, don't write anything
    os.unlink(f.name)  # Delete it so ConfigManager creates it fresh
    yield f.name
    # Cleanup if it exists
    if os.path.exists(f.name):
        os.unlink(f.name)


@pytest.fixture
def mock_redis_client():
    """Create a mock Redis client for testing without a real Redis server."""
    if FAKEREDIS_AVAILABLE:
        return fakeredis.FakeRedis(decode_responses=True)
    else:
        # Fallback to MagicMock if fakeredis isn't available
        mock = MagicMock()
        mock.ping.return_value = True
        mock.set.return_value = True
        mock.hset.return_value = 1
        mock.lpush.return_value = 1
        mock.rpush.return_value = 1
        mock.sadd.return_value = 1
        mock.zadd.return_value = 1
        mock.xadd.return_value = "1234567890-0"
        mock.pfadd.return_value = 1
        mock.geoadd.return_value = 1
        mock.setbit.return_value = 0
        mock.pipeline.return_value = mock
        mock.execute.return_value = []
        mock.module_list.return_value = []
        mock.info.return_value = {
            'redis_version': '7.0.0',
            'os': 'Linux',
            'connected_clients': 1,
            'used_memory_human': '1M',
        }
        return mock


@pytest.fixture
def mock_redis_with_modules():
    """Create a mock Redis client that reports having all modules."""
    mock = MagicMock()
    mock.ping.return_value = True
    mock.module_list.return_value = [
        {'name': 'ReJSON', 'ver': 20000},
        {'name': 'search', 'ver': 20600},
        {'name': 'timeseries', 'ver': 10800},
        {'name': 'bf', 'ver': 20400},
    ]
    mock.info.return_value = {
        'redis_version': '7.2.0',
        'os': 'Linux',
        'connected_clients': 5,
        'used_memory_human': '100M',
    }
    mock.pipeline.return_value = mock
    mock.execute.return_value = []
    return mock


@pytest.fixture
def sample_connection_profile():
    """Create a sample ConnectionProfile for testing."""
    from lib.config import ConnectionProfile
    return ConnectionProfile(
        name="Test Profile",
        host="localhost",
        port=6379,
        password="testpass",
        ssl=False
    )


# Markers for tests that require specific conditions
def pytest_configure(config):
    """Configure custom pytest markers."""
    config.addinivalue_line(
        "markers", "requires_redis: mark test as requiring a real Redis connection"
    )
    config.addinivalue_line(
        "markers", "requires_modules: mark test as requiring Redis modules"
    )


# Skip tests that require real Redis if not available
def pytest_collection_modifyitems(config, items):
    """Modify test collection to skip tests based on markers."""
    skip_redis = pytest.mark.skip(reason="Requires real Redis connection")
    skip_modules = pytest.mark.skip(reason="Requires Redis modules")
    
    for item in items:
        if "requires_redis" in item.keywords:
            # Could add logic here to check if Redis is available
            pass
        if "requires_modules" in item.keywords:
            # Could add logic here to check if modules are available
            pass
