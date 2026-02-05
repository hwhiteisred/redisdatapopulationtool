"""
Tests for the connection module.
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from lib.connection import (
    RedisConnectionManager,
    ModuleStatus,
    get_connection_manager
)
from lib.config import ConnectionProfile


class TestModuleStatus:
    """Tests for the ModuleStatus dataclass."""
    
    def test_default_values(self):
        """Test that ModuleStatus defaults to all False."""
        status = ModuleStatus()
        
        assert status.json is False
        assert status.search is False
        assert status.timeseries is False
        assert status.bloom is False
    
    def test_to_dict(self):
        """Test converting ModuleStatus to dictionary."""
        status = ModuleStatus(json=True, search=True, timeseries=False, bloom=True)
        
        result = status.to_dict()
        
        assert result == {
            'json': True,
            'search': True,
            'timeseries': False,
            'bloom': True
        }


class TestRedisConnectionManager:
    """Tests for the RedisConnectionManager class."""
    
    def test_initial_state(self):
        """Test initial state of connection manager."""
        manager = RedisConnectionManager()
        
        assert manager.is_connected() is False
        assert manager.get_client() is None
        assert manager.get_current_profile() is None
    
    def test_get_module_status_not_connected(self):
        """Test getting module status when not connected."""
        manager = RedisConnectionManager()
        
        status = manager.get_module_status()
        
        assert status.json is False
        assert status.search is False
        assert status.timeseries is False
        assert status.bloom is False
    
    @patch('lib.connection.redis.Redis')
    @patch('lib.connection.ConnectionPool')
    def test_connect_success(self, mock_pool_class, mock_redis_class):
        """Test successful connection."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_client.module_list.return_value = []
        mock_redis_class.return_value = mock_client
        
        manager = RedisConnectionManager()
        profile = ConnectionProfile(
            name="Test",
            host="localhost",
            port=6379
        )
        
        success, message = manager.connect(profile)
        
        assert success is True
        assert "Connected" in message
        assert manager.get_current_profile() == profile
    
    @patch('lib.connection.redis.Redis')
    @patch('lib.connection.ConnectionPool')
    def test_connect_failure(self, mock_pool_class, mock_redis_class):
        """Test connection failure."""
        import redis
        mock_client = MagicMock()
        mock_client.ping.side_effect = redis.ConnectionError("Connection refused")
        mock_redis_class.return_value = mock_client
        
        manager = RedisConnectionManager()
        profile = ConnectionProfile(
            name="Test",
            host="localhost",
            port=6379
        )
        
        success, message = manager.connect(profile)
        
        assert success is False
        assert "Connection failed" in message
    
    @patch('lib.connection.redis.Redis')
    @patch('lib.connection.ConnectionPool')
    def test_connect_auth_failure(self, mock_pool_class, mock_redis_class):
        """Test authentication failure."""
        import redis
        mock_client = MagicMock()
        mock_client.ping.side_effect = redis.AuthenticationError("Invalid password")
        mock_redis_class.return_value = mock_client
        
        manager = RedisConnectionManager()
        profile = ConnectionProfile(
            name="Test",
            host="localhost",
            port=6379,
            password="wrongpass"
        )
        
        success, message = manager.connect(profile)
        
        assert success is False
        assert "Authentication failed" in message
    
    @patch('lib.connection.redis.Redis')
    @patch('lib.connection.ConnectionPool')
    def test_module_detection_all_modules(self, mock_pool_class, mock_redis_class):
        """Test module detection with all modules present."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_client.module_list.return_value = [
            {'name': 'ReJSON', 'ver': 20000},
            {'name': 'search', 'ver': 20600},
            {'name': 'timeseries', 'ver': 10800},
            {'name': 'bf', 'ver': 20400},
        ]
        mock_redis_class.return_value = mock_client
        
        manager = RedisConnectionManager()
        profile = ConnectionProfile(name="Test", host="localhost", port=6379)
        
        manager.connect(profile)
        status = manager.get_module_status()
        
        assert status.json is True
        assert status.search is True
        assert status.timeseries is True
        assert status.bloom is True
    
    @patch('lib.connection.redis.Redis')
    @patch('lib.connection.ConnectionPool')
    def test_module_detection_partial_modules(self, mock_pool_class, mock_redis_class):
        """Test module detection with only some modules present."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_client.module_list.return_value = [
            {'name': 'ReJSON', 'ver': 20000},
        ]
        mock_redis_class.return_value = mock_client
        
        manager = RedisConnectionManager()
        profile = ConnectionProfile(name="Test", host="localhost", port=6379)
        
        manager.connect(profile)
        status = manager.get_module_status()
        
        assert status.json is True
        assert status.search is False
        assert status.timeseries is False
        assert status.bloom is False
    
    @patch('lib.connection.redis.Redis')
    @patch('lib.connection.ConnectionPool')
    def test_module_detection_alternative_names(self, mock_pool_class, mock_redis_class):
        """Test module detection with alternative module names."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_client.module_list.return_value = [
            {'name': 'redisjson', 'ver': 20000},  # Alternative name
            {'name': 'ft', 'ver': 20600},  # Alternative name for search
            {'name': 'redisbloom', 'ver': 20400},  # Alternative name
        ]
        mock_redis_class.return_value = mock_client
        
        manager = RedisConnectionManager()
        profile = ConnectionProfile(name="Test", host="localhost", port=6379)
        
        manager.connect(profile)
        status = manager.get_module_status()
        
        assert status.json is True
        assert status.search is True
        assert status.bloom is True
    
    @patch('lib.connection.redis.Redis')
    @patch('lib.connection.ConnectionPool')
    def test_disconnect(self, mock_pool_class, mock_redis_class):
        """Test disconnecting."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_client.module_list.return_value = []
        mock_redis_class.return_value = mock_client
        
        manager = RedisConnectionManager()
        profile = ConnectionProfile(name="Test", host="localhost", port=6379)
        
        manager.connect(profile)
        manager.disconnect()
        
        assert manager.is_connected() is False
        assert manager.get_current_profile() is None
        mock_client.close.assert_called_once()
    
    @patch('lib.connection.redis.Redis')
    @patch('lib.connection.ConnectionPool')
    def test_get_server_info(self, mock_pool_class, mock_redis_class):
        """Test getting server information."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_client.module_list.return_value = []
        mock_client.info.return_value = {
            'redis_version': '7.2.0',
            'os': 'Linux 5.15.0',
            'connected_clients': 10,
            'used_memory_human': '50M',
            'db0': {'keys': 100, 'expires': 10},
            'db1': {'keys': 50, 'expires': 5},
        }
        mock_redis_class.return_value = mock_client
        
        manager = RedisConnectionManager()
        profile = ConnectionProfile(name="Test", host="localhost", port=6379)
        
        manager.connect(profile)
        info = manager.get_server_info()
        
        assert info['redis_version'] == '7.2.0'
        assert info['connected_clients'] == 10
        assert info['used_memory_human'] == '50M'
        assert info['total_keys'] == 150
    
    @patch('lib.connection.redis.Redis')
    @patch('lib.connection.ConnectionPool')
    def test_flush_db(self, mock_pool_class, mock_redis_class):
        """Test flushing database."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_client.module_list.return_value = []
        mock_client.flushdb.return_value = True
        mock_redis_class.return_value = mock_client
        
        manager = RedisConnectionManager()
        profile = ConnectionProfile(name="Test", host="localhost", port=6379)
        
        manager.connect(profile)
        success, message = manager.flush_db()
        
        assert success is True
        mock_client.flushdb.assert_called_once()
    
    def test_flush_db_not_connected(self):
        """Test flushing database when not connected."""
        manager = RedisConnectionManager()
        
        success, message = manager.flush_db()
        
        assert success is False
        assert "Not connected" in message


class TestGetConnectionManager:
    """Tests for the get_connection_manager function."""
    
    def test_returns_same_instance(self):
        """Test that get_connection_manager returns a singleton."""
        # Reset the global
        import lib.connection as conn_module
        conn_module._connection_manager = None
        
        manager1 = get_connection_manager()
        manager2 = get_connection_manager()
        
        assert manager1 is manager2
    
    def test_creates_instance_if_none(self):
        """Test that get_connection_manager creates an instance if none exists."""
        import lib.connection as conn_module
        conn_module._connection_manager = None
        
        manager = get_connection_manager()
        
        assert manager is not None
        assert isinstance(manager, RedisConnectionManager)
