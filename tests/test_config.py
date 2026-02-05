"""
Tests for the config module.
"""

import pytest
import json
import os
from pathlib import Path

from lib.config import ConfigManager, ConnectionProfile


class TestConnectionProfile:
    """Tests for the ConnectionProfile dataclass."""
    
    def test_create_profile_with_defaults(self):
        """Test creating a profile with default values."""
        profile = ConnectionProfile(
            name="Test",
            host="localhost",
            port=6379
        )
        
        assert profile.name == "Test"
        assert profile.host == "localhost"
        assert profile.port == 6379
        assert profile.password == ""
        assert profile.ssl is False
    
    def test_create_profile_with_all_values(self):
        """Test creating a profile with all values specified."""
        profile = ConnectionProfile(
            name="Production",
            host="redis.example.com",
            port=10000,
            password="secret123",
            ssl=True
        )
        
        assert profile.name == "Production"
        assert profile.host == "redis.example.com"
        assert profile.port == 10000
        assert profile.password == "secret123"
        assert profile.ssl is True
    
    def test_to_dict(self):
        """Test converting profile to dictionary."""
        profile = ConnectionProfile(
            name="Test",
            host="localhost",
            port=6379,
            password="pass",
            ssl=True
        )
        
        result = profile.to_dict()
        
        assert result == {
            "name": "Test",
            "host": "localhost",
            "port": 6379,
            "password": "pass",
            "ssl": True
        }
    
    def test_from_dict(self):
        """Test creating profile from dictionary."""
        data = {
            "name": "From Dict",
            "host": "192.168.1.1",
            "port": 12000,
            "password": "mypass",
            "ssl": False
        }
        
        profile = ConnectionProfile.from_dict(data)
        
        assert profile.name == "From Dict"
        assert profile.host == "192.168.1.1"
        assert profile.port == 12000
        assert profile.password == "mypass"
        assert profile.ssl is False
    
    def test_from_dict_with_missing_values(self):
        """Test creating profile from dict with missing optional values."""
        data = {
            "name": "Minimal",
            "host": "localhost"
        }
        
        profile = ConnectionProfile.from_dict(data)
        
        assert profile.name == "Minimal"
        assert profile.host == "localhost"
        assert profile.port == 6379  # Default
        assert profile.password == ""  # Default
        assert profile.ssl is False  # Default


class TestConfigManager:
    """Tests for the ConfigManager class."""
    
    def test_load_existing_config(self, temp_config_file):
        """Test loading an existing config file."""
        manager = ConfigManager(temp_config_file)
        
        profiles = manager.get_profiles()
        
        assert "test-local" in profiles
        assert "test-remote" in profiles
        assert profiles["test-local"].name == "Test Local"
        assert profiles["test-remote"].port == 10000
    
    def test_create_default_config(self, empty_config_file):
        """Test creating a default config when file doesn't exist."""
        manager = ConfigManager(empty_config_file)
        
        # Should create default config
        assert os.path.exists(empty_config_file)
        
        profiles = manager.get_profiles()
        assert "local" in profiles
    
    def test_get_profile(self, temp_config_file):
        """Test getting a specific profile."""
        manager = ConfigManager(temp_config_file)
        
        profile = manager.get_profile("test-local")
        
        assert profile is not None
        assert profile.name == "Test Local"
        assert profile.host == "localhost"
    
    def test_get_nonexistent_profile(self, temp_config_file):
        """Test getting a profile that doesn't exist."""
        manager = ConfigManager(temp_config_file)
        
        profile = manager.get_profile("nonexistent")
        
        assert profile is None
    
    def test_add_profile(self, temp_config_file):
        """Test adding a new profile."""
        manager = ConfigManager(temp_config_file)
        
        new_profile = ConnectionProfile(
            name="New Profile",
            host="new.host.com",
            port=7000,
            password="newpass",
            ssl=True
        )
        
        manager.add_profile("new-profile", new_profile)
        
        # Verify it was added
        retrieved = manager.get_profile("new-profile")
        assert retrieved is not None
        assert retrieved.name == "New Profile"
        assert retrieved.host == "new.host.com"
        
        # Verify it was saved to file
        with open(temp_config_file, 'r') as f:
            saved_config = json.load(f)
        assert "new-profile" in saved_config["profiles"]
    
    def test_update_existing_profile(self, temp_config_file):
        """Test updating an existing profile."""
        manager = ConfigManager(temp_config_file)
        
        updated_profile = ConnectionProfile(
            name="Updated Local",
            host="127.0.0.1",
            port=6380,
            password="updated",
            ssl=False
        )
        
        manager.add_profile("test-local", updated_profile)
        
        retrieved = manager.get_profile("test-local")
        assert retrieved.name == "Updated Local"
        assert retrieved.port == 6380
    
    def test_delete_profile(self, temp_config_file):
        """Test deleting a profile."""
        manager = ConfigManager(temp_config_file)
        
        result = manager.delete_profile("test-remote")
        
        assert result is True
        assert manager.get_profile("test-remote") is None
    
    def test_delete_nonexistent_profile(self, temp_config_file):
        """Test deleting a profile that doesn't exist."""
        manager = ConfigManager(temp_config_file)
        
        result = manager.delete_profile("nonexistent")
        
        assert result is False
    
    def test_delete_active_profile_updates_active(self, temp_config_file):
        """Test that deleting the active profile updates active_profile."""
        manager = ConfigManager(temp_config_file)
        
        # Make sure test-local is active
        manager.set_active_profile("test-local")
        
        # Delete the active profile
        manager.delete_profile("test-local")
        
        # Active should now be the other profile
        active_id = manager.get_active_profile_id()
        assert active_id == "test-remote"
    
    def test_get_active_profile(self, temp_config_file):
        """Test getting the active profile."""
        manager = ConfigManager(temp_config_file)
        
        profile = manager.get_active_profile()
        
        assert profile is not None
        assert profile.name == "Test Local"
    
    def test_set_active_profile(self, temp_config_file):
        """Test setting the active profile."""
        manager = ConfigManager(temp_config_file)
        
        result = manager.set_active_profile("test-remote")
        
        assert result is True
        assert manager.get_active_profile_id() == "test-remote"
    
    def test_set_nonexistent_active_profile(self, temp_config_file):
        """Test setting an active profile that doesn't exist."""
        manager = ConfigManager(temp_config_file)
        
        result = manager.set_active_profile("nonexistent")
        
        assert result is False
        assert manager.get_active_profile_id() == "test-local"
    
    def test_get_profile_names(self, temp_config_file):
        """Test getting profile ID to name mapping."""
        manager = ConfigManager(temp_config_file)
        
        names = manager.get_profile_names()
        
        assert names == {
            "test-local": "Test Local",
            "test-remote": "Test Remote"
        }
