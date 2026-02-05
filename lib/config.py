"""
Configuration management for Redis Data Population Tool.
Handles loading, saving, and managing multiple Redis connection profiles.
"""

import json
import os
from typing import Dict, Optional, Any
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class ConnectionProfile:
    """Represents a Redis connection profile."""
    name: str
    host: str
    port: int
    password: str = ""
    ssl: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConnectionProfile":
        return cls(
            name=data.get("name", "Unnamed"),
            host=data.get("host", "localhost"),
            port=data.get("port", 6379),
            password=data.get("password", ""),
            ssl=data.get("ssl", False),
        )


class ConfigManager:
    """Manages configuration file operations for Redis profiles."""
    
    DEFAULT_CONFIG = {
        "profiles": {
            "local": {
                "name": "Local Redis",
                "host": "localhost",
                "port": 6379,
                "password": "",
                "ssl": False
            }
        },
        "active_profile": "local"
    }
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the config manager.
        
        Args:
            config_path: Path to the config file. Defaults to config.json in the app directory.
        """
        if config_path is None:
            # Default to config.json in the same directory as this file's parent
            self.config_path = Path(__file__).parent.parent / "config.json"
        else:
            self.config_path = Path(config_path)
        
        self._config: Dict[str, Any] = {}
        self.load()
    
    def load(self) -> None:
        """Load configuration from file, creating default if it doesn't exist."""
        if self.config_path.exists():
            try:
                with open(self.config_path, 'r') as f:
                    self._config = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Could not load config file: {e}. Using defaults.")
                self._config = self.DEFAULT_CONFIG.copy()
        else:
            self._config = self.DEFAULT_CONFIG.copy()
            self.save()
    
    def save(self) -> None:
        """Save current configuration to file."""
        try:
            with open(self.config_path, 'w') as f:
                json.dump(self._config, f, indent=2)
        except IOError as e:
            print(f"Error saving config: {e}")
    
    def get_profiles(self) -> Dict[str, ConnectionProfile]:
        """Get all connection profiles."""
        profiles = {}
        for profile_id, profile_data in self._config.get("profiles", {}).items():
            profiles[profile_id] = ConnectionProfile.from_dict(profile_data)
        return profiles
    
    def get_profile(self, profile_id: str) -> Optional[ConnectionProfile]:
        """Get a specific profile by ID."""
        profiles = self._config.get("profiles", {})
        if profile_id in profiles:
            return ConnectionProfile.from_dict(profiles[profile_id])
        return None
    
    def add_profile(self, profile_id: str, profile: ConnectionProfile) -> None:
        """Add or update a profile."""
        if "profiles" not in self._config:
            self._config["profiles"] = {}
        self._config["profiles"][profile_id] = profile.to_dict()
        self.save()
    
    def delete_profile(self, profile_id: str) -> bool:
        """Delete a profile. Returns True if successful."""
        if profile_id in self._config.get("profiles", {}):
            del self._config["profiles"][profile_id]
            # If we deleted the active profile, reset to first available or None
            if self._config.get("active_profile") == profile_id:
                remaining = list(self._config.get("profiles", {}).keys())
                self._config["active_profile"] = remaining[0] if remaining else None
            self.save()
            return True
        return False
    
    def get_active_profile_id(self) -> Optional[str]:
        """Get the ID of the currently active profile."""
        return self._config.get("active_profile")
    
    def set_active_profile(self, profile_id: str) -> bool:
        """Set the active profile. Returns True if the profile exists."""
        if profile_id in self._config.get("profiles", {}):
            self._config["active_profile"] = profile_id
            self.save()
            return True
        return False
    
    def get_active_profile(self) -> Optional[ConnectionProfile]:
        """Get the currently active connection profile."""
        active_id = self.get_active_profile_id()
        if active_id:
            return self.get_profile(active_id)
        return None
    
    def get_profile_names(self) -> Dict[str, str]:
        """Get a mapping of profile IDs to display names."""
        return {
            profile_id: profile_data.get("name", profile_id)
            for profile_id, profile_data in self._config.get("profiles", {}).items()
        }
