"""
Redis connection management with connection pooling and module detection.
"""

import redis
from redis import ConnectionPool
from typing import Dict, Optional, Any, Tuple
from dataclasses import dataclass
import ipaddress

from .config import ConnectionProfile


def _is_private_ip(host: str) -> bool:
    """Return True if host is a private IP (10.x, 172.16-31.x, 192.168.x) or localhost."""
    if host in ("localhost", "127.0.0.1", "::1"):
        return True
    try:
        ip = ipaddress.ip_address(host)
        return ip.is_private
    except ValueError:
        return False  # hostname, not an IP


def _connection_error_hint(host: str, port: int, exc: Exception) -> str:
    """Build a user-friendly error message with hints for common failure reasons."""
    base = str(exc).strip() or "Connection failed"
    if not base.endswith("."):
        base += "."
    hints = []
    try:
        ip = ipaddress.ip_address(host)
        if ip.is_private and host not in ("localhost", "127.0.0.1"):
            hints.append(
                f" {host} is a private/VPC address — ensure this machine can reach it "
                "(same network, VPN, or SSH tunnel to the instance)."
            )
    except ValueError:
        pass
    if "timed out" in base.lower() or "timeout" in base.lower():
        hints.append(" Try a longer timeout or check firewall/security groups allow port {}.".format(port))
    if "refused" in base.lower() or "rejected" in base.lower() or "connection refused" in base.lower():
        hints.append(
            " The server is rejecting the connection. On the Redis host, check: (1) Redis is bound to 0.0.0.0 "
            "(or to this IP), not only 127.0.0.1 — e.g. in redis.conf set bind 0.0.0.0 and restart Redis; "
            "(2) protected-mode allows your client (set a password with requirepass, or ensure the client IP is trusted); "
            "(3) a local firewall allows inbound port {}.".format(port)
        )
    if hints:
        base += "".join(hints)
    return base


@dataclass
class ModuleStatus:
    """Status of Redis modules availability."""
    json: bool = False
    search: bool = False
    timeseries: bool = False
    bloom: bool = False
    
    def to_dict(self) -> Dict[str, bool]:
        return {
            'json': self.json,
            'search': self.search,
            'timeseries': self.timeseries,
            'bloom': self.bloom,
        }


class RedisConnectionManager:
    """Manages Redis connections with pooling and module detection."""
    
    def __init__(self):
        self._pool: Optional[ConnectionPool] = None
        self._client: Optional[redis.Redis] = None
        self._current_profile: Optional[ConnectionProfile] = None
        self._module_status: Optional[ModuleStatus] = None
    
    def connect(self, profile: ConnectionProfile) -> Tuple[bool, str]:
        """
        Connect to Redis using the given profile.
        
        Args:
            profile: Connection profile with host, port, password, etc.
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            # Close existing connection if any
            self.disconnect()
            
            # Use longer timeouts for remote hosts (private IPs / VPC often need more time)
            connect_timeout = 25 if _is_private_ip(profile.host) or profile.host not in ("localhost", "127.0.0.1") else 10
            pool_kwargs = {
                'host': profile.host,
                'port': profile.port,
                'decode_responses': True,
                'socket_timeout': max(connect_timeout, 15),
                'socket_connect_timeout': connect_timeout,
            }
            
            if profile.password:
                pool_kwargs['password'] = profile.password
            
            if profile.ssl:
                pool_kwargs['ssl'] = True
                pool_kwargs['ssl_cert_reqs'] = None  # Skip certificate verification for self-signed certs
            
            # Create connection pool
            self._pool = ConnectionPool(**pool_kwargs)
            
            # Create client and test connection
            self._client = redis.Redis(connection_pool=self._pool)
            self._client.ping()
            
            self._current_profile = profile
            
            # Detect modules
            self._module_status = self._detect_modules()
            
            return True, f"Connected to {profile.host}:{profile.port}"
            
        except redis.AuthenticationError as e:
            self.disconnect()
            return False, f"Authentication failed: {str(e)}"
        except redis.ConnectionError as e:
            self.disconnect()
            return False, "Connection failed: " + _connection_error_hint(profile.host, profile.port, e)
        except Exception as e:
            self.disconnect()
            hint = _connection_error_hint(profile.host, profile.port, e) if "connect" in str(e).lower() or "timeout" in str(e).lower() else str(e)
            return False, f"Error: {hint}"
    
    def disconnect(self) -> None:
        """Close the current connection."""
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
        
        if self._pool:
            try:
                self._pool.disconnect()
            except Exception:
                pass
            self._pool = None
        
        self._current_profile = None
        self._module_status = None
    
    def is_connected(self) -> bool:
        """Check if currently connected to Redis."""
        if not self._client:
            return False
        try:
            self._client.ping()
            return True
        except Exception:
            return False
    
    def get_client(self) -> Optional[redis.Redis]:
        """Get the Redis client. Returns None if not connected."""
        if self.is_connected():
            return self._client
        return None
    
    def get_current_profile(self) -> Optional[ConnectionProfile]:
        """Get the current connection profile."""
        return self._current_profile
    
    def _detect_modules(self) -> ModuleStatus:
        """Detect which Redis modules are available."""
        status = ModuleStatus()
        
        if not self._client:
            return status
        
        try:
            modules = self._client.module_list()
            module_names = {m['name'].lower() for m in modules}
            
            # Check for RedisJSON (can be named 'ReJSON' or 'RedisJSON')
            status.json = any(
                name in module_names 
                for name in ['rejson', 'redisjson', 'json']
            )
            
            # Check for RediSearch
            status.search = any(
                name in module_names 
                for name in ['search', 'redisearch', 'ft']
            )
            
            # Check for RedisTimeSeries
            status.timeseries = any(
                name in module_names 
                for name in ['timeseries', 'redistimeseries', 'ts']
            )
            
            # Check for RedisBloom
            status.bloom = any(
                name in module_names 
                for name in ['bf', 'redisbloom', 'bloom']
            )
            
        except Exception as e:
            # If MODULE LIST fails, modules might not be supported
            print(f"Warning: Could not detect modules: {e}")
        
        return status
    
    def get_module_status(self) -> ModuleStatus:
        """Get the current module availability status."""
        if self._module_status is None:
            return ModuleStatus()
        return self._module_status
    
    def refresh_modules(self) -> ModuleStatus:
        """Re-detect available modules."""
        if self._client:
            self._module_status = self._detect_modules()
        return self.get_module_status()
    
    def get_server_info(self) -> Dict[str, Any]:
        """Get Redis server information."""
        if not self._client:
            return {}
        try:
            info = self._client.info()
            return {
                'redis_version': info.get('redis_version', 'Unknown'),
                'os': info.get('os', 'Unknown'),
                'connected_clients': info.get('connected_clients', 0),
                'used_memory_human': info.get('used_memory_human', 'Unknown'),
                'total_keys': sum(
                    info.get(f'db{i}', {}).get('keys', 0) 
                    for i in range(16)
                ),
            }
        except Exception:
            return {}
    
    def flush_db(self) -> Tuple[bool, str]:
        """Flush the current database. Use with caution!"""
        if not self._client:
            return False, "Not connected"
        try:
            self._client.flushdb()
            return True, "Database flushed successfully"
        except Exception as e:
            return False, f"Failed to flush database: {str(e)}"


# Global connection manager instance for use with Streamlit
_connection_manager: Optional[RedisConnectionManager] = None


def get_connection_manager() -> RedisConnectionManager:
    """Get or create the global connection manager."""
    global _connection_manager
    if _connection_manager is None:
        _connection_manager = RedisConnectionManager()
    return _connection_manager
