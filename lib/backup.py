"""
Redis Enterprise Backup Management via REST API.

Provides functions to manage database backups through the Redis Enterprise REST API.
See: https://redis.io/docs/latest/operate/rs/references/rest-api/
"""

import requests
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime
import urllib3

# Disable SSL warnings for self-signed certs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@dataclass
class ClusterCredentials:
    """Redis Enterprise cluster admin credentials."""
    host: str
    port: int = 9443  # Default REST API port
    username: str = ""
    password: str = ""
    
    @property
    def base_url(self) -> str:
        return f"https://{self.host}:{self.port}"


@dataclass
class DatabaseInfo:
    """Basic database information."""
    uid: int
    name: str
    status: str
    memory_size: int
    backup_enabled: bool
    backup_interval: Optional[int]
    backup_location: Optional[str]
    last_backup_time: Optional[str]


@dataclass
class BackupStatus:
    """Status of a backup operation."""
    uid: int
    db_name: str
    status: str
    progress: Optional[int]
    started_at: Optional[str]
    completed_at: Optional[str]
    file_path: Optional[str]
    error: Optional[str]


class RedisEnterpriseAPI:
    """Client for Redis Enterprise REST API."""
    
    def __init__(self, credentials: ClusterCredentials):
        self.credentials = credentials
        self.session = requests.Session()
        self.session.verify = False  # Self-signed certs
        # Don't auto-follow redirects (Docker internal IPs won't be reachable)
        self.session.max_redirects = 0
        if credentials.username and credentials.password:
            self.session.auth = (credentials.username, credentials.password)
    
    def _request(self, method: str, endpoint: str, **kwargs) -> Tuple[bool, Any]:
        """Make an API request."""
        url = f"{self.credentials.base_url}{endpoint}"
        try:
            # Don't follow redirects - Docker internal IPs won't be reachable from host
            response = self.session.request(method, url, timeout=30, allow_redirects=False, **kwargs)
            
            # Handle redirect (cluster leader is on different node)
            if response.status_code in (301, 302, 303, 307, 308):
                redirect_url = response.headers.get('Location', '')
                return False, f"Cluster redirected to {redirect_url}. Try connecting to the cluster leader node's port instead (check docker ps for port mappings)."
            
            if response.status_code == 401:
                return False, "Authentication failed. Check username/password."
            elif response.status_code == 403:
                return False, "Permission denied."
            elif response.status_code >= 400:
                return False, f"API error {response.status_code}: {response.text}"
            
            if response.content:
                return True, response.json()
            return True, None
            
        except requests.exceptions.ConnectionError as e:
            return False, f"Connection failed to {self.credentials.host}:{self.credentials.port} - {str(e)}"
        except requests.exceptions.Timeout:
            return False, "Request timed out"
        except requests.exceptions.SSLError as e:
            return False, f"SSL Error: {str(e)}"
        except Exception as e:
            return False, f"{type(e).__name__}: {str(e)}"
    
    def test_connection(self) -> Tuple[bool, str]:
        """Test API connection."""
        success, result = self._request("GET", "/v1/cluster")
        if success:
            cluster_name = result.get("name", "Unknown")
            return True, f"Connected to cluster: {cluster_name}"
        return False, result
    
    def get_databases(self) -> Tuple[bool, List[DatabaseInfo]]:
        """Get list of all databases."""
        success, result = self._request("GET", "/v1/bdbs")
        if not success:
            return False, result
        
        databases = []
        for db in result:
            databases.append(DatabaseInfo(
                uid=db.get("uid"),
                name=db.get("name", "Unknown"),
                status=db.get("status", "unknown"),
                memory_size=db.get("memory_size", 0),
                backup_enabled=db.get("backup", False),
                backup_interval=db.get("backup_interval"),
                backup_location=db.get("backup_location"),
                last_backup_time=db.get("last_backup_time")
            ))
        
        return True, databases
    
    def get_database(self, db_uid: int) -> Tuple[bool, Dict]:
        """Get detailed info for a specific database."""
        return self._request("GET", f"/v1/bdbs/{db_uid}")
    
    def trigger_backup(self, db_uid: int, location: str = "/var/opt/redislabs/backup") -> Tuple[bool, str]:
        """Trigger an immediate backup/export for a database."""
        # Use export action with mount_point type for local storage
        payload = {
            "export_location": {
                "type": "mount_point",
                "path": location
            }
        }
        success, result = self._request(
            "POST",
            f"/v1/bdbs/{db_uid}/actions/export",
            json=payload
        )
        if success:
            action_uid = result.get('action_uid', '') if result else ''
            return True, f"Backup initiated successfully (action: {action_uid})"
        return False, result
    
    def get_backup_status(self, db_uid: int) -> Tuple[bool, BackupStatus]:
        """Get the current backup status for a database."""
        success, result = self._request("GET", f"/v1/bdbs/{db_uid}")
        if not success:
            return False, result
        
        return True, BackupStatus(
            uid=db_uid,
            db_name=result.get("name", "Unknown"),
            status=result.get("backup_status", "unknown"),
            progress=result.get("backup_progress"),
            started_at=result.get("backup_start_time"),
            completed_at=result.get("last_backup_time"),
            file_path=result.get("backup_location"),
            error=result.get("backup_failure_reason")
        )
    
    def configure_backup(
        self,
        db_uid: int,
        enabled: bool = True,
        interval: int = 86400,  # 24 hours in seconds
        location: str = "/var/opt/redislabs/backup"
    ) -> Tuple[bool, str]:
        """Configure backup settings for a database."""
        payload = {
            "backup": enabled,
            "backup_interval": interval,
            "backup_location": f"file://{location}"
        }
        
        success, result = self._request(
            "PUT",
            f"/v1/bdbs/{db_uid}",
            json=payload
        )
        
        if success:
            return True, "Backup configuration updated"
        return False, result
    
    def export_database(self, db_uid: int, location: str) -> Tuple[bool, str]:
        """Export database to a specific location (one-time backup)."""
        # Determine export type based on location prefix
        if location.startswith("s3://"):
            export_loc = {"type": "s3", "uri": location}
        elif location.startswith("gs://"):
            export_loc = {"type": "google_cloud_storage", "uri": location}
        elif location.startswith("ftp://") or location.startswith("ftps://"):
            export_loc = {"type": "ftp", "url": location}
        elif location.startswith("sftp://"):
            export_loc = {"type": "sftp", "url": location}
        else:
            # Local mount point - strip file:// prefix if present
            path = location.replace("file://", "")
            export_loc = {"type": "mount_point", "path": path}
        
        payload = {"export_location": export_loc}
        
        success, result = self._request(
            "POST",
            f"/v1/bdbs/{db_uid}/actions/export",
            json=payload
        )
        
        if success:
            action_uid = result.get('action_uid', '') if result else ''
            return True, f"Export initiated (action: {action_uid})"
        return False, result
    
    def import_database(self, db_uid: int, source_location: str) -> Tuple[bool, str]:
        """Import/restore database from a backup file."""
        # Determine import type based on location prefix
        if source_location.startswith("s3://"):
            import_loc = {"type": "s3", "uri": source_location}
        elif source_location.startswith("gs://"):
            import_loc = {"type": "google_cloud_storage", "uri": source_location}
        elif source_location.startswith("ftp://") or source_location.startswith("ftps://"):
            import_loc = {"type": "ftp", "url": source_location}
        elif source_location.startswith("sftp://"):
            import_loc = {"type": "sftp", "url": source_location}
        elif source_location.startswith("http://") or source_location.startswith("https://"):
            import_loc = {"type": "url", "url": source_location}
        else:
            # Local mount point - strip file:// prefix if present
            path = source_location.replace("file://", "")
            import_loc = {"type": "mount_point", "path": path}
        
        payload = {"dataset_import_sources": [import_loc]}
        
        success, result = self._request(
            "POST",
            f"/v1/bdbs/{db_uid}/actions/import",
            json=payload
        )
        
        if success:
            action_uid = result.get('action_uid', '') if result else ''
            return True, f"Import initiated (action: {action_uid})"
        return False, result
    
    def get_cluster_info(self) -> Tuple[bool, Dict]:
        """Get cluster information."""
        return self._request("GET", "/v1/cluster")
    
    def get_nodes(self) -> Tuple[bool, List[Dict]]:
        """Get list of cluster nodes."""
        return self._request("GET", "/v1/nodes")


def format_bytes(size: int) -> str:
    """Format bytes to human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"


def format_interval(seconds: Optional[int]) -> str:
    """Format seconds to human readable interval."""
    if not seconds:
        return "Not set"
    
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m"
    elif seconds < 86400:
        return f"{seconds // 3600}h"
    else:
        return f"{seconds // 86400}d"
