"""
Redis monitoring and analysis tools.
Provides bigkeys analysis, memory stats, slow log, client info, and more.
"""

import redis
import subprocess
import re
from typing import Dict, List, Any, Tuple, Optional, Generator, Union
from dataclasses import dataclass
from collections import defaultdict
import time


@dataclass
class KeyInfo:
    """Information about a Redis key."""
    key: str
    type: str
    size: int  # Memory size in bytes
    length: int  # Number of elements (for collections)
    ttl: int  # TTL in seconds, -1 for no expiry, -2 for not exists
    encoding: str


@dataclass
class BigKeysReport:
    """Report from bigkeys analysis."""
    total_keys_scanned: int
    largest_by_type: Dict[str, KeyInfo]
    type_distribution: Dict[str, int]
    memory_by_type: Dict[str, int]
    top_keys_by_memory: List[KeyInfo]
    scan_time_seconds: float
    raw_output: str = ""  # Raw output from redis-cli for reference


@dataclass 
class BigKeysSummary:
    """Summary of biggest keys from redis-cli --bigkeys."""
    total_keys: int
    avg_key_length: float
    biggest_by_type: Dict[str, Dict[str, Any]]  # type -> {key, size, unit}
    type_stats: Dict[str, Dict[str, Any]]  # type -> {count, total, pct, avg}
    raw_output: str


def get_server_stats(r: redis.Redis) -> Dict[str, Any]:
    """Get comprehensive server statistics."""
    try:
        info = r.info()
        
        # Extract key metrics
        stats = {
            # Server
            'redis_version': info.get('redis_version', 'Unknown'),
            'redis_mode': info.get('redis_mode', 'Unknown'),
            'os': info.get('os', 'Unknown'),
            'uptime_days': info.get('uptime_in_days', 0),
            'uptime_seconds': info.get('uptime_in_seconds', 0),
            
            # Clients
            'connected_clients': info.get('connected_clients', 0),
            'blocked_clients': info.get('blocked_clients', 0),
            'max_clients': info.get('maxclients', 0),
            
            # Memory
            'used_memory': info.get('used_memory', 0),
            'used_memory_human': info.get('used_memory_human', 'Unknown'),
            'used_memory_peak': info.get('used_memory_peak', 0),
            'used_memory_peak_human': info.get('used_memory_peak_human', 'Unknown'),
            'used_memory_rss': info.get('used_memory_rss', 0),
            'used_memory_rss_human': info.get('used_memory_rss_human', 'Unknown'),
            'mem_fragmentation_ratio': info.get('mem_fragmentation_ratio', 0),
            'maxmemory': info.get('maxmemory', 0),
            'maxmemory_human': info.get('maxmemory_human', 'unlimited'),
            
            # Stats
            'total_connections_received': info.get('total_connections_received', 0),
            'total_commands_processed': info.get('total_commands_processed', 0),
            'instantaneous_ops_per_sec': info.get('instantaneous_ops_per_sec', 0),
            'total_net_input_bytes': info.get('total_net_input_bytes', 0),
            'total_net_output_bytes': info.get('total_net_output_bytes', 0),
            'keyspace_hits': info.get('keyspace_hits', 0),
            'keyspace_misses': info.get('keyspace_misses', 0),
            'expired_keys': info.get('expired_keys', 0),
            'evicted_keys': info.get('evicted_keys', 0),
            
            # Replication
            'role': info.get('role', 'Unknown'),
            'connected_slaves': info.get('connected_slaves', 0),
            
            # Persistence
            'rdb_last_save_time': info.get('rdb_last_save_time', 0),
            'rdb_changes_since_last_save': info.get('rdb_changes_since_last_save', 0),
            'aof_enabled': info.get('aof_enabled', 0),
        }
        
        # Calculate hit rate
        hits = stats['keyspace_hits']
        misses = stats['keyspace_misses']
        total = hits + misses
        stats['hit_rate'] = (hits / total * 100) if total > 0 else 0
        
        # Get keyspace info
        keyspace = {}
        total_keys = 0
        for key, value in info.items():
            if key.startswith('db'):
                keyspace[key] = value
                total_keys += value.get('keys', 0)
        stats['keyspace'] = keyspace
        stats['total_keys'] = total_keys
        
        return stats
    except Exception as e:
        return {'error': str(e)}


def get_memory_stats(r: redis.Redis) -> Dict[str, Any]:
    """Get detailed memory statistics."""
    try:
        # Try MEMORY STATS command
        memory_stats = r.execute_command('MEMORY', 'STATS')
        
        # Convert list to dict (alternating key-value pairs)
        result = {}
        if isinstance(memory_stats, list):
            for i in range(0, len(memory_stats), 2):
                if i + 1 < len(memory_stats):
                    result[memory_stats[i]] = memory_stats[i + 1]
        else:
            result = memory_stats
            
        return result
    except Exception as e:
        return {'error': str(e)}


def get_slowlog(r: redis.Redis, count: int = 20) -> List[Dict[str, Any]]:
    """Get slow log entries."""
    try:
        slowlog = r.slowlog_get(count)
        
        entries = []
        for entry in slowlog:
            entries.append({
                'id': entry.get('id', 0),
                'timestamp': entry.get('start_time', 0),
                'duration_us': entry.get('duration', 0),
                'duration_ms': entry.get('duration', 0) / 1000,
                'command': ' '.join(str(arg) for arg in entry.get('command', [])),
                'client_address': entry.get('client_address', 'Unknown'),
                'client_name': entry.get('client_name', ''),
            })
        
        return entries
    except Exception as e:
        return [{'error': str(e)}]


def get_client_list(r: redis.Redis) -> List[Dict[str, Any]]:
    """Get list of connected clients."""
    try:
        clients = r.client_list()
        
        # Parse and enrich client info
        result = []
        for client in clients:
            result.append({
                'id': client.get('id', 'Unknown'),
                'addr': client.get('addr', 'Unknown'),
                'name': client.get('name', ''),
                'age': client.get('age', 0),
                'idle': client.get('idle', 0),
                'flags': client.get('flags', ''),
                'db': client.get('db', 0),
                'cmd': client.get('cmd', ''),
                'sub': client.get('sub', 0),
                'psub': client.get('psub', 0),
            })
        
        return result
    except Exception as e:
        return [{'error': str(e)}]


def scan_bigkeys(
    r: redis.Redis,
    sample_size: int = 1000,
    top_n: int = 20
) -> Generator[Union[Tuple[int, int, str], BigKeysReport], None, None]:
    """
    Scan for big keys (similar to redis-cli --bigkeys).
    
    Args:
        r: Redis client
        sample_size: Maximum number of keys to scan (0 = all)
        top_n: Number of top keys to return per category
        
    Yields:
        Tuple of (keys_scanned, total_estimate, current_key) for progress,
        then finally yields the BigKeysReport
    """
    start_time = time.time()
    
    type_distribution = defaultdict(int)
    memory_by_type = defaultdict(int)
    largest_by_type = {}
    all_keys = []
    
    cursor = 0
    keys_scanned = 0
    
    # Estimate total keys
    try:
        dbsize = r.dbsize()
    except Exception:
        dbsize = sample_size or 10000
    
    max_to_scan = sample_size if sample_size > 0 else float('inf')
    
    while keys_scanned < max_to_scan:
        # Scan batch of keys
        cursor, keys = r.scan(cursor=cursor, count=100)
        
        for key in keys:
            if keys_scanned >= max_to_scan:
                break
                
            keys_scanned += 1
            
            try:
                # Get key type
                key_type = r.type(key)
                type_distribution[key_type] += 1
                
                # Get memory usage
                try:
                    memory = r.execute_command('MEMORY', 'USAGE', key) or 0
                except Exception:
                    memory = 0
                
                memory_by_type[key_type] += memory
                
                # Get length/size based on type
                length = 0
                encoding = 'unknown'
                
                try:
                    if key_type == 'string':
                        length = r.strlen(key)
                    elif key_type == 'list':
                        length = r.llen(key)
                    elif key_type == 'set':
                        length = r.scard(key)
                    elif key_type == 'zset':
                        length = r.zcard(key)
                    elif key_type == 'hash':
                        length = r.hlen(key)
                    elif key_type == 'stream':
                        length = r.xlen(key)
                    
                    # Get encoding
                    encoding = r.object('encoding', key) or 'unknown'
                except Exception:
                    pass
                
                # Get TTL
                ttl = r.ttl(key)
                
                key_info = KeyInfo(
                    key=key,
                    type=key_type,
                    size=memory,
                    length=length,
                    ttl=ttl,
                    encoding=encoding
                )
                
                all_keys.append(key_info)
                
                # Track largest by type
                if key_type not in largest_by_type or memory > largest_by_type[key_type].size:
                    largest_by_type[key_type] = key_info
                
            except Exception:
                continue
            
            # Yield progress every 100 keys
            if keys_scanned % 100 == 0:
                yield (keys_scanned, dbsize, key)
        
        # Check if scan is complete
        if cursor == 0:
            break
    
    # Sort and get top keys by memory
    all_keys.sort(key=lambda x: x.size, reverse=True)
    top_keys = all_keys[:top_n]
    
    scan_time = time.time() - start_time
    
    # Yield the final report
    yield BigKeysReport(
        total_keys_scanned=keys_scanned,
        largest_by_type=largest_by_type,
        type_distribution=dict(type_distribution),
        memory_by_type=dict(memory_by_type),
        top_keys_by_memory=top_keys,
        scan_time_seconds=scan_time
    )


def run_redis_cli_scan(
    host: str = "localhost",
    port: int = 6379,
    password: str = None,
    scan_type: str = "bigkeys",
    sleep_interval: float = 0.0
) -> Tuple[bool, Union[BigKeysSummary, str]]:
    """
    Run redis-cli key scanning commands and parse the output.
    
    Supports --bigkeys, --memkeys, and --keystats options.
    See: https://redis.io/docs/latest/develop/tools/cli/
    
    Args:
        host: Redis host
        port: Redis port
        password: Redis password (optional)
        scan_type: One of "bigkeys", "memkeys", or "keystats"
        sleep_interval: Sleep interval between SCAN commands (e.g., 0.1)
        
    Returns:
        Tuple of (success, BigKeysSummary or error message)
    """
    # Validate scan type
    valid_types = ["bigkeys", "memkeys", "keystats"]
    if scan_type not in valid_types:
        return False, f"Invalid scan type. Must be one of: {valid_types}"
    
    # Build command
    cmd = ["redis-cli", "-h", host, "-p", str(port)]
    
    if password:
        cmd.extend(["-a", password])
        cmd.append("--no-auth-warning")
    
    if sleep_interval > 0:
        cmd.extend(["-i", str(sleep_interval)])
    
    cmd.append(f"--{scan_type}")
    
    try:
        # Run redis-cli with the specified scan option
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        output = result.stdout + result.stderr
        
        if result.returncode != 0:
            return False, f"redis-cli failed: {output}"
        
        # Parse the output
        return True, parse_bigkeys_output(output)
        
    except FileNotFoundError:
        return False, "redis-cli not found. Please install Redis CLI tools (e.g., `brew install redis` on macOS)."
    except subprocess.TimeoutExpired:
        return False, "Scan timed out after 5 minutes."
    except Exception as e:
        return False, f"Error running redis-cli: {str(e)}"


# Keep the old function name for backwards compatibility
def run_bigkeys_cli(
    host: str = "localhost",
    port: int = 6379,
    password: str = None,
    sleep_interval: float = 0.0
) -> Tuple[bool, Union[BigKeysSummary, str]]:
    """Backwards compatible wrapper for run_redis_cli_scan with --bigkeys."""
    return run_redis_cli_scan(host, port, password, "bigkeys", sleep_interval)


def parse_bigkeys_output(output: str) -> BigKeysSummary:
    """Parse the output of redis-cli --bigkeys."""
    
    # Default values
    total_keys = 0
    avg_key_length = 0.0
    biggest_by_type = {}
    type_stats = {}
    
    lines = output.split('\n')
    in_summary = False
    
    for line in lines:
        line = line.strip()
        
        # Parse "Sampled X keys in the keyspace!"
        match = re.search(r'Sampled (\d+) keys', line)
        if match:
            total_keys = int(match.group(1))
        
        # Parse "Total key length in bytes is X (avg len Y)"
        match = re.search(r'avg len ([\d.]+)', line)
        if match:
            avg_key_length = float(match.group(1))
        
        # Parse "Biggest X found" lines in summary
        # e.g., "Biggest string found "string:0" has 1000000 bytes"
        # e.g., "Biggest list found "list:36" has 100 items"
        match = re.search(r'Biggest\s+(\w+)\s+found\s+"([^"]+)"\s+has\s+([\d]+)\s+(\w+)', line)
        if match:
            key_type = match.group(1)
            key_name = match.group(2)
            size = int(match.group(3))
            unit = match.group(4)
            biggest_by_type[key_type] = {
                'key': key_name,
                'size': size,
                'unit': unit
            }
        
        # Parse type stats lines
        # e.g., "10096 strings with 12225904 bytes (96.59% of keys, avg size 1210.97)"
        # e.g., "51 lists with 5002 items (00.49% of keys, avg size 98.08)"
        match = re.search(
            r'(\d+)\s+(\w+)\s+with\s+([\d]+)\s+(\w+)\s+\(([\d.]+)%\s+of keys,\s+avg size\s+([\d.]+)\)',
            line
        )
        if match:
            count = int(match.group(1))
            key_type = match.group(2).rstrip('s')  # Remove trailing 's' (strings -> string)
            total_size = int(match.group(3))
            unit = match.group(4)
            pct = float(match.group(5))
            avg_size = float(match.group(6))
            
            type_stats[key_type] = {
                'count': count,
                'total': total_size,
                'unit': unit,
                'pct': pct,
                'avg': avg_size
            }
    
    return BigKeysSummary(
        total_keys=total_keys,
        avg_key_length=avg_key_length,
        biggest_by_type=biggest_by_type,
        type_stats=type_stats,
        raw_output=output
    )


def get_key_patterns(r: redis.Redis, sample_size: int = 500) -> Dict[str, Dict[str, int]]:
    """
    Analyze key patterns/prefixes.
    
    Returns dict mapping prefix to count and estimated memory.
    """
    patterns = defaultdict(lambda: {'count': 0, 'memory': 0})
    
    cursor = 0
    scanned = 0
    
    while scanned < sample_size:
        cursor, keys = r.scan(cursor=cursor, count=100)
        
        for key in keys:
            if scanned >= sample_size:
                break
            scanned += 1
            
            # Extract prefix (part before first : or entire key)
            if ':' in key:
                prefix = key.split(':')[0]
            else:
                prefix = key[:10] + '...' if len(key) > 10 else key
            
            patterns[prefix]['count'] += 1
            
            try:
                memory = r.execute_command('MEMORY', 'USAGE', key) or 0
                patterns[prefix]['memory'] += memory
            except Exception:
                pass
        
        if cursor == 0:
            break
    
    return dict(patterns)


def get_command_stats(r: redis.Redis) -> Dict[str, Dict[str, Any]]:
    """Get command statistics from INFO commandstats."""
    try:
        info = r.info('commandstats')
        
        stats = {}
        for key, value in info.items():
            if key.startswith('cmdstat_'):
                cmd_name = key.replace('cmdstat_', '')
                stats[cmd_name] = {
                    'calls': value.get('calls', 0),
                    'usec': value.get('usec', 0),
                    'usec_per_call': value.get('usec_per_call', 0),
                }
        
        return stats
    except Exception as e:
        return {'error': str(e)}


def format_bytes(bytes_val: int) -> str:
    """Format bytes to human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(bytes_val) < 1024:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.2f} PB"
