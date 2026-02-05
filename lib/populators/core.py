"""
Core Redis data type populators.
Supports: Strings, Hashes, Lists, Sets, Sorted Sets, Streams, HyperLogLog, Geospatial, Bitmaps

Note: These populators are designed to work with Redis Cluster / Redis Enterprise
with multiple shards. They avoid cross-key pipelines which would fail when keys
hash to different slots.
"""

import random
import string
import time
from typing import Generator, Tuple, Any
import redis


def _random_string(length: int) -> str:
    """Generate a random alphanumeric string."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def _random_coords() -> Tuple[float, float]:
    """Generate random longitude and latitude coordinates."""
    # Longitude: -180 to 180, Latitude: -85.05 to 85.05 (Redis geo limits)
    longitude = random.uniform(-180, 180)
    latitude = random.uniform(-85.05, 85.05)
    return longitude, latitude


def populate_strings(
    r: redis.Redis,
    count: int = 100,
    key_prefix: str = "string",
    value_size: int = 100,
    batch_size: int = 100
) -> Generator[Tuple[int, int], None, None]:
    """
    Populate Redis with string keys.
    
    Args:
        r: Redis client
        count: Number of string keys to create
        key_prefix: Prefix for key names
        value_size: Size of random string values
        batch_size: Progress update frequency
        
    Yields:
        Tuple of (current_count, total_count) for progress tracking
    """
    for i in range(count):
        value = _random_string(value_size)
        r.set(f"{key_prefix}:{i}", value)
        
        if (i + 1) % batch_size == 0:
            yield i + 1, count
    
    yield count, count


def populate_hashes(
    r: redis.Redis,
    count: int = 100,
    key_prefix: str = "hash",
    fields_per_hash: int = 5,
    batch_size: int = 100
) -> Generator[Tuple[int, int], None, None]:
    """
    Populate Redis with hash keys containing user-like data.
    
    Args:
        r: Redis client
        count: Number of hash keys to create
        key_prefix: Prefix for key names
        fields_per_hash: Number of fields per hash
        batch_size: Progress update frequency
        
    Yields:
        Tuple of (current_count, total_count) for progress tracking
    """
    for i in range(count):
        mapping = {
            "id": str(i),
            "name": f"User_{_random_string(8)}",
            "email": f"user{i}@example.com",
            "age": str(random.randint(18, 80)),
            "score": str(random.randint(0, 10000)),
        }
        # Add extra fields if needed
        for j in range(max(0, fields_per_hash - 5)):
            mapping[f"field_{j}"] = _random_string(20)
        
        r.hset(f"{key_prefix}:{i}", mapping=mapping)
        
        if (i + 1) % batch_size == 0:
            yield i + 1, count
    
    yield count, count


def populate_lists(
    r: redis.Redis,
    count: int = 50,
    key_prefix: str = "list",
    items_per_list: int = 100,
    batch_size: int = 10
) -> Generator[Tuple[int, int], None, None]:
    """
    Populate Redis with list keys.
    
    Args:
        r: Redis client
        count: Number of list keys to create
        key_prefix: Prefix for key names
        items_per_list: Number of items per list
        batch_size: Progress update frequency
        
    Yields:
        Tuple of (current_count, total_count) for progress tracking
    """
    for i in range(count):
        items = [f"item_{j}_{_random_string(10)}" for j in range(items_per_list)]
        r.rpush(f"{key_prefix}:{i}", *items)
        
        if (i + 1) % batch_size == 0:
            yield i + 1, count
    
    yield count, count


def populate_sets(
    r: redis.Redis,
    count: int = 100,
    key_prefix: str = "set",
    members_per_set: int = 50,
    batch_size: int = 50
) -> Generator[Tuple[int, int], None, None]:
    """
    Populate Redis with set keys.
    
    Args:
        r: Redis client
        count: Number of set keys to create
        key_prefix: Prefix for key names
        members_per_set: Number of members per set
        batch_size: Progress update frequency
        
    Yields:
        Tuple of (current_count, total_count) for progress tracking
    """
    for i in range(count):
        members = [f"member_{j}_{_random_string(8)}" for j in range(members_per_set)]
        r.sadd(f"{key_prefix}:{i}", *members)
        
        if (i + 1) % batch_size == 0:
            yield i + 1, count
    
    yield count, count


def populate_sorted_sets(
    r: redis.Redis,
    count: int = 100,
    key_prefix: str = "zset",
    members_per_set: int = 50,
    batch_size: int = 50
) -> Generator[Tuple[int, int], None, None]:
    """
    Populate Redis with sorted set keys (leaderboard-style data).
    
    Args:
        r: Redis client
        count: Number of sorted set keys to create
        key_prefix: Prefix for key names
        members_per_set: Number of members per sorted set
        batch_size: Progress update frequency
        
    Yields:
        Tuple of (current_count, total_count) for progress tracking
    """
    for i in range(count):
        # Create score->member mapping
        mapping = {
            f"player_{j}": random.uniform(0, 100000)
            for j in range(members_per_set)
        }
        r.zadd(f"{key_prefix}:{i}", mapping)
        
        if (i + 1) % batch_size == 0:
            yield i + 1, count
    
    yield count, count


def populate_streams(
    r: redis.Redis,
    count: int = 20,
    key_prefix: str = "stream",
    entries_per_stream: int = 100,
    batch_size: int = 5
) -> Generator[Tuple[int, int], None, None]:
    """
    Populate Redis with stream keys (event log-style data).
    
    Args:
        r: Redis client
        count: Number of stream keys to create
        key_prefix: Prefix for key names
        entries_per_stream: Number of entries per stream
        batch_size: Progress update frequency
        
    Yields:
        Tuple of (current_count, total_count) for progress tracking
    """
    for i in range(count):
        key = f"{key_prefix}:{i}"
        # Use pipeline for single key (all commands go to same slot)
        pipeline = r.pipeline()
        
        for j in range(entries_per_stream):
            pipeline.xadd(
                key,
                {
                    "event_id": str(j),
                    "timestamp": str(int(time.time() * 1000) + j),
                    "type": random.choice(["click", "view", "purchase", "login", "logout"]),
                    "user_id": str(random.randint(1, 10000)),
                    "data": _random_string(50),
                }
            )
        
        pipeline.execute()
        yield i + 1, count


def populate_hyperloglog(
    r: redis.Redis,
    count: int = 50,
    key_prefix: str = "hll",
    elements_per_hll: int = 1000,
    batch_size: int = 10
) -> Generator[Tuple[int, int], None, None]:
    """
    Populate Redis with HyperLogLog keys for cardinality estimation.
    
    Args:
        r: Redis client
        count: Number of HyperLogLog keys to create
        key_prefix: Prefix for key names
        elements_per_hll: Number of elements to add per HyperLogLog
        batch_size: Progress update frequency
        
    Yields:
        Tuple of (current_count, total_count) for progress tracking
    """
    for i in range(count):
        key = f"{key_prefix}:{i}"
        # Use pipeline for single key (all commands go to same slot)
        pipeline = r.pipeline()
        
        # Add elements in chunks to avoid huge argument lists
        chunk_size = 100
        for chunk_start in range(0, elements_per_hll, chunk_size):
            elements = [
                f"user_{random.randint(1, elements_per_hll * 2)}" 
                for _ in range(min(chunk_size, elements_per_hll - chunk_start))
            ]
            pipeline.pfadd(key, *elements)
        
        pipeline.execute()
        yield i + 1, count


def populate_geospatial(
    r: redis.Redis,
    count: int = 50,
    key_prefix: str = "geo",
    locations_per_key: int = 100,
    batch_size: int = 10
) -> Generator[Tuple[int, int], None, None]:
    """
    Populate Redis with geospatial keys (store/location data).
    
    Args:
        r: Redis client
        count: Number of geo keys to create
        key_prefix: Prefix for key names
        locations_per_key: Number of locations per geo key
        batch_size: Progress update frequency
        
    Yields:
        Tuple of (current_count, total_count) for progress tracking
    """
    # City names for more realistic data
    cities = [
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
        "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
        "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte",
        "Seattle", "Denver", "Boston", "Detroit", "Nashville"
    ]
    
    for i in range(count):
        key = f"{key_prefix}:{i}"
        # Use pipeline for single key (all commands go to same slot)
        pipeline = r.pipeline()
        
        # Add locations to this geo key
        for j in range(locations_per_key):
            lon, lat = _random_coords()
            name = f"{random.choice(cities)}_{j}"
            pipeline.geoadd(key, (lon, lat, name))
        
        pipeline.execute()
        
        if (i + 1) % batch_size == 0:
            yield i + 1, count
    
    yield count, count


def populate_bitmaps(
    r: redis.Redis,
    count: int = 50,
    key_prefix: str = "bitmap",
    bits_per_bitmap: int = 10000,
    density: float = 0.3,
    batch_size: int = 10
) -> Generator[Tuple[int, int], None, None]:
    """
    Populate Redis with bitmap keys (feature flags, user activity tracking).
    
    Args:
        r: Redis client
        count: Number of bitmap keys to create
        key_prefix: Prefix for key names
        bits_per_bitmap: Number of bits in each bitmap
        density: Probability of each bit being set (0.0 to 1.0)
        batch_size: Progress update frequency
        
    Yields:
        Tuple of (current_count, total_count) for progress tracking
    """
    for i in range(count):
        key = f"{key_prefix}:{i}"
        # Use pipeline for single key (all commands go to same slot)
        pipeline = r.pipeline()
        
        # Set random bits based on density
        for bit_pos in range(bits_per_bitmap):
            if random.random() < density:
                pipeline.setbit(key, bit_pos, 1)
        
        pipeline.execute()
        yield i + 1, count
