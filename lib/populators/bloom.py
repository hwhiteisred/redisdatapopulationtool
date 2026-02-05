"""
RedisBloom data type populators.
Supports: Bloom Filters, Cuckoo Filters, Count-Min Sketch, Top-K
"""

import random
import string
from typing import Generator, Tuple, List
import redis


def _random_string(length: int) -> str:
    """Generate a random alphanumeric string."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def _generate_items(count: int, prefix: str = "item") -> List[str]:
    """Generate a list of unique items."""
    return [f"{prefix}:{i}:{_random_string(8)}" for i in range(count)]


def _generate_weighted_items(count: int) -> List[Tuple[str, int]]:
    """Generate items with Zipf-like frequency distribution (some items are much more common)."""
    items = []
    # Create a Zipf-like distribution
    for i in range(count):
        item = f"item:{i}"
        # Higher ranked items appear more frequently
        frequency = int(1000 / (i + 1))  # Zipf law approximation
        items.append((item, max(1, frequency)))
    return items


def populate_bloom_filter(
    r: redis.Redis,
    count: int = 10,
    key_prefix: str = "bf",
    items_per_filter: int = 10000,
    error_rate: float = 0.01,
    batch_size: int = 1000
) -> Generator[Tuple[int, int], None, None]:
    """
    Populate Redis with Bloom Filter data structures.
    
    Bloom filters are space-efficient probabilistic data structures for
    membership testing (e.g., "has this email been seen before?").
    
    Args:
        r: Redis client
        count: Number of Bloom filters to create
        key_prefix: Prefix for key names
        items_per_filter: Number of items to add per filter
        error_rate: Target false positive rate (0.01 = 1%)
        batch_size: Number of items per batch add
        
    Yields:
        Tuple of (current_count, total_count) for progress tracking
    """
    for i in range(count):
        key = f"{key_prefix}:{i}"
        
        # Reserve the bloom filter with specified capacity and error rate
        try:
            r.execute_command(
                "BF.RESERVE", key,
                str(error_rate),
                str(items_per_filter)
            )
        except Exception:
            # Filter might already exist
            pass
        
        # Add items in batches
        items = _generate_items(items_per_filter, prefix=f"user_{i}")
        
        for batch_start in range(0, items_per_filter, batch_size):
            batch_end = min(batch_start + batch_size, items_per_filter)
            batch_items = items[batch_start:batch_end]
            
            # Use BF.MADD for batch insertion
            try:
                r.execute_command("BF.MADD", key, *batch_items)
            except Exception:
                # Fall back to individual adds
                for item in batch_items:
                    try:
                        r.execute_command("BF.ADD", key, item)
                    except Exception:
                        pass
        
        yield i + 1, count
    
    yield count, count


def populate_cuckoo_filter(
    r: redis.Redis,
    count: int = 10,
    key_prefix: str = "cf",
    items_per_filter: int = 10000,
    bucket_size: int = 2,
    batch_size: int = 1000
) -> Generator[Tuple[int, int], None, None]:
    """
    Populate Redis with Cuckoo Filter data structures.
    
    Cuckoo filters are similar to Bloom filters but support deletion
    and typically have better lookup performance.
    
    Args:
        r: Redis client
        count: Number of Cuckoo filters to create
        key_prefix: Prefix for key names
        items_per_filter: Capacity of each filter
        bucket_size: Number of items per bucket (affects performance)
        batch_size: Number of items per batch add
        
    Yields:
        Tuple of (current_count, total_count) for progress tracking
    """
    for i in range(count):
        key = f"{key_prefix}:{i}"
        
        # Reserve the cuckoo filter
        try:
            r.execute_command(
                "CF.RESERVE", key,
                str(items_per_filter),
                "BUCKETSIZE", str(bucket_size)
            )
        except Exception:
            pass
        
        # Add items in batches
        items = _generate_items(items_per_filter, prefix=f"session_{i}")
        
        for batch_start in range(0, items_per_filter, batch_size):
            batch_end = min(batch_start + batch_size, items_per_filter)
            batch_items = items[batch_start:batch_end]
            
            # Use CF.ADDNX (add if not exists) for batch insertion
            for item in batch_items:
                try:
                    r.execute_command("CF.ADDNX", key, item)
                except Exception:
                    pass
        
        yield i + 1, count
    
    yield count, count


def populate_countmin_sketch(
    r: redis.Redis,
    count: int = 10,
    key_prefix: str = "cms",
    unique_items: int = 1000,
    total_increments: int = 100000,
    width: int = 2000,
    depth: int = 5
) -> Generator[Tuple[int, int], None, None]:
    """
    Populate Redis with Count-Min Sketch data structures.
    
    Count-Min Sketch is used for frequency estimation - counting how many
    times items appear (e.g., tracking page views, word frequencies).
    
    Args:
        r: Redis client
        count: Number of CMS structures to create
        key_prefix: Prefix for key names
        unique_items: Number of unique items to track
        total_increments: Total number of increment operations
        width: Width of the sketch (more = more accurate)
        depth: Depth of the sketch (more = lower error probability)
        
    Yields:
        Tuple of (current_count, total_count) for progress tracking
    """
    for i in range(count):
        key = f"{key_prefix}:{i}"
        
        # Initialize the Count-Min Sketch
        try:
            r.execute_command("CMS.INITBYDIM", key, str(width), str(depth))
        except Exception:
            pass
        
        # Generate items with Zipf-like distribution
        weighted_items = _generate_weighted_items(unique_items)
        
        # Create increment batches based on weights
        batch_size = 100
        increments_done = 0
        
        while increments_done < total_increments:
            # Pick items to increment based on their weights
            incrby_args = []
            
            for _ in range(min(batch_size, total_increments - increments_done)):
                # Select item based on weight (higher weight = more likely)
                item, weight = random.choices(
                    weighted_items,
                    weights=[w for _, w in weighted_items]
                )[0]
                increment = random.randint(1, 10)
                incrby_args.extend([item, str(increment)])
                increments_done += 1
            
            if incrby_args:
                try:
                    r.execute_command("CMS.INCRBY", key, *incrby_args)
                except Exception:
                    pass
        
        yield i + 1, count
    
    yield count, count


def populate_topk(
    r: redis.Redis,
    count: int = 10,
    key_prefix: str = "topk",
    k: int = 100,
    unique_items: int = 10000,
    total_adds: int = 100000,
    width: int = 2000,
    depth: int = 7,
    decay: float = 0.9
) -> Generator[Tuple[int, int], None, None]:
    """
    Populate Redis with Top-K data structures.
    
    Top-K tracks the most frequent items (heavy hitters) in a stream,
    useful for trending topics, popular products, etc.
    
    Args:
        r: Redis client
        count: Number of Top-K structures to create
        key_prefix: Prefix for key names
        k: Number of top items to track
        unique_items: Number of unique items in the stream
        total_adds: Total number of items to add
        width: Width of underlying Count-Min Sketch
        depth: Depth of underlying Count-Min Sketch
        decay: Decay factor for aging out old items
        
    Yields:
        Tuple of (current_count, total_count) for progress tracking
    """
    for i in range(count):
        key = f"{key_prefix}:{i}"
        
        # Reserve the Top-K structure
        try:
            r.execute_command(
                "TOPK.RESERVE", key,
                str(k),
                str(width),
                str(depth),
                str(decay)
            )
        except Exception:
            pass
        
        # Generate items with power-law distribution (some items much more popular)
        weighted_items = _generate_weighted_items(unique_items)
        
        # Add items in batches
        batch_size = 100
        adds_done = 0
        
        while adds_done < total_adds:
            batch_items = []
            
            for _ in range(min(batch_size, total_adds - adds_done)):
                # Select item based on weight
                item, _ = random.choices(
                    weighted_items,
                    weights=[w for _, w in weighted_items]
                )[0]
                batch_items.append(item)
                adds_done += 1
            
            if batch_items:
                try:
                    r.execute_command("TOPK.ADD", key, *batch_items)
                except Exception:
                    pass
        
        yield i + 1, count
    
    yield count, count
