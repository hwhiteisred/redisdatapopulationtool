"""
RedisTimeSeries data populator.
Creates time series data for metrics like sensors, stocks, server metrics, etc.
"""

import random
import time
from typing import Generator, Tuple, List, Dict, Any
import redis


def _generate_metric_value(metric_type: str, base_value: float, noise: float = 0.1) -> float:
    """Generate a realistic metric value with some noise."""
    variation = base_value * noise * random.uniform(-1, 1)
    return round(base_value + variation, 2)


def populate_timeseries(
    r: redis.Redis,
    count: int = 10,
    key_prefix: str = "ts",
    samples_per_series: int = 1000,
    metric_type: str = "sensor",
    retention_ms: int = 0,
    start_time: int = None
) -> Generator[Tuple[int, int], None, None]:
    """
    Populate Redis with TimeSeries data.
    
    Args:
        r: Redis client
        count: Number of time series keys to create
        key_prefix: Prefix for key names
        samples_per_series: Number of data points per time series
        metric_type: Type of metric ("sensor", "stock", "server", "iot")
        retention_ms: Data retention in milliseconds (0 = unlimited)
        start_time: Starting timestamp in ms (default: 1 hour ago)
        
    Yields:
        Tuple of (current_count, total_count) for progress tracking
    """
    if start_time is None:
        # Start from 1 hour ago
        start_time = int((time.time() - 3600) * 1000)
    
    # Define metric configurations based on type
    metric_configs = {
        "sensor": {
            "labels": {"type": "sensor", "unit": "celsius"},
            "base_values": [20.0, 25.0, 30.0, 22.0, 28.0],  # Temperature-like
            "noise": 0.05,
            "interval_ms": 1000,  # 1 sample per second
        },
        "stock": {
            "labels": {"type": "stock", "unit": "usd"},
            "base_values": [100.0, 250.0, 50.0, 175.0, 500.0],
            "noise": 0.02,  # 2% volatility
            "interval_ms": 60000,  # 1 sample per minute
        },
        "server": {
            "labels": {"type": "server", "unit": "percent"},
            "base_values": [45.0, 60.0, 30.0, 75.0, 55.0],  # CPU/Memory-like
            "noise": 0.15,
            "interval_ms": 5000,  # 1 sample per 5 seconds
        },
        "iot": {
            "labels": {"type": "iot", "unit": "mixed"},
            "base_values": [1.0, 0.5, 100.0, 50.0, 25.0],
            "noise": 0.1,
            "interval_ms": 10000,  # 1 sample per 10 seconds
        },
    }
    
    config = metric_configs.get(metric_type, metric_configs["sensor"])
    
    for i in range(count):
        key = f"{key_prefix}:{metric_type}:{i}"
        base_value = config["base_values"][i % len(config["base_values"])]
        
        # Create the time series with labels
        labels = {**config["labels"], "sensor_id": str(i)}
        label_args = []
        for k, v in labels.items():
            label_args.extend([k, v])
        
        try:
            # Create the time series
            create_args = ["TS.CREATE", key]
            if retention_ms > 0:
                create_args.extend(["RETENTION", str(retention_ms)])
            create_args.extend(["LABELS"] + label_args)
            
            r.execute_command(*create_args)
        except Exception:
            # Key might already exist, continue anyway
            pass
        
        # Add samples in batches using TS.MADD for efficiency
        batch_size = 100
        current_time = start_time
        
        for batch_start in range(0, samples_per_series, batch_size):
            batch_end = min(batch_start + batch_size, samples_per_series)
            
            # Build MADD command arguments
            madd_args = ["TS.MADD"]
            
            for j in range(batch_start, batch_end):
                value = _generate_metric_value(metric_type, base_value, config["noise"])
                # Add some trend to make data more interesting
                trend = (j / samples_per_series) * base_value * 0.1  # 10% trend
                value += trend * random.choice([-1, 1])
                
                timestamp = current_time + (j * config["interval_ms"])
                madd_args.extend([key, str(timestamp), str(round(value, 2))])
            
            try:
                r.execute_command(*madd_args)
            except Exception as e:
                # Fall back to individual adds if MADD fails
                for j in range(batch_start, batch_end):
                    value = _generate_metric_value(metric_type, base_value, config["noise"])
                    timestamp = current_time + (j * config["interval_ms"])
                    try:
                        r.execute_command("TS.ADD", key, str(timestamp), str(round(value, 2)))
                    except Exception:
                        pass
        
        yield i + 1, count
    
    yield count, count


def populate_timeseries_with_rules(
    r: redis.Redis,
    source_key: str = "ts:raw",
    samples: int = 10000,
    create_aggregations: bool = True
) -> Generator[Tuple[int, int], None, None]:
    """
    Create a time series with compaction rules for downsampling.
    
    This creates:
    - A raw data series with high-frequency samples
    - Aggregated series (avg, min, max) at different time buckets
    
    Args:
        r: Redis client
        source_key: Key name for the source time series
        samples: Number of raw samples to create
        create_aggregations: Whether to create aggregation rules
        
    Yields:
        Tuple of (current_count, total_count) for progress tracking
    """
    # Create source time series
    try:
        r.execute_command(
            "TS.CREATE", source_key,
            "RETENTION", "86400000",  # 24 hours retention
            "LABELS", "type", "raw", "source", "main"
        )
    except Exception:
        pass
    
    if create_aggregations:
        # Create aggregation time series and rules
        aggregations = [
            (f"{source_key}:avg:1m", "avg", 60000),      # 1 minute average
            (f"{source_key}:min:1m", "min", 60000),      # 1 minute minimum
            (f"{source_key}:max:1m", "max", 60000),      # 1 minute maximum
            (f"{source_key}:avg:5m", "avg", 300000),     # 5 minute average
            (f"{source_key}:avg:1h", "avg", 3600000),    # 1 hour average
        ]
        
        for dest_key, agg_type, bucket_ms in aggregations:
            try:
                r.execute_command(
                    "TS.CREATE", dest_key,
                    "LABELS", "type", agg_type, "bucket", str(bucket_ms)
                )
                r.execute_command(
                    "TS.CREATERULE", source_key, dest_key,
                    "AGGREGATION", agg_type, str(bucket_ms)
                )
            except Exception:
                pass
    
    yield 0, samples
    
    # Add samples
    start_time = int((time.time() - 3600) * 1000)  # Start 1 hour ago
    interval_ms = 1000  # 1 second interval
    base_value = 50.0
    
    batch_size = 100
    for batch_start in range(0, samples, batch_size):
        batch_end = min(batch_start + batch_size, samples)
        
        madd_args = ["TS.MADD"]
        for i in range(batch_start, batch_end):
            timestamp = start_time + (i * interval_ms)
            # Generate value with some patterns
            hour_cycle = 10 * (1 + 0.5 * (i % 3600) / 3600)  # Hourly pattern
            noise = random.uniform(-5, 5)
            value = base_value + hour_cycle + noise
            madd_args.extend([source_key, str(timestamp), str(round(value, 2))])
        
        try:
            r.execute_command(*madd_args)
        except Exception:
            pass
        
        yield batch_end, samples
    
    yield samples, samples
