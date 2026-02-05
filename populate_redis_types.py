#!/usr/bin/env python3
"""
Redis Data Type Population Script
Populates Redis with different data types: strings, hashes, lists, sets, sorted sets, and streams
"""

import redis
import random
import string
import argparse
import sys

def populate_strings(r, count=100, key_prefix="string", value_size=100):
    """Populate Redis with string keys"""
    print(f"Populating {count} string keys...")
    for i in range(count):
        value = ''.join(random.choices(string.ascii_letters + string.digits, k=value_size))
        r.set(f"{key_prefix}:{i}", value)
        if (i + 1) % 10 == 0:
            print(f"  Created {i + 1}/{count} strings")
    print(f"✓ Created {count} string keys\n")


def populate_hashes(r, count=100, key_prefix="hash", fields_per_hash=4):
    """Populate Redis with hash keys"""
    print(f"Populating {count} hash keys...")
    for i in range(count):
        mapping = {
            "name": f"User_{i}",
            "age": random.randint(18, 80),
            "email": f"user{i}@example.com",
            "score": random.randint(0, 1000)
        }
        # Add extra fields if requested
        for j in range(fields_per_hash - 4):
            mapping[f"field_{j}"] = ''.join(random.choices(string.ascii_letters, k=20))
        r.hset(f"{key_prefix}:{i}", mapping=mapping)
        if (i + 1) % 10 == 0:
            print(f"  Created {i + 1}/{count} hashes")
    print(f"✓ Created {count} hash keys\n")


def populate_lists(r, count=10, key_prefix="list", items_per_list=100):
    """Populate Redis with list keys"""
    print(f"Populating {count} list keys...")
    for i in range(count):
        items = [f"item_{j}" for j in range(items_per_list)]
        r.lpush(f"{key_prefix}:{i}", *items)
        if (i + 1) % 5 == 0:
            print(f"  Created {i + 1}/{count} lists")
    print(f"✓ Created {count} list keys\n")


def populate_sets(r, count=100, key_prefix="set", members_per_set=50):
    """Populate Redis with set keys"""
    print(f"Populating {count} set keys...")
    for i in range(count):
        members = [f"member_{j}" for j in range(members_per_set)]
        r.sadd(f"{key_prefix}:{i}", *members)
        if (i + 1) % 10 == 0:
            print(f"  Created {i + 1}/{count} sets")
    print(f"✓ Created {count} set keys\n")


def populate_sorted_sets(r, count=100, key_prefix="zset", members_per_set=50):
    """Populate Redis with sorted set keys"""
    print(f"Populating {count} sorted set keys...")
    for i in range(count):
        mapping = {}
        for j in range(members_per_set):
            mapping[f"member_{j}"] = random.randint(0, 10000)
        r.zadd(f"{key_prefix}:{i}", mapping)
        if (i + 1) % 10 == 0:
            print(f"  Created {i + 1}/{count} sorted sets")
    print(f"✓ Created {count} sorted set keys\n")


def populate_streams(r, count=10, key_prefix="stream", entries_per_stream=100):
    """Populate Redis with stream keys"""
    print(f"Populating {count} stream keys...")
    for i in range(count):
        for j in range(entries_per_stream):
            r.xadd(f"{key_prefix}:{i}", {
                "field1": f"value_{j}",
                "field2": random.randint(0, 100),
                "field3": ''.join(random.choices(string.ascii_letters, k=20))
            })
        if (i + 1) % 5 == 0:
            print(f"  Created {i + 1}/{count} streams")
    print(f"✓ Created {count} stream keys\n")


def main():
    parser = argparse.ArgumentParser(description='Populate Redis with different data types')
    parser.add_argument('-h', '--host', default='localhost', help='Redis host (default: localhost)')
    parser.add_argument('-p', '--port', type=int, default=12000, help='Redis port (default: 12000)')
    parser.add_argument('--strings', type=int, default=100, help='Number of string keys to create')
    parser.add_argument('--hashes', type=int, default=100, help='Number of hash keys to create')
    parser.add_argument('--lists', type=int, default=10, help='Number of list keys to create')
    parser.add_argument('--sets', type=int, default=100, help='Number of set keys to create')
    parser.add_argument('--sorted-sets', type=int, default=100, help='Number of sorted set keys to create')
    parser.add_argument('--streams', type=int, default=10, help='Number of stream keys to create')
    parser.add_argument('--all', action='store_true', help='Populate all data types with default counts')
    parser.add_argument('--skip-strings', action='store_true', help='Skip populating strings')
    parser.add_argument('--skip-hashes', action='store_true', help='Skip populating hashes')
    parser.add_argument('--skip-lists', action='store_true', help='Skip populating lists')
    parser.add_argument('--skip-sets', action='store_true', help='Skip populating sets')
    parser.add_argument('--skip-sorted-sets', action='store_true', help='Skip populating sorted sets')
    parser.add_argument('--skip-streams', action='store_true', help='Skip populating streams')
    
    args = parser.parse_args()
    
    # Connect to Redis
    try:
        r = redis.Redis(host=args.host, port=args.port, decode_responses=True)
        r.ping()
        print(f"✓ Connected to Redis at {args.host}:{args.port}\n")
    except redis.ConnectionError as e:
        print(f"✗ Failed to connect to Redis: {e}")
        sys.exit(1)
    
    # Populate data types
    if args.all or not any([args.skip_strings, args.skip_hashes, args.skip_lists, 
                            args.skip_sets, args.skip_sorted_sets, args.skip_streams]):
        if not args.skip_strings:
            populate_strings(r, args.strings)
        if not args.skip_hashes:
            populate_hashes(r, args.hashes)
        if not args.skip_lists:
            populate_lists(r, args.lists)
        if not args.skip_sets:
            populate_sets(r, args.sets)
        if not args.skip_sorted_sets:
            populate_sorted_sets(r, args.sorted_sets)
        if not args.skip_streams:
            populate_streams(r, args.streams)
    else:
        # Only populate types that aren't skipped
        if not args.skip_strings:
            populate_strings(r, args.strings)
        if not args.skip_hashes:
            populate_hashes(r, args.hashes)
        if not args.skip_lists:
            populate_lists(r, args.lists)
        if not args.skip_sets:
            populate_sets(r, args.sets)
        if not args.skip_sorted_sets:
            populate_sorted_sets(r, args.sorted_sets)
        if not args.skip_streams:
            populate_streams(r, args.streams)
    
    print("=" * 60)
    print("✓ Population complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
