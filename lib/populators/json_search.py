"""
RedisJSON and RediSearch data type populators.

Note: These populators are designed to work with Redis Cluster / Redis Enterprise
with multiple shards. They avoid cross-key pipelines which would fail when keys
hash to different slots.
"""

import random
import string
import json as json_lib
from typing import Generator, Tuple, List, Dict, Any
import redis


def _random_string(length: int) -> str:
    """Generate a random alphanumeric string."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def _random_sentence(word_count: int = 10) -> str:
    """Generate a random sentence for text search testing."""
    words = [
        "the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
        "redis", "database", "cache", "memory", "fast", "scalable", "distributed",
        "cloud", "server", "client", "data", "store", "key", "value", "hash",
        "list", "set", "stream", "search", "index", "query", "filter",
        "product", "user", "order", "item", "price", "stock", "category",
        "name", "description", "rating", "review", "comment", "author"
    ]
    return ' '.join(random.choices(words, k=word_count))


def _generate_product() -> Dict[str, Any]:
    """Generate a random product document."""
    categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys"]
    brands = ["Acme", "TechCorp", "StyleCo", "HomeBrand", "SportMax", "BookWorld"]
    
    return {
        "name": f"Product {_random_string(8)}",
        "description": _random_sentence(20),
        "category": random.choice(categories),
        "brand": random.choice(brands),
        "price": round(random.uniform(9.99, 999.99), 2),
        "stock": random.randint(0, 1000),
        "rating": round(random.uniform(1.0, 5.0), 1),
        "reviews_count": random.randint(0, 5000),
        "tags": random.sample(["sale", "new", "popular", "featured", "limited", "exclusive"], k=random.randint(1, 3)),
        "specs": {
            "weight": f"{random.uniform(0.1, 50.0):.2f} kg",
            "dimensions": f"{random.randint(5, 100)}x{random.randint(5, 100)}x{random.randint(5, 100)} cm",
            "color": random.choice(["red", "blue", "green", "black", "white", "silver"]),
        }
    }


def _generate_user() -> Dict[str, Any]:
    """Generate a random user document."""
    first_names = ["John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Seattle", "Boston"]
    
    first = random.choice(first_names)
    last = random.choice(last_names)
    
    return {
        "firstName": first,
        "lastName": last,
        "fullName": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}@example.com",
        "age": random.randint(18, 80),
        "city": random.choice(cities),
        "bio": _random_sentence(15),
        "interests": random.sample(["sports", "music", "movies", "gaming", "reading", "travel", "cooking"], k=random.randint(2, 4)),
        "settings": {
            "notifications": random.choice([True, False]),
            "theme": random.choice(["light", "dark", "auto"]),
            "language": random.choice(["en", "es", "fr", "de"]),
        },
        "stats": {
            "posts": random.randint(0, 1000),
            "followers": random.randint(0, 10000),
            "following": random.randint(0, 500),
        }
    }


def populate_json(
    r: redis.Redis,
    count: int = 100,
    key_prefix: str = "json",
    doc_type: str = "product",
    batch_size: int = 50
) -> Generator[Tuple[int, int], None, None]:
    """
    Populate Redis with JSON documents using RedisJSON.
    
    Args:
        r: Redis client
        count: Number of JSON documents to create
        key_prefix: Prefix for key names
        doc_type: Type of document to generate ("product" or "user")
        batch_size: Progress update frequency
        
    Yields:
        Tuple of (current_count, total_count) for progress tracking
    """
    generator = _generate_product if doc_type == "product" else _generate_user
    
    for i in range(count):
        doc = generator()
        doc["id"] = i
        
        # Use JSON.SET command - execute directly (no cross-key pipeline)
        r.execute_command("JSON.SET", f"{key_prefix}:{i}", "$", json_lib.dumps(doc))
        
        if (i + 1) % batch_size == 0:
            yield i + 1, count
    
    yield count, count


def populate_search_index(
    r: redis.Redis,
    count: int = 100,
    index_name: str = "idx:products",
    key_prefix: str = "product",
    doc_type: str = "product"
) -> Generator[Tuple[int, int], None, None]:
    """
    Populate Redis with searchable documents and create a RediSearch index.
    
    This creates both the search index schema and populates documents.
    
    Args:
        r: Redis client
        count: Number of documents to create
        index_name: Name for the search index
        key_prefix: Prefix for key names
        doc_type: Type of document to generate ("product" or "user")
        
    Yields:
        Tuple of (current_count, total_count) for progress tracking
    """
    # First, try to drop any existing index
    try:
        r.execute_command("FT.DROPINDEX", index_name)
    except Exception:
        pass  # Index doesn't exist, that's fine
    
    # Create the index schema based on doc_type
    if doc_type == "product":
        # Create product index
        r.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", f"{key_prefix}:",
            "SCHEMA",
            "name", "TEXT", "SORTABLE",
            "description", "TEXT",
            "category", "TAG", "SORTABLE",
            "brand", "TAG",
            "price", "NUMERIC", "SORTABLE",
            "stock", "NUMERIC",
            "rating", "NUMERIC", "SORTABLE",
            "reviews_count", "NUMERIC",
        )
        
        yield 0, count
        
        # Populate documents as hashes (no cross-key pipeline)
        batch_size = 50
        
        for i in range(count):
            doc = _generate_product()
            # Flatten for hash storage
            flat_doc = {
                "name": doc["name"],
                "description": doc["description"],
                "category": doc["category"],
                "brand": doc["brand"],
                "price": str(doc["price"]),
                "stock": str(doc["stock"]),
                "rating": str(doc["rating"]),
                "reviews_count": str(doc["reviews_count"]),
                "tags": ",".join(doc["tags"]),
            }
            r.hset(f"{key_prefix}:{i}", mapping=flat_doc)
            
            if (i + 1) % batch_size == 0:
                yield i + 1, count
    
    else:  # user
        # Create user index
        r.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", f"{key_prefix}:",
            "SCHEMA",
            "fullName", "TEXT", "SORTABLE",
            "email", "TEXT",
            "bio", "TEXT",
            "city", "TAG", "SORTABLE",
            "age", "NUMERIC", "SORTABLE",
        )
        
        yield 0, count
        
        # Populate user documents (no cross-key pipeline)
        batch_size = 50
        
        for i in range(count):
            doc = _generate_user()
            flat_doc = {
                "fullName": doc["fullName"],
                "firstName": doc["firstName"],
                "lastName": doc["lastName"],
                "email": doc["email"],
                "bio": doc["bio"],
                "city": doc["city"],
                "age": str(doc["age"]),
            }
            r.hset(f"{key_prefix}:{i}", mapping=flat_doc)
            
            if (i + 1) % batch_size == 0:
                yield i + 1, count
    
    yield count, count
