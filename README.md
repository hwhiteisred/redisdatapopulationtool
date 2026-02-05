# Redis Data Population Tool

A comprehensive Streamlit web application for populating Redis Enterprise databases with various data types for testing and development purposes.

## Features

- **Multi-profile Configuration**: Save and switch between different Redis clusters
- **Module Detection**: Automatically detects available Redis modules (RedisJSON, RediSearch, RedisTimeSeries, RedisBloom)
- **All Redis Enterprise Data Types**:
  - **Core Types**: Strings, Hashes, Lists, Sets, Sorted Sets, Streams, HyperLogLog, Geospatial, Bitmaps
  - **RedisJSON**: Nested JSON documents (product/user schemas)
  - **RediSearch**: Full-text search indexes with documents
  - **RedisTimeSeries**: Time series metrics (sensor, stock, server, IoT)
  - **RedisBloom**: Bloom filters, Cuckoo filters, Count-Min Sketch, Top-K
- **Progress Tracking**: Real-time progress bars during data population
- **Configurable Parameters**: Count, key prefixes, and type-specific options

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd "RE playground"

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Running the Web UI

```bash
streamlit run app.py
```

This opens the web interface at `http://localhost:8501`.

### Configuration

1. Copy the example config file:
```bash
cp config.example.json config.json
```

2. Edit `config.json` with your Redis connection details:
```json
{
  "profiles": {
    "local": {
      "name": "Local Redis",
      "host": "localhost",
      "port": 6379,
      "password": "",
      "ssl": false
    }
  },
  "active_profile": "local"
}
```

You can also add, edit, and delete profiles through the web UI sidebar.

**Note:** `config.json` is gitignored to prevent accidentally committing credentials.

## Development

### Running Tests

```bash
# Install test dependencies
pip install -r requirements-dev.txt

# Run all tests
pytest

# Run with coverage
pytest --cov=lib --cov-report=html

# Run specific test file
pytest tests/test_populators.py

# Run with verbose output
pytest -v
```

### Project Structure

```
RE playground/
├── app.py                    # Streamlit web application
├── config.example.json       # Example connection profiles (copy to config.json)
├── requirements.txt          # Production dependencies
├── requirements-dev.txt      # Development/test dependencies
├── README.md
├── lib/
│   ├── __init__.py
│   ├── config.py             # Profile management
│   ├── connection.py         # Redis connection + module detection
│   ├── monitoring.py         # Monitoring tools (bigkeys, memkeys, slowlog)
│   ├── backup.py             # Redis Enterprise backup management via REST API
│   └── populators/
│       ├── __init__.py
│       ├── core.py           # Core Redis types
│       ├── json_search.py    # RedisJSON + RediSearch
│       ├── timeseries.py     # RedisTimeSeries
│       └── bloom.py          # RedisBloom types
└── tests/
    ├── __init__.py
    ├── conftest.py           # Pytest fixtures
    ├── test_config.py        # Config module tests
    ├── test_connection.py    # Connection module tests
    └── test_populators.py    # Populator function tests
```

## Requirements

- Python 3.8+
- Redis server (for actual data population)
- Redis Enterprise (for module-specific features)

## License

MIT
