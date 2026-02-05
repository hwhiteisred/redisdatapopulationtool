"""
Tests for the populator modules.
"""

import pytest
from unittest.mock import MagicMock, call, patch


class TestCorePopulators:
    """Tests for core Redis type populators."""
    
    def test_populate_strings(self, mock_redis_client):
        """Test string population."""
        from lib.populators.core import populate_strings
        
        # Run populator
        results = list(populate_strings(mock_redis_client, count=10, batch_size=5))
        
        # Should yield progress updates
        assert len(results) >= 1
        assert results[-1] == (10, 10)  # Final progress
    
    def test_populate_strings_with_mock(self):
        """Test string population with mocked client."""
        from lib.populators.core import populate_strings
        
        mock_client = MagicMock()
        
        results = list(populate_strings(
            mock_client, 
            count=5, 
            key_prefix="test_str",
            value_size=50,
            batch_size=3
        ))
        
        # Verify set was called for each key (no cross-key pipeline for cluster compatibility)
        assert mock_client.set.call_count == 5
        assert results[-1] == (5, 5)
    
    def test_populate_hashes(self):
        """Test hash population."""
        from lib.populators.core import populate_hashes
        
        mock_client = MagicMock()
        
        results = list(populate_hashes(
            mock_client,
            count=5,
            key_prefix="test_hash",
            fields_per_hash=3,
            batch_size=2
        ))
        
        # Direct calls (no cross-key pipeline for cluster compatibility)
        assert mock_client.hset.call_count == 5
        assert results[-1] == (5, 5)
    
    def test_populate_lists(self):
        """Test list population."""
        from lib.populators.core import populate_lists
        
        mock_client = MagicMock()
        
        results = list(populate_lists(
            mock_client,
            count=3,
            key_prefix="test_list",
            items_per_list=10,
            batch_size=2
        ))
        
        # Direct calls (no cross-key pipeline for cluster compatibility)
        assert mock_client.rpush.call_count == 3
        assert results[-1] == (3, 3)
    
    def test_populate_sets(self):
        """Test set population."""
        from lib.populators.core import populate_sets
        
        mock_client = MagicMock()
        
        results = list(populate_sets(
            mock_client,
            count=5,
            key_prefix="test_set",
            members_per_set=20,
            batch_size=3
        ))
        
        # Direct calls (no cross-key pipeline for cluster compatibility)
        assert mock_client.sadd.call_count == 5
        assert results[-1] == (5, 5)
    
    def test_populate_sorted_sets(self):
        """Test sorted set population."""
        from lib.populators.core import populate_sorted_sets
        
        mock_client = MagicMock()
        
        results = list(populate_sorted_sets(
            mock_client,
            count=5,
            key_prefix="test_zset",
            members_per_set=10,
            batch_size=3
        ))
        
        # Direct calls (no cross-key pipeline for cluster compatibility)
        assert mock_client.zadd.call_count == 5
        assert results[-1] == (5, 5)
    
    def test_populate_streams(self):
        """Test stream population."""
        from lib.populators.core import populate_streams
        
        mock_client = MagicMock()
        mock_pipeline = MagicMock()
        mock_client.pipeline.return_value = mock_pipeline
        
        results = list(populate_streams(
            mock_client,
            count=2,
            key_prefix="test_stream",
            entries_per_stream=10,
            batch_size=1
        ))
        
        # Each stream gets entries_per_stream xadd calls
        assert mock_pipeline.xadd.call_count == 20  # 2 streams * 10 entries
        assert results[-1] == (2, 2)
    
    def test_populate_hyperloglog(self):
        """Test HyperLogLog population."""
        from lib.populators.core import populate_hyperloglog
        
        mock_client = MagicMock()
        mock_pipeline = MagicMock()
        mock_client.pipeline.return_value = mock_pipeline
        
        results = list(populate_hyperloglog(
            mock_client,
            count=2,
            key_prefix="test_hll",
            elements_per_hll=200,
            batch_size=1
        ))
        
        # pfadd should be called for each chunk
        assert mock_pipeline.pfadd.called
        assert results[-1] == (2, 2)
    
    def test_populate_geospatial(self):
        """Test geospatial population."""
        from lib.populators.core import populate_geospatial
        
        mock_client = MagicMock()
        mock_pipeline = MagicMock()
        mock_client.pipeline.return_value = mock_pipeline
        
        results = list(populate_geospatial(
            mock_client,
            count=2,
            key_prefix="test_geo",
            locations_per_key=10,
            batch_size=1
        ))
        
        # geoadd should be called for each location
        assert mock_pipeline.geoadd.call_count == 20  # 2 keys * 10 locations
        assert results[-1] == (2, 2)
    
    def test_populate_bitmaps(self):
        """Test bitmap population."""
        from lib.populators.core import populate_bitmaps
        
        mock_client = MagicMock()
        mock_pipeline = MagicMock()
        mock_client.pipeline.return_value = mock_pipeline
        
        results = list(populate_bitmaps(
            mock_client,
            count=2,
            key_prefix="test_bitmap",
            bits_per_bitmap=100,
            density=0.5,  # 50% of bits should be set
            batch_size=1
        ))
        
        # setbit should be called (approximately 50% of 100 bits per bitmap)
        assert mock_pipeline.setbit.called
        assert results[-1] == (2, 2)


class TestJsonSearchPopulators:
    """Tests for JSON and Search populators."""
    
    def test_populate_json(self):
        """Test JSON document population."""
        from lib.populators.json_search import populate_json
        
        mock_client = MagicMock()
        
        results = list(populate_json(
            mock_client,
            count=5,
            key_prefix="test_json",
            doc_type="product",
            batch_size=3
        ))
        
        # Should call JSON.SET via execute_command (no cross-key pipeline)
        assert mock_client.execute_command.call_count == 5
        assert results[-1] == (5, 5)
    
    def test_populate_json_user_type(self):
        """Test JSON population with user document type."""
        from lib.populators.json_search import populate_json
        
        mock_client = MagicMock()
        
        results = list(populate_json(
            mock_client,
            count=3,
            key_prefix="test_user",
            doc_type="user",
            batch_size=2
        ))
        
        assert mock_client.execute_command.call_count == 3
        # Verify JSON.SET command structure
        calls = mock_client.execute_command.call_args_list
        for c in calls:
            assert c[0][0] == "JSON.SET"
    
    def test_populate_search_index_product(self):
        """Test search index creation with product documents."""
        from lib.populators.json_search import populate_search_index
        
        mock_client = MagicMock()
        
        results = list(populate_search_index(
            mock_client,
            count=5,
            index_name="idx:test",
            key_prefix="product",
            doc_type="product"
        ))
        
        # Should try to drop existing index
        mock_client.execute_command.assert_any_call("FT.DROPINDEX", "idx:test")
        
        # Should create index with FT.CREATE
        create_calls = [c for c in mock_client.execute_command.call_args_list 
                       if c[0][0] == "FT.CREATE"]
        assert len(create_calls) == 1
        
        # Should populate documents as hashes (no cross-key pipeline)
        assert mock_client.hset.call_count == 5


class TestTimeseriesPopulators:
    """Tests for TimeSeries populators."""
    
    def test_populate_timeseries_sensor(self):
        """Test time series population with sensor type."""
        from lib.populators.timeseries import populate_timeseries
        
        mock_client = MagicMock()
        
        results = list(populate_timeseries(
            mock_client,
            count=2,
            key_prefix="test_ts",
            samples_per_series=100,
            metric_type="sensor"
        ))
        
        # Should create time series
        create_calls = [c for c in mock_client.execute_command.call_args_list 
                       if c[0][0] == "TS.CREATE"]
        assert len(create_calls) == 2
        
        # Should add samples
        madd_calls = [c for c in mock_client.execute_command.call_args_list 
                     if c[0][0] == "TS.MADD"]
        assert len(madd_calls) >= 2
        
        assert results[-1] == (2, 2)
    
    def test_populate_timeseries_different_types(self):
        """Test time series population with different metric types."""
        from lib.populators.timeseries import populate_timeseries
        
        for metric_type in ["sensor", "stock", "server", "iot"]:
            mock_client = MagicMock()
            
            results = list(populate_timeseries(
                mock_client,
                count=1,
                samples_per_series=10,
                metric_type=metric_type
            ))
            
            assert results[-1] == (1, 1)


class TestBloomPopulators:
    """Tests for RedisBloom populators."""
    
    def test_populate_bloom_filter(self):
        """Test Bloom filter population."""
        from lib.populators.bloom import populate_bloom_filter
        
        mock_client = MagicMock()
        
        results = list(populate_bloom_filter(
            mock_client,
            count=2,
            key_prefix="test_bf",
            items_per_filter=100,
            error_rate=0.01,
            batch_size=50
        ))
        
        # Should reserve bloom filters
        reserve_calls = [c for c in mock_client.execute_command.call_args_list 
                        if c[0][0] == "BF.RESERVE"]
        assert len(reserve_calls) == 2
        
        # Should add items
        madd_calls = [c for c in mock_client.execute_command.call_args_list 
                     if c[0][0] == "BF.MADD"]
        assert len(madd_calls) >= 2
        
        assert results[-1] == (2, 2)
    
    def test_populate_cuckoo_filter(self):
        """Test Cuckoo filter population."""
        from lib.populators.bloom import populate_cuckoo_filter
        
        mock_client = MagicMock()
        
        results = list(populate_cuckoo_filter(
            mock_client,
            count=2,
            key_prefix="test_cf",
            items_per_filter=100,
            bucket_size=2,
            batch_size=50
        ))
        
        # Should reserve cuckoo filters
        reserve_calls = [c for c in mock_client.execute_command.call_args_list 
                        if c[0][0] == "CF.RESERVE"]
        assert len(reserve_calls) == 2
        
        assert results[-1] == (2, 2)
    
    def test_populate_countmin_sketch(self):
        """Test Count-Min Sketch population."""
        from lib.populators.bloom import populate_countmin_sketch
        
        mock_client = MagicMock()
        
        results = list(populate_countmin_sketch(
            mock_client,
            count=2,
            key_prefix="test_cms",
            unique_items=50,
            total_increments=500,
            width=1000,
            depth=3
        ))
        
        # Should initialize CMS
        init_calls = [c for c in mock_client.execute_command.call_args_list 
                     if c[0][0] == "CMS.INITBYDIM"]
        assert len(init_calls) == 2
        
        # Should increment counts
        incr_calls = [c for c in mock_client.execute_command.call_args_list 
                     if c[0][0] == "CMS.INCRBY"]
        assert len(incr_calls) >= 2
        
        assert results[-1] == (2, 2)
    
    def test_populate_topk(self):
        """Test Top-K population."""
        from lib.populators.bloom import populate_topk
        
        mock_client = MagicMock()
        
        results = list(populate_topk(
            mock_client,
            count=2,
            key_prefix="test_topk",
            k=10,
            unique_items=100,
            total_adds=500
        ))
        
        # Should reserve Top-K
        reserve_calls = [c for c in mock_client.execute_command.call_args_list 
                        if c[0][0] == "TOPK.RESERVE"]
        assert len(reserve_calls) == 2
        
        # Should add items
        add_calls = [c for c in mock_client.execute_command.call_args_list 
                    if c[0][0] == "TOPK.ADD"]
        assert len(add_calls) >= 2
        
        assert results[-1] == (2, 2)


class TestPopulatorProgressTracking:
    """Tests for progress tracking in populators."""
    
    def test_progress_yields_correct_format(self):
        """Test that all populators yield (current, total) tuples."""
        from lib.populators.core import populate_strings
        
        mock_client = MagicMock()
        
        for current, total in populate_strings(mock_client, count=10, batch_size=3):
            assert isinstance(current, int)
            assert isinstance(total, int)
            assert current <= total
            assert total == 10
    
    def test_final_progress_equals_total(self):
        """Test that final progress equals total count."""
        from lib.populators.core import populate_hashes
        
        mock_client = MagicMock()
        
        results = list(populate_hashes(mock_client, count=7, batch_size=3))
        
        final_current, final_total = results[-1]
        assert final_current == final_total == 7
