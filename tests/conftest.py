"""Test configuration and fixtures for simple_queue tests."""
import pytest
from unittest.mock import MagicMock


@pytest.fixture
def mock_redis_client():
  """Create a mock Redis client with common methods."""
  mock_client = MagicMock()
  mock_client.ping.return_value = True
  mock_client.get.return_value = None
  mock_client.set.return_value = True
  mock_client.exists.return_value = True
  mock_client.llen.return_value = 0
  mock_client.rpush.return_value = 1
  mock_client.blpop.return_value = None
  mock_client.close.return_value = None
  return mock_client


@pytest.fixture
def redis_uri():
  """Standard Redis URI for testing."""
  return "redis://localhost:6379/0"


@pytest.fixture
def queue_name():
  """Standard queue name for testing."""
  return "test_queue"


@pytest.fixture
def queue_size():
  """Standard queue size for testing."""
  return 100


@pytest.fixture
def retry_times():
  """Standard retry times for testing."""
  return 3


@pytest.fixture
def retry_delay():
  """Standard retry delay for testing."""
  return 0.1
