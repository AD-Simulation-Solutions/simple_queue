"""Tests for QueueConnection class."""
import pytest
import redis
from unittest.mock import patch, MagicMock
from simple_queue.core import QueueConnection


class TestQueueConnection:
  """Test suite for QueueConnection class."""

  @patch('simple_queue.core.redis.from_url')
  def test_init_creates_connection(self, mock_from_url, redis_uri, mock_redis_client):
    """Test that __init__ establishes a Redis connection."""
    mock_from_url.return_value = mock_redis_client

    conn = QueueConnection(redis_uri)

    assert conn.uri == redis_uri
    assert conn.client is not None
    mock_from_url.assert_called_once()
    mock_redis_client.ping.assert_called_once()

  @patch('simple_queue.core.redis.from_url')
  def test_connect_closes_existing_connection(self, mock_from_url, redis_uri, mock_redis_client):
    """Test that _connect closes existing connection before creating new one."""
    mock_from_url.return_value = mock_redis_client

    conn = QueueConnection(redis_uri)
    old_client = conn.client
    conn._connect()

    assert old_client.close.called

  @patch('simple_queue.core.redis.from_url')
  def test_connect_handles_close_exception(self, mock_from_url, redis_uri, mock_redis_client):
    """Test that _connect handles exceptions when closing old connection."""
    mock_from_url.return_value = mock_redis_client
    conn = QueueConnection(redis_uri)

    conn.client.close.side_effect = Exception("Close error")
    conn._connect()  # Should not raise

    assert conn.client is not None

  @patch('simple_queue.core.redis.from_url')
  def test_ensure_connected_passes_when_connected(self, mock_from_url, redis_uri, mock_redis_client):
    """Test that _ensure_connected passes when connection is active."""
    mock_from_url.return_value = mock_redis_client
    conn = QueueConnection(redis_uri)

    conn._ensure_connected()

    mock_redis_client.ping.assert_called()

  @patch('simple_queue.core.redis.from_url')
  @patch('simple_queue.core.time.sleep')
  def test_ensure_connected_reconnects_on_connection_error(self, mock_sleep, mock_from_url, redis_uri):
    """Test that _ensure_connected reconnects when connection fails."""
    mock_client = MagicMock()
    # Initial connection succeeds
    mock_client.ping.return_value = True
    mock_from_url.return_value = mock_client

    conn = QueueConnection(redis_uri)
    # Reset the mock after initialization
    mock_client.ping.reset_mock()
    # Now test reconnection: first ping fails, then succeeds after reconnect
    mock_client.ping.side_effect = [redis.ConnectionError("Connection lost"), True]

    conn._ensure_connected()

    assert mock_client.ping.call_count >= 2

  @patch('simple_queue.core.redis.from_url')
  @patch('simple_queue.core.time.sleep')
  def test_ensure_connected_reconnects_on_timeout_error(self, mock_sleep, mock_from_url, redis_uri):
    """Test that _ensure_connected reconnects on timeout."""
    mock_client = MagicMock()
    # Initial connection succeeds
    mock_client.ping.return_value = True
    mock_from_url.return_value = mock_client

    conn = QueueConnection(redis_uri)
    # Reset the mock after initialization
    mock_client.ping.reset_mock()
    # Now test reconnection: first ping fails, then succeeds after reconnect
    mock_client.ping.side_effect = [redis.TimeoutError("Timeout"), True]

    conn._ensure_connected()

    assert mock_client.ping.call_count >= 2

  @patch('simple_queue.core.redis.from_url')
  @patch('simple_queue.core.time.sleep')
  def test_ensure_connected_handles_none_client(self, mock_sleep, mock_from_url, redis_uri, mock_redis_client):
    """Test that _ensure_connected handles None client."""
    mock_from_url.return_value = mock_redis_client
    conn = QueueConnection(redis_uri)
    conn.client = None

    conn._ensure_connected()

    assert conn.client is not None

  @patch('simple_queue.core.redis.from_url')
  @patch('simple_queue.core.time.sleep')
  def test_ensure_connected_raises_after_max_retries(self, mock_sleep, mock_from_url, redis_uri):
    """Test that _ensure_connected raises ConnectionError after max retries."""
    mock_client = MagicMock()
    mock_client.ping.side_effect = redis.ConnectionError("Persistent error")
    mock_from_url.side_effect = Exception("Can't reconnect")

    conn = QueueConnection.__new__(QueueConnection)
    conn.uri = redis_uri
    conn.client = mock_client

    with pytest.raises(ConnectionError, match="Failed to reconnect after 3 attempts"):
      conn._ensure_connected(retry_count=0, max_retries=3)

  @patch('simple_queue.core.redis.from_url')
  @patch('simple_queue.core.time.sleep')
  def test_ensure_connected_exponential_backoff(self, mock_sleep, mock_from_url, redis_uri):
    """Test that _ensure_connected uses exponential backoff on retries."""
    mock_client = MagicMock()
    # First call for init, subsequent calls fail then succeed
    mock_from_url.side_effect = [
        mock_client,
        Exception("Fail 1"),
        mock_client
    ]
    mock_client.ping.return_value = True

    conn = QueueConnection(redis_uri)

    # Reset and setup failure scenario
    mock_from_url.side_effect = [Exception("Fail 1"), mock_client]
    mock_client.ping.side_effect = [redis.ConnectionError("Error"), True, True]

    conn._ensure_connected(retry_count=0, max_retries=3)

    # Should have slept with exponential backoff
    assert mock_sleep.called

  @patch('simple_queue.core.redis.from_url')
  def test_close_closes_client(self, mock_from_url, redis_uri, mock_redis_client):
    """Test that close() closes the Redis client."""
    mock_from_url.return_value = mock_redis_client
    conn = QueueConnection(redis_uri)

    conn.close()

    mock_redis_client.close.assert_called_once()
    assert conn.client is None

  @patch('simple_queue.core.redis.from_url')
  def test_close_handles_exception(self, mock_from_url, redis_uri, mock_redis_client):
    """Test that close() handles exceptions gracefully."""
    mock_from_url.return_value = mock_redis_client
    mock_redis_client.close.side_effect = Exception("Close error")
    conn = QueueConnection(redis_uri)

    conn.close()  # Should not raise

    assert conn.client is None

  @patch('simple_queue.core.redis.from_url')
  def test_close_when_client_is_none(self, mock_from_url, redis_uri):
    """Test that close() handles None client gracefully."""
    conn = QueueConnection.__new__(QueueConnection)
    conn.client = None

    conn.close()  # Should not raise

    assert conn.client is None
