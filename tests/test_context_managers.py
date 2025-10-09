"""Tests for context manager functions."""
import pytest
from unittest.mock import patch, MagicMock
from simple_queue.core import queue_publisher, queue_consumer


class TestQueuePublisherContextManager:
  """Test suite for queue_publisher context manager function."""

  @patch('simple_queue.core.QueueConnection')
  def test_queue_publisher_context_manager(self, mock_connection_class, queue_name, redis_uri,
                                           queue_size, retry_times, retry_delay, mock_redis_client):
    """Test that queue_publisher context manager works correctly."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_connection_class.return_value = mock_conn_instance

    with queue_publisher(queue_name, redis_uri, queue_size, retry_times, retry_delay) as pub:
      assert pub is not None
      assert pub.queue_name == queue_name
      assert pub.connection is not None

    # Connection should be closed after exiting context
    mock_conn_instance.close.assert_called_once()

  @patch('simple_queue.core.QueueConnection')
  def test_queue_publisher_closes_on_exception(self, mock_connection_class, queue_name, redis_uri,
                                               queue_size, retry_times, retry_delay, mock_redis_client):
    """Test that queue_publisher closes connection even on exception."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_connection_class.return_value = mock_conn_instance

    with pytest.raises(ValueError):
      with queue_publisher(queue_name, redis_uri, queue_size, retry_times, retry_delay):
        raise ValueError("Test error")

    # Connection should still be closed
    mock_conn_instance.close.assert_called_once()

  @patch('simple_queue.core.QueueConnection')
  def test_queue_publisher_can_push_messages(self, mock_connection_class, queue_name, redis_uri,
                                             queue_size, retry_times, retry_delay, mock_redis_client):
    """Test that messages can be pushed using context manager."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_redis_client.llen.return_value = 0
    mock_redis_client.exists.return_value = True
    mock_connection_class.return_value = mock_conn_instance

    with queue_publisher(queue_name, redis_uri, queue_size, retry_times, retry_delay) as pub:
      result = pub.push("test message")

    assert result is True
    mock_redis_client.rpush.assert_called()


class TestQueueConsumerContextManager:
  """Test suite for queue_consumer context manager function."""

  @patch('simple_queue.core.QueueConnection')
  def test_queue_consumer_context_manager(self, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that queue_consumer context manager works correctly."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_connection_class.return_value = mock_conn_instance

    with queue_consumer(queue_name, redis_uri, queue_size) as cons:
      assert cons is not None
      assert cons.queue_name == queue_name
      assert cons.connection is not None

    # Connection should be closed after exiting context
    mock_conn_instance.close.assert_called_once()

  @patch('simple_queue.core.QueueConnection')
  def test_queue_consumer_closes_on_exception(self, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that queue_consumer closes connection even on exception."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_connection_class.return_value = mock_conn_instance

    with pytest.raises(ValueError):
      with queue_consumer(queue_name, redis_uri, queue_size):
        raise ValueError("Test error")

    # Connection should still be closed
    mock_conn_instance.close.assert_called_once()

  @patch('simple_queue.core.QueueConnection')
  def test_queue_consumer_can_read_messages(self, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that messages can be read using context manager."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_redis_client.blpop.return_value = (queue_name.encode(), b"test message")
    mock_connection_class.return_value = mock_conn_instance

    with queue_consumer(queue_name, redis_uri, queue_size) as cons:
      result = cons.read(timeout=5)

    assert result == "test message"
    mock_redis_client.blpop.assert_called()
