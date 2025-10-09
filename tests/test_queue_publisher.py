"""Tests for QueuePublisher class."""
import pytest
import json
from unittest.mock import patch, MagicMock
from simple_queue.core import QueuePublisher, QueueNotFoundError


class TestQueuePublisher:
  """Test suite for QueuePublisher class."""

  def test_init_sets_attributes(self, queue_name, redis_uri, queue_size, retry_times, retry_delay):
    """Test that __init__ sets all attributes correctly."""
    publisher = QueuePublisher(queue_name, redis_uri, queue_size, retry_times, retry_delay)

    assert publisher.queue_name == queue_name
    assert publisher.uri == redis_uri
    assert publisher.queue_size == queue_size
    assert publisher.retry_times == retry_times
    assert publisher.retry_delay == retry_delay
    assert publisher.connection is None
    assert publisher._config_key == f"{queue_name}:config"

  @patch('simple_queue.core.QueueConnection')
  def test_enter_creates_connection(self, mock_connection_class, queue_name, redis_uri, queue_size, retry_times, retry_delay, mock_redis_client):
    """Test that __enter__ creates connection and sets up queue."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_connection_class.return_value = mock_conn_instance

    publisher = QueuePublisher(queue_name, redis_uri, queue_size, retry_times, retry_delay)
    result = publisher.__enter__()

    assert publisher.connection is not None
    assert result is publisher
    mock_connection_class.assert_called_once_with(redis_uri)

  @patch('simple_queue.core.QueueConnection')
  def test_exit_closes_connection(self, mock_connection_class, queue_name, redis_uri, queue_size, retry_times, retry_delay, mock_redis_client):
    """Test that __exit__ closes the connection."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_connection_class.return_value = mock_conn_instance

    publisher = QueuePublisher(queue_name, redis_uri, queue_size, retry_times, retry_delay)
    publisher.__enter__()
    publisher.__exit__(None, None, None)

    mock_conn_instance.close.assert_called_once()

  @patch('simple_queue.core.QueueConnection')
  def test_setup_queue_creates_new_config(self, mock_connection_class, queue_name, redis_uri,
                                          queue_size, retry_times, retry_delay, mock_redis_client):
    """Test that _setup_queue creates new config when none exists."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_redis_client.get.return_value = None
    mock_connection_class.return_value = mock_conn_instance

    publisher = QueuePublisher(queue_name, redis_uri, queue_size, retry_times, retry_delay)
    publisher.__enter__()

    expected_config = json.dumps({'max_length': queue_size})
    mock_redis_client.set.assert_called_with(f"{queue_name}:config", expected_config)

  @patch('simple_queue.core.QueueConnection')
  def test_setup_queue_loads_existing_config(self, mock_connection_class, queue_name, redis_uri,
                                             queue_size, retry_times, retry_delay, mock_redis_client):
    """Test that _setup_queue loads existing config."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    existing_config = json.dumps({'max_length': 200})
    mock_redis_client.get.return_value = existing_config
    mock_connection_class.return_value = mock_conn_instance

    publisher = QueuePublisher(queue_name, redis_uri, 100, retry_times, retry_delay)
    publisher.__enter__()

    assert publisher.queue_size == 200

  @patch('simple_queue.core.QueueConnection')
  def test_set_queue_size_updates_config(self, mock_connection_class, queue_name, redis_uri, queue_size, retry_times, retry_delay, mock_redis_client):
    """Test that set_queue_size updates the queue size."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_connection_class.return_value = mock_conn_instance

    publisher = QueuePublisher(queue_name, redis_uri, queue_size, retry_times, retry_delay)
    publisher.__enter__()

    new_size = 200
    publisher.set_queue_size(new_size)

    assert publisher.queue_size == new_size
    expected_config = json.dumps({'max_length': new_size})
    # Should be called twice: once in setup, once in set_queue_size
    calls = [call for call in mock_redis_client.set.call_args_list if call[0][1] == expected_config]
    assert len(calls) > 0

  @patch('simple_queue.core.QueueConnection')
  def test_update_queue_size_from_config_success(self, mock_connection_class, queue_name, redis_uri,
                                                 queue_size, retry_times, retry_delay, mock_redis_client):
    """Test that _update_queue_size_from_config loads size from Redis."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_redis_client.exists.return_value = True
    mock_redis_client.get.return_value = json.dumps({'max_length': 150})
    mock_connection_class.return_value = mock_conn_instance

    publisher = QueuePublisher(queue_name, redis_uri, queue_size, retry_times, retry_delay)
    publisher.__enter__()
    publisher._update_queue_size_from_config()

    assert publisher.queue_size == 150

  @patch('simple_queue.core.QueueConnection')
  def test_update_queue_size_raises_when_config_not_found(
          self, mock_connection_class, queue_name, redis_uri, queue_size, retry_times, retry_delay, mock_redis_client):
    """Test that _update_queue_size_from_config raises QueueNotFoundError."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_redis_client.exists.return_value = False
    mock_connection_class.return_value = mock_conn_instance

    publisher = QueuePublisher(queue_name, redis_uri, queue_size, retry_times, retry_delay)
    publisher.__enter__()

    with pytest.raises(QueueNotFoundError):
      publisher._update_queue_size_from_config()

  @patch('simple_queue.core.QueueConnection')
  def test_push_dict_message_success(self, mock_connection_class, queue_name, redis_uri, queue_size, retry_times, retry_delay, mock_redis_client):
    """Test pushing a dict message successfully."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_redis_client.llen.return_value = 0
    mock_redis_client.exists.return_value = True
    mock_connection_class.return_value = mock_conn_instance

    publisher = QueuePublisher(queue_name, redis_uri, queue_size, retry_times, retry_delay)
    publisher.__enter__()

    message = {"key": "value", "number": 42}
    result = publisher.push(message)

    assert result is True
    expected_body = json.dumps(message)
    mock_redis_client.rpush.assert_called_with(queue_name, expected_body)

  @patch('simple_queue.core.QueueConnection')
  def test_push_list_message_success(self, mock_connection_class, queue_name, redis_uri, queue_size, retry_times, retry_delay, mock_redis_client):
    """Test pushing a list message successfully."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_redis_client.llen.return_value = 0
    mock_redis_client.exists.return_value = True
    mock_connection_class.return_value = mock_conn_instance

    publisher = QueuePublisher(queue_name, redis_uri, queue_size, retry_times, retry_delay)
    publisher.__enter__()

    message = [1, 2, 3, "test"]
    result = publisher.push(message)

    assert result is True
    expected_body = json.dumps(message)
    mock_redis_client.rpush.assert_called_with(queue_name, expected_body)

  @patch('simple_queue.core.QueueConnection')
  def test_push_string_message_success(self, mock_connection_class, queue_name, redis_uri, queue_size, retry_times, retry_delay, mock_redis_client):
    """Test pushing a string message successfully."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_redis_client.llen.return_value = 0
    mock_redis_client.exists.return_value = True
    mock_connection_class.return_value = mock_conn_instance

    publisher = QueuePublisher(queue_name, redis_uri, queue_size, retry_times, retry_delay)
    publisher.__enter__()

    message = "plain text message"
    result = publisher.push(message)

    assert result is True
    mock_redis_client.rpush.assert_called_with(queue_name, message)

  @patch('simple_queue.core.QueueConnection')
  @patch('simple_queue.core.time.sleep')
  def test_push_blocks_when_queue_full(self, mock_sleep, mock_connection_class, queue_name, redis_uri, retry_times, retry_delay, mock_redis_client):
    """Test that push blocks when queue is full then succeeds."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    # First call: queue full, second call: space available
    mock_redis_client.llen.side_effect = [10, 10, 5]
    mock_redis_client.exists.return_value = True
    mock_connection_class.return_value = mock_conn_instance

    publisher = QueuePublisher(queue_name, redis_uri, 10, retry_times, retry_delay)
    publisher.__enter__()

    result = publisher.push("message")

    assert result is True
    assert mock_sleep.called
    mock_redis_client.rpush.assert_called_once()

  @patch('simple_queue.core.QueueConnection')
  @patch('simple_queue.core.time.sleep')
  def test_push_backoff_increases(self, mock_sleep, mock_connection_class, queue_name, redis_uri, retry_times, retry_delay, mock_redis_client):
    """Test that push increases backoff time when queue is full."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    # Queue full multiple times, then space available
    mock_redis_client.llen.side_effect = [10, 10, 10, 5]
    mock_redis_client.exists.return_value = True
    mock_connection_class.return_value = mock_conn_instance

    publisher = QueuePublisher(queue_name, redis_uri, 10, retry_times, retry_delay)
    publisher.__enter__()

    result = publisher.push("message")

    assert result is True
    # Check that sleep was called multiple times with increasing values
    assert mock_sleep.call_count >= 2
    sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
    # Backoff should increase (with 1.5x multiplier)
    assert sleep_calls[1] > sleep_calls[0]

  @patch('simple_queue.core.QueueConnection')
  @patch('simple_queue.core.time.sleep')
  def test_push_retries_on_error(self, mock_sleep, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that push retries on errors."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    # First attempt fails, second succeeds
    mock_redis_client.llen.side_effect = [Exception("Network error"), 0]
    mock_redis_client.exists.return_value = True
    mock_connection_class.return_value = mock_conn_instance

    publisher = QueuePublisher(queue_name, redis_uri, queue_size, 3, 0.1)
    publisher.__enter__()

    result = publisher.push("message")

    assert result is True
    mock_redis_client.rpush.assert_called_once()

  @patch('simple_queue.core.QueueConnection')
  @patch('simple_queue.core.time.sleep')
  def test_push_fails_after_max_retries(self, mock_sleep, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that push returns False after max retries."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_redis_client.llen.side_effect = Exception("Persistent error")
    mock_redis_client.exists.return_value = True
    mock_connection_class.return_value = mock_conn_instance

    publisher = QueuePublisher(queue_name, redis_uri, queue_size, 2, 0.01)
    publisher.__enter__()

    result = publisher.push("message")

    assert result is False

  @patch('simple_queue.core.QueueConnection')
  def test_push_raises_queue_not_found_error(self, mock_connection_class, queue_name, redis_uri,
                                             queue_size, retry_times, retry_delay, mock_redis_client):
    """Test that push raises QueueNotFoundError when queue doesn't exist."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_redis_client.exists.return_value = False
    mock_connection_class.return_value = mock_conn_instance

    publisher = QueuePublisher(queue_name, redis_uri, queue_size, retry_times, retry_delay)
    publisher.__enter__()

    with pytest.raises(QueueNotFoundError):
      publisher.push("message")

  @patch('simple_queue.core.QueueConnection')
  @patch('simple_queue.core.time.sleep')
  def test_push_reconnects_during_blocking(self, mock_sleep, mock_connection_class, queue_name,
                                           redis_uri, retry_times, retry_delay, mock_redis_client):
    """Test that push calls _ensure_connected during blocking wait."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    # Queue full, then space available
    mock_redis_client.llen.side_effect = [10, 5]
    mock_redis_client.exists.return_value = True
    mock_connection_class.return_value = mock_conn_instance

    publisher = QueuePublisher(queue_name, redis_uri, 10, retry_times, retry_delay)
    publisher.__enter__()

    result = publisher.push("message")

    assert result is True
    # Should call _ensure_connected multiple times (initial + during loop)
    assert mock_conn_instance._ensure_connected.call_count >= 2

  @patch('simple_queue.core.QueueConnection')
  def test_push_number_converted_to_string(self, mock_connection_class, queue_name, redis_uri,
                                           queue_size, retry_times, retry_delay, mock_redis_client):
    """Test that push converts numbers to strings."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_redis_client.llen.return_value = 0
    mock_redis_client.exists.return_value = True
    mock_connection_class.return_value = mock_conn_instance

    publisher = QueuePublisher(queue_name, redis_uri, queue_size, retry_times, retry_delay)
    publisher.__enter__()

    result = publisher.push(42)

    assert result is True
    mock_redis_client.rpush.assert_called_with(queue_name, "42")
