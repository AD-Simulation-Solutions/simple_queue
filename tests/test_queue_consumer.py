"""Tests for QueueConsumer class."""
import pytest
import json
from unittest.mock import patch, MagicMock
from simple_queue.core import QueueConsumer


class TestQueueConsumer:
  """Test suite for QueueConsumer class."""

  def test_init_sets_attributes(self, queue_name, redis_uri, queue_size):
    """Test that __init__ sets all attributes correctly."""
    consumer = QueueConsumer(queue_name, redis_uri, queue_size)

    assert consumer.queue_name == queue_name
    assert consumer.uri == redis_uri
    assert consumer.queue_size == queue_size
    assert consumer.connection is None
    assert consumer._config_key == f"{queue_name}:config"

  @patch('simple_queue.core.QueueConnection')
  def test_enter_creates_connection(self, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that __enter__ creates connection and sets up queue."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, queue_size)
    result = consumer.__enter__()

    assert consumer.connection is not None
    assert result is consumer
    mock_connection_class.assert_called_once_with(redis_uri)

  @patch('simple_queue.core.QueueConnection')
  def test_exit_closes_connection(self, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that __exit__ closes the connection."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, queue_size)
    consumer.__enter__()
    consumer.__exit__(None, None, None)

    mock_conn_instance.close.assert_called_once()

  @patch('simple_queue.core.QueueConnection')
  def test_setup_queue_loads_existing_config(self, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that _setup_queue loads existing config."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    existing_config = json.dumps({'max_length': 200})
    mock_redis_client.get.return_value = existing_config
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, 100)
    consumer.__enter__()

    assert consumer.queue_size == 200

  @patch('simple_queue.core.QueueConnection')
  def test_setup_queue_creates_new_config_if_none(self, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that _setup_queue creates new config when none exists."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_redis_client.get.return_value = None
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, queue_size)
    consumer.__enter__()

    expected_config = json.dumps({'max_length': queue_size})
    mock_redis_client.set.assert_called_with(f"{queue_name}:config", expected_config)

  @patch('simple_queue.core.QueueConnection')
  def test_read_returns_dict_message(self, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that read returns a dict message correctly."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    message = {"key": "value", "number": 42}
    mock_redis_client.blpop.return_value = (queue_name.encode(), json.dumps(message).encode())
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, queue_size)
    consumer.__enter__()

    result = consumer.read(timeout=5)

    assert result == message
    mock_redis_client.blpop.assert_called_once_with(queue_name, timeout=5)

  @patch('simple_queue.core.QueueConnection')
  def test_read_returns_string_message(self, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that read returns a string message correctly."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    message = "plain text message"
    mock_redis_client.blpop.return_value = (queue_name.encode(), message.encode())
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, queue_size)
    consumer.__enter__()

    result = consumer.read(timeout=5)

    assert result == message

  @patch('simple_queue.core.QueueConnection')
  def test_read_returns_list_as_string(self, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that read returns list JSON as string (not dict)."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    message = [1, 2, 3, "test"]
    mock_redis_client.blpop.return_value = (queue_name.encode(), json.dumps(message).encode())
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, queue_size)
    consumer.__enter__()

    result = consumer.read(timeout=5)

    # Lists should be returned as strings according to the code logic
    assert isinstance(result, str)

  @patch('simple_queue.core.QueueConnection')
  def test_read_returns_none_on_timeout(self, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that read returns None when timeout occurs."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_redis_client.blpop.return_value = None
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, queue_size)
    consumer.__enter__()

    result = consumer.read(timeout=5)

    assert result is None

  @patch('simple_queue.core.QueueConnection')
  def test_read_with_no_timeout(self, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that read uses 0 timeout when None is provided."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    message = "test"
    mock_redis_client.blpop.return_value = (queue_name.encode(), message.encode())
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, queue_size)
    consumer.__enter__()

    consumer.read()

    mock_redis_client.blpop.assert_called_once_with(queue_name, timeout=0)

  @patch('simple_queue.core.QueueConnection')
  @patch('simple_queue.core.time.sleep')
  def test_read_retries_on_error(self, mock_sleep, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that read retries on errors."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    message = "test message"
    # First attempt fails, second succeeds
    mock_redis_client.blpop.side_effect = [
        Exception("Network error"),
        (queue_name.encode(), message.encode())
    ]
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, queue_size)
    consumer.__enter__()

    result = consumer.read(timeout=5)

    assert result == message
    mock_sleep.assert_called_with(1)

  @patch('simple_queue.core.QueueConnection')
  @patch('simple_queue.core.time.sleep')
  def test_read_raises_after_max_retries(self, mock_sleep, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that read raises exception after max retries on the third attempt."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    # First 2 attempts fail, third attempt raises
    mock_redis_client.blpop.side_effect = [
        Exception("Error 1"),
        Exception("Error 2"),
        Exception("Persistent error")
    ]
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, queue_size)
    consumer.__enter__()

    with pytest.raises(Exception, match="Persistent error"):
      consumer.read(timeout=5)

    # Should retry 3 times
    assert mock_redis_client.blpop.call_count == 3
    # Should sleep twice (after first and second failure)
    assert mock_sleep.call_count == 2

  @patch('simple_queue.core.QueueConnection')
  @patch('simple_queue.core.time.sleep')
  def test_read_returns_none_after_retries_without_raise(
          self, mock_sleep, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that read returns None after retries if no exception on last attempt."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    # All attempts return None (timeout), but don't raise
    mock_redis_client.blpop.return_value = None
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, queue_size)
    consumer.__enter__()

    # Force an error on first 2 attempts, None on third
    mock_redis_client.blpop.side_effect = [
        Exception("Error 1"),
        Exception("Error 2"),
        None  # Last attempt succeeds but returns None
    ]

    result = consumer.read(timeout=5)

    assert result is None
    assert mock_redis_client.blpop.call_count == 3

  @patch('simple_queue.core.QueueConnection')
  def test_read_continuous_yields_messages(self, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that read_continuous yields messages continuously."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    messages = [
        {"msg": 1},
        {"msg": 2},
        None  # Simulate timeout, should continue
    ]
    mock_redis_client.blpop.side_effect = [
        (queue_name.encode(), json.dumps(messages[0]).encode()),
        (queue_name.encode(), json.dumps(messages[1]).encode()),
        None,
        KeyboardInterrupt()  # Stop iteration
    ]
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, queue_size)
    consumer.__enter__()

    results = []
    try:
      for msg in consumer.read_continuous():
        results.append(msg)
    except KeyboardInterrupt:
      pass

    assert len(results) == 2
    assert results[0] == messages[0]
    assert results[1] == messages[1]

  @patch('simple_queue.core.QueueConnection')
  def test_read_continuous_stops_on_keyboard_interrupt(self, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that read_continuous stops gracefully on KeyboardInterrupt."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_redis_client.blpop.side_effect = KeyboardInterrupt()
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, queue_size)
    consumer.__enter__()

    results = []
    for msg in consumer.read_continuous():
      results.append(msg)

    assert len(results) == 0  # Should break without yielding

  @patch('simple_queue.core.QueueConnection')
  @patch('simple_queue.core.time.sleep')
  def test_read_continuous_handles_errors(self, mock_sleep, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that read_continuous handles errors and continues."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    message = {"msg": "success"}
    mock_redis_client.blpop.side_effect = [
        Exception("Network error"),
        (queue_name.encode(), json.dumps(message).encode()),
        KeyboardInterrupt()
    ]
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, queue_size)
    consumer.__enter__()

    results = []
    for msg in consumer.read_continuous():
      results.append(msg)

    assert len(results) == 1
    assert results[0] == message
    mock_sleep.assert_called_with(5)

  @patch('simple_queue.core.QueueConnection')
  def test_read_continuous_yields_string_messages(self, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that read_continuous yields string messages."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    message = "plain text"
    mock_redis_client.blpop.side_effect = [
        (queue_name.encode(), message.encode()),
        KeyboardInterrupt()
    ]
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, queue_size)
    consumer.__enter__()

    results = []
    for msg in consumer.read_continuous():
      results.append(msg)

    assert len(results) == 1
    assert results[0] == message

  @patch('simple_queue.core.QueueConnection')
  def test_read_continuous_continues_on_none_result(self, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that read_continuous continues when blpop returns None."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    message = {"msg": "test"}
    mock_redis_client.blpop.side_effect = [
        None,  # Timeout
        None,  # Another timeout
        (queue_name.encode(), json.dumps(message).encode()),
        KeyboardInterrupt()
    ]
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, queue_size)
    consumer.__enter__()

    results = []
    for msg in consumer.read_continuous():
      results.append(msg)

    assert len(results) == 1
    assert results[0] == message

  @patch('simple_queue.core.QueueConnection')
  def test_read_calls_ensure_connected(self, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that read calls _ensure_connected."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    message = "test"
    mock_redis_client.blpop.return_value = (queue_name.encode(), message.encode())
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, queue_size)
    consumer.__enter__()

    consumer.read(timeout=5)

    mock_conn_instance._ensure_connected.assert_called()

  @patch('simple_queue.core.QueueConnection')
  def test_read_continuous_calls_ensure_connected(self, mock_connection_class, queue_name, redis_uri, queue_size, mock_redis_client):
    """Test that read_continuous calls _ensure_connected."""
    mock_conn_instance = MagicMock()
    mock_conn_instance.client = mock_redis_client
    mock_redis_client.blpop.side_effect = [
        (queue_name.encode(), b"test"),
        KeyboardInterrupt()
    ]
    mock_connection_class.return_value = mock_conn_instance

    consumer = QueueConsumer(queue_name, redis_uri, queue_size)
    consumer.__enter__()

    for _ in consumer.read_continuous():
      pass

    mock_conn_instance._ensure_connected.assert_called()
