"""
Tests for QueuePublisher class
"""

import json
from unittest.mock import Mock, patch
from simple_queue.core import QueuePublisher


class TestQueuePublisher:
  """Test cases for QueuePublisher class"""

  def test_init_sets_attributes(self):
    """Test that __init__ sets all attributes correctly"""
    queue_name = "test_queue"
    uri = "amqp://localhost:5672"
    queue_size = 1000
    retry_times = 3
    retry_delay = 1.0

    publisher = QueuePublisher(queue_name, uri, queue_size, retry_times, retry_delay)

    assert publisher.queue_name == queue_name
    assert publisher.uri == uri
    assert publisher.queue_size == queue_size
    assert publisher.retry_times == retry_times
    assert publisher.retry_delay == retry_delay
    assert publisher.connection is None
    assert publisher._exchange_name == f"{queue_name}_exchange"

  @patch("simple_queue.core.QueueConnection")
  def test_enter_creates_connection_and_sets_up_queue(self, mock_queue_conn_class):
    """Test that __enter__ creates connection and sets up queue"""
    mock_connection = Mock()
    mock_queue_conn_class.return_value = mock_connection

    publisher = QueuePublisher("test_queue", "amqp://localhost:5672", 1000, 3, 1.0)

    with patch.object(publisher, "_setup_queue") as mock_setup:
      result = publisher.__enter__()

      # Verify connection was created
      mock_queue_conn_class.assert_called_once_with("amqp://localhost:5672")
      assert publisher.connection == mock_connection

      # Verify setup was called
      mock_setup.assert_called_once()

      # Verify it returns self
      assert result == publisher

  def test_exit_closes_connection(self):
    """Test that __exit__ closes connection"""
    publisher = QueuePublisher("test_queue", "amqp://localhost:5672", 1000, 3, 1.0)
    mock_connection = Mock()
    publisher.connection = mock_connection

    publisher.__exit__(None, None, None)

    mock_connection.close.assert_called_once()

  def test_exit_handles_none_connection(self):
    """Test that __exit__ handles None connection gracefully"""
    publisher = QueuePublisher("test_queue", "amqp://localhost:5672", 1000, 3, 1.0)
    publisher.connection = None

    # Should not raise exception
    publisher.__exit__(None, None, None)

  @patch("simple_queue.core.rabbitpy.Queue")
  @patch("simple_queue.core.rabbitpy.Exchange")
  def test_setup_queue_creates_queue_and_exchange(self, mock_exchange_class, mock_queue_class):
    """Test that _setup_queue creates queue, exchange and bindings"""
    publisher = QueuePublisher("test_queue", "amqp://localhost:5672", 1000, 3, 1.0)

    # Mock connection and channel
    mock_connection = Mock()
    mock_channel = Mock()
    mock_connection.channel = mock_channel
    publisher.connection = mock_connection

    # Mock queue and exchange
    mock_queue = Mock()
    mock_exchange = Mock()
    mock_queue_class.return_value = mock_queue
    mock_exchange_class.return_value = mock_exchange

    # Make the first queue.declare(passive=True) fail to simulate queue doesn't exist
    mock_queue.declare.side_effect = [Exception("Queue not found"), None]

    publisher._setup_queue()

    # Verify connection check
    mock_connection._ensure_connected.assert_called_once()

    # Verify queue creation - first call is passive check, second call is actual creation
    assert mock_queue_class.call_count == 2
    # First call: passive check
    mock_queue_class.assert_any_call(mock_channel, "test_queue")
    # Second call: actual creation (happens in except block since passive check fails)
    mock_queue_class.assert_any_call(
        mock_channel,
        "test_queue",
        durable=True,
        max_length=1000,
        arguments={"x-overflow": "reject-publish"},
    )
    # Verify declare was called twice: once with passive=True, once without
    assert mock_queue.declare.call_count == 2
    mock_queue.declare.assert_any_call(passive=True)
    mock_queue.declare.assert_any_call()

    # Verify exchange creation
    mock_exchange_class.assert_called_once_with(mock_channel, "test_queue_exchange")
    mock_exchange.declare.assert_called_once()

    # Verify binding
    mock_queue.bind.assert_called_once_with(mock_exchange, "test_queue")

    # Verify publisher confirms enabled
    mock_channel.enable_publisher_confirms.assert_called_once()

  @patch("simple_queue.core.rabbitpy.Queue")
  @patch("simple_queue.core.rabbitpy.Message")
  @patch("simple_queue.core.rabbitpy.Exchange")
  def test_push_dict_message_success(self, mock_exchange_class, mock_message_class, mock_queue_class):
    """Test pushing a dictionary message successfully"""
    publisher = QueuePublisher("test_queue", "amqp://localhost:5672", 1000, 3, 1.0)

    # Mock connection and channel
    mock_connection = Mock()
    mock_channel = Mock()
    mock_connection.channel = mock_channel
    publisher.connection = mock_connection

    # Mock queue for _ensure_connected_and_queue_exists
    mock_queue = Mock()
    mock_queue_class.return_value = mock_queue

    # Mock message and exchange
    mock_message = Mock()
    mock_exchange = Mock()
    mock_message_class.return_value = mock_message
    mock_exchange_class.return_value = mock_exchange
    mock_message.publish.return_value = True

    test_data = {"key": "value", "number": 42}
    result = publisher.push(test_data)

    # Verify connection check
    mock_connection._ensure_connected.assert_called_once()

    # Verify message creation with JSON serialized data
    expected_body = json.dumps(test_data)
    mock_message_class.assert_called_once_with(mock_channel, expected_body)

    # Verify exchange creation
    mock_exchange_class.assert_called_once_with(mock_channel, "test_queue_exchange")

    # Verify publish
    mock_message.publish.assert_called_once_with(mock_exchange, "test_queue", mandatory=True)

    # Verify return value
    assert result is True

  @patch("simple_queue.core.rabbitpy.Queue")
  @patch("simple_queue.core.rabbitpy.Message")
  @patch("simple_queue.core.rabbitpy.Exchange")
  def test_push_list_message_success(self, mock_exchange_class, mock_message_class, mock_queue_class):
    """Test pushing a list message successfully"""
    publisher = QueuePublisher("test_queue", "amqp://localhost:5672", 1000, 3, 1.0)

    # Mock connection and channel
    mock_connection = Mock()
    mock_channel = Mock()
    mock_connection.channel = mock_channel
    publisher.connection = mock_connection

    # Mock message and exchange
    mock_message = Mock()
    mock_exchange = Mock()
    mock_message_class.return_value = mock_message
    mock_exchange_class.return_value = mock_exchange
    mock_message.publish.return_value = True

    test_data = [1, 2, 3, "test"]
    result = publisher.push(test_data)

    # Verify message creation with JSON serialized data
    expected_body = json.dumps(test_data)
    mock_message_class.assert_called_once_with(mock_channel, expected_body)

    assert result is True

  @patch("simple_queue.core.rabbitpy.Queue")
  @patch("simple_queue.core.rabbitpy.Message")
  @patch("simple_queue.core.rabbitpy.Exchange")
  def test_push_string_message_success(self, mock_exchange_class, mock_message_class, mock_queue_class):
    """Test pushing a string message successfully"""
    publisher = QueuePublisher("test_queue", "amqp://localhost:5672", 1000, 3, 1.0)

    # Mock connection and channel
    mock_connection = Mock()
    mock_channel = Mock()
    mock_connection.channel = mock_channel
    publisher.connection = mock_connection

    # Mock message and exchange
    mock_message = Mock()
    mock_exchange = Mock()
    mock_message_class.return_value = mock_message
    mock_exchange_class.return_value = mock_exchange
    mock_message.publish.return_value = True

    test_data = "Hello, World!"
    result = publisher.push(test_data)

    # Verify message creation with string data
    mock_message_class.assert_called_once_with(mock_channel, test_data)

    assert result is True

  @patch("simple_queue.core.rabbitpy.Queue")
  @patch("simple_queue.core.rabbitpy.Message")
  @patch("simple_queue.core.rabbitpy.Exchange")
  def test_push_publish_fails(self, mock_exchange_class, mock_message_class, mock_queue_class):
    """Test push when publish returns False"""
    publisher = QueuePublisher("test_queue", "amqp://localhost:5672", 1000, 3, 1.0)

    # Mock connection and channel
    mock_connection = Mock()
    mock_channel = Mock()
    mock_connection.channel = mock_channel
    publisher.connection = mock_connection

    # Mock message and exchange
    mock_message = Mock()
    mock_exchange = Mock()
    mock_message_class.return_value = mock_message
    mock_exchange_class.return_value = mock_exchange
    mock_message.publish.return_value = False

    result = publisher.push("test message")

    # Should return False when publish fails
    assert result is False

  @patch("simple_queue.core.rabbitpy.Queue")
  @patch("simple_queue.core.rabbitpy.Message")
  @patch("simple_queue.core.rabbitpy.Exchange")
  @patch("simple_queue.core.time.sleep")
  def test_push_retries_on_exception(self, mock_sleep, mock_exchange_class, mock_message_class, mock_queue_class):
    """Test push retries on exception"""
    publisher = QueuePublisher("test_queue", "amqp://localhost:5672", 1000, 3, 1.0)

    # Mock connection and channel
    mock_connection = Mock()
    mock_channel = Mock()
    mock_connection.channel = mock_channel
    publisher.connection = mock_connection

    # Mock message and exchange
    mock_message = Mock()
    mock_exchange = Mock()
    mock_message_class.return_value = mock_message
    mock_exchange_class.return_value = mock_exchange

    # First two attempts fail, third succeeds
    mock_message.publish.side_effect = [
        Exception("Connection error"),
        Exception("Another error"),
        True,
    ]

    result = publisher.push("test message")

    # Verify retries
    assert mock_message.publish.call_count == 3
    assert mock_sleep.call_count == 2  # Sleep between retries
    mock_sleep.assert_called_with(1.0)  # retry_delay

    # Should succeed on third attempt
    assert result is True

  @patch("simple_queue.core.rabbitpy.Queue")
  @patch("simple_queue.core.rabbitpy.Message")
  @patch("simple_queue.core.rabbitpy.Exchange")
  @patch("simple_queue.core.time.sleep")
  def test_push_fails_after_all_retries(self, mock_sleep, mock_exchange_class, mock_message_class, mock_queue_class):
    """Test push fails after all retries exhausted"""
    publisher = QueuePublisher("test_queue", "amqp://localhost:5672", 1000, 3, 1.0)

    # Mock connection and channel
    mock_connection = Mock()
    mock_channel = Mock()
    mock_connection.channel = mock_channel
    publisher.connection = mock_connection

    # Mock queue for _ensure_connected_and_queue_exists
    mock_queue = Mock()
    mock_queue_class.return_value = mock_queue

    # Mock message and exchange
    mock_message = Mock()
    mock_exchange = Mock()
    mock_message_class.return_value = mock_message
    mock_exchange_class.return_value = mock_exchange

    # All attempts fail
    mock_message.publish.side_effect = Exception("Persistent error")

    result = publisher.push("test message")

    # Verify all retries were attempted
    assert mock_message.publish.call_count == 3
    assert mock_sleep.call_count == 2  # Sleep between retries

    # Should return False after all retries fail
    assert result is False

  @patch("simple_queue.core.QueueConnection")
  def test_context_manager_usage(self, mock_queue_conn_class):
    """Test using QueuePublisher as context manager"""
    mock_connection = Mock()
    mock_queue_conn_class.return_value = mock_connection

    publisher = QueuePublisher("test_queue", "amqp://localhost:5672", 1000, 3, 1.0)

    with patch.object(publisher, "_setup_queue"):
      with publisher as pub:
        assert pub == publisher
        assert publisher.connection == mock_connection

      # Verify connection was closed
      mock_connection.close.assert_called_once()
