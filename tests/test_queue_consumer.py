"""
Tests for QueueConsumer class
"""

import json
import pytest
from unittest.mock import Mock, patch
from simple_queue.core import QueueConsumer


class TestQueueConsumer:
  """Test cases for QueueConsumer class"""

  def test_init_sets_attributes(self):
    """Test that __init__ sets all attributes correctly"""
    queue_name = "test_queue"
    uri = "amqp://localhost:5672"
    queue_size = 1000

    consumer = QueueConsumer(queue_name, uri, queue_size)

    assert consumer.queue_name == queue_name
    assert consumer.uri == uri
    assert consumer.queue_size == queue_size
    assert consumer.connection is None
    assert consumer._queue is None

  @patch("simple_queue.core.QueueConnection")
  def test_enter_creates_connection_and_sets_up_queue(self, mock_queue_conn_class):
    """Test that __enter__ creates connection and sets up queue"""
    mock_connection = Mock()
    mock_queue_conn_class.return_value = mock_connection

    consumer = QueueConsumer("test_queue", "amqp://localhost:5672", 1000)

    with patch.object(consumer, "_setup_queue") as mock_setup:
      result = consumer.__enter__()

      # Verify connection was created
      mock_queue_conn_class.assert_called_once_with("amqp://localhost:5672")
      assert consumer.connection == mock_connection

      # Verify setup was called
      mock_setup.assert_called_once()

      # Verify it returns self
      assert result == consumer

  def test_exit_closes_connection(self):
    """Test that __exit__ closes connection"""
    consumer = QueueConsumer("test_queue", "amqp://localhost:5672", 1000)
    mock_connection = Mock()
    consumer.connection = mock_connection

    consumer.__exit__(None, None, None)

    mock_connection.close.assert_called_once()

  def test_exit_handles_none_connection(self):
    """Test that __exit__ handles None connection gracefully"""
    consumer = QueueConsumer("test_queue", "amqp://localhost:5672", 1000)
    consumer.connection = None

    # Should not raise exception
    consumer.__exit__(None, None, None)

  @patch("simple_queue.core.rabbitpy.Queue")
  def test_setup_queue_creates_queue(self, mock_queue_class):
    """Test that _setup_queue creates queue and sets prefetch"""
    consumer = QueueConsumer("test_queue", "amqp://localhost:5672", 1000)

    # Mock connection and channel
    mock_connection = Mock()
    mock_channel = Mock()
    mock_connection.channel = mock_channel
    consumer.connection = mock_connection

    # Mock queue
    mock_queue = Mock()
    mock_queue_class.return_value = mock_queue

    # Make the first queue.declare(passive=True) fail to simulate queue doesn't exist
    mock_queue.declare.side_effect = [Exception("Queue not found"), None]

    consumer._setup_queue()

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

    # Verify prefetch count set
    mock_channel.prefetch_count.assert_called_once_with(1)

    # Verify queue is stored
    assert consumer._queue == mock_queue

  def test_read_json_message_success(self):
    """Test reading a JSON message successfully"""
    consumer = QueueConsumer("test_queue", "amqp://localhost:5672", 1000)

    # Mock connection and queue
    mock_connection = Mock()
    mock_queue = Mock()
    consumer.connection = mock_connection
    consumer._queue = mock_queue

    # Mock message
    mock_message = Mock()
    test_data = {"key": "value", "number": 42}
    mock_message.body.decode.return_value = json.dumps(test_data)
    mock_queue.consume_messages.return_value = iter([mock_message])

    result = consumer.read()

    # Verify connection check
    mock_connection._ensure_connected.assert_called_once()

    # Verify message was acknowledged
    mock_message.ack.assert_called_once()

    # Verify JSON was parsed correctly
    assert result == test_data

  def test_read_string_message_success(self):
    """Test reading a string message successfully"""
    consumer = QueueConsumer("test_queue", "amqp://localhost:5672", 1000)

    # Mock connection and queue
    mock_connection = Mock()
    mock_queue = Mock()
    consumer.connection = mock_connection
    consumer._queue = mock_queue

    # Mock message with non-JSON content
    mock_message = Mock()
    test_data = "Hello, World!"
    mock_message.body.decode.return_value = test_data
    mock_queue.consume_messages.return_value = iter([mock_message])

    result = consumer.read()

    # Verify connection check
    mock_connection._ensure_connected.assert_called_once()

    # Verify message was acknowledged
    mock_message.ack.assert_called_once()

    # Verify string was returned as-is
    assert result == test_data

  def test_read_no_messages_available(self):
    """Test read when no messages are available"""
    consumer = QueueConsumer("test_queue", "amqp://localhost:5672", 1000)

    # Mock connection and queue
    mock_connection = Mock()
    mock_queue = Mock()
    consumer.connection = mock_connection
    consumer._queue = mock_queue

    # Mock empty iterator
    mock_queue.consume_messages.return_value = iter([])

    result = consumer.read()

    # Verify connection check
    mock_connection._ensure_connected.assert_called_once()

    # Should return None when no messages
    assert result is None

  @patch("simple_queue.core.time.sleep")
  def test_read_retries_on_exception(self, mock_sleep):
    """Test read retries on exception"""
    consumer = QueueConsumer("test_queue", "amqp://localhost:5672", 1000)

    # Mock connection and queue
    mock_connection = Mock()
    mock_queue = Mock()
    consumer.connection = mock_connection
    consumer._queue = mock_queue

    # Mock message for successful attempt
    mock_message = Mock()
    mock_message.body.decode.return_value = "test message"

    # First two attempts fail, third succeeds
    mock_queue.consume_messages.side_effect = [
        Exception("Connection error"),
        Exception("Another error"),
        iter([mock_message]),
    ]

    result = consumer.read()

    # Verify retries
    assert mock_queue.consume_messages.call_count == 3
    assert mock_sleep.call_count == 2  # Sleep between retries
    mock_sleep.assert_called_with(1)

    # Should succeed on third attempt
    assert result == "test message"
    mock_message.ack.assert_called_once()

  @patch("simple_queue.core.time.sleep")
  def test_read_fails_after_all_retries(self, mock_sleep):
    """Test read fails after all retries exhausted"""
    consumer = QueueConsumer("test_queue", "amqp://localhost:5672", 1000)

    # Mock connection and queue
    mock_connection = Mock()
    mock_queue = Mock()
    consumer.connection = mock_connection
    consumer._queue = mock_queue

    # All attempts fail
    mock_queue.consume_messages.side_effect = Exception("Persistent error")

    with pytest.raises(Exception, match="Persistent error"):
      consumer.read()

    # Verify all retries were attempted
    assert mock_queue.consume_messages.call_count == 3
    assert mock_sleep.call_count == 2  # Sleep between retries

  @patch("simple_queue.core.time.sleep")
  def test_read_continuous_yields_messages(self, mock_sleep):
    """Test read_continuous yields messages continuously"""
    consumer = QueueConsumer("test_queue", "amqp://localhost:5672", 1000)

    # Mock connection and queue
    mock_connection = Mock()
    mock_queue = Mock()
    consumer.connection = mock_connection
    consumer._queue = mock_queue

    # Mock messages
    mock_message1 = Mock()
    mock_message1.body.decode.return_value = json.dumps({"id": 1})
    mock_message2 = Mock()
    mock_message2.body.decode.return_value = "string message"
    mock_message3 = Mock()
    mock_message3.body.decode.return_value = json.dumps({"id": 3})

    # Mock consume_messages to return messages then raise KeyboardInterrupt
    def mock_consume_messages():
      yield mock_message1
      yield mock_message2
      yield mock_message3
      raise KeyboardInterrupt("User stopped")

    mock_queue.consume_messages.return_value = mock_consume_messages()

    messages = []
    try:
      for message in consumer.read_continuous():
        messages.append(message)
    except StopIteration:
      pass

    # Verify all messages were yielded and acknowledged
    assert len(messages) == 3
    assert messages[0] == {"id": 1}
    assert messages[1] == "string message"
    assert messages[2] == {"id": 3}

    mock_message1.ack.assert_called_once()
    mock_message2.ack.assert_called_once()
    mock_message3.ack.assert_called_once()

  @patch("simple_queue.core.time.sleep")
  def test_read_continuous_handles_message_processing_error(self, mock_sleep):
    """Test read_continuous handles errors in message processing"""
    consumer = QueueConsumer("test_queue", "amqp://localhost:5672", 1000)

    # Mock connection and queue
    mock_connection = Mock()
    mock_queue = Mock()
    consumer.connection = mock_connection
    consumer._queue = mock_queue

    # Mock messages - one good, one that causes error, one good
    mock_message1 = Mock()
    mock_message1.body.decode.return_value = "good message 1"

    mock_message2 = Mock()
    mock_message2.body.decode.side_effect = Exception("Decode error")

    mock_message3 = Mock()
    mock_message3.body.decode.return_value = "good message 2"

    def mock_consume_messages():
      yield mock_message1
      yield mock_message2
      yield mock_message3
      raise KeyboardInterrupt("User stopped")

    mock_queue.consume_messages.return_value = mock_consume_messages()

    messages = []
    try:
      for message in consumer.read_continuous():
        messages.append(message)
    except StopIteration:
      pass

    # Should get good messages, error message should be acknowledged but not yielded
    assert len(messages) == 2
    assert messages[0] == "good message 1"
    assert messages[1] == "good message 2"

    # All messages should be acknowledged
    mock_message1.ack.assert_called_once()
    mock_message2.ack.assert_called_once()
    mock_message3.ack.assert_called_once()

  @patch("simple_queue.core.time.sleep")
  def test_read_continuous_handles_connection_error(self, mock_sleep):
    """Test read_continuous handles connection errors and reconnects"""
    consumer = QueueConsumer("test_queue", "amqp://localhost:5672", 1000)

    # Mock connection and queue
    mock_connection = Mock()
    mock_queue = Mock()
    consumer.connection = mock_connection
    consumer._queue = mock_queue

    # Mock message for successful attempt
    mock_message = Mock()
    mock_message.body.decode.return_value = "test message"

    call_count = 0

    def mock_consume_messages():
      nonlocal call_count
      call_count += 1
      if call_count == 1:
        raise Exception("Connection lost")
      elif call_count == 2:
        yield mock_message
        raise KeyboardInterrupt("User stopped")

    mock_queue.consume_messages.side_effect = mock_consume_messages

    messages = []
    try:
      for message in consumer.read_continuous():
        messages.append(message)
    except StopIteration:
      pass

    # Should recover and get the message
    assert len(messages) == 1
    assert messages[0] == "test message"

    # Should have slept for reconnection
    mock_sleep.assert_called_with(5)
    mock_message.ack.assert_called_once()

  @patch("simple_queue.core.QueueConnection")
  def test_context_manager_usage(self, mock_queue_conn_class):
    """Test using QueueConsumer as context manager"""
    mock_connection = Mock()
    mock_queue_conn_class.return_value = mock_connection

    consumer = QueueConsumer("test_queue", "amqp://localhost:5672", 1000)

    with patch.object(consumer, "_setup_queue"):
      with consumer as cons:
        assert cons == consumer
        assert consumer.connection == mock_connection

      # Verify connection was closed
      mock_connection.close.assert_called_once()
