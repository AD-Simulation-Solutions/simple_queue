"""
Integration tests and edge cases for simple_queue
"""

import pytest
import json
from unittest.mock import Mock, patch
from simple_queue.core import (
    QueueConnection,
    QueuePublisher,
    QueueConsumer,
)


class TestIntegration:
  """Integration tests for the complete workflow"""

  @patch("simple_queue.core.rabbitpy.Connection")
  @patch("simple_queue.core.rabbitpy.Queue")
  @patch("simple_queue.core.rabbitpy.Exchange")
  @patch("simple_queue.core.rabbitpy.Message")
  def test_publisher_consumer_integration(self, mock_message_class, mock_exchange_class, mock_queue_class, mock_conn_class):
    """Test complete publisher-consumer integration"""
    # Setup mocks
    mock_connection = Mock()
    mock_channel = Mock()
    mock_connection.channel.return_value = mock_channel
    mock_connection.closed = False
    mock_channel.closed = False
    mock_conn_class.return_value = mock_connection

    mock_queue = Mock()
    mock_exchange = Mock()
    mock_message = Mock()
    mock_queue_class.return_value = mock_queue
    mock_exchange_class.return_value = mock_exchange
    mock_message_class.return_value = mock_message
    mock_message.publish.return_value = True

    # Test data
    test_data = {"id": 1, "message": "Hello World"}

    # Test publisher
    with QueuePublisher("test_queue", "amqp://localhost", 1000, 3, 1.0) as publisher:
      result = publisher.push(test_data)
      assert result is True

    # Verify publisher setup
    mock_queue_class.assert_called()
    mock_exchange_class.assert_called()
    mock_message_class.assert_called_with(mock_channel, json.dumps(test_data))
    mock_message.publish.assert_called()

    # Reset mocks for consumer test
    mock_queue_class.reset_mock()
    mock_message_class.reset_mock()

    # Setup consumer mock
    mock_received_message = Mock()
    mock_received_message.body.decode.return_value = json.dumps(test_data)
    mock_queue.consume_messages.return_value = iter([mock_received_message])

    # Test consumer
    with QueueConsumer("test_queue", "amqp://localhost", 1000) as consumer:
      received_data = consumer.read()
      assert received_data == test_data

    # Verify consumer setup
    mock_queue_class.assert_called()
    mock_received_message.ack.assert_called_once()


class TestEdgeCases:
  """Edge case tests for various scenarios"""

  @patch("simple_queue.core.rabbitpy.Connection")
  def test_connection_with_invalid_uri(self, mock_conn_class):
    """Test connection handling with invalid URI"""
    mock_conn_class.side_effect = Exception("Invalid URI")

    with pytest.raises(Exception, match="Invalid URI"):
      QueueConnection("invalid://uri")

  @patch("simple_queue.core.QueueConnection")
  def test_publisher_with_empty_queue_name(self, mock_queue_conn_class):
    """Test publisher with empty queue name"""
    mock_connection = Mock()
    mock_queue_conn_class.return_value = mock_connection

    publisher = QueuePublisher("", "amqp://localhost", 1000, 3, 1.0)
    assert publisher.queue_name == ""
    assert publisher._exchange_name == "_exchange"

  @patch("simple_queue.core.QueueConnection")
  def test_consumer_with_zero_queue_size(self, mock_queue_conn_class):
    """Test consumer with zero queue size"""
    mock_connection = Mock()
    mock_queue_conn_class.return_value = mock_connection

    consumer = QueueConsumer("test_queue", "amqp://localhost", 0)
    assert consumer.queue_size == 0

  def test_publisher_with_negative_retry_times(self):
    """Test publisher with negative retry times"""
    publisher = QueuePublisher("test_queue", "amqp://localhost", 1000, -1, 1.0)
    assert publisher.retry_times == -1
    # This should be handled gracefully in the actual push method

  def test_publisher_with_zero_retry_delay(self):
    """Test publisher with zero retry delay"""
    publisher = QueuePublisher("test_queue", "amqp://localhost", 1000, 3, 0.0)
    assert publisher.retry_delay == 0.0

  @patch("simple_queue.core.rabbitpy.Message")
  @patch("simple_queue.core.rabbitpy.Exchange")
  @patch("simple_queue.core.time.sleep")
  def test_publisher_push_with_zero_retries(self, mock_sleep, mock_exchange_class, mock_message_class):
    """Test publisher push with zero retry times"""
    publisher = QueuePublisher("test_queue", "amqp://localhost", 1000, 0, 1.0)

    # Mock connection and channel
    mock_connection = Mock()
    mock_channel = Mock()
    mock_connection.channel = mock_channel
    publisher.connection = mock_connection

    # Mock message and exchange that fail
    mock_message = Mock()
    mock_exchange = Mock()
    mock_message_class.return_value = mock_message
    mock_exchange_class.return_value = mock_exchange
    mock_message.publish.side_effect = Exception("Connection error")

    result = publisher.push("test message")

    # Should not retry and return False
    assert result is False
    assert mock_message.publish.call_count == 0  # No retries means no attempts
    mock_sleep.assert_not_called()

  def test_publisher_push_with_none_message(self):
    """Test publisher push with None message"""
    publisher = QueuePublisher("test_queue", "amqp://localhost", 1000, 3, 1.0)

    # Mock connection and channel
    mock_connection = Mock()
    mock_channel = Mock()
    mock_connection.channel = mock_channel
    publisher.connection = mock_connection

    with patch("simple_queue.core.rabbitpy.Message") as mock_message_class:
      with patch("simple_queue.core.rabbitpy.Exchange") as mock_exchange_class:
        with patch("simple_queue.core.rabbitpy.Queue") as mock_queue_class:
          mock_message = Mock()
          mock_exchange = Mock()
          mock_queue = Mock()
          mock_message_class.return_value = mock_message
          mock_exchange_class.return_value = mock_exchange
          mock_queue_class.return_value = mock_queue
          mock_message.publish.return_value = True

          result = publisher.push(None)

          # None should be converted to string "None"
          mock_message_class.assert_called_with(mock_channel, "None")
          assert result is True

  def test_publisher_push_with_complex_data_types(self):
    """Test publisher push with complex data types"""
    publisher = QueuePublisher("test_queue", "amqp://localhost", 1000, 3, 1.0)

    # Mock connection and channel
    mock_connection = Mock()
    mock_channel = Mock()
    mock_connection.channel = mock_channel
    publisher.connection = mock_connection

    with patch("simple_queue.core.rabbitpy.Message") as mock_message_class:
      with patch("simple_queue.core.rabbitpy.Exchange") as mock_exchange_class:
        with patch("simple_queue.core.rabbitpy.Queue") as mock_queue_class:
          mock_message = Mock()
          mock_exchange = Mock()
          mock_queue = Mock()
          mock_message_class.return_value = mock_message
          mock_exchange_class.return_value = mock_exchange
          mock_queue_class.return_value = mock_queue
          mock_message.publish.return_value = True

          # Test with nested dict
          complex_data = {
              "nested": {
                  "key": "value"
              },
              "list": [1, 2, 3],
              "boolean": True,
              "null": None,
          }

          result = publisher.push(complex_data)

          expected_json = json.dumps(complex_data)
          mock_message_class.assert_called_with(mock_channel, expected_json)
          assert result is True

  def test_consumer_read_with_malformed_json(self):
    """Test consumer read with malformed JSON"""
    consumer = QueueConsumer("test_queue", "amqp://localhost", 1000)

    # Mock connection and queue
    mock_connection = Mock()
    mock_queue = Mock()
    consumer.connection = mock_connection
    consumer._queue = mock_queue

    # Mock message with malformed JSON
    mock_message = Mock()
    mock_message.body.decode.return_value = '{"invalid": json}'
    mock_queue.consume_messages.return_value = iter([mock_message])

    result = consumer.read()

    # Should return the raw string when JSON parsing fails
    assert result == '{"invalid": json}'
    mock_message.ack.assert_called_once()

  def test_consumer_read_with_empty_message(self):
    """Test consumer read with empty message"""
    consumer = QueueConsumer("test_queue", "amqp://localhost", 1000)

    # Mock connection and queue
    mock_connection = Mock()
    mock_queue = Mock()
    consumer.connection = mock_connection
    consumer._queue = mock_queue

    # Mock message with empty content
    mock_message = Mock()
    mock_message.body.decode.return_value = ""
    mock_queue.consume_messages.return_value = iter([mock_message])

    result = consumer.read()

    # Should return empty string
    assert result == ""
    mock_message.ack.assert_called_once()

  def test_consumer_read_with_unicode_message(self):
    """Test consumer read with unicode message"""
    consumer = QueueConsumer("test_queue", "amqp://localhost", 1000)

    # Mock connection and queue
    mock_connection = Mock()
    mock_queue = Mock()
    consumer.connection = mock_connection
    consumer._queue = mock_queue

    # Mock message with unicode content
    unicode_message = "Hello 世界 🌍 émojis"
    mock_message = Mock()
    mock_message.body.decode.return_value = unicode_message
    mock_queue.consume_messages.return_value = iter([mock_message])

    result = consumer.read()

    # Should handle unicode correctly
    assert result == unicode_message
    mock_message.ack.assert_called_once()


class TestErrorHandling:
  """Tests for error handling scenarios"""

  @patch("simple_queue.core.rabbitpy.Connection")
  def test_connection_reconnect_on_multiple_failures(self, mock_conn_class):
    """Test connection reconnection on multiple failures"""
    # First connection fails, second succeeds
    mock_conn1 = Mock()
    mock_conn1.channel.side_effect = Exception("Channel creation failed")
    mock_conn2 = Mock()
    mock_channel = Mock()
    mock_conn2.channel.return_value = mock_channel

    mock_conn_class.side_effect = [mock_conn1, mock_conn2]

    # This should trigger reconnection
    with pytest.raises(Exception, match="Channel creation failed"):
      QueueConnection("amqp://localhost")

  def test_publisher_context_manager_exception_in_setup(self):
    """Test publisher context manager when setup fails"""
    publisher = QueuePublisher("test_queue", "amqp://localhost", 1000, 3, 1.0)

    with patch("simple_queue.core.QueueConnection") as mock_conn_class:
      mock_connection = Mock()
      mock_conn_class.return_value = mock_connection

      with patch.object(publisher, "_setup_queue", side_effect=Exception("Setup failed")):
        with pytest.raises(Exception, match="Setup failed"):
          with publisher:
            pass

        # When __enter__ fails, __exit__ is not called, so connection
        # is not closed. This is the expected behavior of Python
        # context managers
        mock_connection.close.assert_not_called()

  def test_consumer_context_manager_exception_in_setup(self):
    """Test consumer context manager when setup fails"""
    consumer = QueueConsumer("test_queue", "amqp://localhost", 1000)

    with patch("simple_queue.core.QueueConnection") as mock_conn_class:
      mock_connection = Mock()
      mock_conn_class.return_value = mock_connection

      with patch.object(consumer, "_setup_queue", side_effect=Exception("Setup failed")):
        with pytest.raises(Exception, match="Setup failed"):
          with consumer:
            pass

        # When __enter__ fails, __exit__ is not called, so connection
        # is not closed. This is the expected behavior of Python
        # context managers
        mock_connection.close.assert_not_called()
