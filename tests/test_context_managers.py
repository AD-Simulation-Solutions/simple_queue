"""
Tests for context manager functions (queue_publisher and queue_consumer)
"""

import pytest
from unittest.mock import Mock, patch
from simple_queue.core import queue_publisher, queue_consumer


class TestContextManagers:
  """Test cases for context manager functions"""

  @patch("simple_queue.core.QueuePublisher")
  def test_queue_publisher_context_manager(self, mock_publisher_class):
    """Test queue_publisher context manager function"""
    # Mock the QueuePublisher instance with MagicMock to support magic methods
    mock_publisher_instance = Mock()
    mock_publisher_instance.__enter__ = Mock(return_value=mock_publisher_instance)
    mock_publisher_instance.__exit__ = Mock(return_value=None)
    mock_publisher_class.return_value = mock_publisher_instance

    queue_name = "test_queue"
    uri = "amqp://localhost:5672"
    queue_size = 1000
    retry_times = 3
    retry_delay = 1.5

    # Use the context manager
    with queue_publisher(queue_name, uri, queue_size, retry_times, retry_delay) as pub:
      # Verify QueuePublisher was created with correct parameters
      mock_publisher_class.assert_called_once_with(queue_name, uri, queue_size, retry_times, retry_delay)

      # Verify __enter__ was called
      mock_publisher_instance.__enter__.assert_called_once()

      # Verify the yielded object is the publisher instance
      assert pub == mock_publisher_instance

    # Verify __exit__ was called after context
    mock_publisher_instance.__exit__.assert_called_once()

  @patch("simple_queue.core.QueueConsumer")
  def test_queue_consumer_context_manager(self, mock_consumer_class):
    """Test queue_consumer context manager function"""
    # Mock the QueueConsumer instance with magic methods
    mock_consumer_instance = Mock()
    mock_consumer_instance.__enter__ = Mock(return_value=mock_consumer_instance)
    mock_consumer_instance.__exit__ = Mock(return_value=None)
    mock_consumer_class.return_value = mock_consumer_instance

    queue_name = "test_queue"
    uri = "amqp://localhost:5672"
    queue_size = 1000

    # Use the context manager
    with queue_consumer(queue_name, uri, queue_size) as cons:
      # Verify QueueConsumer was created with correct parameters
      mock_consumer_class.assert_called_once_with(queue_name, uri, queue_size)

      # Verify __enter__ was called
      mock_consumer_instance.__enter__.assert_called_once()

      # Verify the yielded object is the consumer instance
      assert cons == mock_consumer_instance

    # Verify __exit__ was called after context
    mock_consumer_instance.__exit__.assert_called_once()

  @patch("simple_queue.core.QueuePublisher")
  def test_queue_publisher_context_manager_with_exception(self, mock_publisher_class):
    """Test queue_publisher context manager handles exceptions properly"""
    # Mock the QueuePublisher instance with magic methods
    mock_publisher_instance = Mock()
    mock_publisher_instance.__enter__ = Mock(return_value=mock_publisher_instance)
    mock_publisher_instance.__exit__ = Mock(return_value=None)
    mock_publisher_class.return_value = mock_publisher_instance

    # Test that exceptions are properly propagated and __exit__ is still called
    with pytest.raises(ValueError, match="Test exception"):
      with queue_publisher("test_queue", "amqp://localhost:5672", 1000, 3, 1.0) as _:
        raise ValueError("Test exception")

    # Verify __exit__ was called even with exception
    mock_publisher_instance.__exit__.assert_called_once()
    # Check that __exit__ was called with exception info
    call_args = mock_publisher_instance.__exit__.call_args[0]
    assert call_args[0] == ValueError  # exc_type
    assert str(call_args[1]) == "Test exception"  # exc_val
    assert call_args[2] is not None  # exc_tb

  @patch("simple_queue.core.QueueConsumer")
  def test_queue_consumer_context_manager_with_exception(self, mock_consumer_class):
    """Test queue_consumer context manager handles exceptions properly"""
    # Mock the QueueConsumer instance with magic methods
    mock_consumer_instance = Mock()
    mock_consumer_instance.__enter__ = Mock(return_value=mock_consumer_instance)
    mock_consumer_instance.__exit__ = Mock(return_value=None)
    mock_consumer_class.return_value = mock_consumer_instance

    # Test that exceptions are properly propagated and __exit__ is still called
    with pytest.raises(RuntimeError, match="Consumer error"):
      with queue_consumer("test_queue", "amqp://localhost:5672", 1000) as _:
        raise RuntimeError("Consumer error")

    # Verify __exit__ was called even with exception
    mock_consumer_instance.__exit__.assert_called_once()
    # Check that __exit__ was called with exception info
    call_args = mock_consumer_instance.__exit__.call_args[0]
    assert call_args[0] == RuntimeError  # exc_type
    assert str(call_args[1]) == "Consumer error"  # exc_val
    assert call_args[2] is not None  # exc_tb
