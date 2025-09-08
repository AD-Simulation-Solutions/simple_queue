"""
Tests for example scripts
"""

from unittest.mock import Mock, patch, call
from examples.publisher_example import main as publisher_main
from examples.consumer_example import main as consumer_main


class TestPublisherExample:
  """Test cases for publisher example script"""

  @patch("examples.publisher_example.time.sleep")
  @patch("examples.publisher_example.queue_publisher")
  def test_publisher_main_sends_messages(self, mock_queue_publisher, mock_sleep):
    """Test that publisher main function sends 100 messages"""
    # Mock the context manager and publisher
    mock_publisher = Mock()
    mock_publisher.push.return_value = True
    mock_queue_publisher.return_value.__enter__.return_value = mock_publisher
    mock_queue_publisher.return_value.__exit__.return_value = None

    # Run the main function
    publisher_main()

    # Verify queue_publisher was called with correct parameters
    mock_queue_publisher.assert_called_once_with(
        queue_name="task_queue",
        uri="amqp://guest:guest@localhost:5672/",
        queue_size=5,
        retry_times=5,
        retry_delay=0.1,
    )

    # Verify 100 messages were sent
    assert mock_publisher.push.call_count == 100

    # Verify messages have correct format
    expected_calls = [call({"content": f"Message #{i+1}"}) for i in range(100)]
    mock_publisher.push.assert_has_calls(expected_calls)

    # Verify sleep was called 100 times with 0.5 seconds
    assert mock_sleep.call_count == 100
    mock_sleep.assert_has_calls([call(0.5)] * 100)

  @patch("examples.publisher_example.time.sleep")
  @patch("examples.publisher_example.queue_publisher")
  @patch("builtins.print")
  def test_publisher_main_handles_failed_messages(self, mock_print, mock_queue_publisher, mock_sleep):
    """Test that publisher handles failed message sends"""
    # Mock the context manager and publisher
    mock_publisher = Mock()
    # Make some messages fail
    mock_publisher.push.side_effect = [True, False, True, False] + [True] * 96
    mock_queue_publisher.return_value.__enter__.return_value = mock_publisher
    mock_queue_publisher.return_value.__exit__.return_value = None

    # Run the main function
    publisher_main()

    # Verify failure messages were printed
    failure_calls = [call for call in mock_print.call_args_list if "❌ Failed to send" in str(call)]
    assert len(failure_calls) == 2  # Messages #2 and #4 failed

  @patch("examples.publisher_example.time.sleep")
  @patch("examples.publisher_example.queue_publisher")
  @patch("builtins.print")
  def test_publisher_main_prints_status_messages(self, mock_print, mock_queue_publisher, mock_sleep):
    """Test that publisher prints appropriate status messages"""
    # Mock the context manager and publisher
    mock_publisher = Mock()
    mock_publisher.push.return_value = True
    mock_queue_publisher.return_value.__enter__.return_value = mock_publisher
    mock_queue_publisher.return_value.__exit__.return_value = None

    # Run the main function
    publisher_main()

    # Check for initialization message
    init_calls = [call for call in mock_print.call_args_list if "✅ Publisher initialized" in str(call)]
    assert len(init_calls) == 1

    # Check for completion message
    finish_calls = [call for call in mock_print.call_args_list if "✅ Finished sending messages" in str(call)]
    assert len(finish_calls) == 1

    # Check for sending messages (should be 100)
    send_calls = [call for call in mock_print.call_args_list if "📤 Sending:" in str(call)]
    assert len(send_calls) == 100


class TestConsumerExample:
  """Test cases for consumer example script"""

  @patch("examples.consumer_example.queue_consumer")
  @patch("builtins.print")
  def test_consumer_main_processes_messages(self, mock_print, mock_queue_consumer):
    """Test that consumer main function processes messages"""
    # Mock the context manager and consumer
    mock_consumer = Mock()

    # Mock messages to be consumed
    test_messages = [
        {
            "content": "Message #1"
        },
        {
            "content": "Message #2"
        },
        "String message",
        {
            "content": "Message #4"
        },
    ]

    def mock_read_continuous():
      for msg in test_messages:
        yield msg
      # Simulate KeyboardInterrupt to stop the loop
      raise KeyboardInterrupt("User stopped")

    mock_consumer.read_continuous.side_effect = mock_read_continuous
    mock_queue_consumer.return_value.__enter__.return_value = mock_consumer
    mock_queue_consumer.return_value.__exit__.return_value = None

    # Run the main function
    consumer_main()

    # Verify queue_consumer was called with correct parameters
    mock_queue_consumer.assert_called_once_with(
        queue_name="task_queue",
        uri="amqp://guest:guest@localhost:5672/",
        queue_size=5,
    )

    # Verify read_continuous was called
    mock_consumer.read_continuous.assert_called_once()

    # Check for received message prints
    received_calls = [call for call in mock_print.call_args_list if "📥 Received:" in str(call)]
    assert len(received_calls) == 4

    # Check for shutdown message
    shutdown_calls = [call for call in mock_print.call_args_list if "✅ Consumer shutdown complete" in str(call)]
    assert len(shutdown_calls) == 1

  @patch("examples.consumer_example.queue_consumer")
  @patch("builtins.print")
  def test_consumer_main_handles_exception(self, mock_print, mock_queue_consumer):
    """Test that consumer handles exceptions properly"""
    # Mock the context manager and consumer
    mock_consumer = Mock()
    mock_consumer.read_continuous.side_effect = Exception("Connection error")
    mock_queue_consumer.return_value.__enter__.return_value = mock_consumer
    mock_queue_consumer.return_value.__exit__.return_value = None

    # Run the main function
    consumer_main()

    # Check for error message
    error_calls = [call for call in mock_print.call_args_list if "❌ Consumer error:" in str(call)]
    assert len(error_calls) == 1

  @patch("examples.consumer_example.queue_consumer")
  @patch("builtins.print")
  def test_consumer_main_prints_startup_messages(self, mock_print, mock_queue_consumer):
    """Test that consumer prints startup messages"""
    # Mock the context manager and consumer
    mock_consumer = Mock()
    mock_consumer.read_continuous.side_effect = KeyboardInterrupt("User stopped")
    mock_queue_consumer.return_value.__enter__.return_value = mock_consumer
    mock_queue_consumer.return_value.__exit__.return_value = None

    # Run the main function
    consumer_main()

    # Check for startup messages
    startup_calls = [call for call in mock_print.call_args_list if "🔄 Starting continuous consumer" in str(call)]
    assert len(startup_calls) == 1

    ready_calls = [call for call in mock_print.call_args_list if "✅ Queue 'task_queue' ready" in str(call)]
    assert len(ready_calls) == 1
