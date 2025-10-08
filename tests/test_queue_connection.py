"""
Tests for QueueConnection class
"""

from unittest.mock import Mock, patch
from simple_queue.core import QueueConnection


class TestQueueConnection:
  """Test cases for QueueConnection class"""

  def test_init_creates_connection(self):
    """Test that __init__ creates connection and channel"""
    with patch("simple_queue.core.rabbitpy.Connection") as mock_conn_class:
      mock_connection = Mock()
      mock_channel = Mock()
      mock_connection.channel.return_value = mock_channel
      mock_conn_class.return_value = mock_connection

      uri = "amqp://localhost:5672"
      conn = QueueConnection(uri)

      # Verify connection was created with correct URI
      mock_conn_class.assert_called_once_with(uri)
      mock_connection.channel.assert_called_once()

      # Verify instance attributes
      assert conn.uri == uri
      assert conn.connection == mock_connection
      assert conn.channel == mock_channel

  def test_connect_closes_existing_connection(self):
    """Test that _connect closes existing connection before creating new one"""
    with patch("simple_queue.core.rabbitpy.Connection") as mock_conn_class:
      # Setup first connection
      mock_old_connection = Mock()
      mock_old_connection.closed = False
      mock_conn_class.return_value = mock_old_connection

      conn = QueueConnection("amqp://localhost:5672")

      # Setup new connection for reconnect
      mock_new_connection = Mock()
      mock_new_channel = Mock()
      mock_new_connection.channel.return_value = mock_new_channel
      mock_conn_class.return_value = mock_new_connection

      # Call _connect again
      conn._connect()

      # Verify old connection was closed
      mock_old_connection.close.assert_called_once()

      # Verify new connection was created
      assert conn.connection == mock_new_connection
      assert conn.channel == mock_new_channel

  def test_connect_handles_close_exception(self):
    """Test that _connect handles exceptions when closing existing connection"""
    with patch("simple_queue.core.rabbitpy.Connection") as mock_conn_class:
      # Setup connection that throws exception on close
      mock_old_connection = Mock()
      mock_old_connection.closed = False
      mock_old_connection.close.side_effect = Exception("Close error")
      mock_conn_class.return_value = mock_old_connection

      conn = QueueConnection("amqp://localhost:5672")

      # Setup new connection
      mock_new_connection = Mock()
      mock_new_channel = Mock()
      mock_new_connection.channel.return_value = mock_new_channel
      mock_conn_class.return_value = mock_new_connection

      # Should not raise exception
      conn._connect()

      # Verify new connection was still created
      assert conn.connection == mock_new_connection

  def test_ensure_connected_reconnects_when_connection_closed(self):
    """Test that _ensure_connected reconnects when connection is closed"""
    with patch("simple_queue.core.rabbitpy.Connection") as mock_conn_class:
      mock_connection = Mock()
      mock_channel = Mock()
      mock_connection.channel.return_value = mock_channel
      mock_conn_class.return_value = mock_connection

      conn = QueueConnection("amqp://localhost:5672")

      # Simulate closed connection
      conn.connection.closed = True

      # Setup new connection for reconnect
      mock_new_connection = Mock()
      mock_new_channel = Mock()
      mock_new_connection.channel.return_value = mock_new_channel
      mock_conn_class.return_value = mock_new_connection

      conn._ensure_connected()

      # Verify reconnection happened
      assert conn.connection == mock_new_connection
      assert conn.channel == mock_new_channel

  def test_ensure_connected_reconnects_when_channel_closed(self):
    """Test that _ensure_connected reconnects when channel is closed"""
    with patch("simple_queue.core.rabbitpy.Connection") as mock_conn_class:
      mock_connection = Mock()
      mock_channel = Mock()
      mock_connection.channel.return_value = mock_channel
      mock_conn_class.return_value = mock_connection

      conn = QueueConnection("amqp://localhost:5672")

      # Simulate closed channel but open connection
      conn.connection.closed = False
      conn.channel.closed = True

      # Setup new connection for reconnect
      mock_new_connection = Mock()
      mock_new_channel = Mock()
      mock_new_connection.channel.return_value = mock_new_channel
      mock_conn_class.return_value = mock_new_connection

      conn._ensure_connected()

      # Verify reconnection happened
      assert conn.connection == mock_new_connection
      assert conn.channel == mock_new_channel

  def test_ensure_connected_does_nothing_when_connected(self):
    """Test that _ensure_connected does nothing when already connected"""
    with patch("simple_queue.core.rabbitpy.Connection") as mock_conn_class:
      mock_connection = Mock()
      mock_channel = Mock()
      mock_connection.closed = False
      mock_channel.closed = False
      mock_connection.channel.return_value = mock_channel
      mock_conn_class.return_value = mock_connection

      conn = QueueConnection("amqp://localhost:5672")
      original_connection = conn.connection
      original_channel = conn.channel

      # Reset call count after __init__
      mock_conn_class.reset_mock()

      conn._ensure_connected()

      # Verify no new connection was created
      mock_conn_class.assert_not_called()
      assert conn.connection == original_connection
      assert conn.channel == original_channel

  def test_close_closes_channel_and_connection(self):
    """Test that close() properly closes channel and connection"""
    with patch("simple_queue.core.rabbitpy.Connection") as mock_conn_class:
      mock_connection = Mock()
      mock_channel = Mock()
      mock_connection.closed = False
      mock_channel.closed = False
      mock_connection.channel.return_value = mock_channel
      mock_conn_class.return_value = mock_connection

      conn = QueueConnection("amqp://localhost:5672")
      conn.close()

      # Verify both channel and connection were closed
      mock_channel.close.assert_called_once()
      mock_connection.close.assert_called_once()

  def test_close_handles_exceptions(self):
    """Test that close() handles exceptions gracefully"""
    with patch("simple_queue.core.rabbitpy.Connection") as mock_conn_class:
      mock_connection = Mock()
      mock_channel = Mock()
      mock_connection.closed = False
      mock_channel.closed = False
      mock_channel.close.side_effect = Exception("Channel close error")
      mock_connection.close.side_effect = Exception("Connection close error")
      mock_connection.channel.return_value = mock_channel
      mock_conn_class.return_value = mock_connection

      conn = QueueConnection("amqp://localhost:5672")

      # Reset mock call counts after __init__
      mock_channel.close.reset_mock()
      mock_connection.close.reset_mock()

      # Should not raise exception
      conn.close()

      # Verify both channel and connection close were attempted
      # The new implementation uses separate try/finally blocks
      mock_channel.close.assert_called_once()
      mock_connection.close.assert_called_once()

  def test_close_handles_channel_exception_but_closes_connection(self):
    """Test that close() handles channel exception but still tries to close
        connection"""
    with patch("simple_queue.core.rabbitpy.Connection") as mock_conn_class:
      mock_connection = Mock()
      mock_channel = Mock()
      mock_connection.closed = False
      mock_channel.closed = False
      # Only channel throws exception
      mock_channel.close.side_effect = Exception("Channel close error")
      mock_connection.channel.return_value = mock_channel
      mock_conn_class.return_value = mock_connection

      conn = QueueConnection("amqp://localhost:5672")

      # Reset mock call counts after __init__
      mock_channel.close.reset_mock()
      mock_connection.close.reset_mock()

      # Should not raise exception
      conn.close()

      # Verify both were attempted - new implementation uses separate try/finally blocks
      # so connection.close() is called even if channel.close() fails
      mock_channel.close.assert_called_once()
      mock_connection.close.assert_called_once()

  def test_close_handles_none_objects(self):
    """Test that close() handles None channel/connection gracefully"""
    with patch("simple_queue.core.rabbitpy.Connection") as mock_conn_class:
      mock_connection = Mock()
      mock_channel = Mock()
      mock_connection.channel.return_value = mock_channel
      mock_conn_class.return_value = mock_connection

      conn = QueueConnection("amqp://localhost:5672")

      # Set to None to simulate uninitialized state
      conn.channel = None
      conn.connection = None

      # Should not raise exception
      conn.close()
