"""
Pytest configuration and fixtures for simple_queue tests
"""

import pytest
from unittest.mock import Mock


@pytest.fixture
def mock_rabbitmq_connection():
  """Fixture providing a mock RabbitMQ connection"""
  mock_connection = Mock()
  mock_channel = Mock()
  mock_connection.channel.return_value = mock_channel
  mock_connection.closed = False
  mock_channel.closed = False
  return mock_connection, mock_channel


@pytest.fixture
def mock_queue():
  """Fixture providing a mock RabbitMQ queue"""
  mock_queue = Mock()
  return mock_queue


@pytest.fixture
def mock_exchange():
  """Fixture providing a mock RabbitMQ exchange"""
  mock_exchange = Mock()
  return mock_exchange


@pytest.fixture
def mock_message():
  """Fixture providing a mock RabbitMQ message"""
  mock_message = Mock()
  mock_message.publish.return_value = True
  return mock_message


@pytest.fixture
def sample_queue_config():
  """Fixture providing sample queue configuration"""
  return {
      "queue_name": "test_queue",
      "uri": "amqp://guest:guest@localhost:5672/",
      "queue_size": 1000,
      "retry_times": 3,
      "retry_delay": 1.0,
  }


@pytest.fixture
def sample_messages():
  """Fixture providing sample test messages"""
  return [
      {
          "id": 1,
          "content": "Hello World"
      },
      {
          "id": 2,
          "content": "Test Message"
      },
      "Simple string message",
      [1, 2, 3, "list message"],
      {
          "nested": {
              "data": True
          },
          "array": [1, 2, 3]
      },
  ]
