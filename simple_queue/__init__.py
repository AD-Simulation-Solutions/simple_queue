"""
simple_queue - A simple RabbitMQ streaming library

This package provides easy-to-use classes and context managers for RabbitMQ
message publishing and consuming with automatic reconnection and error handling.
"""

from .core import queue_publisher, queue_consumer, QueuePublisher, QueueConsumer, QueueNotFoundError

__version__ = "0.1.0"
__author__ = "Alex"
__email__ = ""
__description__ = "A simple RabbitMQ streaming library"

__all__ = ["queue_publisher", "queue_consumer", "QueuePublisher", "QueueConsumer", "QueueNotFoundError"]
