import rabbitpy
import json
import time
import logging
from contextlib import contextmanager
from typing import Optional, Any, Generator, Union

log = logging.getLogger(__name__)


class QueueConnection:
    """Manages RabbitMQ connection with automatic reconnection"""

    def __init__(self, uri):
        self.uri = uri
        self.connection = None
        self.channel = None
        self._connect()

    def _connect(self) -> None:
        """Establish connection and channel"""
        try:
            if self.connection and not self.connection.closed:
                self.connection.close()
        except Exception as e:
            log.debug(f"Error closing existing connection: {e}")

        self.connection = rabbitpy.Connection(self.uri)
        self.channel = self.connection.channel()
        log.debug(f"Connected to RabbitMQ: {self.uri}")

    def _ensure_connected(self) -> None:
        """Ensure connection is active, reconnect if needed"""
        if (
            not self.connection
            or self.connection.closed
            or not self.channel
            or self.channel.closed
        ):
            log.info("Connection lost, reconnecting...")
            self._connect()

    def close(self) -> None:
        """Close connection"""
        try:
            if self.channel and not self.channel.closed:
                self.channel.close()
            if self.connection and not self.connection.closed:
                self.connection.close()
        except Exception as e:
            log.debug(f"Error closing connection: {e}")


class QueuePublisher:
    """Publisher for a specific queue with retry logic"""

    def __init__(
        self,
        queue_name: str,
        uri: str,
        queue_size: int,
        retry_times: int,
        retry_delay: float,
    ):
        self.queue_name = queue_name
        self.uri = uri
        self.queue_size = queue_size
        self.retry_times = retry_times
        self.retry_delay = retry_delay
        self.connection: Optional[QueueConnection] = None
        self._exchange_name = f"{queue_name}_exchange"

    def __enter__(self) -> "QueuePublisher":
        self.connection = QueueConnection(self.uri)
        self._setup_queue()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.connection:
            self.connection.close()

    def _setup_queue(self) -> None:
        """Setup queue with exchange and binding"""
        assert self.connection is not None, "Connection must be established first"
        self.connection._ensure_connected()

        # Declare queue with overflow protection (durable=True for persistence)
        queue = rabbitpy.Queue(
            self.connection.channel,
            self.queue_name,
            durable=True,
            max_length=self.queue_size,
            arguments={"x-overflow": "reject-publish"},
        )
        queue.declare()

        # Declare exchange and bind queue
        exchange = rabbitpy.Exchange(self.connection.channel, self._exchange_name)
        exchange.declare()
        queue.bind(exchange, self.queue_name)

        # Enable publisher confirms
        self.connection.channel.enable_publisher_confirms()

        log.info(
            f"Queue '{self.queue_name}' setup complete (max_length={self.queue_size})"
        )

    def push(self, message_data: Union[dict, list, str]) -> bool:
        """Push message to queue with retry logic"""
        assert self.connection is not None, "Connection must be established first"
        
        if isinstance(message_data, (dict, list)):
            message_body = json.dumps(message_data)
        else:
            message_body = str(message_data)

        for attempt in range(self.retry_times):
            try:
                self.connection._ensure_connected()

                message = rabbitpy.Message(self.connection.channel, message_body)
                exchange = rabbitpy.Exchange(
                    self.connection.channel, self._exchange_name
                )

                published = message.publish(exchange, self.queue_name, mandatory=True)

                if published:
                    log.info(
                        f"Published to queue '{self.queue_name}': "
                        f"{message_body[:100]}..."
                    )
                    return True
                else:
                    log.warning(
                        f"Publish failed (attempt {attempt+1}/{self.retry_times})"
                    )

            except Exception as e:
                log.warning(
                    f"Publish error (attempt {attempt+1}/{self.retry_times}): {e}"
                )

            if attempt < self.retry_times - 1:
                time.sleep(self.retry_delay)

        log.error(f"Failed to publish after {self.retry_times} attempts")
        return False


class QueueConsumer:
    """Consumer for a specific queue with automatic reconnection"""

    def __init__(self, queue_name: str, uri: str, queue_size: int):
        self.queue_name = queue_name
        self.uri = uri
        self.queue_size = queue_size
        self.connection: Optional[QueueConnection] = None
        self._queue = None

    def __enter__(self) -> "QueueConsumer":
        self.connection = QueueConnection(self.uri)
        self._setup_queue()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.connection:
            self.connection.close()

    def _setup_queue(self) -> None:
        """Setup queue for consumption"""
        assert self.connection is not None, "Connection must be established first"
        self.connection._ensure_connected()

        # Declare queue (should already exist from publisher)
        self._queue = rabbitpy.Queue(
            self.connection.channel,
            self.queue_name,
            durable=True,
            max_length=self.queue_size,
            arguments={"x-overflow": "reject-publish"},
        )
        self._queue.declare()

        # Set prefetch count
        self.connection.channel.prefetch_count(1)

        log.info(f"Consumer ready for queue '{self.queue_name}'")

    def read(self, timeout: Optional[float] = None) -> Optional[Union[dict, str]]:
        """Read one message from queue with automatic reconnection handling"""
        assert self.connection is not None, "Connection must be established first"
        assert self._queue is not None, "Queue must be set up first"
        
        max_retries = 3

        for retry in range(max_retries):
            try:
                self.connection._ensure_connected()

                # Get message iterator
                message_iter = self._queue.consume_messages()

                if timeout:
                    # For timeout support, we'd need to implement non-blocking read
                    # For now, we'll use next() which blocks until message arrives
                    pass

                message = next(message_iter)
                body = message.body.decode()

                # Acknowledge message
                message.ack()

                log.debug(f"Received message from '{self.queue_name}': {body[:100]}...")

                # Try to parse as JSON, fallback to string
                try:
                    return json.loads(body)
                except json.JSONDecodeError:
                    return body

            except StopIteration:
                # No messages available
                return None
            except Exception as e:
                log.warning(f"Read error (attempt {retry+1}/{max_retries}): {e}")
                if retry < max_retries - 1:
                    time.sleep(1)
                    # Reconnect will happen on next iteration
                else:
                    raise

        return None

    def read_continuous(self) -> Generator[Union[dict, str], None, None]:
        """Generator that continuously reads messages with automatic reconnection"""
        assert self.connection is not None, "Connection must be established first"
        assert self._queue is not None, "Queue must be set up first"
        
        while True:
            try:
                self.connection._ensure_connected()

                for message in self._queue.consume_messages():
                    try:
                        body = message.body.decode()
                        message.ack()

                        log.debug(
                            f"Received message from '{self.queue_name}': "
                            f"{body[:100]}..."
                        )

                        # Try to parse as JSON, fallback to string
                        try:
                            yield json.loads(body)
                        except json.JSONDecodeError:
                            yield body

                    except Exception as e:
                        log.error(f"Error processing message: {e}")
                        try:
                            message.ack()  # Acknowledge to avoid redelivery
                        except Exception as ack_error:
                            log.debug(f"Error acknowledging message: {ack_error}")

            except KeyboardInterrupt:
                log.info("Consumer stopped by user")
                break
            except Exception as e:
                log.error(f"Consumer error: {e}")
                log.info("Reconnecting in 5 seconds...")
                time.sleep(5)
                # Connection will be re-established on next iteration


# Context manager functions for easier usage
@contextmanager
def queue_publisher(
    queue_name: str, uri: str, queue_size: int, retry_times: int, retry_delay: float
) -> Generator[QueuePublisher, None, None]:
    """Context manager for queue publisher"""
    publisher = QueuePublisher(queue_name, uri, queue_size, retry_times, retry_delay)
    with publisher as pub:
        yield pub


@contextmanager
def queue_consumer(
    queue_name: str, uri: str, queue_size: int
) -> Generator[QueueConsumer, None, None]:
    """Context manager for queue consumer"""
    consumer = QueueConsumer(queue_name, uri, queue_size)
    with consumer as cons:
        yield cons
