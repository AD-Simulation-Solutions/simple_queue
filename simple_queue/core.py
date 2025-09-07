import rabbitpy
import json
import time
import logging
from contextlib import contextmanager
from typing import Optional, Any, Generator, Union

log = logging.getLogger(__name__)


class QueueConnection:
    """Manages RabbitMQ connection with automatic reconnection.
    
    This class handles the low-level connection management to RabbitMQ,
    including automatic reconnection when the connection is lost.
    It maintains both a connection and a channel for RabbitMQ operations.
    
    Attributes:
        uri (str): The RabbitMQ connection URI
        connection: The RabbitMQ connection object
        channel: The RabbitMQ channel object
    
    Note:
        The connection is established immediately upon instantiation.
        If the connection fails, it will be automatically re-established
        when needed through the _ensure_connected method.
    """

    def __init__(self, uri):
        self.uri = uri
        self.connection = None
        self.channel = None
        self._connect()

    def _connect(self) -> None:
        """Establish connection and channel.
        
        Creates a new RabbitMQ connection and channel. If an existing
        connection exists, it will be closed first to prevent resource leaks.
        
        Raises:
            Exception: If connection to RabbitMQ fails
        """
        try:
            if self.connection and not self.connection.closed:
                self.connection.close()
        except Exception as e:
            log.debug(f"Error closing existing connection: {e}")

        self.connection = rabbitpy.Connection(self.uri)
        self.channel = self.connection.channel()
        log.debug(f"Connected to RabbitMQ: {self.uri}")

    def _ensure_connected(self) -> None:
        """Ensure connection is active, reconnect if needed.
        
        Checks if both connection and channel are open and functional.
        If either is closed or None, establishes a new connection.
        This method is called before any RabbitMQ operation to ensure
        connectivity.
        
        Note:
            This method is idempotent - calling it multiple times
            when already connected has no effect.
        """
        if (
            not self.connection
            or self.connection.closed
            or not self.channel
            or self.channel.closed
        ):
            log.info("Connection lost, reconnecting...")
            self._connect()

    def close(self) -> None:
        """Close connection and channel gracefully.
        
        Closes both the channel and connection in the proper order.
        Exceptions during closing are logged but not raised to ensure
        cleanup always completes.
        
        Note:
            After calling this method, the connection cannot be reused.
            A new QueueConnection instance must be created.
        """
        try:
            if self.channel and not self.channel.closed:
                self.channel.close()
            if self.connection and not self.connection.closed:
                self.connection.close()
        except Exception as e:
            log.debug(f"Error closing connection: {e}")


class QueuePublisher:
    """Publisher for a specific queue with retry logic and automatic queue management.
    
    This class handles publishing messages to a RabbitMQ queue with built-in retry
    mechanisms and automatic queue creation. It uses a dedicated exchange for each
    queue to ensure proper message routing.
    
    Args:
        queue_name (str): Name of the queue to publish to
        uri (str): RabbitMQ connection URI
        queue_size (int): Maximum number of messages the queue can hold
        retry_times (int): Number of retry attempts for failed publishes
        retry_delay (float): Delay in seconds between retry attempts
    
    Important Queue Size Behavior:
        If the queue already exists in RabbitMQ, the queue_size parameter is IGNORED.
        The existing queue's configuration (including max_length) will be preserved.
        This is a RabbitMQ limitation - queue properties cannot be changed after creation.
        A warning will be logged when this occurs.
        
        To change queue size for an existing queue, you must:
        1. Delete the existing queue manually, or
        2. Use a different queue name
    
    Usage:
        Can be used as a context manager for automatic connection management:
        
        with QueuePublisher('my_queue', 'amqp://localhost', 1000, 3, 1.0) as pub:
            pub.push({'message': 'hello'})
    
    Attributes:
        queue_name (str): The queue name
        uri (str): Connection URI
        queue_size (int): Requested queue size (may be ignored if queue exists)
        retry_times (int): Number of retry attempts
        retry_delay (float): Delay between retries
        connection (Optional[QueueConnection]): Active connection instance
    """

    def __init__(
        self,
        queue_name: str,
        uri: str,
        queue_size: int,
        retry_times: int,
        retry_delay: float,
    ):
        """Initialize QueuePublisher with connection and retry parameters.
        
        Args:
            queue_name: Name of the queue to publish to
            uri: RabbitMQ connection URI (e.g., 'amqp://user:pass@host:port/vhost')
            queue_size: Maximum queue length (ignored if queue already exists)
            retry_times: Number of retry attempts for failed operations
            retry_delay: Seconds to wait between retry attempts
        """
        self.queue_name = queue_name
        self.uri = uri
        self.queue_size = queue_size
        self.retry_times = retry_times
        self.retry_delay = retry_delay
        self.connection: Optional[QueueConnection] = None
        self._exchange_name = f"{queue_name}_exchange"

    def __enter__(self) -> "QueuePublisher":
        """Enter context manager - establishes connection and sets up queue."""
        self.connection = QueueConnection(self.uri)
        self._setup_queue()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager - closes connection gracefully."""
        if self.connection:
            self.connection.close()

    def _setup_queue(self) -> None:
        """Setup queue with exchange and binding.
        
        Creates or references the queue and sets up the associated exchange.
        This method implements the critical queue size behavior:
        
        1. First attempts to declare the queue passively (check if exists)
        2. If queue exists: Uses existing queue with its current configuration
           - The queue_size parameter is IGNORED in this case
           - A warning is logged to inform about this behavior
        3. If queue doesn't exist: Creates new queue with specified constraints
           - Sets max_length to queue_size
           - Configures x-overflow as 'reject-publish' (rejects new messages when full)
           - Makes queue durable (survives broker restarts)
        
        Also creates a dedicated exchange for the queue and binds them together.
        Enables publisher confirms for reliable message delivery.
        
        Raises:
            AssertionError: If connection is not established
            Exception: If queue/exchange setup fails
        """
        assert self.connection is not None, "Connection must be established first"
        self.connection._ensure_connected()

        # Check if queue exists, if not create with constraints
        try:
            queue = rabbitpy.Queue(self.connection.channel, self.queue_name)
            queue.declare(passive=True)
            # Queue exists, just reference it
            queue = rabbitpy.Queue(self.connection.channel, self.queue_name)
            log.warning(f"Queue '{self.queue_name}' already exists use existing constraints instead of requested: {self.queue_size}")
        except Exception:
            # Queue doesn't exist, reconnect and create with constraints
            self.connection._connect()
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

    def push(self, message_data: Union[dict, list, str]) -> bool:
        """Push message to queue with retry logic and automatic serialization.
        
        Publishes a message to the configured queue with automatic retry on failure.
        Messages are automatically serialized based on their type:
        - dict/list: Converted to JSON string
        - str: Used as-is
        - Other types: Converted to string
        
        Args:
            message_data: The message to publish (dict, list, or string)
            
        Returns:
            bool: True if message was successfully published, False otherwise
            
        Behavior:
            - Uses mandatory=True to ensure message reaches a queue
            - Retries on failure with configurable delay
            - Logs success/failure for monitoring
            - Automatically reconnects if connection is lost
            
        Note:
            If the queue is full and configured with x-overflow='reject-publish',
            the publish will fail and be retried according to retry_times.
        """
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
    """Consumer for a specific queue with automatic reconnection and queue management.
    
    This class handles consuming messages from a RabbitMQ queue with built-in
    reconnection logic and automatic queue creation. It provides both single
    message reading and continuous consumption capabilities.
    
    Args:
        queue_name (str): Name of the queue to consume from
        uri (str): RabbitMQ connection URI
        queue_size (int): Maximum number of messages the queue can hold
    
    Important Queue Size Behavior:
        If the queue already exists in RabbitMQ, the queue_size parameter is IGNORED.
        The existing queue's configuration (including max_length) will be preserved.
        This is a RabbitMQ limitation - queue properties cannot be changed after creation.
        A warning will be logged when this occurs.
        
        To change queue size for an existing queue, you must:
        1. Delete the existing queue manually, or
        2. Use a different queue name
    
    Usage:
        Can be used as a context manager for automatic connection management:
        
        with QueueConsumer('my_queue', 'amqp://localhost', 1000) as consumer:
            message = consumer.read()
            # or
            for message in consumer.read_continuous():
                process(message)
    
    Attributes:
        queue_name (str): The queue name
        uri (str): Connection URI
        queue_size (int): Requested queue size (may be ignored if queue exists)
        connection (Optional[QueueConnection]): Active connection instance
    """

    def __init__(self, queue_name: str, uri: str, queue_size: int):
        """Initialize QueueConsumer with connection parameters.
        
        Args:
            queue_name: Name of the queue to consume from
            uri: RabbitMQ connection URI (e.g., 'amqp://user:pass@host:port/vhost')
            queue_size: Maximum queue length (ignored if queue already exists)
        """
        self.queue_name = queue_name
        self.uri = uri
        self.queue_size = queue_size
        self.connection: Optional[QueueConnection] = None
        self._queue = None

    def __enter__(self) -> "QueueConsumer":
        """Enter context manager - establishes connection and sets up queue."""
        self.connection = QueueConnection(self.uri)
        self._setup_queue()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager - closes connection gracefully."""
        if self.connection:
            self.connection.close()

    def _setup_queue(self) -> None:
        """Setup queue for consumption.
        
        Creates or references the queue for message consumption.
        This method implements the same critical queue size behavior as QueuePublisher:
        
        1. First attempts to declare the queue passively (check if exists)
        2. If queue exists: Uses existing queue with its current configuration
           - The queue_size parameter is IGNORED in this case
           - A warning is logged to inform about this behavior
        3. If queue doesn't exist: Creates new queue with specified constraints
           - Sets max_length to queue_size
           - Configures x-overflow as 'reject-publish' (rejects new messages when full)
           - Makes queue durable (survives broker restarts)
        
        Also sets prefetch_count to 1 for fair message distribution among consumers.
        
        Raises:
            AssertionError: If connection is not established
            Exception: If queue setup fails
        """
        assert self.connection is not None, "Connection must be established first"
        self.connection._ensure_connected()

        # Check if queue exists, if not create with constraints
        try:
            queue = rabbitpy.Queue(self.connection.channel, self.queue_name)
            queue.declare(passive=True)
            # Queue exists, just reference it
            self._queue = rabbitpy.Queue(self.connection.channel, self.queue_name)
            log.warning(f"Queue '{self.queue_name}' already exists. Do not recreate it.")
        except Exception:
            # Queue doesn't exist, reconnect and create with constraints
            self.connection._connect()
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

    def read(self, timeout: Optional[float] = None) -> Optional[Union[dict, str]]:
        """Read one message from queue with automatic reconnection handling.
        
        Reads a single message from the queue and automatically acknowledges it.
        Messages are automatically deserialized:
        - Valid JSON strings are parsed into dict/list objects
        - Non-JSON strings are returned as-is
        
        Args:
            timeout: Currently not implemented - method blocks until message arrives
            
        Returns:
            Optional[Union[dict, str]]: The message content, or None if no message available
            
        Behavior:
            - Blocks until a message is available (timeout not yet implemented)
            - Automatically acknowledges messages after successful retrieval
            - Retries up to 3 times on connection errors with 1-second delays
            - Automatically reconnects if connection is lost
            
        Raises:
            Exception: If all retry attempts fail
            
        Note:
            Messages are acknowledged immediately after retrieval, so they won't
            be redelivered even if processing fails later.
        """
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
        """Generator that continuously reads messages with automatic reconnection.
        
        Yields messages from the queue indefinitely until interrupted or an
        unrecoverable error occurs. Each message is automatically acknowledged
        after being yielded.
        
        Yields:
            Union[dict, str]: Messages from the queue, automatically deserialized
            
        Behavior:
            - Runs indefinitely until KeyboardInterrupt or unrecoverable error
            - Automatically acknowledges messages after yielding them
            - Handles connection errors with automatic reconnection (5-second delay)
            - Acknowledges messages even if processing raises an exception
            - Logs errors for monitoring and debugging
            
        Error Handling:
            - KeyboardInterrupt: Gracefully stops the generator
            - Processing errors: Logged but don't stop consumption
            - Connection errors: Automatic reconnection with 5-second delay
            
        Note:
            Messages are acknowledged immediately after being yielded, so they
            won't be redelivered even if the consuming code raises an exception.
        """
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
    """Context manager for queue publisher with automatic resource management.
    
    Provides a convenient way to create and manage a QueuePublisher instance
    with automatic connection setup and cleanup.
    
    Args:
        queue_name: Name of the queue to publish to
        uri: RabbitMQ connection URI
        queue_size: Maximum queue length (ignored if queue already exists)
        retry_times: Number of retry attempts for failed operations
        retry_delay: Seconds to wait between retry attempts
        
    Yields:
        QueuePublisher: Configured publisher instance ready for use
        
    Example:
        with queue_publisher('my_queue', 'amqp://localhost', 1000, 3, 1.0) as pub:
            pub.push({'message': 'hello world'})
    
    Note:
        The connection is automatically closed when exiting the context,
        even if an exception occurs during message publishing.
    """
    publisher = QueuePublisher(queue_name, uri, queue_size, retry_times, retry_delay)
    with publisher as pub:
        yield pub


@contextmanager
def queue_consumer(
    queue_name: str, uri: str, queue_size: int
) -> Generator[QueueConsumer, None, None]:
    """Context manager for queue consumer with automatic resource management.
    
    Provides a convenient way to create and manage a QueueConsumer instance
    with automatic connection setup and cleanup.
    
    Args:
        queue_name: Name of the queue to consume from
        uri: RabbitMQ connection URI
        queue_size: Maximum queue length (ignored if queue already exists)
        
    Yields:
        QueueConsumer: Configured consumer instance ready for use
        
    Example:
        with queue_consumer('my_queue', 'amqp://localhost', 1000) as consumer:
            message = consumer.read()
            # or
            for message in consumer.read_continuous():
                process(message)
    
    Note:
        The connection is automatically closed when exiting the context,
        even if an exception occurs during message consumption.
    """
    consumer = QueueConsumer(queue_name, uri, queue_size)
    with consumer as cons:
        yield cons
