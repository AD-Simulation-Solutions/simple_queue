import json
import logging
import time
from contextlib import contextmanager
from typing import Optional, Any, Generator, Union, Dict

import redis

log = logging.getLogger(__name__)

# Backoff constants for blocking when queue is full
DEFAULT_BACKOFF = 0.1
MAX_BACKOFF = 5.0


class QueueNotFoundError(Exception):
  """Exception raised when queue configuration doesn't exist."""

  def __init__(self, queue_name: str, message: Optional[str] = None):
    self.queue_name = queue_name
    self.message = message or f"Queue '{queue_name}' does not exist"
    super().__init__(self.message)


class QueueConnection:
  """Manages Redis connection with automatic reconnection."""

  def __init__(self, uri: str):
    self.uri = uri
    self.client: Optional[redis.Redis] = None
    self._connect()

  def _connect(self) -> None:
    """Establish connection to Redis."""
    if self.client:
      try:
        self.client.close()
      except Exception:
        pass
    self.client = redis.from_url(
        self.uri,
        decode_responses=False,
        socket_keepalive=True,
        health_check_interval=30,
    )
    self.client.ping()
    log.debug(f"Connected to Redis: {self.uri}")

  def _ensure_connected(self, retry_count: int = 0, max_retries: int = 3) -> None:
    """Ensure connection is active, reconnect if needed."""
    if retry_count >= max_retries:
      raise ConnectionError(f"Failed to reconnect after {max_retries} attempts")
    try:
      if self.client is None:
        raise redis.ConnectionError("Client is None")
      self.client.ping()
    except (redis.ConnectionError, redis.TimeoutError, AttributeError):
      log.info(f"Reconnecting... (attempt {retry_count + 1}/{max_retries})")
      try:
        self._connect()
      except Exception as e:
        log.error(f"Reconnection failed: {e}")
        delay = min(2 ** retry_count, 30)
        time.sleep(delay)
        self._ensure_connected(retry_count + 1, max_retries)

  def close(self) -> None:
    """Close connection gracefully."""
    if self.client:
      try:
        self.client.close()
      except Exception:
        pass
      finally:
        self.client = None


class QueuePublisher:
  """Publisher with blocking behavior when queue is full.

  Uses Redis Lists (RPUSH/LLEN). Blocks until space is available.
  Queue size can be changed dynamically via set_queue_size().
  """

  def __init__(self, queue_name: str, uri: str, queue_size: int, retry_times: int, retry_delay: float):
    self.queue_name = queue_name
    self.uri = uri
    self.queue_size = queue_size
    self.retry_times = retry_times
    self.retry_delay = retry_delay
    self.connection: Optional[QueueConnection] = None
    self._config_key = f"{queue_name}:config"

  def __enter__(self) -> "QueuePublisher":
    self.connection = QueueConnection(self.uri)
    self._setup_queue()
    return self

  def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
    if self.connection:
      self.connection.close()

  def _setup_queue(self) -> None:
    """Setup queue configuration."""
    assert self.connection is not None
    self.connection._ensure_connected()
    existing_config = self.connection.client.get(self._config_key)
    if existing_config:
      config = json.loads(existing_config)
      self.queue_size = config.get('max_length', self.queue_size)
    config = {'max_length': self.queue_size}
    self.connection.client.set(self._config_key, json.dumps(config))
    log.info(f"Queue '{self.queue_name}' configured with max_length={self.queue_size}")

  def set_queue_size(self, new_size: int) -> None:
    """Change queue size dynamically without data loss."""
    assert self.connection is not None
    self.connection._ensure_connected()
    old_size = self.queue_size
    self.queue_size = new_size
    config = {'max_length': new_size}
    self.connection.client.set(self._config_key, json.dumps(config))
    log.info(f"Queue '{self.queue_name}' size changed: {old_size} -> {new_size}")

  def _update_queue_size_from_config(self) -> None:
    """Reload queue size from Redis config."""
    if not self.connection.client.exists(self._config_key):
      raise QueueNotFoundError(self.queue_name, "Queue configuration not found")
    config_data = self.connection.client.get(self._config_key)
    if config_data:
      config = json.loads(config_data)
      self.queue_size = config.get('max_length', self.queue_size)

  def push(self, message_data: Union[dict, list, str]) -> bool:
    """Push message, blocking if queue is full."""
    assert self.connection is not None
    if isinstance(message_data, (dict, list)):
      message_body = json.dumps(message_data)
    else:
      message_body = str(message_data)
    for attempt in range(self.retry_times):
      try:
        self.connection._ensure_connected()
        self._update_queue_size_from_config()
        backoff = DEFAULT_BACKOFF
        while True:
          current_length = self.connection.client.llen(self.queue_name)
          if current_length < self.queue_size:
            self.connection.client.rpush(self.queue_name, message_body)
            log.info(f"Published to '{self.queue_name}': {message_body[:100]}...")
            return True
          else:
            log.debug(f"Queue full ({current_length}/{self.queue_size}), blocking {backoff:.2f}s")
            time.sleep(backoff)
            backoff = min(backoff * 1.5, MAX_BACKOFF)
            self.connection._ensure_connected()
            self._update_queue_size_from_config()
      except QueueNotFoundError:
        raise
      except Exception as e:
        log.warning(f"Publish error (attempt {attempt + 1}/{self.retry_times}): {e}")
      if attempt < self.retry_times - 1:
        time.sleep(self.retry_delay)
    log.error(f"Failed to publish after {self.retry_times} attempts")
    return False


class QueueConsumer:
  """Consumer with blocking reads when queue is empty.

  Uses Redis BLPOP for efficient blocking. No polling needed.
  """

  def __init__(self, queue_name: str, uri: str, queue_size: int):
    self.queue_name = queue_name
    self.uri = uri
    self.queue_size = queue_size
    self.connection: Optional[QueueConnection] = None
    self._config_key = f"{queue_name}:config"

  def __enter__(self) -> "QueueConsumer":
    self.connection = QueueConnection(self.uri)
    self._setup_queue()
    return self

  def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
    if self.connection:
      self.connection.close()

  def _setup_queue(self) -> None:
    """Setup queue configuration."""
    assert self.connection is not None
    self.connection._ensure_connected()
    config_data = self.connection.client.get(self._config_key)
    if config_data:
      config = json.loads(config_data)
      self.queue_size = config.get('max_length', self.queue_size)
      log.info(f"Queue '{self.queue_name}' loaded with max_length={self.queue_size}")
    else:
      config = {'max_length': self.queue_size}
      self.connection.client.set(self._config_key, json.dumps(config))
      log.info(f"Created queue '{self.queue_name}' with max_length={self.queue_size}")

  def read(self, timeout: Optional[float] = None) -> Optional[Union[Dict[Any, Any], str]]:
    """Read one message, blocking until available or timeout."""
    assert self.connection is not None
    timeout_seconds = int(timeout) if timeout else 0
    for retry in range(3):
      try:
        self.connection._ensure_connected()
        result = self.connection.client.blpop(self.queue_name, timeout=timeout_seconds)
        if not result:
          return None
        _, message_body = result
        body = message_body.decode('utf-8')
        log.debug(f"Received from '{self.queue_name}': {body[:100]}...")
        try:
          parsed = json.loads(body)
          return parsed if isinstance(parsed, dict) else str(body)
        except json.JSONDecodeError:
          return str(body)
      except Exception as e:
        log.warning(f"Read error (attempt {retry + 1}/3): {e}")
        if retry < 2:
          time.sleep(1)
        else:
          raise
    return None

  def read_continuous(self) -> Generator[Union[Dict[Any, Any], str], None, None]:
    """Continuously read messages, blocking when queue is empty."""
    assert self.connection is not None
    while True:
      try:
        self.connection._ensure_connected()
        result = self.connection.client.blpop(self.queue_name, timeout=5)
        if not result:
          continue
        _, message_body = result
        body = message_body.decode('utf-8')
        log.debug(f"Received from '{self.queue_name}': {body[:100]}...")
        try:
          yield json.loads(body)
        except json.JSONDecodeError:
          yield body
      except KeyboardInterrupt:
        log.info("Consumer stopped by user")
        break
      except Exception as e:
        log.error(f"Consumer error: {e}")
        time.sleep(5)


@contextmanager
def queue_publisher(queue_name: str, uri: str, queue_size: int, retry_times: int, retry_delay: float) -> Generator[QueuePublisher, None, None]:
  """Context manager for queue publisher."""
  publisher = QueuePublisher(queue_name, uri, queue_size, retry_times, retry_delay)
  with publisher as pub:
    yield pub


@contextmanager
def queue_consumer(queue_name: str, uri: str, queue_size: int) -> Generator[QueueConsumer, None, None]:
  """Context manager for queue consumer."""
  consumer = QueueConsumer(queue_name, uri, queue_size)
  with consumer as cons:
    yield cons
