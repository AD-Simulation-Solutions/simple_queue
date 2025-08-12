#!/usr/bin/env python3
"""
Example consumer script demonstrating how to use simple_queue package
"""

from simple_queue import queue_consumer


def main():
    """Continuously consume messages using the generator approach"""
    queue_name = "task_queue"
    rabbit_uri = "amqp://guest:guest@localhost:5672/"
    print("🔄 Starting continuous consumer...")
    try:
        with queue_consumer(
            queue_name=queue_name, uri=rabbit_uri, queue_size=5
        ) as queue:
            print(f"✅ Queue '{queue_name}' ready for continuous consumption")
            for message_data in queue.read_continuous():
                print(f"📥 Received: {message_data}")
    except KeyboardInterrupt:
        print("\n✅ Consumer shutdown complete")
    except Exception as e:
        print(f"❌ Consumer error: {e}")


if __name__ == "__main__":
    main()
