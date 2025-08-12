#!/usr/bin/env python3
"""
Example publisher script demonstrating how to use simple_queue package
"""

import time
from simple_queue import queue_publisher


def main():
    queue_name = "task_queue"
    rabbit_uri = "amqp://guest:guest@localhost:5672/"
    with queue_publisher(
        queue_name=queue_name,
        uri=rabbit_uri,
        queue_size=5,  # Small queue to test overflow
        retry_times=5,
        retry_delay=0.1,
    ) as queue:
        print(
            f"✅ Publisher initialized for queue '{queue_name}' with overflow protection"
        )
        for i in range(100):
            print(f"📤 Sending: {i}")
            success = queue.push({"content": f"Message #{i+1}"})
            if not success:
                print(f"❌ Failed to send message #{i+1}")
            time.sleep(0.5)
        print("✅ Finished sending messages")


if __name__ == "__main__":
    main()
