#!/usr/bin/env python3
"""
Performance benchmark script for simple_queue package.

This script creates publisher and consumer threads to benchmark the queue performance
and provides detailed metrics including messages/sec and bytes/sec for both operations.
"""

import argparse
import threading
import time
import json
import sys
from typing import List, Dict, Any
from simple_queue import queue_publisher, queue_consumer


class PerformanceMetrics:
    """Class to track performance metrics during benchmarking"""
    
    def __init__(self):
        self.reset()
    
    def reset(self):
        self.messages_sent = 0
        self.messages_received = 0
        self.bytes_sent = 0
        self.bytes_received = 0
        self.start_time = None
        self.end_time = None
        self.publisher_start_time = None
        self.publisher_end_time = None
        self.consumer_start_time = None
        self.consumer_end_time = None
        self.errors = []


def generate_message(message_size: int) -> Dict[str, Any]:
    """Generate a message of specified size in bytes"""
    # Create base message structure
    base_msg = {
        "id": 0,
        "timestamp": time.time(),
        "data": ""
    }
    
    # Calculate how much padding we need
    base_size = len(json.dumps(base_msg).encode('utf-8'))
    padding_needed = max(0, message_size - base_size)
    
    # Add padding to reach desired size
    base_msg["data"] = "x" * padding_needed
    
    return base_msg


def publisher_thread(rabbit_uri: str, queue_name: str, message_size: int, 
                    num_messages: int, queue_size: int, retry_times: int, 
                    retry_delay: float, metrics: PerformanceMetrics, 
                    stop_event: threading.Event):
    """Publisher thread function"""
    try:
        metrics.publisher_start_time = time.time()
        
        with queue_publisher(
            queue_name=queue_name,
            uri=rabbit_uri,
            queue_size=queue_size,
            retry_times=retry_times,
            retry_delay=retry_delay
        ) as publisher:
            
            for i in range(num_messages):
                if stop_event.is_set():
                    break
                    
                message = generate_message(message_size)
                message["id"] = i + 1
                message["timestamp"] = time.time()
                
                success = publisher.push(message)
                if success:
                    metrics.messages_sent += 1
                    message_bytes = len(json.dumps(message).encode('utf-8'))
                    metrics.bytes_sent += message_bytes
                else:
                    metrics.errors.append(f"Failed to send message {i + 1}")
                    
    except Exception as e:
        metrics.errors.append(f"Publisher error: {e}")
    finally:
        metrics.publisher_end_time = time.time()


def consumer_thread(rabbit_uri: str, queue_name: str, queue_size: int, 
                   expected_messages: int, metrics: PerformanceMetrics, 
                   stop_event: threading.Event):
    """Consumer thread function"""
    try:
        metrics.consumer_start_time = time.time()
        
        with queue_consumer(
            queue_name=queue_name,
            uri=rabbit_uri,
            queue_size=queue_size
        ) as consumer:
            
            messages_received = 0
            start_time = time.time()
            
            # Use read_continuous generator
            for message_data in consumer.read_continuous():
                if stop_event.is_set():
                    break
                    
                metrics.messages_received += 1
                messages_received += 1
                
                # Calculate message size
                if isinstance(message_data, dict):
                    message_bytes = len(json.dumps(message_data).encode('utf-8'))
                else:
                    message_bytes = len(str(message_data).encode('utf-8'))
                
                metrics.bytes_received += message_bytes
                
                # Stop when we've received all expected messages
                if messages_received >= expected_messages:
                    break
                    
                # Timeout check - if we haven't received messages for 10 seconds, stop
                if time.time() - start_time > 10 and messages_received == 0:
                    break
                    
    except Exception as e:
        metrics.errors.append(f"Consumer error: {e}")
    finally:
        metrics.consumer_end_time = time.time()


def run_benchmark(rabbit_uri: str, message_size: int, num_messages: int, 
                 queue_size: int, retry_times: int, retry_delay: float) -> None:
    """Run the performance benchmark"""
    
    queue_name = f"perf_test_queue_{int(time.time())}"
    metrics = PerformanceMetrics()
    stop_event = threading.Event()
    
    print(f"🚀 Starting performance benchmark")
    print(f"   Queue: {queue_name}")
    print(f"   RabbitMQ URI: {rabbit_uri}")
    print(f"   Message size: {message_size} bytes")
    print(f"   Number of messages: {num_messages}")
    print(f"   Queue size: {queue_size}")
    print(f"   Retry times: {retry_times}")
    print(f"   Retry delay: {retry_delay}s")
    print("-" * 60)
    
    # Record overall start time
    metrics.start_time = time.time()
    
    # Create and start threads
    publisher = threading.Thread(
        target=publisher_thread,
        args=(rabbit_uri, queue_name, message_size, num_messages, queue_size, 
              retry_times, retry_delay, metrics, stop_event)
    )
    
    consumer = threading.Thread(
        target=consumer_thread,
        args=(rabbit_uri, queue_name, queue_size, num_messages, metrics, stop_event)
    )
    
    # Start consumer first to ensure it's ready
    consumer.start()
    time.sleep(0.5)  # Give consumer time to initialize
    
    # Start publisher
    publisher.start()
    
    try:
        # Wait for publisher to complete
        publisher.join()
        
        # Give consumer some time to process remaining messages
        time.sleep(2)
        
        # Stop consumer
        stop_event.set()
        consumer.join(timeout=5)
        
    except KeyboardInterrupt:
        print("\n⚠️  Benchmark interrupted by user")
        stop_event.set()
        publisher.join(timeout=2)
        consumer.join(timeout=2)
    
    # Record overall end time
    metrics.end_time = time.time()
    
    # Print results
    print_results(metrics)


def print_results(metrics: PerformanceMetrics) -> None:
    """Print benchmark results"""
    
    print("\n" + "=" * 60)
    print("📊 BENCHMARK RESULTS")
    print("=" * 60)
    
    # Calculate durations
    total_duration = metrics.end_time - metrics.start_time if metrics.start_time and metrics.end_time else 0
    
    publisher_duration = 0
    if metrics.publisher_start_time and metrics.publisher_end_time:
        publisher_duration = metrics.publisher_end_time - metrics.publisher_start_time
    
    consumer_duration = 0
    if metrics.consumer_start_time and metrics.consumer_end_time:
        consumer_duration = metrics.consumer_end_time - metrics.consumer_start_time
    
    # Publisher metrics
    print(f"📤 PUBLISHER METRICS:")
    print(f"   Messages sent: {metrics.messages_sent}")
    print(f"   Bytes sent: {metrics.bytes_sent:,}")
    if publisher_duration > 0:
        pub_msg_per_sec = metrics.messages_sent / publisher_duration
        pub_bytes_per_sec = metrics.bytes_sent / publisher_duration
        print(f"   Messages per second: {pub_msg_per_sec:.2f}")
        print(f"   Bytes per second: {pub_bytes_per_sec:,.2f}")
    else:
        print(f"   Messages per second: N/A")
        print(f"   Bytes per second: N/A")
    
    print()
    
    # Consumer metrics
    print(f"📥 CONSUMER METRICS:")
    print(f"   Messages received: {metrics.messages_received}")
    print(f"   Bytes received: {metrics.bytes_received:,}")
    if consumer_duration > 0:
        cons_msg_per_sec = metrics.messages_received / consumer_duration
        cons_bytes_per_sec = metrics.bytes_received / consumer_duration
        print(f"   Messages per second: {cons_msg_per_sec:.2f}")
        print(f"   Bytes per second: {cons_bytes_per_sec:,.2f}")
    else:
        print(f"   Messages per second: N/A")
        print(f"   Bytes per second: N/A")
    
    print()
    
    # Timing information
    print(f"⏱️  TIMING:")
    print(f"   Total processing time: {total_duration:.2f} seconds")
    print(f"   Publisher duration: {publisher_duration:.2f} seconds")
    print(f"   Consumer duration: {consumer_duration:.2f} seconds")
    
    # Error reporting
    if metrics.errors:
        print(f"\n❌ ERRORS ({len(metrics.errors)}):")
        for error in metrics.errors[:10]:  # Show first 10 errors
            print(f"   {error}")
        if len(metrics.errors) > 10:
            print(f"   ... and {len(metrics.errors) - 10} more errors")
    
    # Summary
    print(f"\n📋 SUMMARY:")
    success_rate = (metrics.messages_received / metrics.messages_sent * 100) if metrics.messages_sent > 0 else 0
    print(f"   Success rate: {success_rate:.1f}%")
    print(f"   Total messages processed: {metrics.messages_received}/{metrics.messages_sent}")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Performance benchmark for simple_queue package",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Required argument
    parser.add_argument(
        "rabbit_uri",
        help="RabbitMQ connection URI (e.g., amqp://guest:guest@localhost:5672/)"
    )
    
    # Optional arguments with defaults
    parser.add_argument(
        "--message-size", "-s",
        type=int,
        default=1024,
        help="Size of each message in bytes"
    )
    
    parser.add_argument(
        "--num-messages", "-n",
        type=int,
        default=1000,
        help="Number of messages to send"
    )
    
    parser.add_argument(
        "--queue-size", "-q",
        type=int,
        default=100,
        help="Maximum queue size"
    )
    
    parser.add_argument(
        "--retry-times", "-r",
        type=int,
        default=3,
        help="Number of retry attempts for failed operations"
    )
    
    parser.add_argument(
        "--retry-delay", "-d",
        type=float,
        default=0.1,
        help="Delay between retry attempts in seconds"
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.message_size <= 0:
        print("❌ Error: Message size must be positive")
        sys.exit(1)
    
    if args.num_messages <= 0:
        print("❌ Error: Number of messages must be positive")
        sys.exit(1)
    
    if args.queue_size <= 0:
        print("❌ Error: Queue size must be positive")
        sys.exit(1)
    
    if args.retry_times < 0:
        print("❌ Error: Retry times cannot be negative")
        sys.exit(1)
    
    if args.retry_delay < 0:
        print("❌ Error: Retry delay cannot be negative")
        sys.exit(1)
    
    # Run benchmark
    try:
        run_benchmark(
            rabbit_uri=args.rabbit_uri,
            message_size=args.message_size,
            num_messages=args.num_messages,
            queue_size=args.queue_size,
            retry_times=args.retry_times,
            retry_delay=args.retry_delay
        )
    except KeyboardInterrupt:
        print("\n⚠️  Benchmark interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Benchmark failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
