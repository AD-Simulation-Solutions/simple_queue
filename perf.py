#!/usr/bin/env python3
"""
Performance test for simple_queue with multiple publishers and consumers.
Measures throughput and latency for 10,000 messages.
Uses multiprocessing instead of threading for true parallel performance.
"""

import time
import multiprocessing
import statistics
from datetime import datetime
from collections import defaultdict
from simple_queue import queue_publisher, queue_consumer


# Configuration
QUEUE_NAME = "perf_test_queue"
REDIS_URI = "redis://localhost:6380/0"
QUEUE_SIZE = 1
TOTAL_MESSAGES = 10
NUM_PUBLISHERS = 3
NUM_CONSUMERS = 3
RETRY_TIMES = 3
RETRY_DELAY = 0.1


def publisher_worker(worker_id, messages_per_worker, publish_counter, publish_dict):
    """Publisher process that sends messages to the queue."""
    print(f"[Publisher {worker_id}] Starting...")
    
    local_published = 0
    local_times = {}
    
    try:
        with queue_publisher(
            queue_name=QUEUE_NAME,
            uri=REDIS_URI,
            queue_size=QUEUE_SIZE,
            retry_times=RETRY_TIMES,
            retry_delay=RETRY_DELAY
        ) as pub:
            for i in range(messages_per_worker):
                msg_id = f"pub{worker_id}_msg{i}"
                timestamp = time.time()
                
                message = {
                    "id": msg_id,
                    "worker_id": worker_id,
                    "sequence": i,
                    "timestamp": timestamp,
                    "data": f"Message from publisher {worker_id}, sequence {i}"
                }
                
                success = pub.push(message)
                
                if success:
                    local_times[msg_id] = timestamp
                    local_published += 1
                    
                    with publish_counter.get_lock():
                        publish_counter.value += 1
                        if publish_counter.value % 1000 == 0:
                            print(f"[Progress] Published {publish_counter.value}/{TOTAL_MESSAGES} messages")
                else:
                    print(f"[Publisher {worker_id}] Failed to publish message {msg_id}")
            
            # Store all publish times in shared dict
            publish_dict.update(local_times)
                    
    except Exception as e:
        print(f"[Publisher {worker_id}] Error: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"[Publisher {worker_id}] Finished - published {local_published} messages")


def consumer_worker(worker_id, consume_counter, latency_list, stop_event, end_time_value, consumed_ids):
    """Consumer process that reads messages from the queue."""
    print(f"[Consumer {worker_id}] Starting...")
    
    local_consumed = 0
    local_latencies = []
    
    try:
        with queue_consumer(
            queue_name=QUEUE_NAME,
            uri=REDIS_URI,
            queue_size=QUEUE_SIZE
        ) as cons:
            for message in cons.read_continuous():
                if stop_event.is_set():
                    break
                
                receive_time = time.time()
                
                try:
                    msg_id = message.get("id")
                    publish_time = message.get("timestamp")
                    
                    if msg_id and publish_time:
                        # Check for duplicate consumption
                        if msg_id in consumed_ids:
                            print(f"[Consumer {worker_id}] ERROR: Duplicate message detected! ID: {msg_id}")
                            raise AssertionError(f"Duplicate message consumed: {msg_id}")
                        
                        # Mark message as consumed
                        consumed_ids[msg_id] = worker_id
                        
                        # Calculate latency
                        latency_ms = (receive_time - publish_time) * 1000
                        local_latencies.append(latency_ms)
                        local_consumed += 1
                        
                        with consume_counter.get_lock():
                            consume_counter.value += 1
                            current_count = consume_counter.value
                            
                            if current_count % 1000 == 0:
                                print(f"[Progress] Consumed {current_count}/{TOTAL_MESSAGES} messages")
                            
                            # Check if we're done
                            if current_count >= TOTAL_MESSAGES:
                                end_time_value.value = time.time()
                                stop_event.set()
                                break
                    else:
                        print(f"[Consumer {worker_id}] Invalid message format: {message}")
                        
                except Exception as e:
                    print(f"[Consumer {worker_id}] Error processing message: {e}")
            
            # Add local latencies to shared list
            latency_list.extend(local_latencies)
                    
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"[Consumer {worker_id}] Error: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"[Consumer {worker_id}] Finished - consumed {local_consumed} messages")


def print_statistics(messages_published, messages_consumed, start_time, end_time, latencies):
    """Print performance statistics."""
    print("\n" + "="*70)
    print("PERFORMANCE TEST RESULTS")
    print("="*70)
    
    print(f"\nConfiguration:")
    print(f"  Total Messages:     {TOTAL_MESSAGES}")
    print(f"  Publisher Processes: {NUM_PUBLISHERS}")
    print(f"  Consumer Processes:  {NUM_CONSUMERS}")
    print(f"  Queue Size:         {QUEUE_SIZE}")
    
    print(f"\nPublishing:")
    print(f"  Messages Published: {messages_published}")
    print(f"  Success Rate:       {(messages_published/TOTAL_MESSAGES)*100:.2f}%")
    
    print(f"\nConsuming:")
    print(f"  Messages Consumed:  {messages_consumed}")
    print(f"  Success Rate:       {(messages_consumed/TOTAL_MESSAGES)*100:.2f}%")
    
    if start_time and end_time:
        duration = end_time - start_time
        print(f"\nThroughput:")
        print(f"  Total Duration:     {duration:.2f} seconds")
        print(f"  Messages/Second:    {messages_consumed/duration:.2f} msg/s")
        print(f"  Avg Time/Message:   {(duration/messages_consumed)*1000:.2f} ms")
    
    if latencies:
        print(f"\nLatency (Publish to Consume):")
        print(f"  Min:                {min(latencies):.2f} ms")
        print(f"  Max:                {max(latencies):.2f} ms")
        print(f"  Mean:               {statistics.mean(latencies):.2f} ms")
        print(f"  Median:             {statistics.median(latencies):.2f} ms")
        if len(latencies) > 1:
            print(f"  Std Dev:            {statistics.stdev(latencies):.2f} ms")
        
        # Percentiles
        sorted_latencies = sorted(latencies)
        p50 = sorted_latencies[int(len(sorted_latencies) * 0.50)]
        p95 = sorted_latencies[int(len(sorted_latencies) * 0.95)]
        p99 = sorted_latencies[int(len(sorted_latencies) * 0.99)]
        
        print(f"\nLatency Percentiles:")
        print(f"  P50 (median):       {p50:.2f} ms")
        print(f"  P95:                {p95:.2f} ms")
        print(f"  P99:                {p99:.2f} ms")
    
    print("\n" + "="*70)


def main():
    """Main function to run the performance test."""
    print("="*70)
    print("SIMPLE_QUEUE PERFORMANCE TEST (Multiprocessing)")
    print("="*70)
    print(f"\nStarting test with {NUM_PUBLISHERS} publishers and {NUM_CONSUMERS} consumers")
    print(f"Processing {TOTAL_MESSAGES} messages...\n")
    
    # Create shared state using multiprocessing primitives
    manager = multiprocessing.Manager()
    publish_counter = multiprocessing.Value('i', 0)
    consume_counter = multiprocessing.Value('i', 0)
    publish_dict = manager.dict()
    latency_list = manager.list()
    consumed_ids = manager.dict()  # Track consumed message IDs to detect duplicates
    stop_event = multiprocessing.Event()
    end_time_value = multiprocessing.Value('d', 0.0)
    
    # Calculate messages per publisher
    messages_per_publisher = TOTAL_MESSAGES // NUM_PUBLISHERS
    remainder = TOTAL_MESSAGES % NUM_PUBLISHERS
    
    # Start consumer processes first
    consumer_processes = []
    for i in range(NUM_CONSUMERS):
        process = multiprocessing.Process(
            target=consumer_worker,
            args=(i, consume_counter, latency_list, stop_event, end_time_value, consumed_ids)
        )
        process.start()
        consumer_processes.append(process)
    
    # Give consumers time to connect
    time.sleep(2)
    
    # Start the timer
    start_time = time.time()
    
    # Start publisher processes
    publisher_processes = []
    for i in range(NUM_PUBLISHERS):
        # Give the last publisher any remainder messages
        msg_count = messages_per_publisher + (remainder if i == NUM_PUBLISHERS - 1 else 0)
        process = multiprocessing.Process(
            target=publisher_worker,
            args=(i, msg_count, publish_counter, publish_dict)
        )
        process.start()
        publisher_processes.append(process)
    
    # Wait for all publishers to finish
    for process in publisher_processes:
        process.join()
    
    print("\n[Main] All publishers finished. Waiting for consumers to finish...")
    
    # Wait for consumers to finish or timeout
    timeout = 60  # 60 seconds timeout
    start_wait = time.time()
    while consume_counter.value < TOTAL_MESSAGES and (time.time() - start_wait) < timeout:
        time.sleep(0.5)
    
    # Signal consumers to stop
    stop_event.set()
    
    # Wait for consumer processes to finish (with timeout)
    for process in consumer_processes:
        process.join(timeout=5)
        if process.is_alive():
            process.terminate()
            process.join(timeout=2)
    
    # Get final values
    final_published = publish_counter.value
    final_consumed = consume_counter.value
    final_end_time = end_time_value.value if end_time_value.value > 0 else time.time()
    final_latencies = list(latency_list)
    
    # Print statistics
    print_statistics(final_published, final_consumed, start_time, final_end_time, final_latencies)


if __name__ == "__main__":
    # Required for multiprocessing on some platforms
    multiprocessing.set_start_method('spawn', force=True)
    
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n[Main] Test interrupted by user")
    except Exception as e:
        print(f"\n[Main] Error: {e}")
        import traceback
        traceback.print_exc()
