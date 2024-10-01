#!/usr/bin/env python3

import argparse
import os
import shutil
import logging
import threading
import time
import signal
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from sys import getsizeof
from threading import Lock


logging.basicConfig(level=logging.DEBUG)  # Enable debug logging
executor = None
executor_running = True
work_queue = None


class UniqueQueue():
    def __init__(self):
        self.lock = Lock()
        self.items = set()

    def put(self, item, **kwargs):
        with self.lock:
            self.items.add(item)

    def get(self, **kwargs):
        with self.lock:
            if len(self.items) != 0:
                return self.items.pop()
            return None

# Function to get access times of files in a directory
def get_atimes(directory):
    directory = os.path.abspath(directory)
    atime_dict = defaultdict(int)
    for root, _, files in os.walk(directory):
        for file in files:
            try:
                filepath = os.path.join(root, file)
                atime = os.path.getatime(filepath)
                atime_dict[filepath] = atime
            except Exception as e:
                if os.path.islink(filepath) and "No such file or directory" in str(e):
                    # print(f"{filepath} is a dead symlink")
                    pass
    return atime_dict


def humanbytes(B):
    """Return the given bytes as a human friendly KB, MB, GB, or TB string."""
    B = float(B)
    KB = float(1024)
    MB = float(KB**2)  # 1,048,576
    GB = float(KB**3)  # 1,073,741,824
    TB = float(KB**4)  # 1,099,511,627,776

    if B < KB:
        return "{0} {1}".format(B, "Bytes" if 0 == B > 1 else "Byte")
    elif KB <= B < MB:
        return "{0:.2f} KB".format(B / KB)
    elif MB <= B < GB:
        return "{0:.2f} MB".format(B / MB)
    elif GB <= B < TB:
        return "{0:.2f} GB".format(B / GB)
    elif TB <= B:
        return "{0:.2f} TB".format(B / TB)


# Define the factory function
def default_values():
    return {"previous_time": 0, "threshold_count": 0}


# Define rsync worker
def _move_file(locations):
    logging.debug(locations)
    src = locations[0]
    dest = locations[1]
    rsync_cmd = [
        "rsync",
        "axqHAXWES",
        "--preallocate",
        "--remove-source-files",
        src,
        dest,
    ]
    logging.info(f"Running command {rsync_cmd}")


# def process_queue(queue):
#     # with ThreadPoolExecutor(max_workers=max_workers) as executor:
#     while True:
#         try:
#             item = queue.get()
#         except Empty:
#             continue
#         else:
#             print(f"Processing item {item}")
#             move_file(item)
#             # executor.submit(move_file, item)
#             # time.sleep(2)
#             queue.task_done()
#             # item = queue.get()


def _process_atimes(directory, queue, tracked_atime_thresholds):

    demote_dest = "/mnt/demote"
    promote_dest = "/mnt/promote"
    atimes = get_atimes(directory)
    # stale_atimes_demote = []
    # fresh_atimes_promote = []

    # Check if cached directory is 85% full
    cached_usage = shutil.disk_usage(directory)
    # if cached_usage.used / cached_usage.total > 0.85:
    if cached_usage.used / cached_usage.total > 0.6:
        # Get list of files sorted by access time
        files_sorted_by_atime = sorted(atimes.items(), key=lambda x: x[1])
        # Move the oldest 33% of data to cold
        for i in range(len(files_sorted_by_atime) // 3):
            filepath, atime = files_sorted_by_atime[i]

            logging.info(f"Capacity threshold reached {filepath} is part of oldest 33%")
            queue.put((filepath, ''.join([demote_dest, filepath])))
            # stale_atimes_demote.append(filepath)
            atimes.pop(filepath)
            if filepath in tracked_atime_thresholds.keys():
                tracked_atime_thresholds.pop(filepath)

    for filepath, atime in atimes.items():
        # Check for stale files to demote
        if time.time() - atime > 5 * 3600 and os.path.exists(filepath):
            # print(f"{filepath} is stale, ready to move")
            # stale_atimes_demote.append(filepath)
            queue.put((filepath, ''.join([demote_dest, filepath])))
        # Check if file is not stale
        # only add if current atime is not previous atime and increase count
        if atime > tracked_atime_thresholds[filepath]["previous_time"]:
            tracked_atime_thresholds[filepath]["previous_time"] = atime
            tracked_atime_thresholds[filepath]["threshold_count"] += 1

        if tracked_atime_thresholds[filepath]["threshold_count"] >= 3:
            queue.put((filepath, os.path.join(promote_dest, filepath)))
            tracked_atime_thresholds.pop(filepath)

        return tracked_atime_thresholds

def process_atimes_thread(queue, directory):
    global executor_running
    tracked_atime_thresholds = defaultdict(default_values)
    while executor_running:
        try:
            _process_atimes(directory, queue, tracked_atime_thresholds)
            time.sleep(5)
            # future = executor.submit(process_atimes, directory, queue, tracked_atime_thresholds)
            # tracked_atime_thresholds = future.result()
        except Exception as e:
            logging.error(str(e))


def manage_queue_thread(executor, queue):
    global executor_running
    while executor_running:
        try:
            item = queue.get()
            if item is not None:
                executor.submit(_move_file(item))
            else:
                time.sleep(.1)
        except Exception as e:
            logging.error(str(e))            

def terminate_handler(sig, frame):
    global executor
    global executor_running
    logging.warning("Received termination signal. Stopping ThreadPoolExecutor...")
    if executor:
        executor_running = False
        executor.shutdown(wait=True, cancel_futures=True)

def main():
    global executor
    global executor_running
    global work_queue
    # Create and start the thread for queue processing
    max_workers = 6
    parser = argparse.ArgumentParser(description="test program to test atime filtering")
    parser.add_argument("directory")
    args = parser.parse_args()

    # atimes = get_atime(args.directory)
    # size = getsizeof(atimes)
    # print(humanbytes(size))
    # print(f"Num of files {len(atimes)}")


    # Create a queue to hold items
    # work_queue = Queue()
    work_queue = UniqueQueue()
    executor = ThreadPoolExecutor(max_workers=max_workers)

    # Submit the get_atimes function to the executor
    signal.signal(signal.SIGINT, terminate_handler)  # Handle SIGINT (Ctrl+C)
    signal.signal(signal.SIGTERM, terminate_handler)

    futures = []
    try:
        if executor_running:
            futures.append(executor.submit(process_atimes_thread, work_queue, args.directory))
            futures.append(executor.submit(manage_queue_thread, executor, work_queue))
            # # Attempt to get an item from the queue (non-blocking with timeout)
            # for i in range(2):
            #     futures.append(executor.submit(infinite_rsync_worker, work_queue))
    except Exception as e:
        # Handle the case where no item is available in the queue
        logging.error(str(e))

    for future in as_completed(futures):
        future.result()



if __name__ == "__main__":
    main()
