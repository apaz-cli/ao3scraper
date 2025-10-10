#!/usr/bin/env python3
"""
Modal Process Manager - Maintains 100 Modal apps running simultaneously
"""

import argparse
import subprocess
import time
import signal
import sys
import atexit
from concurrent.futures import ThreadPoolExecutor
import threading

TARGET_COUNT = 100
CHECK_INTERVAL = 1  # seconds


def shutdown_handler(signum, frame):
    """Kill all Modal processes on exit"""
    print(f"Received signal {signum}. Initiating shutdown...")
    shutdown_flag.set()
    cleanup()
    sys.exit(0)


def cleanup():
    """Kill all Modal processes on exit"""

    # Kill all Modal apps using the provided command
    cleanup_cmd = "modal app list | awk -F '│' 'NR>3 && NF>1 {gsub(/^[ \t]+|[ \t]+$/, \"\", $2); print $2}' | xargs -I {} modal app stop {}"

    print("Cleaning up all Modal processes...")
    result = subprocess.run(
        cleanup_cmd,
        shell=True,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        print("Cleanup complete")
    else:
        print(f"Error stopping Modal apps: {result.stderr}")
    exit(0)


def get_running_count():
    """Get the current number of running Modal processes"""
    try:
        result = subprocess.run(
            "modal app list --json | grep State | grep ephemeral | wc -l",
            shell=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        count = int(result.stdout.strip())
        return max(0, count)  # Ensure non-negative
    except (subprocess.TimeoutExpired, ValueError, subprocess.SubprocessError) as e:
        print(f"Error getting running count: {e}")
        return 0


def start_modal_process(server, port):
    """Start a single Modal process"""
    import os
    process = subprocess.Popen(
        "modal run run_modal.py",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={**os.environ, "SERVER": server, "PORT": str(port)},
        shell=True
    )
    return process


def start_processes(count, server, port):
    """Start the specified number of Modal processes"""
    if count <= 0:
        return

    print(f"Starting {count} new Modal processes...")

    with ThreadPoolExecutor(max_workers=min(count, 20)) as executor:
        futures = []
        for _ in range(count):
            if shutdown_flag.is_set():
                break
            future = executor.submit(start_modal_process, server, port)
            futures.append(future)

        # Collect successfully started processes
        new_processes = []
        for future in futures:
            process = future.result()
            if process:
                new_processes.append(process)

        print(f"Successfully started {len(new_processes)} processes")


def maintain_processes(server, port):
    """Main loop to maintain the target number of processes"""
    print(f"Starting Modal Process Manager with target of {TARGET_COUNT} processes")

    # Initial startup
    current_count = get_running_count()
    print(f"Initial count: {current_count} Modal processes running")
    needed = TARGET_COUNT - current_count
    if needed > 0:
        start_processes(needed, server, port)

    # Maintenance loop
    last_check = time.time()
    while not shutdown_flag.is_set():
        try:
            # Sleep in small intervals to be responsive to shutdown
            if time.time() - last_check >= CHECK_INTERVAL:

                # Check current count and start new processes if needed
                current_count = get_running_count()
                print(f"Current count: {current_count} Modal processes running")

                needed = TARGET_COUNT - current_count
                if needed > 0:
                    print(f"Need to start {needed} more processes")
                    start_processes(needed, server, port)
                elif needed < 0:
                    print(f"Running {-needed} processes over target")
                else:
                    print("Target count maintained")

                last_check = time.time()

            # Short sleep to be responsive to shutdown signals
            time.sleep(1)

        except KeyboardInterrupt:
            print("Keyboard interrupt received")
            break
        except Exception as e:
            print(f"Error in maintenance loop: {e}")
            time.sleep(5)  # Brief pause before retrying


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Modal Process Manager - Maintains 100 Modal apps running simultaneously')
    parser.add_argument('--server', required=True, help='Server address (IP or hostname)')
    parser.add_argument('--port', type=int, default=8000, help='Server port')
    args = parser.parse_args()

    shutdown_flag = threading.Event()
    atexit.register(cleanup)
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        maintain_processes(args.server, args.port)
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        cleanup()
