#!/usr/bin/env python3
import argparse
import json
import threading
import collections
import subprocess
import re
import os
import time
import signal
from pathlib import Path
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import uvicorn


QUEUE_BUMP_SIZE = 30000
QUEUE_MIN_SIZE = 5000

app = FastAPI()

def get_disk_usage(path: str) -> int:
    """Get disk usage percentage for the filesystem containing the given path"""
    try:
        result = subprocess.run(['df', path], capture_output=True, text=True, check=True)
        lines = result.stdout.strip().split('\n')
        if len(lines) >= 2:
            # Parse the second line which contains the disk usage info
            fields = lines[1].split()
            if len(fields) >= 5:
                # The Use% field is typically the 5th field (0-indexed: 4)
                use_percent_str = fields[4]
                # Remove the % sign and convert to int
                if use_percent_str.endswith('%'):
                    return int(use_percent_str[:-1])
        return 0
    except (subprocess.CalledProcessError, ValueError, IndexError):
        return 0

def get_file_size(file_path: Path) -> int:
    """Get file size in bytes"""
    try:
        return file_path.stat().st_size
    except (FileNotFoundError, OSError):
        return 0

class WorkData(BaseModel):
    id: str
    title: str
    metadata: dict
    chapters: list[dict]

class Config:
    def __init__(self, output_dir: str = "output", start_id: int = 1, end_id: int = 1_000_000_000):
        self.output_dir = output_dir
        self.start_id = start_id
        self.end_id = end_id

        self.public_file = Path(output_dir) / "public.txt"
        self.private_file = Path(output_dir) / "private.txt"
        self.results_file = Path(output_dir) / "results.jsonl"

        # Create if do not exist
        Path(output_dir).mkdir(exist_ok=True)
        self.public_file.touch(exist_ok=True)
        self.private_file.touch(exist_ok=True)
        self.results_file.touch(exist_ok=True)

class WorkManager:
    def __init__(self, config: Config):
        self.config = config
        self.completed: set[int] = set()
        self.private: set[int] = set()
        self.assigned: set[int] = set()
        self.next_id: int = 0
        self.available_queue = collections.deque()
        self.last_queued_id: int = 0
        self.worker_ips: set[str] = set()  # Track unique worker IPs
        self.lock = threading.Lock()
        self.load_completed_work()

    def load_completed_work(self):
        """Load completed work IDs from public.txt and private.txt"""
        print("Loading completed work IDs...")
        if self.config.public_file.exists():
            with open(self.config.public_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            self.completed.add(int(line))
                        except ValueError:
                            pass

        print("Loading private work IDs...")
        if self.config.private_file.exists():
            with open(self.config.private_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            self.private.add(int(line))
                        except ValueError:
                            pass

        # Find the next ID to assign based on completed/private work
        print("Determining next work ID offset...")
        self.next_id = self.config.start_id
        while self.next_id in self.completed or self.next_id in self.private:
            self.next_id += 1

        # Initialize queue tracking
        self.last_queued_id = self.next_id - 1

        # Start background queue manager
        self.queue_thread = threading.Thread(target=self._queue_manager, daemon=True)
        self.queue_thread.start()

    def _queue_manager(self):
        """Background thread that keeps queue populated"""
        while True:
            try:
                # Quick status check
                with self.lock:
                    queue_size = len(self.available_queue)
                    can_generate = self.last_queued_id < self.config.end_id

                # Populate queue if it needs to be filled
                added = False
                if queue_size < QUEUE_MIN_SIZE and can_generate:

                    # Snapshot current state. Copying the exclusion sets is expensive but necessary.
                    with self.lock:
                        last_id = self.last_queued_id
                        not_finished = last_id < self.config.end_id
                        if not_finished:
                            start_id = self.last_queued_id + 1
                            end_id = min(start_id + QUEUE_BUMP_SIZE - 1, self.config.end_id)
                            excluded_ids = self.completed | self.private | self.assigned

                    if not_finished:
                        # Generate candidates outside lock
                        new_ids = set(range(start_id, end_id + 1)) - excluded_ids

                        # Add the new IDs to the queue. Lock for this.
                        with self.lock:
                            # This could actually add work that is already in a worker's queue, or is already done.
                            # That is fine, because when the work comes back as completed subsequent times
                            # it will not be appended to the files because it will be in the completed set.
                            self.available_queue.extend(new_ids)
                            self.last_queued_id = end_id

                        print(f"Added {len(new_ids)} IDs to queue, starting from {start_id}.")
                        added = len(new_ids) > 0

                # Sleep if nothing was added to avoid busy loop
                if not added:
                    time.sleep(1)

            except Exception as e:
                print(f"Queue manager error: {e}")
                time.sleep(10)

    def get_work_batch(self, batch_size: int = 1000) -> list[int]:
        """Get a batch of work IDs to scrape."""
        with self.lock:
            pending = []
            for _ in range(min(batch_size, len(self.available_queue))):
                if not self.available_queue:
                    break
                work_id = self.available_queue.popleft()
                pending.append(work_id)
                self.assigned.add(work_id)

            return pending

    def mark_private(self, work_id: int):
        """Mark work as private and add to private.txt"""
        with self.lock:
            if work_id not in self.private:
                try:
                    # Write to private file
                    with open(self.config.private_file, 'a') as f:
                        f.write(f"{work_id}\n")
                        f.flush()
                        os.fsync(f.fileno())

                    # Move to the private set if the write was successful.
                    # This will cause it to be skipped by subsequent calls to mark_private,
                    # in this process and if it is killed and restarted, because we know the file was written.
                    self.private.add(work_id)
                    self.assigned.discard(work_id)
                except OSError as e:
                    raise Exception(f"Failed to write to private file: {e}")

    def save_work_data(self, work_data: WorkData):
        """Save work data to results.jsonl and public.txt"""
        with self.lock:
            work_id = int(work_data.id)
            try:
                # Write to results file first
                with open(self.config.results_file, 'a') as f:
                    f.write(work_data.model_dump_json() + '\n')
                    f.flush()
                    os.fsync(f.fileno())

                if work_id not in self.completed:
                    with open(self.config.public_file, 'a') as f:
                        f.write(f"{work_id}\n")
                        f.flush()
                        os.fsync(f.fileno())

                    # Move from assigned to completed if writes were successful
                    self.completed.add(work_id)
                    self.assigned.discard(work_id)
            except OSError as e:
                # Don't mark as completed in memory if we can't write to files
                raise Exception(f"Failed to write work data: {e}")

    def shutdown(self):
        """Gracefully shutdown the work manager"""
        print("Initiating graceful shutdown...")
        with self.lock:
            print("Files are consistent, see ya later nerd.")
            exit(0)

# Global instances
config: Config = None # type: ignore
work_manager: WorkManager = None # type: ignore

@app.get("/work-batch")
def get_work_batch(request: Request, batch_size: int = 100):
    """Get a batch of work IDs to scrape"""

    # Track worker IP
    if request.client:
        client_ip = request.client.host
        work_manager.worker_ips.add(client_ip)

    work_ids = work_manager.get_work_batch(batch_size)
    return {"work_ids": work_ids}

@app.post("/work-completed")
def submit_completed_work(request: Request, work_data: WorkData):
    """Submit completed work data"""
    if request.client:
        client_ip = request.client.host
        work_manager.worker_ips.add(client_ip)
    try:
        work_manager.save_work_data(work_data)
        return {"status": "success", "message": f"Work {int(work_data.id)} saved successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving work: {str(e)}")

@app.post("/work-private")
def submit_private_work(request: Request, work_id: int):
    """Mark work as private (404 response)"""
    if request.client:
        client_ip = request.client.host
        work_manager.worker_ips.add(client_ip)
    work_manager.mark_private(work_id)
    return {"status": "success", "message": f"Work {work_id} marked as private"}

@app.get("/progress")
def get_progress():
    """Get current scraping statistics"""
    total_completed = len(work_manager.completed)
    total_private = len(work_manager.private)
    total_processed = total_completed + total_private
    total_range = config.end_id - config.start_id + 1
    remaining = total_range - total_processed
    disk_usage = get_disk_usage(config.output_dir)
    connected_workers = len(work_manager.worker_ips)
    results_file_size = get_file_size(config.results_file)
    available_queue_size = len(work_manager.available_queue)

    return {
        "completed": total_completed,
        "private": total_private,
        "total_processed": total_processed,
        "remaining": remaining,
        "progress_percent": (total_processed / total_range) * 100 if total_range > 0 else 0,
        "disk_usage_percent": disk_usage,
        "connected_workers": connected_workers,
        "results_file_size": results_file_size,
        "available_queue_size": available_queue_size
    }

@app.get("/file-status")
def get_file_status():
    """Get current results.jsonl file size for datafetch monitoring"""
    results_file_size = get_file_size(config.results_file)
    return {
        "results_file_size": results_file_size,
        "results_file_path": str(config.results_file)
    }

@app.post("/rotate-file")
def rotate_file():
    """Rotate results.jsonl file and compress it"""
    with work_manager.lock:
        try:
            # Find next available filename
            counter = 0
            while True:
                rotated_name = f"results_{counter}.jsonl"
                rotated_path = Path(config.output_dir) / rotated_name
                if not rotated_path.exists():
                    break
                counter += 1

             # Rotate and allow any pending writes to complete
            config.results_file.rename(rotated_path)
            time.sleep(2)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error rotating file: {str(e)}")

    # Compress the rotated file
    compressed_path = f"{rotated_path}.gz"
    subprocess.run(['gzip', '-k', str(rotated_path)], check=True)

    # Return the new compressed filename and path
    return {
        "status": "success",
        "rotated_file": [f"{rotated_name}", f"{rotated_name}.gz"],
        "compressed_path": compressed_path
    }

@app.post("/cleanup-file")
def cleanup_file(filename: str):
    """Remove a transferred file from the output directory"""
    try:
        file_path = Path(config.output_dir) / filename
        if file_path.exists() and file_path.is_file():
            # Safety check - only delete .gz files in output dir
            if filename.endswith('.gz') and file_path.parent == Path(config.output_dir):
                file_path.unlink()
                return {"status": "success", "message": f"File {filename} deleted"}
            else:
                raise HTTPException(status_code=400, detail="Only .gz files in output directory can be deleted")
        else:
            raise HTTPException(status_code=404, detail=f"File {filename} not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting file: {str(e)}")

@app.post("/shutdown")
def shutdown_server():
    """Gracefully shutdown the server"""
    work_manager.shutdown()
    return {"status": "success", "message": "Server shutdown initiated"}

def shutdown_handler(signum, frame):
    """Handle shutdown signals"""
    sigdict = dict((getattr(signal, n), n) for n in dir(signal) if n.startswith('SIG') and '_' not in n)
    print("-" * 60 + f"\nReceived signal {sigdict[signum]}, shutting down gracefully...\n" + "-" * 60)
    if work_manager:
        work_manager.shutdown()

def main():
    global config, work_manager

    # Set up signal handlers
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    parser = argparse.ArgumentParser(description="AO3 Scraper Server")
    parser.add_argument('--output', default='output', help='Output directory')
    parser.add_argument('--start-id', type=int, default=1, help='Starting ID')
    parser.add_argument('--end-id', type=int, default=16_000_000, help='Ending ID')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=8000, help='Port to bind to')
    args = parser.parse_args()

    config = Config(output_dir=args.output, start_id=args.start_id, end_id=args.end_id)
    work_manager = WorkManager(config)

    print(f"Starting server with output directory: {args.output}")
    print(f"ID range: {args.start_id} to {args.end_id}")
    print(f"Already completed: {len(work_manager.completed)} works")
    print(f"Already private: {len(work_manager.private)} works")

    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == '__main__':
    main()
