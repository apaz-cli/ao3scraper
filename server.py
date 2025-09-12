#!/usr/bin/env python3
import argparse
import json
import threading
import collections
import subprocess
import re
import time
from pathlib import Path
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import uvicorn

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
                    
                # Populate if queue is getting low
                if queue_size < 5000 and can_generate:
                    self._populate_batch_async()
                
                time.sleep(2)  # Check every 2 seconds
                
            except Exception as e:
                print(f"Queue manager error: {e}")
                time.sleep(10)

    def _populate_batch_async(self):
        """Generate IDs with minimal lock time"""
        batch_size = 30000
        
        # Snapshot current state (brief lock)
        with self.lock:
            if self.last_queued_id >= self.config.end_id:
                return
            
            start_id = self.last_queued_id + 1  
            end_id = min(start_id + batch_size - 1, self.config.end_id)
            
            # Copy exclusion sets - this is expensive but necessary
            excluded_ids = self.completed | self.private | self.assigned
        
        # Generate candidates outside lock (expensive operation)
        new_ids = [id for id in range(start_id, end_id + 1) 
                   if id not in excluded_ids]
        
        # Add results with validation (brief lock)  
        if new_ids:
            with self.lock:
                # Double-check IDs haven't been processed since snapshot
                valid_ids = [id for id in new_ids 
                            if (id not in self.completed and 
                                id not in self.private and 
                                id not in self.assigned)]
                
                if valid_ids:
                    self.available_queue.extend(valid_ids)
                    self.last_queued_id = end_id
                    print(f"Background: Added {len(valid_ids)}/{len(new_ids)} IDs to queue")

    def get_work_batch(self, batch_size: int = 1000) -> list[int]:
        """Get a batch of work IDs to scrape."""
        with self.lock:
            # Get batch from pre-populated queue
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
                    with open(self.config.private_file, 'a') as f:
                        f.write(f"{work_id}\n")
                        f.flush()
                    self.private.add(work_id)
                    self.assigned.discard(work_id)  # Remove from assigned set
                except OSError as e:
                    # Don't mark as private in memory if we can't write to file
                    raise Exception(f"Failed to write to private file: {e}")

    def save_work_data(self, work_data: WorkData):
        """Save work data to results.jsonl and public.txt"""
        with self.lock:
            work_id = int(work_data.id)
            try:
                # Write to results file first
                with open(self.config.results_file, 'a') as f:
                    json.dump(work_data.model_dump(), f)
                    f.write('\n')
                    f.flush()

                # Then write to public file
                if work_id not in self.completed:
                    with open(self.config.public_file, 'a') as f:
                        f.write(f"{work_id}\n")
                        f.flush()
                    self.completed.add(work_id)
                    self.assigned.discard(work_id)  # Remove from assigned set
            except OSError as e:
                # Don't mark as completed in memory if we can't write to files
                raise Exception(f"Failed to write work data: {e}")

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
    try:
        work_manager.save_work_data(work_data)
        return {"status": "success", "message": f"Work {int(work_data.id)} saved successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving work: {str(e)}")

@app.post("/work-private")
def submit_private_work(request: Request, work_id: int):
    """Mark work as private (404 response)"""
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

    return {
        "completed": total_completed,
        "private": total_private,
        "total_processed": total_processed,
        "remaining": remaining,
        "progress_percent": (total_processed / total_range) * 100 if total_range > 0 else 0,
        "disk_usage_percent": disk_usage,
        "connected_workers": connected_workers,
        "results_file_size": results_file_size,
    }

def main():
    global config, work_manager

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