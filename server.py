#!/usr/bin/env python3
import argparse
import json
import threading
from pathlib import Path
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

app = FastAPI()

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
        self.lock = threading.Lock()
        self.load_completed_work()

    def load_completed_work(self):
        """Load completed work IDs from public.txt and private.txt"""
        if self.config.public_file.exists():
            with open(self.config.public_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            self.completed.add(int(line))
                        except ValueError:
                            pass

        if self.config.private_file.exists():
            with open(self.config.private_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            self.private.add(int(line))
                        except ValueError:
                            pass

    def get_work_batch(self, batch_size: int = 100) -> list[int]:
        """Get a batch of work IDs to scrape"""
        with self.lock:
            all_processed = self.completed.union(self.private)
            pending = []

            current_id = self.config.start_id
            while len(pending) < batch_size and current_id <= self.config.end_id:
                if current_id not in all_processed:
                    pending.append(current_id)
                current_id += 1

            return pending

    def mark_completed(self, work_id: int):
        """Mark work as completed and add to public.txt"""
        with self.lock:
            if work_id not in self.completed:
                self.completed.add(work_id)
                with open(self.config.public_file, 'a') as f:
                    f.write(f"{work_id}\n")

    def mark_private(self, work_id: int):
        """Mark work as private and add to private.txt"""
        with self.lock:
            if work_id not in self.private:
                self.private.add(work_id)
                with open(self.config.private_file, 'a') as f:
                    f.write(f"{work_id}\n")

    def save_work_data(self, work_data: WorkData):
        """Save work data to results.jsonl"""
        with self.lock:
            with open(self.config.results_file, 'a') as f:
                json.dump(work_data.model_dump(), f)
                f.write('\n')

# Global instances
config: Config = None # type: ignore
work_manager: WorkManager = None # type: ignore

@app.get("/work-batch")
def get_work_batch(batch_size: int = 100):
    """Get a batch of work IDs to scrape"""
    work_ids = work_manager.get_work_batch(batch_size)
    return {"work_ids": work_ids}

@app.post("/work-completed")
def submit_completed_work(work_data: WorkData):
    """Submit completed work data"""
    try:
        work_id = int(work_data.id)
        work_manager.save_work_data(work_data)
        work_manager.mark_completed(work_id)
        return {"status": "success", "message": f"Work {work_id} saved successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving work: {str(e)}")

@app.post("/work-private")
def submit_private_work(work_id: int):
    """Mark work as private (404 response)"""
    work_manager.mark_private(work_id)
    return {"status": "success", "message": f"Work {work_id} marked as private"}

@app.get("/stats")
def get_stats():
    """Get current scraping statistics"""
    total_completed = len(work_manager.completed)
    total_private = len(work_manager.private)
    total_processed = total_completed + total_private
    total_range = config.end_id - config.start_id + 1
    remaining = total_range - total_processed

    return {
        "completed": total_completed,
        "private": total_private,
        "total_processed": total_processed,
        "remaining": remaining,
        "progress_percent": (total_processed / total_range) * 100 if total_range > 0 else 0
    }

def main():
    global config, work_manager

    parser = argparse.ArgumentParser(description="AO3 Scraper Server")
    parser.add_argument('--output', default='output', help='Output directory')
    parser.add_argument('--start-id', type=int, default=1, help='Starting ID')
    parser.add_argument('--end-id', type=int, default=1_000_000_000, help='Ending ID')
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