#!/usr/bin/env python3
import argparse
import time
import subprocess
import requests
from pathlib import Path

class Config:
    def __init__(self, server: str, port: int, remote_output: str, threshold: int, local_dir: str):
        self.server_url = f"http://{server}:{port}"
        self.remote_output = remote_output
        self.threshold_bytes = threshold * 1024 * 1024 * 1024  # Convert GB to bytes
        self.local_dir = Path(local_dir)
        self.local_dir.mkdir(exist_ok=True)

class DataFetcher:
    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()

    def get_file_status(self) -> dict:
        """Get current results.jsonl file size from server"""
        try:
            response = self.session.get(f"{self.config.server_url}/file-status")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error getting file status: {e}")
            return {}

    def rotate_file(self) -> dict:
        """Trigger file rotation on server"""
        try:
            response = self.session.post(f"{self.config.server_url}/rotate-file")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error rotating file: {e}")
            return {}

    def transfer_file(self, filename: str, remote_path: str) -> bool:
        """Transfer compressed file from server using rsync"""
        try:
            remote_file = f"{self.config.server_url.split('//')[1].split(':')[0]}:{remote_path}"
            local_file = self.config.local_dir / filename
            
            # Use rsync to transfer the file
            cmd = ['rsync', '-v', remote_file, str(local_file)]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"Successfully transferred {filename}")
                return True
            else:
                print(f"rsync failed: {result.stderr}")
                return False
        except Exception as e:
            print(f"Error transferring file {filename}: {e}")
            return False

    def cleanup_file(self, filename: str) -> bool:
        """Request server to cleanup transferred file"""
        try:
            response = self.session.post(f"{self.config.server_url}/cleanup-file", params={"filename": filename})
            response.raise_for_status()
            print(f"Server cleanup of {filename}: {response.json()['message']}")
            return True
        except Exception as e:
            print(f"Error cleaning up file {filename}: {e}")
            return False

    def run_cycle(self) -> bool:
        """Run one fetch cycle - check, rotate, transfer, cleanup if needed"""
        status = self.get_file_status()
        if not status:
            return False

        file_size = status.get('results_file_size', 0)
        print(f"Current results.jsonl size: {file_size / (1024*1024*1024):.2f} GB")

        if file_size >= self.config.threshold_bytes:
            print(f"Size threshold ({self.config.threshold_bytes / (1024*1024*1024):.1f} GB) exceeded, rotating file...")
            
            rotation_result = self.rotate_file()
            if rotation_result.get('status') == 'success':
                rotated_filename = rotation_result['rotated_file']
                compressed_path = rotation_result['compressed_path']
                
                print(f"File rotated to {rotated_filename}, transferring...")
                
                if self.transfer_file(rotated_filename, compressed_path):
                    print(f"Transfer successful, cleaning up {rotated_filename} on server...")
                    self.cleanup_file(rotated_filename)
                    return True
                else:
                    print(f"Transfer failed, keeping {rotated_filename} on server")
                    return False
            else:
                print("File rotation failed or no rotation needed")
                return False
        else:
            threshold_gb = self.config.threshold_bytes / (1024*1024*1024)
            print(f"Size below threshold ({threshold_gb:.1f} GB), no action needed")
            return True

    def run(self):
        """Main run loop - check every 60 seconds"""
        print(f"Starting datafetch monitoring:")
        print(f"  Server: {self.config.server_url}")
        print(f"  Remote output: {self.config.remote_output}")
        print(f"  Threshold: {self.config.threshold_bytes / (1024*1024*1024):.1f} GB")
        print(f"  Local directory: {self.config.local_dir}")
        print(f"  Check interval: 60 seconds")
        print()

        while True:
            try:
                self.run_cycle()
            except KeyboardInterrupt:
                print("\nShutting down datafetch...")
                break
            except Exception as e:
                print(f"Unexpected error in cycle: {e}")
            
            print("Waiting 60 seconds before next check...\n")
            time.sleep(60)

def main():
    parser = argparse.ArgumentParser(description="AO3 Scraper Data Fetcher")
    parser.add_argument('--server', required=True, help='Server IP address')
    parser.add_argument('--port', type=int, default=8000, help='Server port')
    parser.add_argument('--remote-output', required=True, help='Remote output directory path')
    parser.add_argument('--threshold', type=int, default=10, help='File size threshold in GB')
    parser.add_argument('--local-dir', default='./downloads', help='Local directory for downloaded files')
    
    args = parser.parse_args()
    
    config = Config(
        server=args.server,
        port=args.port,
        remote_output=args.remote_output,
        threshold=args.threshold,
        local_dir=args.local_dir
    )
    
    fetcher = DataFetcher(config)
    fetcher.run()

if __name__ == '__main__':
    main()