
import modal
import sys
import os
import re

SERVER_IP = "204.52.25.131" # os.environ.get('SERVER_IP')
if not SERVER_IP:
    print("Error: SERVER_IP environment variable required", file=sys.stderr)
    sys.exit(1)

# Validate IP format (IPv4 or hostname)
ip_pattern = r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^localhost$|^[\w.-]+$'
if not re.match(ip_pattern, SERVER_IP):
    print(f"Error: Invalid server IP format: {SERVER_IP}", file=sys.stderr)
    sys.exit(1)

ONE_MINUTE = 60
FIFTEEN_MINUTES = ONE_MINUTE * 15
SEVENTEEN_MINUTES = ONE_MINUTE * 17
THIRTY_MINUTES = ONE_MINUTE * 30
ONE_HOUR = ONE_MINUTE * 60
TWO_HOURS = ONE_HOUR * 2
ONE_DAY = ONE_HOUR * 24

app = modal.App("AO3 Scraper")
playwright_image = modal.Image.debian_slim(python_version="3.10").run_commands(
    "apt-get update",
    "apt-get install -y git python3-venv python3-pip",
    "git clone https://github.com/apaz-cli/ao3scraper",
    "python -m venv ao3scraper/.venv/",
    "ao3scraper/.venv/bin/pip install -r ao3scraper/requirements.txt",
)

@app.function(image=playwright_image, timeout=SEVENTEEN_MINUTES)
def scrape():
   import os
   os.system(f"/ao3scraper/.venv/bin/python /ao3scraper/worker.py --server {SERVER_IP}")

@app.local_entrypoint()
def main():
  scrape.remote()
