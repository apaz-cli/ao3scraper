
import modal
import sys
import os

ONE_MINUTE = 60
FIFTEEN_MINUTES = ONE_MINUTE * 15
SEVENTEEN_MINUTES = ONE_MINUTE * 17
THIRTY_MINUTES = ONE_MINUTE * 30
ONE_HOUR = ONE_MINUTE * 60
TWO_HOURS = ONE_HOUR * 2
ONE_DAY = ONE_HOUR * 24

app = modal.App("AO3 Scraper")
playwright_image = modal.Image.debian_slim(python_version="3.10").run_commands(
    "echo", # Bypass cache
    "apt-get update",
    "apt-get install -y git python3-venv python3-pip",
    "git clone https://github.com/apaz-cli/ao3scraper",
    "python -m venv ao3scraper/.venv/",
    "ao3scraper/.venv/bin/pip install -r ao3scraper/requirements.txt",
)

@app.function(image=playwright_image, timeout=SEVENTEEN_MINUTES)
def scrape(server, port):
    os.system(f"/ao3scraper/.venv/bin/python /ao3scraper/worker.py --server {server} --port {port}")

@app.local_entrypoint()
def main():
    server = os.environ.get("SERVER")
    port = os.environ.get("PORT", "8000")

    if not server:
        print("Error: SERVER environment variable not set", file=sys.stderr)
        sys.exit(1)

    scrape.remote(server, port)
