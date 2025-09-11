#!/usr/bin/env python3
import argparse
import re
import time
from pathlib import Path
import requests
from bs4 import BeautifulSoup

class Config:
    def __init__(self, server_url: str = "http://localhost:8000"):
        self.server_url = server_url
        self.base_url = "https://download.archiveofourown.org/downloads"
        self.user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

class AO3Scraper:
    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.user_agent,
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate'
        })

    def get_work_batch(self, batch_size: int = 100) -> list[int]:
        """Get a batch of work IDs from the server"""
        try:
            response = self.session.get(f"{self.config.server_url}/work-batch", params={"batch_size": batch_size})
            response.raise_for_status()
            return response.json()["work_ids"]
        except Exception as e:
            print(f"Error getting work batch: {e}")
            return []

    def submit_completed_work(self, work_data: dict) -> bool:
        """Submit completed work data to the server"""
        try:
            response = self.session.post(f"{self.config.server_url}/work-completed", json=work_data)
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"Error submitting work {work_data.get('id', 'unknown')}: {e}")
            return False

    def submit_private_work(self, work_id: int) -> bool:
        """Submit private work ID to the server"""
        try:
            response = self.session.post(f"{self.config.server_url}/work-private", params={"work_id": work_id})
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"Error submitting private work {work_id}: {e}")
            return False

    def fetch_work(self, work_id: int) -> dict | None:
        """Fetch a work from AO3"""
        url = f"{self.config.base_url}/{work_id}/a.html"

        while True:
            try:
                # First check if resource exists
                head_response = self.session.head(url)

                if head_response.status_code == 429:
                    retry_after = int(head_response.headers.get('retry-after', 300))
                    print(f"ID {work_id}: Rate limited (429) - Retrying after {retry_after}s")
                    time.sleep(retry_after)
                    continue

                if head_response.status_code == 503:
                    retry_after = int(head_response.headers.get('retry-after', 300))
                    print(f"ID {work_id}: Service unavailable (503) - Retrying after {retry_after}s")
                    time.sleep(retry_after)
                    continue

                if head_response.status_code == 404:
                    print(f"ID {work_id}: Private/Not found (404)")
                    return None

                if head_response.status_code != 200:
                    print(f"ID {work_id}: HEAD request failed with status {head_response.status_code}")
                    return None

                # Get the actual content
                response = self.session.get(url)

                if response.status_code == 429:
                    retry_after = int(response.headers.get('retry-after', 300))
                    print(f"ID {work_id}: Rate limited (429) during GET - Retrying after {retry_after}s")
                    time.sleep(retry_after)
                    continue

                if response.status_code == 503:
                    retry_after = int(response.headers.get('retry-after', 300))
                    print(f"ID {work_id}: Service unavailable (503) during GET - Retrying after {retry_after}s")
                    time.sleep(retry_after)
                    continue

                if response.status_code == 404:
                    print(f"ID {work_id}: Private/Not found (404)")
                    return None

                if response.status_code != 200:
                    print(f"ID {work_id}: GET request failed with status {response.status_code}")
                    time.sleep(1)
                    continue

                print(f"ID {work_id}: Status: {response.status_code}")

                # Get filename from Content-Disposition header
                filename = self.get_filename_from_response(response)
                title = Path(filename).stem.replace('_', ' ') if filename != "unknown.html" else f"work_{work_id}"

                # Parse the HTML content
                metadata, chapters = self.parse_html(response.text)

                return {
                    "id": str(work_id),
                    "title": title,
                    "metadata": metadata,
                    "chapters": chapters
                }

            except requests.exceptions.Timeout:
                print(f"ID {work_id}: Timeout error - Retrying")
                time.sleep(2)
                continue
            except requests.exceptions.ConnectionError:
                print(f"ID {work_id}: Connection error - Retrying")
                time.sleep(2)
                continue
            except Exception as e:
                print(f"ID {work_id}: Error: {e} - Retrying")
                time.sleep(1)
                continue

    def get_filename_from_response(self, response: requests.Response) -> str:
        """Extract filename from Content-Disposition header"""
        cd = response.headers.get('Content-Disposition', '')
        if not cd:
            return "unknown.html"

        match = re.search(r'filename\*?=[\'"]?(?:UTF-8[\'\']?)?([^;\'"]*)[\'"]?', cd)
        if match:
            return match.group(1)

        return response.url.split('/')[-1] if response.url else "unknown.html"

    def parse_html(self, html: str) -> tuple[dict[str, str], list[dict[str, str]]]:
        """Parse HTML content and extract metadata and chapters"""
        soup = BeautifulSoup(html, 'html.parser')
        metadata = {}

        # Extract title
        title_elem = soup.find('h1')
        if title_elem:
            metadata['title'] = title_elem.get_text(strip=True)

        # Extract author
        byline = soup.find('div', class_='byline')
        if byline:
            metadata['author'] = byline.get_text(strip=True)

        # Extract tags and metadata
        tags_section = soup.find('dl', class_='tags')
        if tags_section:
            current_tag = None
            assert hasattr(tags_section, 'find_all')
            for elem in tags_section.find_all(['dt', 'dd']): # type: ignore
                assert hasattr(elem, 'name')
                if elem.name == 'dt': # type: ignore
                    current_tag = elem.get_text(strip=True).rstrip(':')
                elif elem.name == 'dd' and current_tag: # type: ignore
                    assert hasattr(elem, 'find_all')
                    links = elem.find_all('a') # type: ignore
                    if links:
                        values = [link.get_text(strip=True) for link in links]
                        metadata[current_tag] = ', '.join(values)
                    else:
                        metadata[current_tag] = elem.get_text(strip=True)

        # Parse stats if available
        if 'Stats' in metadata:
            stats = metadata.pop('Stats')
            stats_data = self.parse_stats(stats)
            metadata.update(stats_data)

        # Extract chapters
        chapters = []
        chapters_div = soup.find(id='chapters')
        if chapters_div:
            # Look for chapter titles and content
            assert hasattr(chapters_div, 'find_all')
            chapter_divs = chapters_div.find_all('div', class_='chapter') # type: ignore
            if not chapter_divs:
                # Look for meta divs with headings (alternative chapter structure)
                meta_divs = chapters_div.find_all('div', class_='meta') # type: ignore
                userstuff_divs = chapters_div.find_all('div', class_='userstuff') # type: ignore
                
                if meta_divs and len(userstuff_divs) > 1:
                    # Multi-chapter work with meta/userstuff structure
                    chapter_index = 0
                    for meta_div in meta_divs:
                        # Look for chapter heading
                        heading = meta_div.find(['h2', 'h3'], class_='heading')
                        if heading and chapter_index < len(userstuff_divs):
                            chapter_title = heading.get_text(strip=True)
                            content_div = userstuff_divs[chapter_index]
                            content = content_div.get_text(separator='\n\n', strip=True)
                            if content:
                                chapters.append({
                                    "title": chapter_title,
                                    "text": content
                                })
                                chapter_index += 1
                elif userstuff_divs:
                    # Single chapter work - just get the content
                    content = userstuff_divs[0].get_text(separator='\n\n', strip=True)
                    if content:
                        chapters.append({
                            "title": "Chapter 1",
                            "text": content
                        })
            else:
                # Multi-chapter work with standard div.chapter structure
                for i, chapter_div in enumerate(chapter_divs, 1):
                    assert hasattr(chapter_div, 'find')
                    title_elem = chapter_div.find('h3', class_='title') # type: ignore
                    chapter_title = title_elem.get_text(strip=True) if title_elem else f"Chapter {i}"

                    content_div = chapter_div.find('div', class_='userstuff') # type: ignore
                    if content_div:
                        content = content_div.get_text(separator='\n', strip=True)
                        if content:
                            chapters.append({
                                "title": chapter_title,
                                "text": content
                            })

        # Remove title from metadata since it's at top level
        metadata.pop('title', None)

        return metadata, chapters

    def parse_stats(self, stats: str) -> dict[str, str]:
        """Parse stats string into structured data"""
        result = {}
        clean_stats = re.sub(r'\s+', ' ', stats)

        patterns = {
            'published': r'Published:\s*(\d{4}-\d{2}-\d{2})',
            'completed': r'Completed:\s*(\d{4}-\d{2}-\d{2})',
            'words': r'Words:\s*([\d,]+)',
            'chapters': r'Chapters:\s*(\d+/\?|\d+/\d+)'
        }

        for key, pattern in patterns.items():
            match = re.search(pattern, clean_stats)
            if match:
                result[key] = match.group(1)

        return result

    def run(self):
        """Main worker loop"""
        print(f"Starting worker, connecting to server at {self.config.server_url}")

        while True:
            # Get a batch of work IDs
            work_ids = self.get_work_batch()

            if not work_ids:
                print("No more work IDs available, sleeping for 30 seconds...")
                time.sleep(30)
                continue

            print(f"Processing batch of {len(work_ids)} work IDs")

            for work_id in work_ids:
                work_data = self.fetch_work(work_id)

                if work_data is None:
                    # Work is private or not found
                    self.submit_private_work(work_id)
                else:
                    # Work was successfully scraped
                    success = self.submit_completed_work(work_data)
                    if not success:
                        print(f"Failed to submit work {work_id}, will retry later")

                # Small delay between requests
                time.sleep(0.1)

def main():
    parser = argparse.ArgumentParser(description="AO3 Scraper Worker")
    parser.add_argument('--server', default='localhost', help='Server address (IP or hostname)')
    parser.add_argument('--port', type=int, default=8000, help='Server port')

    args = parser.parse_args()

    # Handle server address
    server_url = f"http://{args.server}:{args.port}"

    config = Config(server_url=server_url)
    scraper = AO3Scraper(config)

    try:
        scraper.run()
    except KeyboardInterrupt:
        print("\nWorker stopped by user")
    except Exception as e:
        print(f"Worker crashed: {e}")

if __name__ == '__main__':
    main()