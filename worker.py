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
                response = self.session.get(url)

                if response.status_code == 429:
                    retry_after = int(response.headers.get('retry-after', 300))
                    print(f"ID {work_id}: Rate limited (429) - Retrying after {retry_after}s")
                    time.sleep(retry_after)
                    continue

                if response.status_code == 503:
                    retry_after = int(response.headers.get('retry-after', 300))
                    print(f"ID {work_id}: Service unavailable (503) - Retrying after {retry_after}s")
                    time.sleep(retry_after)
                    continue

                if response.status_code == 404:
                    print(f"ID {work_id}: Private/Not found (404)")
                    return None

                if response.status_code != 200:
                    print(f"ID {work_id}: Request failed with status {response.status_code}")
                    time.sleep(1)
                    continue

                print(f"ID {work_id}: Status: {response.status_code}")

                # Parse the HTML content
                title, metadata, chapters = self.parse_html(response.text)

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


    def parse_html(self, html: str) -> tuple[dict[str, str], list[dict[str, str]]]:
        """Parse HTML content and extract metadata and chapters"""
        soup = BeautifulSoup(html, 'html.parser')
        metadata = {}

        # Extract title from h1 tag in the meta section
        work_title = ""
        meta_section = soup.find('div', class_='meta')
        if meta_section:
            title_h1 = meta_section.find('h1')
            if title_h1:
                work_title = title_h1.get_text(strip=True)


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

        # Parse series information if available
        series_name = ""
        series_id = 0
        series_number = 0

        # Extract series info from the tags section
        if tags_section:
            # Look for Series dt tag
            for dt in tags_section.find_all('dt'):
                if dt.get_text(strip=True) == 'Series:':
                    series_dd = dt.find_next_sibling('dd')
                    if series_dd:
                        series_data = self.parse_metadata_content(series_dd, 'series')
                        series_name = series_data['name']
                        series_id = series_data['id']
                        series_number = series_data['number']
                    break

        metadata['series_name'] = series_name
        metadata['series_id'] = series_id
        metadata['series_number'] = series_number

        # Extract summary
        summary = ""
        summary_section = soup.find('div', class_='meta')
        if summary_section:
            # Look for "Summary" text followed by blockquote
            summary_p = summary_section.find('p', string='Summary')
            if summary_p:
                summary_blockquote = summary_p.find_next_sibling('blockquote', class_='userstuff')
                if summary_blockquote:
                    summary = summary_blockquote.decode_contents().strip()
        metadata['summary'] = summary

        # Extract start notes (chapter notes in preface)
        start_notes = ""
        start_notes_p = soup.find('p', string='Notes')
        if start_notes_p:
            start_notes_blockquote = start_notes_p.find_next_sibling('blockquote', class_='userstuff')
            if start_notes_blockquote:
                start_notes = start_notes_blockquote.decode_contents().strip()
        metadata['start_notes'] = start_notes

        # Extract end notes (from afterword section)
        end_notes = ""
        afterword = soup.find('div', id='afterword')
        if afterword:
            endnotes_div = afterword.find('div', id='endnotes')
            if endnotes_div:
                end_notes_p = endnotes_div.find('p', string='End Notes')
                if end_notes_p:
                    end_notes_blockquote = end_notes_p.find_next_sibling('blockquote', class_='userstuff')
                    if end_notes_blockquote:
                        end_notes = end_notes_blockquote.decode_contents().strip()
        metadata['end_notes'] = end_notes

        # Parse stats if available
        if 'Stats' in metadata:
            stats = metadata.pop('Stats')
            stats_data = self.parse_metadata_content(stats, 'stats')
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
                            content = content_div.decode_contents().strip()

                            # Extract chapter start/end notes from meta_div
                            chapter_start_notes = ""
                            notes_section = meta_div.find('div', class_='summary') or meta_div.find('div', class_='notes')
                            if notes_section:
                                blockquote = notes_section.find('blockquote', class_='userstuff')
                                if blockquote:
                                    chapter_start_notes = blockquote.decode_contents().strip().strip()

                            chapter_end_notes = ""
                            endnotes = meta_div.find('div', class_='endnotes')
                            if endnotes:
                                blockquote = endnotes.find('blockquote', class_='userstuff')
                                if blockquote:
                                    chapter_end_notes = blockquote.decode_contents().strip().strip()

                            if content:
                                chapters.append({
                                    "title": chapter_title,
                                    "text": content,
                                    "start_notes": chapter_start_notes,
                                    "end_notes": chapter_end_notes
                                })
                                chapter_index += 1
                elif userstuff_divs:
                    # Single chapter work - just get the content
                    content = userstuff_divs[0].decode_contents().strip()
                    if content:
                        chapters.append({
                            "title": "Chapter 1",
                            "text": content,
                            "start_notes": "",
                            "end_notes": ""
                        })
            else:
                # Multi-chapter work with standard div.chapter structure
                for i, chapter_div in enumerate(chapter_divs, 1):
                    assert hasattr(chapter_div, 'find')
                    title_elem = chapter_div.find('h3', class_='title') # type: ignore
                    chapter_title = title_elem.get_text(strip=True) if title_elem else f"Chapter {i}"

                    content_div = chapter_div.find('div', class_='userstuff') # type: ignore

                    # Extract chapter start/end notes
                    chapter_start_notes = ""
                    notes_section = chapter_div.find('div', class_='summary') or chapter_div.find('div', class_='notes')
                    if notes_section:
                        blockquote = notes_section.find('blockquote', class_='userstuff')
                        if blockquote:
                            chapter_start_notes = blockquote.decode_contents().strip()

                    chapter_end_notes = ""
                    endnotes = chapter_div.find('div', class_='endnotes')
                    if endnotes:
                        blockquote = endnotes.find('blockquote', class_='userstuff')
                        if blockquote:
                            chapter_end_notes = blockquote.decode_contents().strip()

                    if content_div:
                        content = str(content_div)
                        if content:
                            chapters.append({
                                "title": chapter_title,
                                "text": content,
                                "start_notes": chapter_start_notes,
                                "end_notes": chapter_end_notes
                            })

        return work_title, metadata, chapters

    def parse_metadata_content(self, content, content_type: str):
        """Parse stats string or series element into structured data"""
        if content_type == 'stats':
            result = {}
            clean_stats = re.sub(r'\s+', ' ', content)

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

        elif content_type == 'series':
            result = {
                'name': '',
                'id': 0,
                'number': 0
            }

            # Get the full text content
            series_text = content.get_text(strip=True)

            # Parse "Part X of Series Name" format
            # Example: "Part 1 of Regender of Evangelion" or "Part 1 ofRegender of Evangelion" (missing space)
            part_match = re.search(r'Part\s+(\d+)\s+of\s*(.+)', series_text)
            if part_match:
                result['number'] = int(part_match.group(1))

                # Get series name and ID from the link
                series_link = content.find('a')
                if series_link:
                    result['name'] = series_link.get_text(strip=True)

                    # Extract series ID from href
                    href = series_link.get('href', '')
                    series_id_match = re.search(r'/series/(\d+)', href)
                    if series_id_match:
                        result['id'] = int(series_id_match.group(1))
                else:
                    # Fallback to text parsing if no link found
                    result['name'] = part_match.group(2).strip()
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