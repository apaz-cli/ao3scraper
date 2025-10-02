#!/usr/bin/env python3
import argparse
import re
import time
from pathlib import Path
import requests
from bs4 import BeautifulSoup


class RateLimitException(Exception):
    """Exception raised when rate limiting is encountered and worker should exit"""
    pass


class AO3Scraper:
    def __init__(self, server_url: str = "http://localhost:8000", die_on_rate_limit: bool = False):
        self.server_url = server_url
        self.die_on_rate_limit = die_on_rate_limit
        self.current_batch: list[int] = []
        self.processed_ids: set[int] = set()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate'
        })

    def get_work_batch(self, batch_size: int = 100) -> list[int]:
        """Get a batch of work IDs from the server"""
        try:
            response = self.session.post(f"{self.server_url}/work-batch", json={"batch_size": batch_size})
            response.raise_for_status()
            batch = response.json()["work_ids"]
            self.current_batch = batch
            return batch
        except Exception as e:
            print(f"Error getting work batch: {e}")
            return []

    def submit_completed_work(self, work_data: dict) -> bool:
        """Submit completed work data to the server"""
        try:
            response = self.session.post(f"{self.server_url}/work-completed", json=work_data)
            response.raise_for_status()
            self.processed_ids.add(int(work_data['id']))
            return True
        except Exception as e:
            print(f"Error submitting work {work_data.get('id', 'unknown')}: {e}")
            return False

    def submit_private_work(self, work_id: int) -> bool:
        """Submit private work ID to the server"""
        try:
            response = self.session.post(f"{self.server_url}/work-private", json={"work_id": work_id})
            response.raise_for_status()
            self.processed_ids.add(work_id)
            return True
        except Exception as e:
            print(f"Error submitting private work {work_id}: {e}")
            return False

    def return_unprocessed_work(self) -> bool:
        """Return unprocessed work IDs to the server"""
        unprocessed = [wid for wid in self.current_batch if wid not in self.processed_ids]
        if not unprocessed:
            return True

        try:
            response = self.session.post(f"{self.server_url}/return-work", json={"work_ids": unprocessed})
            response.raise_for_status()
            print(f"Returned {len(unprocessed)} unprocessed work IDs to server")
            return True
        except Exception as e:
            print(f"Error returning unprocessed work: {e}")
            return False

    def fetch_work(self, work_id: int) -> dict | None:
        """Fetch a work from AO3"""

        url = f"https://download.archiveofourown.org/downloads/{work_id}/a.html"
        while True:
            try:
                response = self.session.get(url)

                if response.status_code == 429:
                    if self.die_on_rate_limit:
                        print(f"ID {work_id}: Rate limited (429) - Exiting due to --die-on-rate-limit")
                        raise RateLimitException("Rate limit encountered, shutting down worker")
                    retry_after = int(response.headers.get('retry-after', 300))
                    print(f"ID {work_id}: Rate limited (429) - Retrying after {retry_after}s")
                    time.sleep(retry_after)
                    continue

                if response.status_code == 503:
                    if self.die_on_rate_limit:
                        print(f"ID {work_id}: Service unavailable (503) - Exiting due to --die-on-rate-limit")
                        raise RateLimitException("Service unavailable, shutting down worker")
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


    def parse_html(self, html: str) -> tuple[str, dict[str, str], list[dict[str, str]]]:
        """Parse HTML content and extract metadata and chapters"""
        soup = BeautifulSoup(html, 'html.parser')
        metadata = {}

        # Extract title from h1 tag in the meta section
        work_title = ""
        meta_section = soup.find('div', class_='meta')
        if meta_section:
            assert hasattr(meta_section, 'find')
            title_h1 = meta_section.find('h1') # type: ignore
            if title_h1:
                assert hasattr(title_h1, 'get_text')
                work_title = title_h1.get_text(strip=True) # type: ignore


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
            assert hasattr(tags_section, 'find_all')
            for dt in tags_section.find_all('dt'): # type: ignore
                if dt.get_text(strip=True) == 'Series:':
                    series_dd = dt.find_next_sibling('dd')
                    if series_dd:
                        series_data = self.parse_metadata_content(series_dd, 'series')
                        assert isinstance(series_data, dict)
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
            assert hasattr(summary_section, 'find')
            summary_p = summary_section.find('p', string='Summary') # type: ignore
            if summary_p:
                summary_blockquote = summary_p.find_next_sibling('blockquote', class_='userstuff')
                if summary_blockquote:
                    assert hasattr(summary_blockquote, 'decode_contents')
                    summary = summary_blockquote.decode_contents().strip() # type: ignore
        metadata['summary'] = summary

        # Extract start notes (chapter notes in preface)
        start_notes = ""
        start_notes_p = soup.find('p', string='Notes')
        if start_notes_p:
            start_notes_blockquote = start_notes_p.find_next_sibling('blockquote', class_='userstuff')
            if start_notes_blockquote:
                assert hasattr(start_notes_blockquote, 'decode_contents')
                start_notes = start_notes_blockquote.decode_contents().strip() # type: ignore
        metadata['start_notes'] = start_notes

        # Extract end notes (from afterword section)
        end_notes = ""
        afterword = soup.find('div', id='afterword')
        if afterword:
            assert hasattr(afterword, 'find')
            endnotes_div = afterword.find('div', id='endnotes') # type: ignore
            if endnotes_div:
                assert hasattr(endnotes_div, 'find')
                end_notes_p = endnotes_div.find('p', string='End Notes') # type: ignore
                if end_notes_p:
                    end_notes_blockquote = end_notes_p.find_next_sibling('blockquote', class_='userstuff')
                    if end_notes_blockquote:
                        assert hasattr(end_notes_blockquote, 'decode_contents')
                        end_notes = end_notes_blockquote.decode_contents().strip() # type: ignore
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
                        assert hasattr(meta_div, 'find')
                        heading = meta_div.find(['h2', 'h3'], class_='heading') # type: ignore
                        if heading and chapter_index < len(userstuff_divs):
                            chapter_title = heading.get_text(strip=True)
                            content_div = userstuff_divs[chapter_index]
                            assert hasattr(content_div, 'decode_contents')
                            content = content_div.decode_contents().strip() # type: ignore

                            # Extract chapter start/end notes from meta_div
                            assert hasattr(meta_div, 'find')
                            chapter_start_notes = ""
                            notes_section = meta_div.find('div', class_='summary') or meta_div.find('div', class_='notes') # type: ignore
                            if notes_section:
                                assert hasattr(notes_section, 'find')
                                blockquote = notes_section.find('blockquote', class_='userstuff') # type: ignore
                                if blockquote:
                                    assert hasattr(blockquote, 'decode_contents')
                                    chapter_start_notes = blockquote.decode_contents().strip().strip() # type: ignore

                            chapter_end_notes = ""
                            assert hasattr(meta_div, 'find')
                            endnotes = meta_div.find('div', class_='endnotes') # type: ignore
                            if endnotes:
                                assert hasattr(endnotes, 'find')
                                blockquote = endnotes.find('blockquote', class_='userstuff') # type: ignore
                                if blockquote:
                                    assert hasattr(blockquote, 'decode_contents')
                                    chapter_end_notes = blockquote.decode_contents().strip().strip() # type: ignore

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
                    first_div = userstuff_divs[0]
                    assert hasattr(first_div, 'decode_contents')
                    content = first_div.decode_contents().strip() # type: ignore
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
                    assert hasattr(chapter_div, 'find')
                    chapter_start_notes = ""
                    notes_section = chapter_div.find('div', class_='summary') or chapter_div.find('div', class_='notes') # type: ignore
                    if notes_section:
                        assert hasattr(notes_section, 'find')
                        blockquote = notes_section.find('blockquote', class_='userstuff') # type: ignore
                        if blockquote:
                            assert hasattr(blockquote, 'decode_contents')
                            chapter_start_notes = blockquote.decode_contents().strip() # type: ignore

                    assert hasattr(chapter_div, 'find')
                    chapter_end_notes = ""
                    endnotes = chapter_div.find('div', class_='endnotes') # type: ignore
                    if endnotes:
                        assert hasattr(endnotes, 'find')
                        blockquote = endnotes.find('blockquote', class_='userstuff') # type: ignore
                        if blockquote:
                            assert hasattr(blockquote, 'decode_contents')
                            chapter_end_notes = blockquote.decode_contents().strip() # type: ignore

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

    def parse_metadata_content(self, content, content_type: str) -> dict:
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
        print(f"Starting worker, connecting to server at {self.server_url}")
        die_on_rate_limit_msg = " (die-on-rate-limit enabled)" if self.die_on_rate_limit else ""
        print(f"Configuration: {die_on_rate_limit_msg}")

        try:
            while True:
                # Get a batch of work IDs
                work_ids = self.get_work_batch()

                if not work_ids:
                    print("No more work IDs available, sleeping for 30 seconds...")
                    time.sleep(30)
                    continue

                print(f"Processing batch of {len(work_ids)} works.")

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
        except RateLimitException as e:
            print(f"Rate limit exception: {e}")
            print("Returning unprocessed work to server...")
            self.return_unprocessed_work()
            print("Worker shutting down due to rate limiting")
            return

def main():
    # Bump the recursion limit to handle that one AOT fic
    # that has 500+ span tags in it for no goddamn reason
    import resource, sys
    resource.setrlimit(resource.RLIMIT_STACK, (2**29,-1))
    sys.setrecursionlimit(10**6)

    parser = argparse.ArgumentParser(description="AO3 Scraper Worker")
    parser.add_argument('--server', default='localhost', help='Server address (IP or hostname)')
    parser.add_argument('--port', type=int, default=8000, help='Server port')
    parser.add_argument('--die-on-rate-limit', action='store_true',
                        help='Exit worker when rate limiting occurs, returning unprocessed work to server')

    args = parser.parse_args()

    try:
        AO3Scraper(
            server_url=f"http://{args.server}:{args.port}",
            die_on_rate_limit=args.die_on_rate_limit
        ).run()
    except KeyboardInterrupt:
        print("\nWorker stopped by user")
    except Exception as e:
        print(f"Worker crashed: {e}")

if __name__ == '__main__':
    main()