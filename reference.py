#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
import re
import ssl
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import aiofiles
import aiohttp
from bs4 import BeautifulSoup


@dataclass
class Work:
    id: str
    title: str
    metadata: Dict[str, str]
    text: str


@dataclass
class Config:
    start_id: int = 1
    end_id: int = 100000
    batch_size: int = 10000
    output_dir: str = "output"
    base_url: str = "https://download.archiveofourown.org/downloads"
    user_agent: str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    progress_file: str = "progress.json"


class ProgressTracker:
    def __init__(self, config: Config):
        self.config = config
        self.progress_file = Path(config.output_dir) / config.progress_file
        self.completed: Set[int] = set()
        self.failed: Set[int] = set()
        self.load()

    def load(self):
        if self.progress_file.exists():
            try:
                with open(self.progress_file) as f:
                    data = json.load(f)
                    self.completed = set(data.get('completed', []))
                    self.failed = set(data.get('failed', []))
                    print(f"Resumed: {len(self.completed)} completed, {len(self.failed)} failed")
            except Exception as e:
                print(f"Could not load progress: {e}")

    async def save(self):
        data = {
            'completed': list(self.completed),
            'failed': list(self.failed),
            'timestamp': time.time()
        }
        async with aiofiles.open(self.progress_file, 'w') as f:
            await f.write(json.dumps(data, indent=2))

    def mark_completed(self, work_id: int):
        self.completed.add(work_id)
        self.failed.discard(work_id)

    def mark_failed(self, work_id: int):
        self.failed.add(work_id)

    def is_completed(self, work_id: int) -> bool:
        return work_id in self.completed

    def get_pending_ids(self, start: int, end: int) -> List[int]:
        return [i for i in range(start, end + 1) if not self.is_completed(i)]


class Downloader:
    def __init__(self, config: Config):
        self.config = config
        self.progress = ProgressTracker(config)
        self.session = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def create_fresh_session(self) -> aiohttp.ClientSession:
        ssl_context = ssl.create_default_context()

        connector = aiohttp.TCPConnector(
            ssl=ssl_context,
            limit=1,
            limit_per_host=1,
            enable_cleanup_closed=True
        )

        return aiohttp.ClientSession(connector=connector)

    def is_timeout_error(self, e: Exception) -> bool:
        return isinstance(e, (asyncio.TimeoutError, aiohttp.ServerTimeoutError)) or 'timeout' in str(e).lower()

    def _get_headers(self) -> Dict[str, str]:
        return {
            'User-Agent': self.config.user_agent,
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate'
        }

    async def check_resource_exists(self, url: str, session: aiohttp.ClientSession) -> Tuple[bool, int, Optional[Exception], Optional[aiohttp.ClientResponse]]:
        try:
            async with session.head(url, headers=self._get_headers()) as resp:
                return resp.status == 200, resp.status, None, resp
        except Exception as e:
            return False, 0, e, None

    async def fetch_html(self, url: str, session: aiohttp.ClientSession) -> Tuple[str, int, Optional[Exception], Optional[aiohttp.ClientResponse]]:
        try:
            async with session.get(url, headers=self._get_headers()) as resp:
                if resp.status != 200:
                    return "", resp.status, Exception(f"HTTP status: {resp.status}"), resp
                html = await resp.text()
                return html, resp.status, None, resp
        except Exception as e:
            return "", 0, e, None

    async def get_filename_from_url(self, url: str, session: aiohttp.ClientSession) -> Tuple[str, Optional[Exception], Optional[aiohttp.ClientResponse]]:
        try:
            async with session.head(url, headers=self._get_headers()) as resp:
                cd = resp.headers.get('Content-Disposition', '')
                if not cd:
                    return "unknown.html", None, resp

                match = re.search(r'filename\*?=[\'"]?(?:UTF-8[\'\']?)?([^;\'"]*)[\'"]?', cd)
                if match:
                    return match.group(1), None, resp

                return url.split('/')[-1], None, resp
        except Exception as e:
            return "unknown.html", e, None

    async def fetch_work(self, work_id: int) -> Optional[Work]:
        url = f"{self.config.base_url}/{work_id}/a.html"

        while True:
            session = None
            try:
                session = await self.create_fresh_session()

                valid_resource, status_code, head_err, head_resp = await self.check_resource_exists(url, session)

                if head_err and self.is_timeout_error(head_err):
                    print(f"ID {work_id}: Timeout error - Retrying")
                    await asyncio.sleep(2)
                    continue

                if status_code == 429:
                    retry_after = int(head_resp.headers.get('retry-after', 300)) if head_resp else 300
                    print(f"ID {work_id}: Rate limited (429) - Retrying after {retry_after}s")
                    await asyncio.sleep(retry_after)
                    continue

                if status_code == 503:
                    retry_after = int(head_resp.headers.get('retry-after', 300)) if head_resp else 300
                    print(f"ID {work_id}: Service unavailable (503) - Retrying after {retry_after}s")
                    await asyncio.sleep(retry_after)
                    continue

                if status_code == 0:
                    print(f"ID {work_id}: Connection issue (status code 0) - Retrying")
                    await asyncio.sleep(2)
                    continue

                if head_err or not valid_resource:
                    print(f"ID {work_id}: HEAD Check: Resource not available - Status: {status_code}")
                    return None

                html_content, get_status_code, get_err, get_resp = await self.fetch_html(url, session)

                if get_err and self.is_timeout_error(get_err):
                    print(f"ID {work_id}: Timeout error during GET - Retrying")
                    await asyncio.sleep(2)
                    continue

                if get_status_code == 429:
                    retry_after = int(get_resp.headers.get('retry-after', 300)) if get_resp else 300
                    print(f"ID {work_id}: Rate limited (429) during GET - Retrying after {retry_after}s")
                    await asyncio.sleep(retry_after)
                    continue

                if get_status_code == 0:
                    print(f"ID {work_id}: Connection issue during GET (status code 0) - Retrying")
                    await asyncio.sleep(2)
                    continue

                print(f"ID {work_id}: Status: {get_status_code}")

                if get_err or get_status_code != 200:
                    await asyncio.sleep(1)
                    continue

                filename, filename_err, filename_resp = await self.get_filename_from_url(url, session)
                if filename_err:
                    if self.is_timeout_error(filename_err):
                        print(f"ID {work_id}: Timeout error getting filename - Retrying")
                        await asyncio.sleep(2)
                        continue
                    print(f"Error getting filename for ID {work_id}: {filename_err}")
                    return None

                title = Path(filename).stem.replace('_', ' ') if filename != "unknown.html" else f"work_{work_id}"

                metadata, story_text = self.parse_html(html_content)
                metadata.pop('title', None)

                if 'Stats' in metadata:
                    stats_data = self.parse_stats(metadata.pop('Stats'))
                    metadata.update(stats_data)

                work = Work(
                    id=str(work_id),
                    title=title,
                    metadata=metadata,
                    text=story_text
                )

                return work

            except Exception as e:
                if self.is_timeout_error(e):
                    print(f"ID {work_id}: Timeout error - Retrying")
                    await asyncio.sleep(2)
                    continue

                print(f"ID {work_id}: Error: {e} - Retrying")
                await asyncio.sleep(1)
            finally:
                if session:
                    await session.close()

    def parse_html(self, html: str) -> Tuple[Dict[str, str], str]:
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
            for elem in tags_section.find_all(['dt', 'dd']):
                if elem.name == 'dt':
                    current_tag = elem.get_text(strip=True).rstrip(':')
                elif elem.name == 'dd' and current_tag:
                    links = elem.find_all('a')
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

        # Extract story text
        text_parts = []
        chapters = soup.find(id='chapters')
        if chapters:
            for div in chapters.find_all('div', class_='userstuff'):
                content = div.get_text(separator='\n', strip=True)
                if content:
                    text_parts.append(content)

        text = '\n\n----- CHAPTER BREAK -----\n\n'.join(text_parts)

        # Remove title from metadata since it's at top level
        metadata.pop('title', None)

        return metadata, text

    def parse_stats(self, stats: str) -> Dict[str, str]:
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

    async def save_work(self, work: Work, batch_start: int, batch_end: int):
        output_file = Path(self.config.output_dir) / f"ao3_works_{batch_start}-{batch_end}.jsonl"

        async with aiofiles.open(output_file, 'a') as f:
            await f.write(json.dumps(asdict(work)) + '\n')

    async def process_work(self, work_id: int):
            if self.progress.is_completed(work_id):
                return

            work = await self.fetch_work(work_id)

            batch_start = ((work_id - self.config.start_id) // self.config.batch_size) * self.config.batch_size + self.config.start_id
            batch_end = min(batch_start + self.config.batch_size - 1, self.config.end_id)

            if work:
                await self.save_work(work, batch_start, batch_end)
                self.progress.mark_completed(work_id)
            else:
                self.progress.mark_failed(work_id)

    async def run(self):
        Path(self.config.output_dir).mkdir(exist_ok=True)

        pending_ids = self.progress.get_pending_ids(self.config.start_id, self.config.end_id)
        total_ids = self.config.end_id - self.config.start_id + 1

        print(f"Configuration: StartID={self.config.start_id}, EndID={self.config.end_id}, BatchSize={self.config.batch_size}")
        print("Running with direct connection")

        print(f"Processing {len(pending_ids)}/{total_ids} works")

        start_time = time.time()
        processed = 0

        for work_id in pending_ids:
            await self.process_work(work_id)
            processed += 1

            # Save progress periodically
            if processed % 100 == 0:
                await self.progress.save()

                # Progress stats
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                remaining = (len(pending_ids) - processed) / rate if rate > 0 else 0

                print(f"Progress: {processed + len(self.progress.completed)}/{total_ids} "
                           f"({(processed + len(self.progress.completed))/total_ids*100:.1f}%), "
                           f"{rate:.2f} works/sec, est. remaining: {remaining/60:.1f} min")

        await self.progress.save()

        elapsed = time.time() - start_time
        print(f"Complete! Processed {len(pending_ids)} works in {elapsed:.1f}s "
                    f"({len(pending_ids)/elapsed:.2f} works/sec)")


def main():
    parser = argparse.ArgumentParser(description="OTW Archive Downloader")
    parser.add_argument('--start-id', type=int, default=1, help='Starting ID')
    parser.add_argument('--end-id', type=int, default=100000, help='Ending ID')
    parser.add_argument('--batch-size', type=int, default=10000, help='IDs per output file')
    parser.add_argument('--output', default='output', help='Output directory')
    parser.add_argument('--base-url', default='https://download.archiveofourown.org/downloads', help='Base URL')
    parser.add_argument('--user-agent', default='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36', help='User agent')

    args = parser.parse_args()

    config = Config(
        start_id=args.start_id,
        end_id=args.end_id,
        batch_size=args.batch_size,
        output_dir=args.output,
        base_url=args.base_url,
        user_agent=args.user_agent
    )

    async def run_downloader():
        async with Downloader(config) as downloader:
            await downloader.run()

    asyncio.run(run_downloader())


if __name__ == '__main__':
    main()
