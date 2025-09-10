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
import aiosocks
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
    concurrent: int = 4
    retries: int = 5
    proxy_file: str = "proxy.txt"
    output_dir: str = "output"
    use_proxies: bool = False
    timeout: int = 60
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


class ProxyManager:
    def __init__(self, proxy_file: str = None):
        self.proxies = []
        self.current_idx = 0
        self.bad_proxies = {}  # proxy_idx -> cooldown_time
        if proxy_file:
            self.load_proxies(proxy_file)

    def load_proxies(self, filename: str):
        try:
            with open(filename) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        parts = line.split(':')
                        if len(parts) >= 2:
                            self.proxies.append({
                                'host': parts[0],
                                'port': int(parts[1]),
                                'username': parts[2] if len(parts) > 2 else None,
                                'password': parts[3] if len(parts) > 3 else None
                            })
            print(f"Loaded {len(self.proxies)} proxies")
        except Exception as e:
            print(f"Could not load proxies: {e}")

    def mark_proxy_bad(self, proxy_idx: int):
        if proxy_idx >= 0:
            self.bad_proxies[proxy_idx] = time.time() + 60  # 1 minute cooldown

    def get_next_proxy(self):
        if not self.proxies:
            return None, -1

        # Clean expired bad proxies
        now = time.time()
        self.bad_proxies = {k: v for k, v in self.bad_proxies.items() if v > now}

        # Find next available proxy
        start_idx = self.current_idx
        while True:
            if self.current_idx not in self.bad_proxies:
                proxy = self.proxies[self.current_idx]
                proxy_idx = self.current_idx
                self.current_idx = (self.current_idx + 1) % len(self.proxies)
                return proxy, proxy_idx

            self.current_idx = (self.current_idx + 1) % len(self.proxies)
            if self.current_idx == start_idx:  # All proxies are bad
                proxy = self.proxies[self.current_idx]
                proxy_idx = self.current_idx
                self.current_idx = (self.current_idx + 1) % len(self.proxies)
                return proxy, proxy_idx


class Downloader:
    def __init__(self, config: Config):
        self.config = config
        self.progress = ProgressTracker(config)
        self.proxy_manager = ProxyManager(config.proxy_file if config.use_proxies else None)
        self.session = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def create_fresh_session(self, proxy: Optional[Dict] = None) -> aiohttp.ClientSession:
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        ssl_context = ssl.create_default_context()

        if proxy:
            try:
                connector = aiosocks.ProxyConnector(
                    proxy_type=aiosocks.ProxyType.SOCKS5,
                    host=proxy['host'],
                    port=proxy['port'],
                    username=proxy.get('username'),
                    password=proxy.get('password')
                )
            except Exception:
                connector = aiohttp.TCPConnector(
                    ssl=ssl_context,
                    limit=1,
                    limit_per_host=1,
                    enable_cleanup_closed=True
                )
        else:
            connector = aiohttp.TCPConnector(
                ssl=ssl_context,
                limit=1,
                limit_per_host=1,
                enable_cleanup_closed=True
            )

        return aiohttp.ClientSession(connector=connector, timeout=timeout)

    def is_timeout_error(self, e: Exception) -> bool:
        return isinstance(e, (asyncio.TimeoutError, aiohttp.ServerTimeoutError)) or 'timeout' in str(e).lower()

    def _get_headers(self) -> Dict[str, str]:
        return {
            'User-Agent': self.config.user_agent,
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate'
        }

    async def check_resource_exists(self, url: str, session: aiohttp.ClientSession) -> Tuple[bool, int, Optional[Exception]]:
        try:
            async with session.head(url, headers=self._get_headers()) as resp:
                return resp.status == 200, resp.status, None
        except Exception as e:
            return False, 0, e

    async def fetch_html(self, url: str, session: aiohttp.ClientSession) -> Tuple[str, int, Optional[Exception]]:
        try:
            async with session.get(url, headers=self._get_headers()) as resp:
                if resp.status != 200:
                    return "", resp.status, Exception(f"HTTP status: {resp.status}")
                html = await resp.text()
                return html, resp.status, None
        except Exception as e:
            return "", 0, e

    async def get_filename_from_url(self, url: str, session: aiohttp.ClientSession) -> Tuple[str, Optional[Exception]]:
        try:
            async with session.head(url, headers=self._get_headers()) as resp:
                cd = resp.headers.get('Content-Disposition', '')
                if not cd:
                    return "unknown.html", None

                match = re.search(r'filename\*?=[\'"]?(?:UTF-8[\'\']?)?([^;\'"]*)[\'"]?', cd)
                if match:
                    return match.group(1), None

                return url.split('/')[-1], None
        except Exception as e:
            return "unknown.html", e

    async def fetch_work(self, work_id: int) -> Optional[Work]:
        url = f"{self.config.base_url}/{work_id}/a.html"

        for attempt in range(self.config.retries):
            proxy, proxy_idx = self.proxy_manager.get_next_proxy() if self.config.use_proxies else (None, -1)

            session = None
            try:
                session = await self.create_fresh_session(proxy)

                valid_resource, status_code, head_err = await self.check_resource_exists(url, session)

                if head_err and self.is_timeout_error(head_err):
                    print(f"ID {work_id}: Timeout error with proxy - Retrying with another proxy")
                    await asyncio.sleep(2)
                    continue

                if status_code == 429:
                    print(f"ID {work_id}: Rate limited (429) - Switching to next proxy and retrying")
                    self.proxy_manager.mark_proxy_bad(proxy_idx)
                    await asyncio.sleep(2)
                    continue

                if status_code == 503:
                    print(f"ID {work_id}: Service unavailable (503) - Retrying with next proxy")
                    await asyncio.sleep(3)
                    continue

                if status_code == 0:
                    print(f"ID {work_id}: Connection issue (status code 0) - Retrying with another proxy")
                    await asyncio.sleep(2)
                    continue

                if head_err or not valid_resource:
                    print(f"ID {work_id}: HEAD Check: Resource not available - Status: {status_code}")
                    return None

                html_content, get_status_code, get_err = await self.fetch_html(url, session)

                if get_err and self.is_timeout_error(get_err):
                    print(f"ID {work_id}: Timeout error during GET - Retrying with another proxy")
                    await asyncio.sleep(2)
                    continue

                if get_status_code == 429:
                    print(f"ID {work_id}: Rate limited (429) during GET - Switching to next proxy and retrying")
                    self.proxy_manager.mark_proxy_bad(proxy_idx)
                    await asyncio.sleep(2)
                    continue

                if get_status_code == 0:
                    print(f"ID {work_id}: Connection issue during GET (status code 0) - Retrying with another proxy")
                    await asyncio.sleep(2)
                    continue

                print(f"ID {work_id}: Status: {get_status_code}")

                if get_err or get_status_code != 200:
                    await asyncio.sleep(1)
                    continue

                filename, filename_err = await self.get_filename_from_url(url, session)
                if filename_err:
                    if self.is_timeout_error(filename_err):
                        print(f"ID {work_id}: Timeout error getting filename - Retrying with another proxy")
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
                    print(f"ID {work_id}: Timeout error - Retrying with another proxy")
                    await asyncio.sleep(2)
                    continue

                print(f"ID {work_id}: Attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(1)
            finally:
                if session:
                    await session.close()

        print(f"ID {work_id}: Failed after {self.config.retries} retries")
        return None

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

    async def process_work(self, work_id: int, semaphore: asyncio.Semaphore):
        async with semaphore:
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

        print(f"Configuration: StartID={self.config.start_id}, EndID={self.config.end_id}, BatchSize={self.config.batch_size}, Concurrent={self.config.concurrent}")
        if self.config.use_proxies:
            print(f"Loaded {len(self.proxy_manager.proxies)} proxies")
        else:
            print("Running without proxies (direct connection)")

        print(f"Processing {len(pending_ids)}/{total_ids} works")

        semaphore = asyncio.Semaphore(self.config.concurrent)
        tasks = []

        start_time = time.time()
        processed = 0

        for work_id in pending_ids:
            task = asyncio.create_task(self.process_work(work_id, semaphore))
            tasks.append(task)

            # Process in chunks and save progress periodically
            if len(tasks) >= 100:
                await asyncio.gather(*tasks)
                tasks = []
                processed += 100

                await self.progress.save()

                # Progress stats
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                remaining = (len(pending_ids) - processed) / rate if rate > 0 else 0

                print(f"Progress: {processed + len(self.progress.completed)}/{total_ids} "
                           f"({(processed + len(self.progress.completed))/total_ids*100:.1f}%), "
                           f"{rate:.2f} works/sec, est. remaining: {remaining/60:.1f} min")

        if tasks:
            await asyncio.gather(*tasks)

        await self.progress.save()

        elapsed = time.time() - start_time
        print(f"Complete! Processed {len(pending_ids)} works in {elapsed:.1f}s "
                    f"({len(pending_ids)/elapsed:.2f} works/sec)")


def main():
    parser = argparse.ArgumentParser(description="OTW Archive Downloader")
    parser.add_argument('--start-id', type=int, default=1, help='Starting ID')
    parser.add_argument('--end-id', type=int, default=100000, help='Ending ID')
    parser.add_argument('--batch-size', type=int, default=10000, help='IDs per output file')
    parser.add_argument('--concurrent', type=int, default=4, help='Concurrent requests')
    parser.add_argument('--retries', type=int, default=5, help='Retry attempts')
    parser.add_argument('--proxy-file', default='proxy.txt', help='Proxy file')
    parser.add_argument('--output', default='output', help='Output directory')
    parser.add_argument('--use-proxies', action='store_true', help='Use proxies')
    parser.add_argument('--timeout', type=int, default=60, help='Request timeout')
    parser.add_argument('--base-url', default='https://download.archiveofourown.org/downloads', help='Base URL')
    parser.add_argument('--user-agent', default='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36', help='User agent')

    args = parser.parse_args()

    config = Config(
        start_id=args.start_id,
        end_id=args.end_id,
        batch_size=args.batch_size,
        concurrent=args.concurrent,
        retries=args.retries,
        proxy_file=args.proxy_file,
        output_dir=args.output,
        use_proxies=args.use_proxies,
        timeout=args.timeout,
        base_url=args.base_url,
        user_agent=args.user_agent
    )

    async def run_downloader():
        async with Downloader(config) as downloader:
            await downloader.run()

    asyncio.run(run_downloader())


if __name__ == '__main__':
    main()
