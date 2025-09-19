#!/usr/bin/env python3

import requests
import time
import json
import sys
import argparse
import os
from datetime import datetime
from collections import deque

# ANSI color codes
class Colors:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'

    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'

    BRIGHT_BLACK = '\033[90m'
    BRIGHT_RED = '\033[91m'
    BRIGHT_GREEN = '\033[92m'
    BRIGHT_YELLOW = '\033[93m'
    BRIGHT_BLUE = '\033[94m'
    BRIGHT_MAGENTA = '\033[95m'
    BRIGHT_CYAN = '\033[96m'
    BRIGHT_WHITE = '\033[97m'

def clear_screen():
    print('\033[2J\033[H', end='')

def format_number(num):
    """Format number with commas for readability"""
    return f"{num:,}"

def format_percentage(completed, total):
    """Calculate and format percentage"""
    if total == 0:
        return "0.00%"
    return f"{(completed / total * 100):.2f}%"

def format_file_size(size_bytes):
    """Format file size in human readable format"""
    if size_bytes == 0:
        return "0 B"

    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    unit_index = 0
    size = float(size_bytes)

    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1

    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    else:
        return f"{size:.2f} {units[unit_index]}"

def format_progress_bar(completed, total):
    """Create a visual progress bar"""
    # Get terminal width, default to 78 if not available
    try:
        terminal_width = os.get_terminal_size().columns
    except OSError:
        terminal_width = 80

    width = min(terminal_width, 100)
    percent_filled: float = (completed / total) if total > 0 else 0
    chars_filled = int(width * percent_filled)

    return f"{Colors.YELLOW}{'█' * chars_filled}{Colors.DIM}{'░' * (width - chars_filled)}{Colors.RESET}"

def calculate_responses_per_second(completed_history):
    """Calculate responses per second since monitor started"""
    if len(completed_history) < 2:
        return 0.0

    # Calculate rate based on first and last entries
    oldest_time, oldest_count = completed_history[0]
    newest_time, newest_count = completed_history[-1]

    time_diff = newest_time - oldest_time
    if time_diff <= 0:
        return 0.0

    count_diff = newest_count - oldest_count
    return count_diff / time_diff

def display_progress(data, responses_per_second=0.0):
    """Display formatted progress information"""
    clear_screen()

    print(f"{Colors.BOLD}{Colors.CYAN}{'=' * 60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}           AO3 SCRAPER PROGRESS MONITOR{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'=' * 60}{Colors.RESET}")
    print()

    # Extract data with the actual field names
    public = data['completed']
    private = data['private']
    remaining = data['remaining']
    session_completed = data['session_completed']
    progress_percent = data['progress_percent']

    total_processed = public + private
    total_estimated = total_processed + remaining

    # STATUS
    print(f"{Colors.BOLD}{Colors.BLUE}📊 STATUS{Colors.RESET}")
    print(f"{Colors.BLUE}{'-' * 9}{Colors.RESET}")
    print(f"{Colors.YELLOW}Connected workers:     {Colors.BRIGHT_WHITE}{data['connected_workers']}{Colors.RESET}")
    print(f"{Colors.YELLOW}Available queue size:  {Colors.BRIGHT_WHITE}{data['available_queue_size']}{Colors.RESET}")
    print(f"{Colors.YELLOW}Responses/sec (total): {Colors.BRIGHT_WHITE}{responses_per_second:.2f}{Colors.RESET}")
    print(f"{Colors.YELLOW}Disk usage:            {Colors.BRIGHT_WHITE}{data['disk_usage_percent']}%{Colors.RESET}")
    print(f"{Colors.YELLOW}Results file size:     {Colors.BRIGHT_WHITE}{format_file_size(data['results_file_size'])}{Colors.RESET}")
    print()

    # PROGRESS
    print(f"{Colors.BOLD}{Colors.BLUE}📈 PROGRESS{Colors.RESET}")
    print(f"{Colors.BLUE}{'-' * 11}{Colors.RESET}")
    print(f"{Colors.YELLOW}Total processed: {Colors.BRIGHT_WHITE}{format_number(total_processed)}{Colors.RESET}")
    print(f"{Colors.YELLOW}Remaining:       {Colors.BRIGHT_WHITE}{format_number(remaining)}{Colors.RESET}")
    print(f"{Colors.YELLOW}Processed this session:{Colors.BRIGHT_WHITE}{format_number(session_completed)}{Colors.RESET}")
    # PROGRESS BAR
    print(f"{Colors.YELLOW}Progress:        {Colors.BRIGHT_WHITE}{progress_percent:.4f}%{Colors.RESET}")
    print(format_progress_bar(total_processed, total_estimated))
    print()

    # DATA
    print(f"{Colors.BOLD}{Colors.BLUE}📋 DATA{Colors.RESET}")
    print(f"{Colors.BLUE}{'-' * 7}{Colors.RESET}")
    print(f"{Colors.GREEN}Public works:{Colors.RESET} {Colors.BRIGHT_WHITE}{format_number(public)}{Colors.RESET} {Colors.DIM}({format_percentage(public, total_processed) if total_processed > 0 else '0.00%'}){Colors.RESET}")
    print(f"{Colors.RED}Private works:{Colors.RESET} {Colors.BRIGHT_WHITE}{format_number(private)}{Colors.RESET} {Colors.DIM}({format_percentage(private, total_processed) if total_processed > 0 else '0.00%'}){Colors.RESET}")

def main():
    parser = argparse.ArgumentParser(description='Monitor AO3 scraper progress')
    parser.add_argument('--server', default='localhost',
                       help='Server address (IP or hostname)')
    parser.add_argument('--port', type=int, default=8000, help='Server port')
    parser.add_argument('--interval', type=int, default=3,
                       help='Refresh interval in seconds (default: 3)')

    args = parser.parse_args()

    server_url = f"http://{args.server}:{args.port}/progress"

    # Track completed responses over time for rate calculation
    completed_history = deque()  # Store all entries since monitor started

    print("\x1b[?25l", end='')  # Hide cursor
    print(f"{Colors.CYAN}🔍 Connecting to {server_url}{Colors.RESET}")
    print(f"{Colors.DIM}Press Ctrl+C to exit{Colors.RESET}")

    try:
        while True:
            try:
                response = requests.get(server_url, timeout=5)
                if response.status_code == 200:
                    data = response.json()

                    # Track completed count with timestamp
                    current_time = time.time()
                    completed_count = data['completed']
                    completed_history.append((current_time, completed_count))

                    # Calculate responses per second
                    responses_per_second = calculate_responses_per_second(completed_history)

                    display_progress(data, responses_per_second)
                else:
                    clear_screen()
                    print(f"{Colors.BOLD}{Colors.RED}❌ ERROR{Colors.RESET}")
                    print(f"{Colors.YELLOW}Server responded with status {response.status_code}{Colors.RESET}")
                    print(f"{Colors.DIM}Response: {response.text}{Colors.RESET}")

            except requests.exceptions.ConnectionError:
                clear_screen()
                print(f"{Colors.BOLD}{Colors.RED}🔌 CONNECTION ERROR{Colors.RESET}")
                print(f"{Colors.YELLOW}Cannot connect to {server_url}{Colors.RESET}")
                print(f"{Colors.DIM}Make sure the server is running...{Colors.RESET}")

            except requests.exceptions.Timeout:
                clear_screen()
                print(f"{Colors.BOLD}{Colors.RED}⏰ TIMEOUT ERROR{Colors.RESET}")
                print(f"{Colors.YELLOW}Server is not responding...{Colors.RESET}")

            except json.JSONDecodeError:
                clear_screen()
                print(f"{Colors.BOLD}{Colors.RED}📄 PARSE ERROR{Colors.RESET}")
                print(f"{Colors.YELLOW}Server returned invalid JSON{Colors.RESET}")

            except Exception as e:
                clear_screen()
                print(f"{Colors.BOLD}{Colors.RED}💥 UNEXPECTED ERROR{Colors.RESET}")
                print(f"{Colors.YELLOW}Error: {str(e)}{Colors.RESET}")
                raise e

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\x1b[?25h", end='')
        clear_screen()
        sys.exit(0)

if __name__ == "__main__":
    main()
