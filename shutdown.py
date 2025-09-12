#!/usr/bin/env python3
import argparse
import requests
import sys

def main():
    parser = argparse.ArgumentParser(description="Shutdown AO3 Scraper Server")
    parser.add_argument('--server', default='localhost', help='Server IP address')
    parser.add_argument('--port', type=int, default=8000, help='Server port')
    args = parser.parse_args()

    url = f"http://{args.server}:{args.port}/shutdown"

    try:
        response = requests.post(url)
        response.raise_for_status()
        result = response.json()
        print(result['message'])
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to server")
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        sys.exit(1)
    except KeyError:
        print("Unexpected response from server")
        sys.exit(1)

if __name__ == '__main__':
    main()