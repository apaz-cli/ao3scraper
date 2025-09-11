#!/usr/bin/env python3
import json
import requests
from bs4 import BeautifulSoup
from worker import AO3Scraper, Config

def debug_html_structure(html):
    """Debug the HTML structure to understand chapter parsing"""
    soup = BeautifulSoup(html, 'html.parser')
    
    print("=== HTML Structure Debug ===")
    
    # Look for chapters div
    chapters_div = soup.find(id='chapters')
    if chapters_div:
        print("Found #chapters div")
        
        # Save a snippet of the HTML for inspection
        with open('debug_chapters.html', 'w') as f:
            f.write(str(chapters_div)[:5000])  # First 5000 chars
        print("Saved chapters HTML snippet to debug_chapters.html")
        
        # Look for chapter divs
        chapter_divs = chapters_div.find_all('div', class_='chapter')
        print(f"Found {len(chapter_divs)} div.chapter elements")
        
        # Check direct children
        direct_children = [child for child in chapters_div.children if hasattr(child, 'name') and child.name]
        print(f"Direct children tags: {[child.name for child in direct_children]}")
        
        # Look for all divs
        all_divs = chapters_div.find_all('div')
        print(f"All div elements: {len(all_divs)}")
        for i, div in enumerate(all_divs[:10]):  # First 10 only
            classes = div.get('class', [])
            print(f"  Div {i}: classes={classes}")
        
        # Look for h3 elements anywhere
        all_h3 = chapters_div.find_all('h3')
        print(f"All h3 tags in chapters div: {len(all_h3)}")
        for h3 in all_h3:
            print(f"  h3: '{h3.get_text(strip=True)}' (class: {h3.get('class')})")
            
        # Look for userstuff divs
        userstuff_divs = chapters_div.find_all('div', class_='userstuff')
        print(f"Found {len(userstuff_divs)} userstuff divs")
        
    else:
        print("No #chapters div found")

def main():
    config = Config()
    scraper = AO3Scraper(config)
    
    # Test the specific work ID
    work_id = 57150256
    print(f"Testing work ID: {work_id}")
    
    # Get the raw HTML first for debugging
    url = f"{config.base_url}/{work_id}/a.html"
    response = scraper.session.get(url)
    
    if response.status_code == 200:
        print("Got HTML content, debugging structure...")
        debug_html_structure(response.text)
        print("\n=== Processing with current parser ===")
    
    work_data = scraper.fetch_work(work_id)
    
    if work_data:
        print(f"Found {len(work_data['chapters'])} chapters:")
        for i, chapter in enumerate(work_data['chapters']):
            print(f"  Chapter {i+1}: {chapter['title']}")
        
        # Save to res.json
        with open('res.json', 'w') as f:
            json.dump(work_data, f, indent=2)
        
        print(f"Saved result to res.json")
    else:
        print("Work not found or private")

if __name__ == '__main__':
    main()