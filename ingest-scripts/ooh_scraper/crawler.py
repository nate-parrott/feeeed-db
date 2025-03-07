from typing import Set, Dict, List
import os
from urllib.parse import urlparse
import requests
from pathlib import Path
import time
import json

from scraper import scrape_category_page, BlogEntry

class CategoryCrawler:
    def __init__(self, opml_dir: str = "opmls"):
        self.opml_dir = Path(opml_dir)
        self.visited_urls: Set[str] = set()
        self.category_to_opml: Dict[str, str] = {}
        
    def url_to_category_path(self, url: str) -> str:
        """Convert URL to category path for filename"""
        parsed = urlparse(url)
        # Remove leading/trailing slashes and 'blogs' prefix
        path = parsed.path.strip('/').replace('blogs/', '', 1)
        return path or 'root'
    
    def download_opml(self, url: str, category_path: str) -> None:
        """Download OPML file and save it"""
        response = requests.get(url)
        response.raise_for_status()
        
        opml_path = self.opml_dir / f"{category_path}.opml"
        opml_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(opml_path, 'w', encoding='utf-8') as f:
            f.write(response.text)
            
    def save_descriptions(self, blogs: List[BlogEntry], category_path: str) -> None:
        """Save blog descriptions to JSON file"""
        # Convert blogs to dicts for JSON serialization
        blog_dicts = [blog._asdict() for blog in blogs]
        
        # Save alongside OPML file
        desc_path = self.opml_dir / f"{category_path}.descriptions.json"
        desc_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save as JSON
        with open(desc_path, 'w', encoding='utf-8') as f:
            json.dump(blog_dicts, f, indent=2)
            
    def crawl(self, root_urls: List[str]) -> Dict[str, str]:
        """
        Recursively crawl category pages starting from root URLs
        
        Args:
            root_urls: List of category URLs to start from
            
        Returns:
            Dict mapping category paths to their OPML URLs
        """
        urls_to_visit = root_urls.copy()
        
        while urls_to_visit:
            url = urls_to_visit.pop(0)
            
            if url in self.visited_urls:
                continue
                
            print(f"Crawling {url}")
            self.visited_urls.add(url)
            
            try:
                result = scrape_category_page(url)
                
                # Save category -> OPML mapping
                category_path = self.url_to_category_path(url)
                self.category_to_opml[category_path] = result.opml_url
                
                # Download OPML and save descriptions
                self.download_opml(result.opml_url, category_path)
                self.save_descriptions(result.blogs, category_path)
                
                # Add new subcategories to visit
                new_urls = [
                    subcat_url for subcat_url in result.subcategory_urls 
                    if subcat_url not in self.visited_urls
                ]
                urls_to_visit.extend(new_urls)
                
                # Be nice to the server
                time.sleep(1)
                
            except Exception as e:
                print(f"Error crawling {url}: {e}")
                continue
                
        return self.category_to_opml