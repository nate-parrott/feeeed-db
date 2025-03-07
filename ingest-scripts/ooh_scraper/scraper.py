from typing import NamedTuple, List, Dict
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


class BlogEntry(NamedTuple):
    """A blog entry from ooh.directory"""
    url: str
    title: str
    description: str


class ScrapedCategory(NamedTuple):
    """Results from scraping a category page"""
    subcategory_urls: List[str]  # Absolute URLs to subcategories
    opml_url: str  # Absolute URL to OPML file
    blogs: List[BlogEntry]  # Blog entries found on this page


def scrape_category_page(url: str) -> ScrapedCategory:
    """
    Scrapes a category page from ooh.directory and returns subcategory and OPML URLs.
    
    Args:
        url: Full URL to category page (e.g., https://ooh.directory/blogs/arts/)
        
    Returns:
        ScrapedCategory containing absolute URLs for subcategories and OPML
    """
    response = requests.get(url)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find OPML link in header first
    opml_link = soup.find('link', attrs={'type': 'application/xml', 'title': lambda t: 'OPML' in str(t) if t else False})
    if opml_link:
        opml_url = opml_link.get('href')
    else:
        # Fallback to link in page body
        opml_link = soup.find('a', title='OPML file')
        opml_url = opml_link.get('href') if opml_link else None
        
    if not opml_url:
        raise ValueError(f"Could not find OPML link in {url}")
    
    # Make OPML URL absolute
    opml_url = urljoin(url, opml_url)
    
    # Find all subcategory links
    subcategory_urls = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.startswith('/blogs/') and href != url:
            subcategory_urls.append(urljoin(url, href))
            
    # Extract blog entries
    blogs = []
    for item in soup.find_all('li', class_='websites__item'):
        link = item.find('a')
        if not link:
            continue
            
        blog_url = link.get('href')
        blog_title = link.text.strip()
        
        # Description is in a <q> tag
        desc_tag = item.find('q')
        description = desc_tag.text.strip() if desc_tag else ""
        
        if blog_url and blog_title:
            blogs.append(BlogEntry(
                url=blog_url,
                title=blog_title,
                description=description
            ))
            
    return ScrapedCategory(
        subcategory_urls=subcategory_urls,
        opml_url=opml_url,
        blogs=blogs
    )