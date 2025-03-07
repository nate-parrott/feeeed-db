#!/usr/bin/env python3

from crawler import CategoryCrawler

def main():
    root_urls = [
        'https://ooh.directory/blogs/arts/',
        'https://ooh.directory/blogs/technology/',
        'https://ooh.directory/blogs/countries/',
        'https://ooh.directory/blogs/economics/',
        'https://ooh.directory/blogs/education/',
        'https://ooh.directory/blogs/politics/',
        'https://ooh.directory/blogs/humanities/',
        'https://ooh.directory/blogs/personal/',
        'https://ooh.directory/blogs/recreation/',
        'https://ooh.directory/blogs/science/',
        'https://ooh.directory/blogs/society/',
        'https://ooh.directory/blogs/uncategorizable/'
    ]
    
    crawler = CategoryCrawler()
    results = crawler.crawl(root_urls)
    
    print(f"\nCrawl complete! Found {len(results)} categories")
    print("Categories -> OPML files mapping saved in ingested_data/ooh_scraper/opmls/")

if __name__ == '__main__':
    main()