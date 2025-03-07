import unittest
from .scraper import scrape_category_page
import os

class TestScraper(unittest.TestCase):
    def test_scrapes_blogs(self):
        result = scrape_category_page('https://ooh.directory/blogs/arts/')
        
        # Print debug info
        print("\nDEBUG INFO:")
        print(f"OPML URL: {result.opml_url}")
        print(f"Found {len(result.blogs)} blogs")
        print(f"Found {len(result.subcategory_urls)} subcategories")
        
        # Write results to inspection directory for review
        inspection_dir = os.environ.get('NAT_INSPECT_DIR', '/tmp')
        with open(os.path.join(inspection_dir, 'blogs.txt'), 'w') as f:
            f.write(f"Found {len(result.blogs)} blogs:\n\n")
            for blog in result.blogs:
                f.write(f"Title: {blog.title}\n")
                f.write(f"URL: {blog.url}\n")
                f.write(f"Description: {blog.description}\n")
                f.write("-" * 80 + "\n")
                
        # Basic validation
        self.assertTrue(len(result.blogs) > 0, "Should find some blogs")
        self.assertTrue(len(result.subcategory_urls) > 0, "Should find subcategories")
        self.assertTrue(result.opml_url.endswith('.xml'), "Should find OPML URL")
        
        # Validate first blog has required fields
        first_blog = result.blogs[0]
        self.assertTrue(first_blog.url.startswith('http'), "Blog should have valid URL")
        self.assertTrue(len(first_blog.title) > 0, "Blog should have title")

if __name__ == '__main__':
    unittest.main()