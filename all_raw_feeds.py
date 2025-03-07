"""Module for fetching all feed items from raw JSONL data files."""
import json
import os
from typing import Iterator
from feed_types import Feed

def iterate_raw_feeds(limit_to=None) -> Iterator[Feed]:
    """Iterate through all raw feed items from JSONL files in raw_data directory."""
    raw_data_dir = os.path.join(os.path.dirname(__file__), 'raw_data')
    
    # Find all .jsonl files in the raw_data directory
    for filename in os.listdir(raw_data_dir):
        if filename.endswith('.feeds.jsonl'):
            if limit_to and filename not in [f'{limit_to}.feeds.jsonl' for limit_to in limit_to]:
                continue
            file_path = os.path.join(raw_data_dir, filename)
            
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():  # Skip empty lines
                        feed = json.loads(line)
                        yield feed

def get_all_raw_feeds(limit_to=None) -> list[Feed]:
    """Return all raw feed items as a list."""
    return list(iterate_raw_feeds(limit_to))

if __name__ == '__main__':
    print(list(iterate_raw_feeds())[:5])