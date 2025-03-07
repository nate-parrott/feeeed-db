from typing import TypedDict, Optional, List, Dict
import json
from feed_types import Feed
from feed_fetching import FeedItem
from llm_json import query_llm_json

# Load categories at module level
with open("categories.json") as f:
    CATEGORIES = json.load(f)

def create_missing_tags_structure():
    """Create empty missing tags structure based on category types"""
    return {cat_type: {} for cat_type in CATEGORIES["tags"].keys()}

class SimpleFeed(TypedDict):
    """A minimal feed type needed for labelling, avoiding circular imports."""
    feed: Feed
    items: List[FeedItem]

prompt = """
I'm building a curated database of feeds (RSS, Youtube, subreddit, etc) and I need your help.

I'm going to show you information about some feeds, 
including data about where I got them, metadata taken from that original source,
and the content of some recent posts.

Your job is to look at the feed and write about it, to help readers decide what to read.
Act as a creative, professional critic and curator; your information should be short but compelling.

Respond using this exact JSON schema:

```
{
  "labels": [
    {
      "feed_id": 0,  // IMPORTANT: This must match the FEED ID number from the input
      "nsfw": boolean,
      "spam_or_junk": boolean,
      "clean_title": string, 
      "clean_author": string | null,
      "description": string,
      "language": string,
      "top_level_tags": string[],
      "detailed_tags": string[],
      "hidden_tags": string[],
      "keywords": string[]
    },
    {
      "feed_id": 1,
      // same structure as above
    },
    // one entry for each feed in the same order as provided
  ]
}
```

IMPORTANT: You MUST output information for EVERY feed I give you. Your array in the "labels" key MUST have exactly the same number of items as the input feeds.

Notes:
- Phrase your information the third person; say "A blog about X" instead of "My blog about X".
- The input description of the feed may come from the author themselves. Deflate it if it's too promotional or self-aggrandizing. (E.g. if a blog's description is "The world's best source for racing news," you may want to write something else instead)
- For super well-known feeds (like NYT, Daring Fireball, Paul Krugman's blog, etc) you can include your general knowledge about them in your description, like "A Nobel-winning economist breaks down economic news."
- I will give you a list of tag taxonomies of various granularities. You must assign at least one tag from all granularities may ONLY use categories provided below. When filling out the top_level_tags array, you must use tags from the top_level tags list, not from other lists. Never invent your own tags.
- Apply ALL applicable tags from hidden_tags.
- If there is an author name in the original title of the blog (e.g. "BitBytes - Josh Jacobsen"), you should clean the title as "BitBytes" and set clean_author to Josh Jacobsen. Do not remove author names from clean_title if they're part of the main title, like "Paul's Blog"
- Normalize all dashes and colons to em-dashes when appropriate
- If "homepage" or "all" or other aggregate terms indicating that this is a feed are in the title, remove them.
- Don't assign top-level 'news' labels to news channels that are super niche or local; assign those the Local tag instead.

# Examples
Orig Title: Astrostyle: Astrology and Daily, Weekly, Monthly Horoscopes by The AstroTwins
Clean title: Astrostyle
Clean author: The AstroTwins

Orig Title: My News | Homepage
Clean title: My News

Orig Title: Comment is Freed - Sam Freedman
Clean title: Comment is Freed
Clean author: Sam Freedman

Orig Title: Pluralistic -- Daily links from Cory Doctorow
Clean title: Pluralistic
Clean author: Cory Doctorow

Orig Title: New York Times: Cooking
Clean title: New York Times Cooking

Here are the available sets of categories you must use:
[[CATEGORIES]]

And here are the feeds I want you to generate responses for:
[[FEEDS]]

Below, write your JSON response:
"""

class FeedLabels(TypedDict):
    nsfw: bool
    spam_or_junk: bool

    clean_title: str
    clean_author: Optional[str]
    
    description: str
    language: str  # ISO 639-1 language code like 'en', 'fr'
    top_level_tags: List[str]
    detailed_tags: List[str]
    hidden_tags: List[str]
    keywords: List[str]

def truncate_text(text: str, max_len: int = 128) -> str:
    """Helper to truncate text, replacing newlines with spaces and adding ellipsis if needed"""
    # Replace newlines with spaces
    text = text.replace("\n", " ")
    # Remove duplicate spaces
    text = " ".join(text.split())
    
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."

def feed_to_text(feed: SimpleFeed, id: int) -> str:
    """Convert an enriched feed to a text representation for the LLM to analyze"""
    # Core feed data
    out = [f"FEED ID: {id}"]
    out.append(f"Title: {feed['feed']['title']}")
    if feed.get('feed_description'): # Feed's own description
        out.append(f"Feed Description: {feed['feed_description']}")
    if feed['feed'].get('summary'):
        out.append(f"Summary: {feed['feed']['summary']}")
    if feed['feed'].get('details'):
        out.append(f"Details: {feed['feed']['details']}")
        
    # Add recent items if available
    if feed['items']:
        out.append("\nRecent posts:")
        for item in feed['items'][:5]:
            title = truncate_text(item['title'])
            out.append(f"- {title}")
            
    return "\n".join(out)

def create_feed_label_schema(id_map: Dict[int, str], cat_data: Dict) -> Dict:
    """Create a JSON schema for the feed labeling output"""
    # Common languages in ISO 639-1 format
    common_languages = [
        "en", "fr", "de", "es", "it", "ja", "zh", "ru", "pt", "ko", 
        "nl", "sv", "no", "fi", "da", "pl", "tr", "ar", "hi"
    ]
    
    # Create schema for a single feed entry
    feed_schema = {
        "type": "object",
        "properties": {
            "feed_id": {"type": "integer"},
            "nsfw": {"type": "boolean"},
            "spam_or_junk": {"type": "boolean"},
            "clean_title": {"type": "string"},
            "clean_author": {"type": ["string", "null"]},
            "description": {"type": "string"},
            "language": {
                "type": "string",
                "enum": common_languages,
                "description": "ISO 639-1 language code"
            },
            "top_level_tags": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": cat_data["tags"]["top_level"]
                }
            },
            "detailed_tags": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": cat_data["tags"]["detailed"]
                }
            },
            "hidden_tags": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": cat_data["tags"]["hidden"]
                }
            },
            "keywords": {
                "type": "array",
                "items": {"type": "string"}
            }
        },
        # All properties are required except clean_author which can be null
        "required": [
            "feed_id", "nsfw", "spam_or_junk", "clean_title", "description",
            "language", "top_level_tags", "detailed_tags", "hidden_tags", "keywords", 
            "clean_author"
        ],
        "additionalProperties": False
    }
    
    # Create the top-level schema with labels array
    schema = {
        "name": "feed_labeling",
        "schema": {
            "type": "object",
            "properties": {
                "labels": {
                    "type": "array",
                    "items": feed_schema,
                    # "minItems": len(id_map),
                    # "maxItems": len(id_map)
                }
            },
            "required": ["labels"],
            "additionalProperties": False
        },
        "strict": True
    }
    
    return schema

def validate_response(response_data: dict, id_map: dict, cat_data: dict) -> None:
    """Validate LLM response format and content"""
    # Check basic structure
    assert isinstance(response_data, dict), "Response must be a dictionary"
    assert "labels" in response_data, "Response must contain a 'labels' property"
    
    labels = response_data.get("labels", [])
    assert isinstance(labels, list), "labels must be an array"
    assert len(labels) == len(id_map), f"Expected {len(id_map)} feed entries, got {len(labels)}"
    
    # Load expected tag sets for filtering
    tag_sets = cat_data["tags"]
    top_level = set(tag_sets["top_level"])
    detailed = set(tag_sets["detailed"])
    hidden = set(tag_sets["hidden"])
    
    # Track invalid tags and their sources using category structure
    missing_tags = create_missing_tags_structure()
    
    # Keep track of feed IDs we've seen
    seen_ids = set()
    
    # Validate each feed response
    for feed_info in labels:
        # Check ID validity and track seen IDs
        assert isinstance(feed_info, dict), "Feed info must be a dictionary"
        feed_id = feed_info.get("feed_id")
        assert isinstance(feed_id, int), f"feed_id must be an integer, got {type(feed_id)}"
        assert feed_id in id_map, f"Invalid feed ID: {feed_id}"
        assert feed_id not in seen_ids, f"Duplicate feed ID: {feed_id}"
        seen_ids.add(feed_id)
        
        # Check required fields exist and have correct types 
        assert isinstance(feed_info.get("clean_title"), str), f"Missing/invalid clean_title for ID {feed_id}"
        assert isinstance(feed_info.get("description"), str), f"Missing/invalid description for ID {feed_id}"
        assert isinstance(feed_info.get("language"), str), f"Missing/invalid language code for ID {feed_id}"
        assert len(feed_info["language"]) == 2, f"Language code must be ISO 639-1 format (2 chars) for ID {feed_id}"
        
        # Validate tag lists are lists
        assert isinstance(feed_info.get("top_level_tags"), list), f"Invalid top_level_tags for ID {feed_id}"
        assert isinstance(feed_info.get("detailed_tags"), list), f"Invalid detailed_tags for ID {feed_id}"
        assert isinstance(feed_info.get("hidden_tags"), list), f"Invalid hidden_tags for ID {feed_id}"

        # Filter to only valid tags and track invalid ones
        def filter_and_log_tags(tags, valid_set, tag_type):
            invalid = [t for t in tags if isinstance(t, str) and t not in valid_set]
            if invalid:
                print(f"Feed {feed_id}: Dropping invalid {tag_type} tags: {invalid}")
                # Track invalid tags with their source
                for tag in invalid:
                    if isinstance(tag, str):  # Only track string tags
                        if tag not in missing_tags[tag_type]:
                            missing_tags[tag_type][tag] = []
                        if feed_info["clean_title"] not in missing_tags[tag_type][tag]:
                            missing_tags[tag_type][tag].append(feed_info["clean_title"])
            return [t for t in tags if isinstance(t, str) and t in valid_set]

        feed_info["top_level_tags"] = filter_and_log_tags(feed_info["top_level_tags"], top_level, "top_level")
        feed_info["detailed_tags"] = filter_and_log_tags(feed_info["detailed_tags"], detailed, "detailed")
        feed_info["hidden_tags"] = filter_and_log_tags(feed_info["hidden_tags"], hidden, "hidden")

    # Check that we've seen all expected feed IDs
    assert len(seen_ids) == len(id_map), f"Some feed IDs are missing from response: {set(id_map.keys()) - seen_ids}"
    
    # Save missing tags to JSON file atomically
    import os
    import tempfile
    
    # Path to the missing tags file
    missing_tags_path = "missing_tags.json"
    
    # Load existing missing tags if the file exists
    try:
        with open(missing_tags_path, "r") as f:
            existing_missing_tags = json.load(f)
            # Merge new missing tags with existing ones
            for tag_type in missing_tags:
                if tag_type not in existing_missing_tags:
                    existing_missing_tags[tag_type] = {}
                for tag, titles in missing_tags[tag_type].items():
                    if tag not in existing_missing_tags[tag_type]:
                        existing_missing_tags[tag_type][tag] = []
                    existing_missing_tags[tag_type][tag] = list(set(
                        existing_missing_tags[tag_type][tag] + titles
                    ))
            missing_tags = existing_missing_tags
    except FileNotFoundError:
        pass
    
    # Create a temporary file in the same directory
    directory = os.path.dirname(os.path.abspath(missing_tags_path))
    with tempfile.NamedTemporaryFile(mode='w', dir=directory, delete=False, suffix='.json') as temp_file:
        # Write the updated data to the temporary file
        json.dump(missing_tags, temp_file, indent=2, sort_keys=True)
        temp_file_path = temp_file.name
    
    # Atomically replace the target file with the temporary file
    # This is atomic on POSIX-compliant filesystems
    os.replace(temp_file_path, missing_tags_path)

def batch_label(feeds: Dict[str, SimpleFeed]) -> Dict[str, FeedLabels]:
    """
    Categorize a batch of feeds using GPT-4, returning FeedInfo objects
    
    Internally uses a dictionary with labels array approach with the LLM for better schema enforcement,
    but maintains the same API contract with a dictionary return value.
    """
    import json
    
    # Create zero-indexed ID mapping
    id_map = {} # new_id -> original_id
    new_id_map = {} # original_id -> new_id 
    for i, original_id in enumerate(feeds.keys()):
        id_map[i] = original_id
        new_id_map[original_id] = i
        
    # Convert feeds to text
    feed_texts = []
    for original_id, feed in feeds.items():
        new_id = new_id_map[original_id]
        feed_texts.append(feed_to_text(feed, new_id))
        
    # Join feeds and categories into final prompt
    categories_path = "categories.json"
    with open(categories_path) as f:
        cat_data = json.load(f)
    categories = json.dumps(cat_data["tags"], indent=2)
    
    final_prompt = prompt.replace("[[CATEGORIES]]", categories)
    final_prompt = final_prompt.replace("[[FEEDS]]", "\n\n".join(feed_texts))
    
    # Create the JSON schema for structured output (dictionary with labels array)
    json_schema = create_feed_label_schema(id_map, cat_data)
    
    # Query LLM with structured output schema
    response = query_llm_json(
        prompt=final_prompt, 
        model='openai/gpt-4o-mini',  # Using a model that supports structured output
        json_schema=json_schema
    )
    
    # Validate response format and content
    validate_response(response, id_map, cat_data)
    
    # Convert the response to the expected output format
    result = {}
    labels_array = response.get("labels", [])
    
    for feed_info in labels_array:
        # Get the numeric ID and map back to the original ID
        numeric_id = feed_info.pop("feed_id")  # Remove feed_id from the result
        original_id = id_map[numeric_id]
        result[original_id] = feed_info
        
    return result
