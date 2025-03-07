from feed_types import Feed
from typing import List, Dict
import chromadb
import ollama
from pathlib import Path
from cached_map import cached_map_batched

MODEL_NAME = 'nomic-embed-text'

def _batch_embed(feeds_dict: Dict[str, Feed]) -> Dict[str, List[float]]:
    """Batch process feeds to get embeddings using ollama API"""
    # Combine relevant fields into text for embedding
    results = {}
    for feed_id, feed in feeds_dict.items():
        text_parts = [
            feed['title'],
            feed.get('summary', ''),
            feed.get('details', ''),
            ' '.join(feed.get('tags', []))
        ]
        text = '\n'.join(part for part in text_parts if part)
        # print("<emb>")
        # print(text)
        # print("</emb>")
        response = ollama.embed(model=MODEL_NAME, input=text)
        results[feed_id] = response['embeddings']  # Already in correct format
    return results

def build_embeddings(feeds: List[Feed]) -> chromadb.Client:
    """Build and return a ChromaDB client containing feed embeddings"""
    client = chromadb.Client()
    
    # Create collection
    collection = client.create_collection(name="feeds")
    
    # Convert list to dict for cached_map
    feeds_dict = {feed['id']: feed for feed in feeds}
    
    # Get embeddings with caching
    print("EMBEDDING FEEDS...")
    embeddings_dict = cached_map_batched(
        inputs=feeds_dict,
        map_fn=_batch_embed,
        batch_size=10,  # Process 10 feeds at a time
        cache_file=Path("caches/embed_cache.sqlite"),
        version="v2"
    )
    
    # Prepare batch data for ChromaDB
    ids = []
    embeddings = []
    documents = []
    metadatas = []
    
    for feed_id, embedding in embeddings_dict.items():
        feed = feeds_dict[feed_id]
        ids.append(feed_id)
        embeddings += embedding
        documents.append(feed.get("summary", ""))
        metadatas.append({
            "title": feed["title"],
            "kind": feed["kind"],
            "language": feed.get("language", ""),
            "tags": ",".join(feed.get("tags", [])),
        })
    
    # Batch add to ChromaDB
    if len(embeddings) > 0:
        collection.add(
            ids=ids,
            embeddings=embeddings, 
            documents=documents,
            metadatas=metadatas
        )
    
    return client