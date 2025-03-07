"""
CachedMap provides a way to map inputs to outputs with disk-based caching.
Results are cached in SQLite with versioning support and optional concurrent processing.
"""

import json
import sqlite3
import threading
import time
import random
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Any, Optional, Callable, TypeVar, List, Set, Tuple
from tqdm.auto import tqdm

InputT = TypeVar('InputT')
OutputT = TypeVar('OutputT')

def _serialize_for_cache(obj: Any) -> str:
    """Deterministically serialize an object to JSON for cache key generation."""
    if isinstance(obj, dict):
        # Sort dictionary keys for deterministic serialization
        return json.dumps(obj, sort_keys=True)
    return json.dumps(obj)

class CachedMap:
    """Maps inputs to outputs with caching to avoid recomputation."""
    
    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize a CachedMap with optional persistence.
        
        Args:
            db_path: Optional path to SQLite database. If None, uses in-memory cache.
        """
        self.db_path = db_path
        self.is_memory_db = db_path is None
        self._thread_local = threading.local()
        self._conn_lock = threading.RLock()  # Reentrant lock for connection management
        
        # Create initial schema
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    input_id TEXT,
                    input_key TEXT,
                    output TEXT,
                    version TEXT,
                    last_used INTEGER,
                    PRIMARY KEY (input_id, input_key, version)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_version ON cache(version)")
    
    def _get_connection(self):
        """Get a thread-local database connection with retry logic."""
        if hasattr(self._thread_local, 'conn'):
            return self._thread_local.conn
            
        # Create a new connection for this thread
        if self.is_memory_db:
            # For in-memory databases, we must share a single connection
            with self._conn_lock:
                if not hasattr(self, '_shared_memory_conn'):
                    self._shared_memory_conn = sqlite3.connect(':memory:', check_same_thread=False)
                    # Initialize schema for in-memory DB
                    with self._shared_memory_conn:
                        self._shared_memory_conn.execute("""
                            CREATE TABLE IF NOT EXISTS cache (
                                input_id TEXT,
                                input_key TEXT,
                                output TEXT,
                                version TEXT,
                                last_used INTEGER,
                                PRIMARY KEY (input_id, input_key, version)
                            )
                        """)
                        self._shared_memory_conn.execute("CREATE INDEX IF NOT EXISTS idx_version ON cache(version)")
                
                self._thread_local.conn = self._shared_memory_conn
        else:
            # File-based DB: create a new connection with retry logic
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    # Set timeout to wait for locks to clear
                    conn = sqlite3.connect(
                        self.db_path, 
                        timeout=20.0,  # Longer timeout to wait for locks
                        isolation_level=None  # Use explicit transaction control
                    )
                    conn.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging for better concurrency
                    conn.execute("PRAGMA synchronous=NORMAL")  # Reasonable compromise for safety/speed
                    self._thread_local.conn = conn
                    break
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e) and attempt < max_retries - 1:
                        # Random backoff to avoid lock contention
                        delay = 0.1 * (2 ** attempt) + random.random()
                        time.sleep(delay)
                        continue
                    raise  # Re-raise if all retries fail or it's a different error
        
        return self._thread_local.conn
    
    def _execute_with_retry(self, operation, *args, max_retries=5):
        """Execute a database operation with retry logic for locked database."""
        conn = self._get_connection()
        
        for attempt in range(max_retries):
            try:
                return operation(conn, *args)
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    # Random exponential backoff
                    delay = 0.1 * (2 ** attempt) + random.random()
                    time.sleep(delay)
                    continue
                raise  # Re-raise if all retries fail or it's a different error
    
    def _get_cached(self, input_id: str, input_obj: Any, version: str) -> Optional[Any]:
        """Retrieve cached output if it exists."""
        cache_key = _serialize_for_cache(input_obj)
        
        def _get_operation(conn, input_id, cache_key, version):
            try:
                # Use explicit transaction
                conn.execute("BEGIN IMMEDIATE")
                
                cursor = conn.execute(
                    "SELECT output FROM cache WHERE input_id = ? AND input_key = ? AND version = ?",
                    (input_id, cache_key, version)
                )
                row = cursor.fetchone()
                
                if row:
                    # Update last_used timestamp
                    conn.execute(
                        "UPDATE cache SET last_used = ? WHERE input_id = ? AND input_key = ? AND version = ?",
                        (int(time.time()), input_id, cache_key, version)
                    )
                
                conn.execute("COMMIT")
                return row[0] if row else None
            except Exception as e:
                conn.execute("ROLLBACK")
                raise e
        
        try:
            result = self._execute_with_retry(_get_operation, input_id, cache_key, version)
            return json.loads(result) if result else None
        except Exception as e:
            print(f"Error retrieving from cache: {e}")
            print("Full stack trace:")
            traceback.print_exc()
            return None

    def _cache_output(self, input_id: str, input_obj: Any, output: Any, version: str):
        """Store output in cache."""
        cache_key = _serialize_for_cache(input_obj)
        output_json = json.dumps(output)
        
        def _cache_operation(conn, input_id, cache_key, output_json, version):
            try:
                conn.execute("BEGIN IMMEDIATE")
                
                conn.execute(
                    """INSERT OR REPLACE INTO cache 
                       (input_id, input_key, output, version, last_used)
                       VALUES (?, ?, ?, ?, ?)""",
                    (input_id, cache_key, output_json, version, int(time.time()))
                )
                
                conn.execute("COMMIT")
            except Exception as e:
                conn.execute("ROLLBACK")
                raise e
        
        try:
            self._execute_with_retry(_cache_operation, input_id, cache_key, output_json, version)
        except Exception as e:
            print(f"Warning: Could not cache output: {e}")

    def _process_batch(self, 
                     batch_inputs: Dict[str, InputT],
                     map_fn: Callable[[Dict[str, InputT]], Dict[str, OutputT]],
                     version: str) -> Dict[str, OutputT]:
        """Process a batch of inputs (already known to be uncached)."""
        try:
            # Process the entire batch
            batch_results = map_fn(batch_inputs)
            
            # Cache each result individually
            for id, result in batch_results.items():
                self._cache_output(id, batch_inputs[id], result, version)
            
            return batch_results
        except Exception:
            # This prints the stack trace showing exactly where in the user function the error occurred
            print("\n----- ORIGINAL EXCEPTION IN MAP FUNCTION -----")
            traceback.print_exc()
            print("----- END OF ORIGINAL EXCEPTION -----\n")
            # Just raise without catching - this preserves the original stack trace completely
            raise

    def cleanup(self, current_input_ids: Set[str], current_version: str):
        """
        Remove cache entries for:
        - Inputs no longer in use
        - Entries from old versions
        """
        def _cleanup_operation(conn, current_input_ids, current_version):
            try:
                conn.execute("BEGIN IMMEDIATE")
                
                # Delete entries for old versions
                conn.execute(
                    "DELETE FROM cache WHERE version != ?",
                    (current_version,)
                )
                
                # Delete entries for inputs no longer in use
                if current_input_ids:
                    placeholders = ','.join('?' * len(current_input_ids))
                    conn.execute(
                        f"DELETE FROM cache WHERE input_id NOT IN ({placeholders}) AND version = ?",
                        tuple(list(current_input_ids) + [current_version])
                    )
                
                conn.execute("COMMIT")
            except Exception as e:
                conn.execute("ROLLBACK")
                raise e
        
        try:
            self._execute_with_retry(_cleanup_operation, current_input_ids, current_version)
        except Exception as e:
            print(f"Warning: Cache cleanup failed: {e}")

    def clear_cache(self):
        """Clear all cached values."""
        def _clear_operation(conn):
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("DELETE FROM cache")
                conn.execute("COMMIT")
            except Exception as e:
                conn.execute("ROLLBACK")
                raise e
                
        try:
            self._execute_with_retry(_clear_operation)
        except Exception as e:
            print(f"Warning: Failed to clear cache: {e}")

    def map(self,
            inputs: Dict[str, InputT],
            map_fn: Callable[[Dict[str, InputT]], Dict[str, OutputT]],
            version: str = "v0",
            num_threads: int = 1,
            batch_size: int = 1) -> Dict[str, OutputT]:
        """
        Map inputs to outputs using the provided function, with caching.
        
        Args:
            inputs: Dictionary of {id: input_object}
            map_fn: Function that takes dict of {id: input} and returns dict of {id: output}
            version: Cache version string
            num_threads: Number of threads to use (1 for sequential processing)
            batch_size: Size of batches to process at once
            
        Returns:
            Dictionary of {id: output_object}
        """
        # First check cache for all inputs and build list of uncached items
        results: Dict[str, OutputT] = {}
        uncached_inputs: Dict[str, InputT] = {}
        
        for input_id, input_obj in inputs.items():
            cached_result = self._get_cached(input_id, input_obj, version)
            if cached_result is not None:
                results[input_id] = cached_result
            else:
                uncached_inputs[input_id] = input_obj
        
        # If everything was cached, just return results
        if not uncached_inputs:
            return results
        
        # If only one thread requested, or very few inputs, just process sequentially
        if num_threads <= 1 or len(uncached_inputs) <= 1:
            batch_results = self._process_batch(uncached_inputs, map_fn, version)
            results.update(batch_results)
            return results
            
        # Create batches for parallel processing
        batches: List[Dict[str, InputT]] = []
        current_batch: Dict[str, InputT] = {}
        
        for input_id, input_obj in uncached_inputs.items():
            current_batch[input_id] = input_obj
            if len(current_batch) >= batch_size:
                batches.append(current_batch)
                current_batch = {}
        
        if current_batch:  # Add any remaining items
            batches.append(current_batch)
        
        # Adjust number of threads based on number of batches
        effective_threads = min(num_threads, len(batches))
        
        # Process batches in parallel
        with ThreadPoolExecutor(max_workers=effective_threads) as executor:
            futures = {
                executor.submit(self._process_batch, batch, map_fn, version): i 
                for i, batch in enumerate(batches)
            }
            
            # Process results as they complete with a progress bar
            with tqdm(total=len(batches), desc="Processing batches") as pbar:
                for future in as_completed(futures):
                    try:
                        batch_results = future.result()
                        results.update(batch_results)
                    except Exception as e:
                        print(f"Error in batch {futures[future]}: {e}")
                    finally:
                        pbar.update(1)
        
        # Clean up old cache entries
        try:
            self.cleanup(set(inputs.keys()), version)
        except Exception as e:
            print(f"Warning: Failed to clean up cache: {e}")
            
        return results
    
    def close(self):
        """Close database connections."""
        # Close thread-local connection if it exists
        if hasattr(self._thread_local, 'conn') and not self.is_memory_db:
            try:
                self._thread_local.conn.close()
                delattr(self._thread_local, 'conn')
            except Exception:
                pass
        
        # Close shared memory connection if it exists
        if hasattr(self, '_shared_memory_conn'):
            try:
                self._shared_memory_conn.close()
                delattr(self, '_shared_memory_conn')
            except Exception:
                pass
    
    def __del__(self):
        """Clean up resources when object is deleted."""
        self.close()


def cached_map(
    inputs: Dict[str, InputT],
    map_fn: Callable[[InputT], OutputT],
    cache_file: Optional[Path] = None,
    version: str = "v0",
    num_threads: int = 1
) -> Dict[str, OutputT]:
    """
    Simple functional interface to CachedMap for single-item processing.
    
    Args:
        inputs: Dictionary of {id: input_object}
        map_fn: Function to map a single input object to output
        cache_file: Optional path to SQLite cache file
        version: Cache version string
        num_threads: Number of threads to use (1 for sequential processing)
        
    Returns:
        Dictionary of {id: output_object}
    """
    # Wrap single-item function in batch interface
    def batch_fn(items: Dict[str, InputT]) -> Dict[str, OutputT]:
        return {id: map_fn(item) for id, item in items.items()}
    
    mapper = CachedMap(cache_file)
    try:
        return mapper.map(inputs, batch_fn, version, num_threads, batch_size=1)
    finally:
        mapper.close()

def cached_map_batched(
    inputs: Dict[str, InputT],
    map_fn: Callable[[Dict[str, InputT]], Dict[str, OutputT]],
    batch_size: int,
    cache_file: Optional[Path] = None,
    version: str = "v0",
    num_threads: int = 1
) -> Dict[str, OutputT]:
    """
    Simple functional interface to CachedMap for batch processing.
    
    Args:
        inputs: Dictionary of {id: input_object}
        map_fn: Function that takes dict of inputs and returns dict of outputs
        batch_size: Size of batches to process at once
        cache_file: Optional path to SQLite cache file
        version: Cache version string
        num_threads: Number of threads to use (1 for sequential processing)
        
    Returns:
        Dictionary of {id: output_object}
    """
    mapper = CachedMap(cache_file)
    try:
        return mapper.map(inputs, map_fn, version, num_threads, batch_size=batch_size)
    finally:
        mapper.close()