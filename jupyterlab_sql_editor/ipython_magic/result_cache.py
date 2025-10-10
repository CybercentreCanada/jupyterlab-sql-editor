import sys
from collections import OrderedDict
from typing import Any

import pandas as pd


class ResultCache:
    def __init__(self, max_bytes=0.25 * 1024**3):  # default 256 MB
        """
        max_bytes: maximum memory to use for results (other metadata ignored)
        """
        self.max_bytes = max_bytes
        self._cache = OrderedDict()
        self._total_bytes = 0

    def _sizeof(self, obj) -> int:
        """
        Return approximate memory usage of object in bytes.
        Supports Pandas DataFrame, list, dict, and basic primitives.
        """
        if isinstance(obj, pd.DataFrame):
            return obj.memory_usage(deep=True).sum()
        elif isinstance(obj, list):
            return sys.getsizeof(obj) + sum(sys.getsizeof(i) for i in obj)
        elif isinstance(obj, dict):
            return sys.getsizeof(obj) + sum(sys.getsizeof(k) + sys.getsizeof(v) for k, v in obj.items())
        else:
            return sys.getsizeof(obj)

    def put(self, result_id: str, results: Any, **metadata):
        """
        Store query results with optional metadata. Keeps latest result even if over max_bytes.
        """
        size = self._sizeof(results)

        # Remove old copy if exists
        if result_id in self._cache:
            old_item = self._cache.pop(result_id)
            self._total_bytes -= old_item["size"]

        # Store new item
        self._cache[result_id] = {"results": results, "size": size, "metadata": metadata}
        self._total_bytes += size
        self._cache.move_to_end(result_id)

        # Evict oldest items while keeping the latest
        keys = list(self._cache.keys())
        while self._total_bytes > self.max_bytes and len(keys) > 1:
            oldest = keys.pop(0)
            if oldest == result_id:
                continue  # never evict the newest
            old_item = self._cache.pop(oldest)
            self._total_bytes -= old_item["size"]

    def get(self, result_id: str):
        """Return (results, metadata) or None if not cached."""
        item = self._cache.get(result_id)
        if item:
            self._cache.move_to_end(result_id)
            return item["results"], item["metadata"]
        return None

    def latest(self):
        """Return the most recently cached (result_id, results, metadata) or None if empty."""
        if not self._cache:
            return "", None, {}
        result_id, item = next(reversed(self._cache.items()))
        return result_id, item["results"], item["metadata"]

    def clear(self):
        """Clear all cached results."""
        self._cache.clear()
        self._total_bytes = 0
