from commandcenter.caching import memo
from commandcenter.config import CC_CACHE_DIR



def setup_caching():
    """Set the caching directory for memoized functions."""
    if CC_CACHE_DIR:
        memo.set_cache_dir(CC_CACHE_DIR)