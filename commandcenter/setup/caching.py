from commandcenter.caching import memo
from commandcenter.config import CC_CACHE_DIR



def setup_caching():
    """Set the caching directory for memoized functions."""
    memo.set_cache_dir(CC_CACHE_DIR)