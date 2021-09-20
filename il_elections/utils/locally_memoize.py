"""Alloes memoizing of functions return values into local files."""
import hashlib
import functools as ft
import logging
import pathlib
import pickle
import sys
from typing import Optional

logger = logging.getLogger('locally_memoize')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

_DEFAULT_CACHE_LOCATION_PATH = pathlib.Path('.locally_memoize')
_CODE_HASH_FILENAME = 'code.hash'


def _lazily_run_function(func, cache_folder: pathlib.Path,
                         cache_only: bool):
    """Returns a version of `func` that uses the internal cache."""
    def _inner_func(*args, **kwargs):
        args_bytes = pickle.dumps((args, kwargs))
        h = hashlib.sha256()
        h.update(args_bytes)
        args_hash = h.hexdigest()

        cache_file = cache_folder / args_hash
        if cache_file.exists():
            with open(cache_file, 'rb') as f:
                result = pickle.load(f)
        else:
            if cache_only:
                raise ValueError(
                    'Running in strict cache-only mode but can\'t find a cached result '
                    f'for {func.__name__} with args={args} and kwargs={kwargs}.')
            result = func(*args, **kwargs)
            with open(cache_file, 'wb') as f:
                pickle.dump(result, f)
        return result
    return _inner_func


def _locally_memoize(func,
                     cache_location_path: Optional[pathlib.Path],
                     clear_cache_on_code_change: bool,
                     cache_only: bool):
    if clear_cache_on_code_change:
        raise NotImplementedError('`clear_cache_on_code_change` is currently not supported.')
    cache_location_path = cache_location_path or _DEFAULT_CACHE_LOCATION_PATH
    function_cache_path = cache_location_path / func.__name__
    function_cache_path.mkdir(parents=True, exist_ok=True)
    code_hash_path = function_cache_path / _CODE_HASH_FILENAME
    logger.info('Locally memoizing output of `%s` into \'%s\'.', func.__name__, cache_location_path)

    if code_hash_path.exists():
        with open(code_hash_path, encoding='utf8') as f:
            stored_code_hash = f.read()
    else:
        stored_code_hash = None
    code_hash = str(hash(func.__code__))

    if stored_code_hash != code_hash:
        # if clear_cache_on_code_change:
        #     logger.info(f'`{func}` code was changed. clearing local cache.')
        #     for p in function_cache_path.glob('*'):
        #         p.unlink()
        with open(code_hash_path, 'wt', encoding='utf8') as out_file:
            out_file.write(code_hash)

    return _lazily_run_function(func, function_cache_path, cache_only)


def locally_memoize(cache_location_path: Optional[pathlib.Path] = None,
                    clear_cache_on_code_change: bool = False,
                    cache_only: bool = False):
    """Decorates a function and memoizes its output to a local folder.

    Notice: Uses pickle to store the results and hash the arguments.

    Args:
        cache_location_path: Where to store the cached results.
        clear_cache_on_code_change: Whether to clear the cache when we
            detect a code change. NOTICE - currently not supported!
        cache_only: Whether to only serve from cache. Prevents the actual code
            from running. Useful if the function is heavy or expensive.
    """
    if clear_cache_on_code_change:
        raise NotImplementedError('`clear_cache_on_code_change` is not supported.')

    return ft.partial(_locally_memoize, cache_location_path=cache_location_path,
                      clear_cache_on_code_change=clear_cache_on_code_change,
                      cache_only=cache_only)
