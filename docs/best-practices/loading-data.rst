============
Loading Data
============

In Hamilton, data loaders are just the same as other functions in the DAG. They take in configuration parameters, and
output datasets in the desired form. Following up on the marketing spend dataset, you might write a data loader that
reads a dataframe saved in csv format on s3 like this:

.. code-block:: python

    import boto3
    import urllib
    import pandas as pd

    from hamilton.function_modifiers import extract_columns

    client = boto3.client("s3")

    @extract_columns('col1', 'col2', 'col3', ...)
    def marketing_spend(marketing_spend_data_path: str) -> pd.DataFrame:
        """Loads marketing spend from specified path on s3
        """
        if not marketing_spend_data_path.startswith("s3://"):
            raise ValueError(f"Invalid s3 URI {marketing_spend_data_path}")
        return pd.read_csv(
            marketing_spend_data_path,
            storage_options = {...}) # See https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html#pandas-read-csv for more info

Loading data is as easy as that! Run your driver with ``marketing_spend_data_path`` as a parameter, and you're good to
go.  However, there are a few considerations you might have prior to productionalizing this dataflow...

Plugging in new Data Sources
----------------------------

An advantage of Hamilton is that it allows for rapid plug-and play for various components of your pipeline. This is
particularly important for data loading, where you might want to load your data from different sources depending on some
context. For instance -- if you're running your pipeline in production, you may want to use the production data sources.
If you're running it in QA, you might want to use the staging data sources. Or, if you're running it locally, you might
want to use abbreviated, in-memory data sources for testing. While Hamilton is not opinionated on exactly _how_ you make
this switch, it presents a variety of tooling that can make it more manageable. Some options. To demonstrate some
techniques, let's continue on the example of loading marketing spend...

Modules as Interfaces
=====================

Say you have multiple data-loading nodes in your DAG. One strategy is to put them all in a single module. That way, if you want to load them up from different sources, you can simply switch the module your driver utilizes. Taking the example from above, you might have the following modules:

.. code-block:: python

    @extract_columns('col1', 'col2', 'col3', ...)
    def marketing_spend(marketing_spend_data_path: str) -> pd.DataFrame:
        """Loads marketing spend from specified path on s3
        """
        if not marketing_spend_data_path.startswith("s3://"):
            raise ValueError(f"Invalid s3 URI {marketing_spend_data_path}")
        return pd.read_csv(
            marketing_spend_data_path,
            storage_options = {...}) # See https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html#pandas-read-csv for more info

.. code-block:: python

    @extract_columns('col1', 'col2', 'col3', ...)
    def marketing_spend(marketing_spend_data_path: str) -> pd.DataFrame:
        """Loads marketing spend from specified path on s3
        """
        if not marketing_spend_data_path.endswith("csv"):
            raise ValueError(f"Invalid local data loading target {marketing_spend_data_path}")
        if not os.path.exists(marketing_spend_data_path):
            raise ValueError(f"Path does not exists")
        return pd.read_csv(marketing_spend_data_path)

Then, in your driver, you can choose between which module you want to use:

.. code-block:: python

    local_data_driver = Driver(config, local_data_loaders, ...)
    prod_data_driver = Driver(config, prod_data_loaders, ...)

Using the Config to Decide Sources
==================================

Note that we can utilize the config to determine where the data comes from as well. By using ``config.when`` you can
arrive at the same effect as above, while making it entirely config driven. If you combine the two functions into the
same module with `@config.when` it will look as follows:

.. code-block:: python

    @config.when(data_source='local')
    @extract_columns('col1', 'col2', 'col3', ...)
    def marketing_spend__local(marketing_spend_data_path: str) -> pd.DataFrame:
        ...

    @config.when(data_source='prod')
    @extract_columns('col1', 'col2', 'col3', ...)
    def marketing_spend__prod(marketing_spend_data_path: str) -> pd.DataFrame:
        ...

Then you can invoke your driver but set the config differently:

.. code-block:: python

    driver = Driver(
        {'data_source' : 'prod', 'marketing_spend_data_path' : 's3://...'},
        data_loaders, ...)

Note that there are a variety of other ways you can organize your code -- at this point its entirely use-case dependent.
Hamilton is a language for declaring dataflows that's applicable towards a multitude of use-cases. It's not going to
dictate how to write your functions or where you put them.

Caching Data Load for Rapid Iteration
-------------------------------------

Hamilton does not yet have caching built into the framework. That said, we are
`working on <https://github.com/stitchfix/hamilton/issues/17>`_ introducing this as a feature and deciding on the scope
of the implementation. For now, however, the following decorator/toolset can allow you to cache data loaders based off
of the parameters as well as the code itself. Use this at your own risk though, as it's not a fully functioning cache.
This can be used as follows:

In data\_loaders.py:

.. code-block:: python

    from caching import function_cache

    @extract_columns('col1', 'col2', 'col3', ...)
    @function_cache
    def marketing_spend(marketing_spend_data_path: str) -> pd.DataFrame:
        ...

In caching.py

.. code-block:: python

    import hashlib
    import inspect
    import itertools
    import logging
    import os
    import pickle
    import shutil
    from pathlib import Path
    from typing import Callable, Any, Dict, Collection, Tuple, Set, List

    import click

    from hamilton import log_setup

    logger = logging.getLogger(__name__)


    class CachingException(Exception):
        pass


    def get_key(fn_to_cache: Callable, args: Collection[collections.Hashable], kwargs: Dict[str, collections.Hashable]) -> Tuple[str, str]:
        """Deterministic hash of function parameters, as well as the function name. All the arguments are assumed to be hashable.
        :param fn_to_cache: Function that we are caching.
        :param args: list-based arguments of the function.
        :param kwargs: keyword arguments of the function.
        :return: A tuple of namespace, hash. Namespace is to ensure we can clear a subset of the cache.
        """
        fn_name = fn_to_cache.__name__
        hasher = hashlib.sha256()
        for key in itertools.chain([fn_name, fn_to_cache.__code__.co_code], args, sorted(kwargs.items())):
            hasher.update(pickle.dumps(key))
        return fn_name, hasher.hexdigest()


    class LocalCache:
        """Class to cache on disk at a certain directory"""

        def __init__(self, cache_directory: str):
            """Initializes the caching object.
            :param cache_directory: Directory under which we want our cache to be held.
            """
            self.cache_directory = cache_directory
            if not os.path.exists(self.cache_directory):
                os.makedirs(self.cache_directory, exist_ok=True)

        def _get_cache_location(self, key: Tuple[str, str]) -> str:
            namespace, cache_dir = key
            return os.path.join(self._get_namespace_location(namespace), cache_dir)

        def _get_namespace_location(self, namespace: str) -> str:
            return os.path.join(self.cache_directory, namespace)

        def save(self, key: Tuple[str, str], to_save: Any):
            """Saves an object with a certain key.
            :param key: Unique key with which to save the object.
            :param to_save:
            """
            cache_location = self._get_cache_location(key)
            parent_dir = os.path.dirname(cache_location)
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir, exist_ok=True)
            with open(self._get_cache_location(key), 'wb') as f:
                pickle.dump(to_save, f)

        def has(self, key: Tuple[str, str]) -> bool:
            """Whether the cahce has the object stored.
            :param key: Unique key to check.
            :return: Boolean, whether it has an object saved under that key or not.
            """
            return os.path.exists(self._get_cache_location(key))

        def load(self, key: Tuple[str, str]):
            """Loads an object previously saved in cache. We assume that the user has already called has()
            :param key: Unique key associated with an object to load
            :return: The loaded object
            """
            with open(self._get_cache_location(key), 'rb') as f:
                return pickle.load(f)

        def clear(self, namespace: str):
            """Clears the entire cache under a certain namespace.
            :param namespace: Namespace to clear (equivalent to the first result of get_key())
            """
            shutil.rmtree(self._get_namespace_location(namespace), ignore_errors=True)

        def clear_all(self):
            """Clears all the caches (under every namespace)"""
            shutil.rmtree(self.cache_directory, ignore_errors=True)

        def get_namespaces(self) -> Set[str]:
            return {item for item in os.listdir(self.cache_directory)}


    class function_cache:
        def __init__(self, cacher: LocalCache):
            """Decorator to cache a function locally, on disk. This is a quick way to unblock us for development
            before we decide on the best way to implement caching in the framework. Lowercase name as it is an annotation.

            :param cacher: Caching object to use (for now its a localcache)
            :return: Decorated function
            """
            self.cacher = cacher

        def __call__(self, fn: Callable) -> Callable:
            signature = inspect.signature(fn)
            unhashable_params = []
            for param_name, param in signature.parameters.items():
                if not issubclass(param.annotation, collections.Hashable):
                    unhashable_params.append((param_name, param))
            if len(unhashable_params) > 0:
                raise CachingException(f'The following parameters are not hashable: {unhashable_params}')

            @functools.wraps(fn)
            def wrapped(*args, **kwargs):
                # TODO -- log that I'm running a fn from cache
                key = get_key(fn, args, kwargs)
                if self.cacher.has(key):
                    return self.cacher.load(key)
                results = fn(*args, **kwargs)
                self.cacher.save(key, results)
                return results

            return wrapped


    DEFAULT_CACHE_PATH = os.path.join(Path.home(), 'my_cache')
    # global variables for convenient use
    # We should do some thinking about how to expose this. This will work for now, but
    # caching should be configurable, and we need to work that out.
    local_cache = LocalCache(cache_directory=DEFAULT_CACHE_PATH)
    cache = function_cache(cacher=local_cache)


    # functions to help manage the cache!
    @click.group()
    @click.pass_context
    @click.option('--cache-dir', type=click.Path(), help=f'cache path to work with. Will default to {DEFAULT_CACHE_PATH}', default=DEFAULT_CACHE_PATH)
    def cli(ctx, cache_dir: str):
        ctx.obj = LocalCache(cache_directory=cache_dir)
        pass


    @cli.command()
    @click.option('--all', 'all_funcs', is_flag=True)
    @click.option('--funcs', type=str, multiple=True, help='functions to clear')
    @click.pass_obj
    def clear(ctx: LocalCache, all_funcs: bool, funcs: List[str]):
        """Clears functions from the cache.
        :param ctx: Context, used to share state between commands
        :param all_funcs: Whether or not to erase all items in the cache
        :param funcs: Which functions in the cache to erase
        """
        if all_funcs:
            if len(funcs) > 0:
                raise ValueError('Can either remove *all* cached functions or a selection, not both')
            funcs = ctx.get_namespaces()
        logger.info(f'Removing cache for fns {" ".join(funcs)}')
        for func in funcs:
            if func not in ctx.get_namespaces():
                logger.error(f'Not clearing function: {func} as it is not currently cached.')
            else:
                ctx.clear(func)


    @cli.command()
    @click.pass_obj
    def list_cache(ctx: LocalCache):
        """Prints all items in the cache out to the terminal
        :param ctx:  Context, used to share state between commands
        """

        print('\n'.join(ctx.get_namespaces()))


    if __name__ == '__main__':
        cli()
