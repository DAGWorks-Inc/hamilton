import pathlib
import sqlite3
import threading
from typing import List, Optional

from hamilton.caching.stores.base import MetadataStore


class SQLiteMetadataStore(MetadataStore):
    def __init__(
        self,
        path: str,
        connection_kwargs: Optional[dict] = None,
    ) -> None:
        self._directory = pathlib.Path(path).resolve()
        self._directory.mkdir(parents=True, exist_ok=True)
        self._path = self._directory.joinpath("metadata_store").with_suffix(".db")
        self.connection_kwargs: dict = connection_kwargs if connection_kwargs else {}

        self._thread_local = threading.local()

    def _get_connection(self):
        if not hasattr(self._thread_local, "connection"):
            self._thread_local.connection = sqlite3.connect(
                str(self._path), check_same_thread=False, **self.connection_kwargs
            )
        return self._thread_local.connection

    def _close_connection(self):
        if hasattr(self._thread_local, "connection"):
            self._thread_local.connection.close()
            del self._thread_local.connection

    @property
    def connection(self):
        return self._get_connection()

    def __del__(self):
        """Close the SQLite connection when the object is deleted"""
        self._close_connection()

    def _create_tables_if_not_exists(self):
        """Create the tables necessary for the cache:

        run_ids: queue of run_ids, ordered by start time.
        history: queue of executed node; allows to query "latest" execution of a node
        cache_metadata: information to determine if a node needs to be computed or not

        In the table ``cache_metadata``, the ``cache_key`` is unique whereas
        ``history`` allows duplicate.
        """
        cur = self.connection.cursor()

        cur.execute(
            """\
            CREATE TABLE IF NOT EXISTS run_ids (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """\
            CREATE TABLE IF NOT EXISTS history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cache_key TEXT,
                run_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (cache_key) REFERENCES cache_metadata(cache_key)
            );
            """
        )
        cur.execute(
            """\
            CREATE TABLE IF NOT EXISTS cache_metadata (
                cache_key TEXT PRIMARY KEY,
                node_name TEXT NOT NULL,
                code_version TEXT NOT NULL,
                data_version TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (cache_key) REFERENCES history(cache_key)
            );
            """
        )
        self.connection.commit()

    def initialize(self, run_id) -> None:
        """Call initialize when starting a run. This will create database tables
        if necessary.
        """
        self._create_tables_if_not_exists()
        cur = self.connection.cursor()
        cur.execute("INSERT INTO run_ids (run_id) VALUES (?)", (run_id,))
        self.connection.commit()

    def __len__(self):
        """Number of entries in cache_metadata"""
        cur = self.connection.cursor()
        cur.execute("SELECT COUNT(*) FROM cache_metadata")
        return cur.fetchone()[0]

    def set(
        self,
        *,
        cache_key: str,
        node_name: str,
        code_version: str,
        data_version: str,
        run_id: str,
    ) -> None:
        cur = self.connection.cursor()

        cur.execute("INSERT INTO history (cache_key, run_id) VALUES (?, ?)", (cache_key, run_id))
        cur.execute(
            """\
            INSERT OR IGNORE INTO cache_metadata (
                cache_key, node_name, code_version, data_version
            ) VALUES (?, ?, ?, ?)
            """,
            (cache_key, node_name, code_version, data_version),
        )

        self.connection.commit()

    def get(self, cache_key: str) -> Optional[str]:
        cur = self.connection.cursor()
        cur.execute(
            """\
            SELECT data_version
            FROM cache_metadata
            WHERE cache_key = ?
            """,
            (cache_key,),
        )
        result = cur.fetchone()

        if result is None:
            data_version = None
        else:
            data_version = result[0]

        return data_version

    def delete(self, cache_key: str) -> None:
        """Delete metadata associated with ``cache_key``."""
        cur = self.connection.cursor()
        cur.execute("DELETE FROM cache_metadata WHERE cache_key = ?", (cache_key,))
        self.connection.commit()

    def delete_all(self):
        """Delete all existing tables from the database"""
        cur = self.connection.cursor()

        for table_name in ["run_ids", "history", "cache_metadata"]:
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")

        self.connection.commit()

    def exists(self, cache_key: str) -> bool:
        """boolean check if a ``data_version`` is found for ``cache_key``
        If True, ``.get()`` should successfully retrieve the ``data_version``.
        """
        cur = self.connection.cursor()
        cur.execute("SELECT cache_key FROM cache_metadata WHERE cache_key = ?", (cache_key,))
        result = cur.fetchone()

        return result is not None

    def get_run_ids(self) -> List[str]:
        cur = self.connection.cursor()
        cur.execute("SELECT run_id FROM history ORDER BY id")
        result = cur.fetchall()

        if result is None:
            raise IndexError("No `run_id` found. Table `history` is empty.")

        return result[0]

    def get_run(self, run_id: str) -> List[dict]:
        """Return all the metadata associated with a run."""
        cur = self.connection.cursor()
        cur.execute(
            """\
            SELECT
                cache_metadata.node_name,
                cache_metadata.code_version,
                cache_metadata.data_version
            FROM (SELECT * FROM history WHERE history.run_id = ?) AS run_history
            JOIN cache_metadata ON run_history.cache_key = cache_metadata.cache_key
            """,
            (run_id,),
        )
        results = cur.fetchall()

        if results is None:
            raise IndexError(f"`run_id` not found in table `history`: {run_id}")

        return [
            dict(node_name=node_name, code_version=code_version, data_version=data_version)
            for node_name, code_version, data_version in results
        ]
