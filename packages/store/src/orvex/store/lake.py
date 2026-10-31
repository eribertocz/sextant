"""DuckLake lake with a local DuckDB-file catalog (contract `store` v1, P1-01).

Writes run inside DuckDB in explicit transactions; each read is pinned to a snapshot by adding
`AT (VERSION => n)` to every lake table it references (rewritten with DuckDB's own parser) and is
read once, as an Arrow record-batch stream (ADR-0025). No row passes through Python.
"""

from __future__ import annotations

import collections
import contextlib
import datetime as dt
import errno
import fnmatch
import itertools
import json
import math
import os
import re
import select
import shutil
import signal
import socket
import sqlite3
import tempfile
import threading
import time
import uuid
import weakref
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import Any, NamedTuple

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from orvex.contracts.store.v1 import (
    AzureStorage,
    CommitResult,
    FileSourceAttachment,
    GcsStorage,
    LakeLocation,
    LocalLakeLocation,
    MaintenanceReport,
    ObjectStorage,
    Parameter,
    PostgresLakeLocation,
    PostgresSourceAttachment,
    ReadRequest,
    S3Storage,
    Snapshot,
    SourceAttachment,
    SqliteLakeLocation,
    SqliteSourceAttachment,
    StatementRequest,
)
from orvex.store.errors import StoreError, register_secret

LAKE = "lake"
MAX_TIMEOUT_MS = 3_600_000
METADATA_PREFIX = "__ducklake_metadata"
METADATA = f"{METADATA_PREFIX}_{LAKE}"
# Any password key (password=, sslpassword=, ?password=) and any URI form (user:pw@host) is
# refused: the password may only come from `password_env`.
_INLINE_SECRET = re.compile(r"password|://", re.IGNORECASE)
_SCHEMA_NAME = re.compile(r"[a-z_][a-z0-9_]*")
# Hosts that are the local machine; a host starting with `/` (or `@`, Linux's abstract namespace)
# is a Unix-socket directory, and an empty host is libpq's default socket.
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
_LOCAL_ADDRESSES = frozenset({"127.0.0.1", "::1"})
_VARIABLE = re.compile(r"\$\{env:([A-Z_][A-Z0-9_]*)\}")
type AnyLakeLocation = LocalLakeLocation | SqliteLakeLocation | PostgresLakeLocation
# Functions that run SQL or open connections outside the lake; never allowed in a statement.
_EXTERNAL_SQL_FUNCTIONS = frozenset(
    {
        "postgres_query",
        "postgres_execute",
        "postgres_scan",
        "postgres_scan_pushdown",
        "postgres_attach",
        "sqlite_query",
        "sqlite_attach",
        "sqlite_scan",
        "query",
        "query_table",
        # Bind or run SQL given as text, on the lake's own connection: the text can name a file
        # no check of the statement sees (#136, Reviewer pass 9).
        "json_serialize_plan",
        "json_execute_serialized_sql",
    }
)
_SUSPICIOUS = re.compile(r"[./\\*?:]")
POSTGRES_CONNECT_TIMEOUT_S = 10
RESERVED_SOURCE_NAMES = frozenset({LAKE, "main", "memory", "system", "temp"})
_GLOB = re.compile(r"[*?\[]")
# Object-storage addresses DuckDB opens as files (httpfs, azure, Hugging Face). A statement never
# names one: only DuckLake reaches the lake's objects, and the sandbox checks these paths as text
# only (`..` is resolved after the check, #140).
_OBJECT_ADDRESS = re.compile(r"^\s*(s3|s3a|s3n|gs|gcs|r2|az|azure|abfs|abfss|hf)://", re.IGNORECASE)
EXTENSIONS = ("icu", "ducklake")
WRITE_STATEMENTS = frozenset(
    {
        duckdb.StatementType.CREATE,
        duckdb.StatementType.INSERT,
        duckdb.StatementType.UPDATE,
        duckdb.StatementType.DELETE,
        duckdb.StatementType.DROP,
        duckdb.StatementType.ALTER,
        duckdb.StatementType.MERGE_INTO,
    }
)


INTEGER_RANGES = {
    "TINYINT": (-(2**7), 2**7 - 1),
    "SMALLINT": (-(2**15), 2**15 - 1),
    "INTEGER": (-(2**31), 2**31 - 1),
    "BIGINT": (-(2**63), 2**63 - 1),
    "HUGEINT": (-(2**127), 2**127 - 1),
    "UTINYINT": (0, 2**8 - 1),
    "USMALLINT": (0, 2**16 - 1),
    "UINTEGER": (0, 2**32 - 1),
    "UBIGINT": (0, 2**64 - 1),
    "UHUGEINT": (0, 2**128 - 1),
}


@dataclass(frozen=True, slots=True)
class Limits:
    """Resource limits of a lake; every field is required (law 7).

    `max_concurrent` statements (reads in progress and open transactions) run at once; up to
    `queue_size` more wait in arrival order; `default_timeout_ms` applies when a request sets no
    `timeout_ms`.
    """

    memory_limit: str | None
    threads: int | None
    max_concurrent: int | None = None
    queue_size: int | None = None
    default_timeout_ms: int | None = None


class Admission:
    """At most `capacity` holders at once, a bounded first-come first-served queue behind them."""

    def __init__(self, capacity: int, queue_size: int) -> None:
        self._capacity = capacity
        self._queue_size = queue_size
        self._running = 0
        self._waiting: collections.deque[object] = collections.deque()
        self._condition = threading.Condition()

    def acquire(self, deadline: float) -> None:
        """Wait for a slot until `deadline` (time.monotonic); STORE_BUSY or STORE_TIMEOUT."""
        with self._condition:
            if self._running < self._capacity and not self._waiting:
                self._running += 1
                return
            if len(self._waiting) >= self._queue_size:
                raise StoreError(
                    "STORE_BUSY",
                    "The lake is at capacity and its queue is full; try again shortly.",
                    {"running": str(self._running), "queued": str(len(self._waiting))},
                )
            ticket = object()
            self._waiting.append(ticket)
            try:
                while not (self._waiting[0] is ticket and self._running < self._capacity):
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise StoreError(
                            "STORE_TIMEOUT",
                            "The request timed out while waiting for a free slot.",
                            {},
                        )
                    self._condition.wait(remaining)
                self._running += 1
            finally:
                self._waiting.remove(ticket)
                self._condition.notify_all()

    def release(self) -> None:
        with self._condition:
            self._running -= 1
            self._condition.notify_all()


INTERRUPT_REPEAT_SECONDS = 0.05


class Guard:
    """Interrupts a DuckDB cursor at a deadline or on request, and remembers why."""

    def __init__(self, cursor: duckdb.DuckDBPyConnection, deadline: float) -> None:
        self._cursor = cursor
        self.reason: str | None = None
        self.deadline = deadline
        # Called after an interruption, for an owner that must also release what is idle.
        self.on_fire: Callable[[], None] | None = None
        self._lock = threading.Lock()
        self._stopped = threading.Event()
        self._repeat: threading.Thread | None = None
        self._timer = threading.Timer(max(0.0, deadline - time.monotonic()), self._expire)
        self._timer.daemon = True
        self._timer.start()

    def _expire(self) -> None:
        self._interrupt("STORE_TIMEOUT")

    def cancel(self) -> None:
        self._interrupt("STORE_CANCELLED")

    def _interrupt(self, reason: str) -> None:
        with self._lock:
            if self.reason is not None:
                return
            self.reason = reason
        if self.on_fire is not None:
            self.on_fire()
        # An interrupt sent while the cursor is between two pieces of work can be lost, so it is
        # repeated until the owner stops the guard.
        with self._lock:
            if self._stopped.is_set():
                return
            self._repeat = threading.Thread(target=self._keep_interrupting, daemon=True)
            self._repeat.start()

    def _keep_interrupting(self) -> None:
        while True:
            # A statement that already ended has a closed cursor: there is nothing to stop.
            with contextlib.suppress(duckdb.Error):
                self._cursor.interrupt()
            if self._stopped.wait(INTERRUPT_REPEAT_SECONDS):
                return

    def stop(self) -> None:
        """No interrupt reaches the cursor once this returns."""
        with self._lock:
            self._stopped.set()
            repeat = self._repeat
        self._timer.cancel()
        if repeat is not None:
            repeat.join()

    def error(self, cause: Exception | None = None) -> StoreError:
        """The store error for an interruption (or `cause` when it was not interrupted)."""
        if self.reason == "STORE_TIMEOUT" or (
            self.reason is None and time.monotonic() >= self.deadline
        ):
            return StoreError("STORE_TIMEOUT", "The statement exceeded its time limit.", {})
        if self.reason == "STORE_CANCELLED":
            return StoreError("STORE_CANCELLED", "The statement was cancelled.", {})
        return StoreError(
            "STORE_QUERY_FAILED", "DuckDB could not run the statement.", {"duckdb": str(cause)}
        )


@dataclass(slots=True)
class ReadHandle:
    """A running read: its Arrow stream and a way to cancel it from another thread."""

    reader: pa.RecordBatchReader
    _guard: Guard

    def cancel(self) -> None:
        self._guard.cancel()


# A DuckDB type name as `load_arrow` accepts it: a name, optionally with numeric arguments.
_ARROW_LOAD_TYPE = re.compile(r"[A-Z][A-Z0-9_]*(?: [A-Z][A-Z0-9_]*)*(?:\([0-9]+(?:,[0-9]+)?\))?")


def _quote(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _conninfo(text: str) -> dict[str, str]:
    """The `keyword=value` pairs of a libpq connection string (values bare or single-quoted,
    with backslash escapes), the last of a repeated keyword winning, as in libpq."""
    pairs: dict[str, str] = {}
    invalid = StoreError(
        "STORE_STATEMENT_INVALID", "The connection must be libpq key=value pairs.", {}
    )
    index, length = 0, len(text)
    while True:
        while index < length and text[index].isspace():
            index += 1
        if index == length:
            return pairs
        start = index
        while index < length and not text[index].isspace() and text[index] != "=":
            index += 1
        keyword = text[start:index]
        while index < length and text[index].isspace():
            index += 1
        if not keyword or index == length or text[index] != "=":
            raise invalid
        index += 1
        while index < length and text[index].isspace():
            index += 1
        value: list[str] = []
        if index < length and text[index] == "'":
            index += 1
            while True:
                if index == length:
                    raise invalid  # unterminated quoted value
                character = text[index]
                if character == "\\" and index + 1 < length:
                    value.append(text[index + 1])
                    index += 2
                elif character == "'":
                    index += 1
                    break
                else:
                    value.append(character)
                    index += 1
        else:
            while index < length and not text[index].isspace():
                if text[index] == "\\" and index + 1 < length:
                    index += 1
                value.append(text[index])
                index += 1
        pairs[keyword.lower()] = "".join(value)


def _inline_secret(connection: str) -> bool:
    """Whether a connection string carries a password: a `password` or `sslpassword` keyword,
    or any URI form (`user:pw@host`, `?password=`). A value that merely contains the word (a
    database named `password_vault`) is not one. A string that is not key=value pairs is
    judged by the word alone."""
    if "://" in connection:
        return True
    try:
        pairs = _conninfo(connection)
    except StoreError:
        return _INLINE_SECRET.search(connection) is not None
    return bool({"password", "sslpassword"} & set(pairs))


def _local_host(host: str) -> bool:
    return host == "" or host.startswith(("/", "@")) or host.lower() in _LOCAL_HOSTS


def _hosts(pairs: dict[str, str]) -> str:
    return pairs["host"] if "host" in pairs else os.environ.get("PGHOST", "")


def _local_connection(pairs: dict[str, str]) -> bool:
    """Whether every host (and `hostaddr`) libpq could use for these connection keywords is the
    local machine. The environment's `PGHOST`/`PGHOSTADDR` count when the keywords name none
    (libpq and DuckDB's postgres extension read them), and a service (`service`, `PGSERVICE`),
    which can name any host, is never local. One rule for catalogs (#120) and sources (#122)."""
    addresses = pairs["hostaddr"] if "hostaddr" in pairs else os.environ.get("PGHOSTADDR", "")
    return (
        "service" not in pairs
        and not os.environ.get("PGSERVICE")
        and all(_local_host(host.strip()) for host in _hosts(pairs).split(","))
        and all(address.strip() in _LOCAL_ADDRESSES for address in addresses.split(",") if address)
    )


def _require_tls(connection: str) -> None:
    """Refuse a PostgreSQL catalog on another machine (`_local_connection`) unless it is reached
    with `sslmode=verify-full`, which encrypts and checks that the certificate is the named
    host's."""
    pairs = _conninfo(connection)
    hosts = _hosts(pairs)
    if _local_connection(pairs) or pairs.get("sslmode") == "verify-full":
        return
    raise StoreError(
        "STORE_TLS_REQUIRED",
        "A PostgreSQL catalog on another machine must be reached with sslmode=verify-full "
        "(TLS that checks the server's certificate and name).",
        {"host": hosts or "(default)", "sslmode": pairs.get("sslmode", "(none)")},
    )


def _with_root_cert(connection: str) -> str:
    """`sslrootcert=system` (the operating system's certificate authorities) unless the
    connection names its own, when TLS is verified."""
    pairs = _conninfo(connection)
    if pairs.get("sslmode") == "verify-full" and "sslrootcert" not in pairs:
        return f"{connection} sslrootcert=system"
    return connection


@dataclass(frozen=True, slots=True)
class _Storage:
    """Object storage settings with their credentials, read from the environment: the key and
    secret (S3, Google Cloud Storage), or the connection string (Azure, in `key_id`)."""

    settings: S3Storage | GcsStorage | AzureStorage
    key_id: str
    secret: str = ""


# The URL scheme of each kind of object storage: the one spelling accepted for a data path.
_SCHEMES = {"s3": "s3://", "gcs": "gs://", "azure": "az://"}


def _data_path(location: AnyLakeLocation) -> str:
    """Where the lake's data files go: `data_path`, or `<path>/data/` for a local lake. An
    object-storage path always ends with `/`, so the secret's `SCOPE` is that folder and not
    every key that starts like it (#147)."""
    if isinstance(location, LocalLakeLocation):
        if location.data_path is None:
            return (Path(location.path) / "data").as_posix() + "/"
        path = location.data_path
    else:
        path = location.data_path
    if "://" in path and not path.endswith("/"):
        return path + "/"
    return path


def _storage_credentials(
    storage: ObjectStorage | S3Storage | GcsStorage | AzureStorage | None, data_path: str
) -> _Storage | None:
    """Check the data location and read its credentials, before anything is opened.

    An object-storage data path (`s3://`, `gs://`, `az://`) needs `storage` of its kind, and
    `storage` needs such a path; any other URL is refused, and so are `.` and `..` segments.
    Unencrypted HTTP is refused unless the endpoint is the local machine
    (`STORE_TLS_REQUIRED`). The credentials are registered so no error can show them.
    """
    settings = storage.root if isinstance(storage, ObjectStorage) else storage
    remote = "://" in data_path
    scheme = next((value for value in _SCHEMES.values() if data_path.startswith(value)), None)
    if remote and scheme is None:
        raise StoreError(
            "STORE_LOCATION_INVALID",
            "Data files can be on a local path or in object storage (s3://, gs://, az://) only.",
            {"data_path": data_path},
        )
    if scheme is not None and any(
        part in {".", ".."} for part in data_path[len(scheme) :].split("/")
    ):
        raise StoreError(
            "STORE_LOCATION_INVALID",
            "An object-storage data path cannot contain . or .. segments.",
            {"data_path": data_path},
        )
    if remote != (settings is not None) or (
        settings is not None and _SCHEMES[settings.type] != scheme
    ):
        raise StoreError(
            "STORE_LOCATION_INVALID",
            "An object-storage data path needs storage settings of its kind (s3:// with s3, "
            "gs:// with gcs, az:// with azure), and storage settings need such a path.",
            {"data_path": data_path},
        )
    if settings is None:
        return None
    if isinstance(settings, AzureStorage):
        connection_string = _variable_value(settings.connection_string_env)
        _check_azure(connection_string)
        return _Storage(settings, connection_string)
    if not settings.use_ssl:
        endpoint = settings.endpoint or ""
        host = endpoint[1 : endpoint.find("]")] if endpoint.startswith("[") else endpoint
        host = host if endpoint.startswith("[") else endpoint.split(":", 1)[0]
        if not host or host.lower() not in _LOCAL_HOSTS:
            raise StoreError(
                "STORE_TLS_REQUIRED",
                "Object storage on another machine must be reached over HTTPS (use_ssl).",
                {"endpoint": settings.endpoint or "(AWS S3)"},
            )
    key_id = _variable_value(settings.key_id_env)
    secret = _variable_value(settings.secret_env)
    return _Storage(settings, key_id, secret)


def _variable_value(variable: str) -> str:
    """A credential from the environment, registered so no error can show it."""
    value = os.environ.get(variable, "")
    if not value:
        raise StoreError(
            "STORE_SECRET_MISSING",
            f"The environment variable {variable} is not set.",
            {"variable": variable},
        )
    register_secret(value)
    return value


# The keys of an Azure connection string as the Azure SDK reads them: exactly, by case, without
# trimming, the last copy winning. A key written another way would be ignored by the SDK while
# a forgiving check read it, so such keys and repeated keys are refused (#171, Reviewer pass 1).
AZURE_KEYS = frozenset(
    {
        "DefaultEndpointsProtocol",
        "AccountName",
        "AccountKey",
        "SharedAccessSignature",
        "EndpointSuffix",
        "BlobEndpoint",
        "BlobSecondaryEndpoint",
        "QueueEndpoint",
        "QueueSecondaryEndpoint",
        "TableEndpoint",
        "TableSecondaryEndpoint",
        "FileEndpoint",
        "FileSecondaryEndpoint",
        "DfsEndpoint",
        "DfsSecondaryEndpoint",
    }
)
_AZURE_KEYS_FOLDED = {key.lower(): key for key in AZURE_KEYS}


def azure_pairs(connection_string: str) -> dict[str, str] | None:
    """The `key=value` pairs of an Azure connection string, read as the Azure SDK reads them;
    None when it is not such pairs (an empty segment included), repeats a key, or writes a known
    key in another case or with spaces around it."""
    pairs: dict[str, str] = {}
    parts = connection_string.split(";")
    if parts[-1] == "":
        parts.pop()  # one final `;`
    for part in parts:
        key, equals, value = part.partition("=")
        # An empty segment (`;;`, a leading `;`) is refused: the SDK reads a key up to the next
        # `=`, across `;`, so the key after it would be hidden from the SDK but not from here.
        if not equals or not key:
            return None
        known = _AZURE_KEYS_FOLDED.get(key.strip().lower())
        if (known is not None and key != known) or key in pairs:
            return None
        pairs[key] = value
    return pairs


def _check_azure(connection_string: str) -> None:
    """An Azure connection string: `key=value` pairs naming an AccountName, over HTTPS unless
    its blob endpoint is the local machine. Its account key is registered too; nothing of it is
    shown in errors."""
    pairs = azure_pairs(connection_string)
    if pairs is None:
        raise StoreError(
            "STORE_STATEMENT_INVALID",
            "The Azure connection string must be key=value pairs separated by ;, each key "
            "written once and as Azure spells it (AccountName, AccountKey, BlobEndpoint, …).",
            {},
        )
    for name in ("AccountKey", "SharedAccessSignature"):
        if pairs.get(name):
            register_secret(pairs[name])
    if not pairs.get("AccountName"):
        raise StoreError(
            "STORE_STATEMENT_INVALID", "The Azure connection string must name an AccountName.", {}
        )
    blob = pairs.get("BlobEndpoint")
    if blob is not None:
        scheme, separator, rest = blob.partition("://")
        host_port = rest.split("/", 1)[0]
        host = (
            host_port[1 : host_port.find("]")]
            if host_port.startswith("[")
            else host_port.split(":", 1)[0]
        )
        local = scheme in {"http", "https"} and host.lower() in _LOCAL_HOSTS
        if not separator or (scheme != "https" and not local):
            raise StoreError(
                "STORE_TLS_REQUIRED",
                "Azure Blob Storage on another machine must be reached over HTTPS "
                "(the connection string's BlobEndpoint).",
                {},
            )
        if local:
            return  # an emulator on this machine: its endpoint decides the protocol
    if pairs.get("DefaultEndpointsProtocol", "https") != "https":
        raise StoreError(
            "STORE_TLS_REQUIRED",
            "Azure Blob Storage on another machine must be reached over HTTPS "
            "(the connection string's DefaultEndpointsProtocol).",
            {},
        )


def _postgres_password(location: PostgresLakeLocation) -> str:
    """Refuse an unsafe PostgreSQL location, then read and register its password."""
    if _inline_secret(location.connection):
        raise StoreError(
            "STORE_SECRET_INLINE",
            "The connection must be key=value pairs without any password; use password_env.",
            {},
        )
    _require_tls(location.connection)
    if location.metadata_schema is not None and not _SCHEMA_NAME.fullmatch(
        location.metadata_schema
    ):
        raise StoreError(
            "STORE_STATEMENT_INVALID",
            "metadata_schema must match [a-z_][a-z0-9_]*.",
            {"metadata_schema": location.metadata_schema},
        )
    password = os.environ.get(location.password_env)
    if not password:
        raise StoreError(
            "STORE_SECRET_MISSING",
            f"The environment variable {location.password_env} is not set.",
            {"variable": location.password_env},
        )
    register_secret(password)
    return password


def _tokens(sql: str) -> list[tuple[int, duckdb.token_type]]:
    """DuckDB's tokens of `sql`, positioned by character.

    `duckdb.tokenize` gives byte offsets into the UTF-8 text; a non-ASCII character before a
    token would shift every later slice of the Python string (#130).
    """
    tokens = duckdb.tokenize(sql)
    if sql.isascii():
        return tokens
    characters: dict[int, int] = {}
    offset = 0
    for index, character in enumerate(sql):
        characters[offset] = index
        offset += len(character.encode("utf-8"))
    characters[offset] = len(sql)
    return [(characters[position], kind) for position, kind in tokens]


# The characters DuckDB 2.0.0's parser reads as whitespace beyond ASCII (measured over every code
# point); `duckdb.tokenize` keeps them inside the neighboring token (#213).
PARSER_SPACES = (
    "\u00a0\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u200b"
    "\u202f\u205f\u2060\u3000\ufeff"
)


def _plain(sql: str) -> str:
    """`sql` as DuckDB 2.0.0's parser reads it: every character in `PARSER_SPACES` that its
    prepass (`Parser::StripUnicodeSpaces`) rewrites is written as a space, and only those.

    The prepass decides what is quoted with its own simple tracker, not with the scanner:
    `'…'` and `"…"` with doubled quotes, `$tag$…$tag$`, `--` comments up to a line break, no
    block comments, no backslash escapes, and none of the last two bytes. A copy that used the
    scanner's view (`duckdb.tokenize`) kept a character inside `'…'` after `/*'*/` or an
    escaped quote in `E'…'` that DuckDB then rewrote, so the guard checked another file name
    than DuckDB opened (#213, Reviewer pass 1). This text is the prepass's own fixed point:
    DuckDB changes nothing in it after the checks, so the text checked is the text run.
    """
    if sql.isascii():
        return sql
    try:
        data = sql.encode("utf-8")
    except UnicodeEncodeError as error:
        raise StoreError(
            "STORE_QUERY_FAILED", "The SQL could not be parsed.", {"duckdb": "invalid UTF-8"}
        ) from error
    spaces = _prepass_spaces(data)
    if not spaces:
        return sql
    parts: list[bytes] = []
    previous = 0
    for position, length in spaces:
        parts.append(data[previous:position])
        parts.append(b" ")
        previous = position + length
    parts.append(data[previous:])
    return b"".join(parts).decode("utf-8")


def _tag_start(byte: int) -> bool:
    return 65 <= byte <= 90 or 97 <= byte <= 122 or byte == 95 or byte >= 0x80


def _tag_byte(byte: int) -> bool:
    return _tag_start(byte) or 48 <= byte <= 57


def _prepass_spaces(data: bytes) -> list[tuple[int, int]]:
    """The (byte position, length) of each Unicode space DuckDB 2.0.0's
    `Parser::StripUnicodeSpaces` rewrites, a line-by-line port of its states (`regular`,
    `in_quotes`, `in_dollar_quotes`, `in_comment`), bounds included."""
    size = len(data)
    found: list[tuple[int, int]] = []
    position = 0
    state = "regular"
    quote = 0
    tag = b""
    while state != "end":
        if state == "regular":
            state = "end"
            while position + 2 < size:
                byte, second, third = data[position], data[position + 1], data[position + 2]
                if byte == 0xC2 and second == 0xA0:
                    found.append((position, 2))
                if byte == 0xE2:
                    if (second == 0x80 and (0x80 <= third <= 0x8B or third == 0xAF)) or (
                        second == 0x81 and third in {0x9F, 0xA0}
                    ):
                        found.append((position, 3))
                elif byte == 0xE3:
                    if second == 0x80 and third == 0x80:
                        found.append((position, 3))
                elif byte == 0xEF:
                    if second == 0xBB and third == 0xBF:
                        found.append((position, 3))
                elif byte in {0x22, 0x27}:
                    quote = byte
                    position += 1
                    state = "quotes"
                    break
                elif byte == 0x24 and (second == 0x24 or _tag_start(second)):
                    position += 1
                    start = position
                    state = "end"
                    while position + 2 < size:
                        if data[position] == 0x24:
                            tag = data[start:position]
                            state = "dollar"
                            break
                        if not _tag_byte(data[position]):
                            state = "regular"  # not a tag: read on from this byte
                            break
                        position += 1
                    break
                elif byte == 0x2D and second == 0x2D:
                    state = "comment"
                    break
                position += 1
        elif state == "quotes":
            state = "end"
            while position + 1 < size:
                if data[position] == quote:
                    if data[position + 1] == quote:
                        position += 2  # a doubled quote
                        continue
                    position += 1
                    state = "regular"
                    break
                position += 1
        elif state == "dollar":
            state = "end"
            length = len(tag)
            while position + 2 < size:
                if (
                    data[position] == 0x24
                    and size - (position + 1) >= length + 1
                    and data[position + length + 1] == 0x24
                    and data.startswith(
                        tag, position + 1
                    )  # stops at the first difference, as memcmp
                ):
                    position += length + 1  # on the closing `$`, read again as regular
                    state = "regular"
                    break
                position += 1
        else:  # comment
            state = "end"
            while position < size:
                if data[position] in {0x0A, 0x0D}:
                    state = "regular"
                    break
                position += 1
    return found


def _guarded(sql: str, table_functions: frozenset[str]) -> None:
    """Refuse SQL that names the catalog database or calls a table function other than generators.

    Table functions read files, run SQL or open connections (`read_text`, `query`,
    `postgres_execute`, …); only `range`, `generate_series` and `unnest` are allowed. DuckLake's
    table macros (`set_option`, `table_changes`, …) and table options (`CREATE TABLE … WITH
    (…)`, `ALTER TABLE … SET (…)`/`RESET (…)`) are refused too: the lake's options are the
    store's. Comments between a name and its `(` are skipped. The catalog database (in
    PostgreSQL the whole database, other lakes included) is only ever touched by DuckLake itself.
    The connection is also sandboxed to the lake's folders (`Lake._sandbox`), so these checks are
    a first layer.
    """
    sql = _plain(sql)
    if "\x00" in sql:
        # DuckDB picks a reader from a whole string but opens the path only up to a NUL, which
        # Python's file checks refuse to look at (#136, Reviewer pass 8).
        raise StoreError(
            "STORE_STATEMENT_INVALID", "A statement cannot contain a NUL character.", {}
        )
    if METADATA_PREFIX in sql.lower():
        raise StoreError(
            "STORE_STATEMENT_INVALID",
            "Statements may not reference the lake's catalog database.",
            {},
        )
    every = _tokens(sql)
    tokens = [token for token in every if token[1] != duckdb.token_type.comment]
    # Each token ends where the next one starts (a comment included), or at the end of the
    # statement: looked up by position, so the checks stay linear in the statement's length.
    positions = [position for position, _ in every] + [len(sql)]
    ends = dict(itertools.pairwise(positions))
    _refuse_file_names(sql, tokens, ends)
    _refuse_show_statements(sql, tokens, ends)
    _refuse_time_travel(sql, tokens, ends)
    first = ""
    for index, (position, kind) in enumerate(tokens):
        if kind not in {duckdb.token_type.identifier, duckdb.token_type.keyword}:
            continue
        end = ends[position]
        name = sql[position:end].strip().strip('"').lower()
        following = tokens[index + 1][0] if index + 1 < len(tokens) else len(sql)
        first = first or name
        # A CTE always names itself before its `(`: `WITH (` is a table's options.
        if (
            sql[following : following + 1] == "("
            and kind == duckdb.token_type.keyword
            and (name == "with" or (first == "alter" and name in {"set", "reset"}))
        ):
            raise StoreError(
                "STORE_STATEMENT_INVALID",
                "Table options are not allowed: the lake's options are set by the store.",
                {"clause": name.upper()},
            )
        if (
            sql[following : following + 1] == "("
            and name in DUCKLAKE_TABLE_MACROS
            and not _names_a_table(sql, tokens, ends, index)
        ):
            raise StoreError(
                "STORE_STATEMENT_INVALID",
                "DuckLake's own functions are not allowed: the store runs maintenance and sets "
                "the lake's options.",
                {"function": name},
            )
        if (
            sql[following : following + 1] == "("
            and (name in table_functions or name in _EXTERNAL_SQL_FUNCTIONS)
            and name not in READ_TABLE_FUNCTIONS
        ):
            raise StoreError(
                "STORE_STATEMENT_INVALID",
                "Only the table functions range, generate_series and unnest are allowed; a "
                "function that shares its name with another table function (histogram) is "
                "refused in every form.",
                {"function": name},
            )


def _names_a_table(
    sql: str, tokens: list[tuple[int, duckdb.token_type]], ends: dict[int, int], index: int
) -> bool:
    """Whether the (possibly qualified) name at `index` follows `INTO`, `TABLE` or
    `REFERENCES`: a table with a column list (`INSERT INTO settings (a)`), not a call."""
    before = index - 1
    while before >= 1 and sql[tokens[before][0] : tokens[before][0] + 1] == ".":
        before -= 2  # `lake.main.settings`: skip each `name .`
    if before < 0:
        return False
    position, kind = tokens[before]
    end = ends[position]
    word = sql[position:end].strip().lower()
    return kind == duckdb.token_type.keyword and word in {"into", "table", "references"}


def _refuse_file_names(
    sql: str, tokens: list[tuple[int, duckdb.token_type]], ends: dict[int, int]
) -> None:
    """Refuse a quoted string or quoted name that names an existing file, matches files, or is an
    object-storage address (`s3://…`, `gs://…`, `az://…`, …).

    DuckDB reads such a string as a file in many places (`FROM '…'`, `MERGE … USING '…'`,
    `PIVOT '…'`, `SUMMARIZE '…'`, …); a model reaches a file only through its source name. Every
    quoting form counts (`'…'`, `E'…'`, `$$…$$`, `$tag$…$tag$`, `"…"`). The file sandbox, which
    allows exactly the declared files, is the boundary behind this check.
    """

    def refused(reason: str) -> StoreError:
        # `reason` tells a named file from a pattern that could not be checked (#193).
        return StoreError(
            "STORE_STATEMENT_INVALID",
            "A file cannot be named in a statement; declare it as a source and use its name.",
            {"reason": reason},
        )

    budget = _Budget()  # one for the whole statement, so many patterns cannot add up (#178)
    texts: list[tuple[str, duckdb.token_type]] = []
    for position, kind in tokens:
        end = ends[position]
        text = sql[position:end].strip()
        texts.append((text, kind))
        quoted = kind == duckdb.token_type.string_const or text.startswith('"')
        if quoted and (reason := _names_a_file(text, budget)) is not None:
            raise refused(reason)
    # A dotted name: DuckDB joins its parts with `.` and reads the result as a file when no
    # table has that name (`"<dir>/x".parquet`, `x.parquet`, `"<dir>/*".csv`; measured on
    # 2.0.0), so every chain of name parts is checked as one path too (#136, Reviewer pass 6).
    names = (duckdb.token_type.identifier, duckdb.token_type.keyword)
    index = 0
    while index < len(texts):
        if texts[index][1] not in names:
            index += 1
            continue
        parts = [_string_content(texts[index][0])]
        index += 1
        while index + 1 < len(texts) and texts[index][0] == "." and texts[index + 1][1] in names:
            parts.append(_string_content(texts[index + 1][0]))
            index += 2
        if len(parts) > 1 and (reason := _names_a_file(".".join(parts), budget)) is not None:
            raise refused(reason)


# A dollar-quoted constant; DuckDB, like PostgreSQL, accepts any tag without `$` or white space
# (non-ASCII letters included), so every such tag is read here (#153).
_DOLLAR_QUOTED = re.compile(r"\$(?P<tag>[^$\s]*)\$(?P<body>.*)\$(?P=tag)\$", re.DOTALL)
# Statement forms that read a table or file by name outside the lake's pinned reads (#153):
# `SUMMARIZE <name or file>`, `DESCRIBE`/`DESC <name>`, `SHOW …`.
_SHOW_STATEMENTS = frozenset({"summarize", "describe", "desc", "show"})
# Keywords that start a clause; `DESC` is an ordering only inside an `ORDER BY` clause.
_CLAUSES = frozenset(
    {
        "select",
        "from",
        "where",
        "group",
        "having",
        "qualify",
        "window",
        "order",
        "limit",
        "offset",
        "union",
        "intersect",
        "except",
        "values",
        "using",
        "on",
        "returning",
        "set",
    }
)


def _string_content(token: str) -> str:
    """The text a quoted token stands for: `'…'` (with `''`), `E'…'`, `$$…$$`, `$tag$…$tag$` and
    quoted names `"…"` (with `""`). An unquoted token is returned as it is."""
    dollar = _DOLLAR_QUOTED.fullmatch(token)
    if dollar is not None:
        return dollar["body"]
    quote = token[-1:]
    if quote not in {"'", '"'}:
        return token
    if token[:1] in {"e", "E"} and token[1:2] == "'":
        return _escape_string_value(token)
    start = token.find(quote)  # after a prefix, if any
    return token[start + 1 : -1].replace(quote * 2, quote)


def _escape_string_value(token: str) -> str:
    """The value of an `E'…'` constant, read by DuckDB itself so that every escape it accepts
    (`\\x2e`, octal, `\\.` for `.`, …) is decoded exactly as it would be. The token must be
    one string constant on its own; the connection has no external access."""
    tokens = duckdb.tokenize(token)
    if len(tokens) != 1 or tokens[0][1] != duckdb.token_type.string_const:
        return token[2:-1]
    parser = _parser()  # one parser database for the process, not a connection each (#178)
    try:
        row = parser.execute(f"SELECT {token}").fetchone()
    except duckdb.Error:
        # Not a valid constant (`E'a\\'`, `E'\\x00'`): the statement fails later, when it is
        # parsed, with `STORE_QUERY_FAILED`.
        return token[2:-1]
    finally:
        parser.close()
    return str(row[0]) if row is not None and row[0] is not None else ""


def _refuse_time_travel(
    sql: str, tokens: list[tuple[int, duckdb.token_type]], ends: dict[int, int]
) -> None:
    """Refuse a table read at another snapshot (`… AT (VERSION => n)`, `AT (TIMESTAMP => t)`):
    a model reads the current data, so rows deleted since cannot come back through it (#147,
    decision #182). A read is pinned to its snapshot by the store itself, after this check. A
    name or label `at` is not followed by `(VERSION =>` or `(TIMESTAMP =>` and stays allowed."""
    texts = [sql[position : ends[position]].strip().lower() for position, _ in tokens]
    for index in range(len(texts) - 3):
        if (
            texts[index] == "at"
            and texts[index + 1] == "("
            and texts[index + 2] in {"version", "timestamp"}
            and texts[index + 3] == "=>"
        ):
            raise StoreError(
                "STORE_STATEMENT_INVALID",
                "A statement reads the current data: a table cannot be read at another "
                "snapshot (AT (VERSION => …) or AT (TIMESTAMP => …)).",
                {"clause": "AT"},
            )


def _refuse_show_statements(
    sql: str, tokens: list[tuple[int, duckdb.token_type]], ends: dict[int, int]
) -> None:
    """Refuse `SUMMARIZE`, `DESCRIBE`, `DESC` and `SHOW` everywhere except where DuckDB reads
    them as something else: a column label after `AS` (`SELECT a AS desc`), a part of a qualified
    name after `.` (`t.desc`), and `DESC` as an ordering inside an `ORDER BY` clause. DuckDB
    reads a quoted target of `SUMMARIZE` as a file, and these statement forms are not pinned to a
    snapshot (#153). The rule lists what is allowed, so a position not thought of is refused.
    Quoted names (`"describe"`) are ordinary names."""
    words: list[tuple[str, duckdb.token_type]] = []
    for position, kind in tokens:
        end = ends[position]
        words.append((sql[position:end].strip().lower(), kind))
    # `CREATE TABLE x AS <query>`: that `AS` starts a query, it does not introduce a label. It is
    # found in every statement, so empty statements before it (`;CREATE …`) do not hide it.
    query_as: set[int] = set()
    starts = [0] + [index + 1 for index, (word, _) in enumerate(words) if word == ";"]
    for start in starts:
        if start >= len(words) or words[start][0] != "create":
            continue
        depth = 0
        for index in range(start, len(words)):
            word = words[index][0]
            if word == ";":
                break
            depth += (word == "(") - (word == ")")
            if word == "as" and depth == 0:
                query_as.add(index)
                break
    clauses = [""]  # the current clause at each parenthesis depth
    for index, (word, kind) in enumerate(words):
        if word == "(":
            clauses.append("")
            continue
        if word == ")":
            if len(clauses) > 1:
                clauses.pop()
            continue
        if kind != duckdb.token_type.keyword:
            continue
        # The `FROM` of `IS [NOT] DISTINCT FROM` is a comparison, not a clause.
        if word in _CLAUSES and not (word == "from" and words[index - 1][0] == "distinct"):
            clauses[-1] = word
        if word not in _SHOW_STATEMENTS:
            continue
        label = index > 0 and words[index - 1][0] == "as" and index - 1 not in query_as
        # After `.`, the word is part of a qualified name (`t.desc`); DuckDB reads it as a name.
        qualified = index > 0 and words[index - 1][0] == "."
        ordering = word == "desc" and clauses[-1] == "order"
        if not (label or qualified or ordering):
            raise StoreError(
                "STORE_STATEMENT_INVALID",
                "SUMMARIZE, DESCRIBE and SHOW are not allowed in statements.",
                {"statement": word.upper()},
            )


# How many directory entries the file checks of one statement may read (#178): a pattern too
# broad to check within it counts as naming a file, so the statement is refused.
PATTERN_BUDGET = 10_000

# What reading one folder costs, in entries: far more than an entry when the disk cache is
# cold (the CI runner took about 2 ms per folder of /usr, so 455 folders took 1 s), so at
# most 200 folders are read for one statement whatever they hold. A folder is read once per
# statement: a pattern that lists it again reads the kept listing, at no cost (#193).
FOLDER_COST = 50

# How many steps the walks of one statement may take together: every place a walk visits and
# every entry it goes through, from a kept listing too. It bounds the work done without reading
# the disk, so patterns over folders already read cannot add up without limit (#193).
STEP_BUDGET = 100_000


class _Budget:
    """What a statement's file checks may still read, and the folders they have read (#193):
    each folder is read once per statement, then its listing is kept for the other patterns.
    One `_Budget` belongs to one statement's check, never shared between statements."""

    def __init__(self, entries: int = PATTERN_BUDGET, steps: int = STEP_BUDGET) -> None:
        self.entries = entries
        self.steps = steps
        self.listings: dict[str, list[tuple[str, bool]]] = {}


class _Unchecked(Exception):
    """A pattern could not be checked: too broad for the budget (`reason` `pattern_too_broad`),
    or it meets a folder that cannot be listed (`pattern_unreadable`)."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _spend(budget: _Budget, units: int = 1) -> None:
    budget.entries -= units
    if budget.entries < 0:
        raise _Unchecked("pattern_too_broad")


def _step(budget: _Budget) -> None:
    budget.steps -= 1
    if budget.steps < 0:
        raise _Unchecked("pattern_too_broad")


def _listed(folder: str, budget: _Budget) -> list[tuple[str, bool]]:
    """A folder's entries as (name, is a folder). The first read of a folder in a statement is
    counted against the budget, the read itself as one entry, or as `FOLDER_COST` when the
    folder is there to be read, and each entry as one; later reads use the kept listing (#193).
    A folder that does not exist, is not one, or whose name is too long to exist has none; one
    that cannot be listed, or an entry whose kind cannot be told (a symbolic-link loop), cannot
    be checked."""
    key = folder or "."
    kept = budget.listings.get(key)
    if kept is not None:
        return kept
    _spend(budget)
    entries: list[tuple[str, bool]] = []
    try:
        with os.scandir(key) as found:
            _spend(budget, FOLDER_COST - 1)  # a folder that is there and is read
            for entry in found:
                _spend(budget)
                entries.append((entry.name, entry.is_dir()))
    except FileNotFoundError, NotADirectoryError:
        entries = []
    except OSError as error:
        # A name too long for the file system names no file DuckDB could open: ordinary text
        # such as "… and/or estimated?" (#178, Reviewer pass 3).
        if error.errno != errno.ENAMETOOLONG:
            raise _Unchecked("pattern_unreadable") from None
        entries = []
    budget.listings[key] = entries
    return entries


def _expanded(pattern: str, budget: _Budget) -> Iterator[str]:
    """The paths a glob pattern names, as DuckDB expands it (`**` at any depth, hidden names
    included, like `glob.iglob(recursive=True, include_hidden=True)`), read within `budget`.
    The walk keeps its own stack (no recursion, whatever the pattern's length), merges a run of
    `**` into one, drops empty parts, and counts every step against the statement's steps and
    every folder it reads against its entries (#178, Reviewer pass 1; #193)."""
    parts: list[str] = []
    for part in pattern.split(os.sep):
        if part and not (part == "**" and parts[-1:] == ["**"]):
            parts.append(part)
    stack: list[tuple[str, int]] = [(os.sep if pattern.startswith(os.sep) else "", 0)]
    while stack:
        _step(budget)
        base, index = stack.pop()
        if index == len(parts):
            yield base
            continue
        head = parts[index]
        if head == "**":
            stack.append((base, index + 1))  # no folder at all
            for name, folder in _listed(base, budget):
                _step(budget)  # each entry, from a kept listing too (#193, Reviewer pass 1)
                below = os.path.join(base, name)
                if index + 1 == len(parts):
                    yield below
                if folder:
                    stack.append((below, index))
        elif _GLOB.search(head):
            for name, _ in _listed(base, budget):
                _step(budget)
                if fnmatch.fnmatchcase(name, head):
                    stack.append((os.path.join(base, name), index + 1))
        else:
            stack.append((os.path.join(base, head), index + 1))


def _names_a_file(token: str, budget: _Budget | None = None) -> str | None:
    """Why `token` counts as naming a file (#193): `names_a_file` when it is an object-storage
    address, a `file:` URI, an existing file or a pattern that matches one;
    `pattern_too_broad` or `pattern_unreadable` when a pattern could not be checked. None when
    it names no file."""
    budget = budget if budget is not None else _Budget()
    content = _string_content(token)
    if _OBJECT_ADDRESS.match(content):
        return "names_a_file"
    if content.startswith("file:/"):
        # DuckDB opens `file:/p`, `file:///p` and `file://localhost/p` as the local file `/p`
        # (measured on 2.0.0), whether or not it exists yet (#136, Reviewer pass 5).
        return "names_a_file"
    candidates = {content}
    for candidate in candidates:
        if not candidate:
            continue
        path = os.path.expanduser(candidate)
        if os.path.isfile(path):
            return "names_a_file"
        looks_like_path = any(mark in candidate for mark in ("/", "\\", "."))
        if looks_like_path and _GLOB.search(candidate):
            try:
                # As DuckDB expands it: `**` at any depth, hidden names included (#136, Reviewer
                # pass 7), within the statement's budget (#178).
                if any(os.path.isfile(match) for match in _expanded(path, budget)):
                    return "names_a_file"
            except _Unchecked as unchecked:
                return unchecked.reason  # too broad, or a folder that cannot be listed: refused
    return None


def _single_statement(sql: str, table_functions: frozenset[str]) -> duckdb.Statement:
    # Parsed first: SQL DuckDB cannot parse fails with the parser's own message, whatever the
    # checks would say (#158). Parsing runs nothing.
    try:
        statements = duckdb.extract_statements(sql)
    except duckdb.Error as error:
        raise StoreError(
            "STORE_QUERY_FAILED", "The SQL could not be parsed.", {"duckdb": str(error)}
        ) from error
    _guarded(sql, table_functions)
    if len(statements) != 1:
        raise StoreError(
            "STORE_STATEMENT_INVALID",
            "Exactly one SQL statement is required.",
            {"statements": str(len(statements))},
        )
    return statements[0]


_INTEGER = re.compile(r"-?[0-9]+")
_DECIMAL = re.compile(r"-?[0-9]+(\.[0-9]+)?")
_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_MOMENT = r"[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?"
_TIMESTAMP = re.compile(_MOMENT)
_TIMESTAMPTZ = re.compile(_MOMENT + r"(Z|[+-][0-9]{2}:[0-9]{2})")
_DOUBLE = re.compile(r"-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?")


def _invalid(parameter: Parameter, reason: str) -> StoreError:
    return StoreError(
        "STORE_PARAMETER_INVALID",
        f"Parameter {parameter.name} is not a valid {parameter.type.root}.",
        {"parameter": parameter.name, "reason": reason},
    )


def _converted(parameter: Parameter) -> str | None:
    """The parameter's text after a strict check against its type.

    The text is bound as is and converted by `CAST(... AS <type>)` inside DuckDB (`_typed`), so
    no value passes through a Python float or a time-zone library.
    """
    value = parameter.value
    kind = parameter.type.root
    if value is None:
        return None
    if kind.startswith("DECIMAL"):
        precision, scale = (int(part) for part in kind[8:-1].split(","))
        if not 1 <= precision <= 38 or scale > precision:
            raise _invalid(parameter, "precision must be 1-38 and scale at most the precision")
        if not _DECIMAL.fullmatch(value):
            raise _invalid(parameter, "expected digits with an optional fraction")
        fraction = len(value.partition(".")[2])
        whole = len(value.lstrip("-").partition(".")[0].lstrip("0"))
        if fraction > scale or whole > precision - scale:
            raise _invalid(parameter, f"does not fit DECIMAL({precision},{scale})")
        return value
    if kind in INTEGER_RANGES:
        if not _INTEGER.fullmatch(value):
            raise _invalid(parameter, "expected digits")
        number = int(value, 10)
        low, high = INTEGER_RANGES[kind]
        if not low <= number <= high:
            raise _invalid(parameter, f"out of range for {kind}")
        return value
    if kind == "BOOLEAN":
        if value not in {"true", "false"}:
            raise _invalid(parameter, "expected true or false")
        return value
    if kind == "DOUBLE":
        if not _DOUBLE.fullmatch(value):
            raise _invalid(parameter, "expected a finite decimal number")
        if not math.isfinite(float(value)):
            raise _invalid(parameter, "out of range for DOUBLE")
        return value
    if kind in {"DATE", "TIMESTAMP", "TIMESTAMPTZ"} and value in {"infinity", "-infinity"}:
        return value  # DuckDB's own infinite dates and times (the lowest value is `-infinity`)
    try:
        if kind == "DATE":
            if not _DATE.fullmatch(value):
                raise ValueError("expected YYYY-MM-DD")
            dt.date.fromisoformat(value)
            return value
        if kind == "TIMESTAMP":
            if not _TIMESTAMP.fullmatch(value):
                raise ValueError("expected YYYY-MM-DDTHH:MM:SS[.ffffff] without a zone")
            dt.datetime.fromisoformat(value)
            return value
        if kind == "TIMESTAMPTZ":
            if not _TIMESTAMPTZ.fullmatch(value):
                raise ValueError("expected YYYY-MM-DDTHH:MM:SS[.ffffff] with Z or an offset")
            dt.datetime.fromisoformat(value)
            return value
    except ValueError as error:
        raise _invalid(parameter, str(error)) from error
    return value


def _bound(statement: duckdb.Statement, parameters: list[Parameter]) -> dict[str, str | None]:
    names = [parameter.name for parameter in parameters]
    expected = set(statement.named_parameters)
    if len(names) != len(set(names)) or set(names) != expected:
        raise StoreError(
            "STORE_PARAMETER_INVALID",
            "The parameters must match the $names used in the SQL exactly once.",
            {
                "missing": ",".join(sorted(expected - set(names))),
                "extra": ",".join(sorted(set(names) - expected)),
            },
        )
    return {parameter.name: _converted(parameter) for parameter in parameters}


def _typed(sql: str, parameters: list[Parameter]) -> str:
    """The SQL with every `$name` wrapped in `CAST($name AS <declared type>)`.

    DuckDB's tokenizer finds the parameters, so text inside strings and comments is untouched.
    The declared type is validated by the contract pattern before it reaches the SQL.
    """
    types = {parameter.name: parameter.type.root for parameter in parameters}
    tokens = _tokens(sql)
    edits: list[tuple[int, int, str]] = []
    for index, (position, kind) in enumerate(tokens[:-1]):
        following, following_kind = tokens[index + 1]
        if (
            kind == duckdb.token_type.operator
            and sql[position] == "$"
            and following == position + 1
            and following_kind == duckdb.token_type.identifier
        ):
            end = tokens[index + 2][0] if index + 2 < len(tokens) else len(sql)
            name = sql[following:end].rstrip()
            if name in types:
                stop = following + len(name)
                edits.append((position, stop, f"CAST(${name} AS {types[name]})"))
    wrapped = {sql[start + 1 : stop] for start, stop, _ in edits}
    if wrapped != set(types):
        raise StoreError(
            "STORE_PARAMETER_INVALID",
            "Every parameter must appear in the SQL as $name.",
            {"unplaced": ",".join(sorted(set(types) - wrapped))},
        )
    for start, stop, replacement in reversed(edits):
        sql = sql[:start] + replacement + sql[stop:]
    return sql


READ_TABLE_FUNCTIONS = frozenset({"range", "generate_series", "unnest"})
# Table functions whose name is also a scalar function, known to read nothing in either form:
# `repeat` generates rows, `version` reports DuckDB's version. Any other name that is a table
# function (`histogram`, whose table form reads a table or file named by any expression, and
# whatever an extension adds) is refused in every form, its scalar or aggregate form included.
SHARED_HARMLESS_FUNCTIONS = frozenset({"repeat", "version"})
# DuckLake's table macros, created in each lake schema on first use (so `duckdb_functions()`
# never lists them): they change the lake's options (`set_option` could turn on deletion
# vectors, which DuckLake writes unencrypted), set commit messages, merge files, or read deleted
# rows and old snapshots. Only the store calls DuckLake's maintenance and options.
DUCKLAKE_TABLE_MACROS = frozenset(
    {
        "set_option",
        "set_commit_message",
        "merge_adjacent_files",
        "options",
        "settings",
        "current_snapshot",
        "last_committed_snapshot",
        "snapshots",
        "table_info",
        "table_changes",
        "table_deletions",
        "table_insertions",
    }
)
TABLE_FUNCTIONS_SQL = (
    "SELECT string_agg(DISTINCT lower(function_name), '|') FROM duckdb_functions() "
    "WHERE function_type IN ('table', 'table_macro', 'pragma')"
)


def _sandbox_folder(data_path: str) -> str:
    """A data path as the sandbox lists it: an object-storage prefix as written (DuckDB checks
    remote paths against it too), a local folder resolved."""
    return data_path if "://" in data_path else Path(data_path).resolve().as_posix()


def _sandbox_entries(
    location: AnyLakeLocation, spill: str, readable: Sequence[str] = ()
) -> tuple[list[str], list[str]]:
    """What the lake's connection may open, as (folders, files): the spill folder, the lake's
    folder or local data folder, a catalog file with its companion files, and the declared source
    files. Nothing else; DuckDB then adds entries of its own for each database it attaches (its
    `.wal.checkpoint` and `.wal.recovery` files) and for its temporary folder."""
    folders = [spill]
    files = list(readable)
    if isinstance(location, LocalLakeLocation):
        folders.append(Path(location.path).resolve().as_posix())
        if location.catalog_path is not None:
            catalog = Path(location.catalog_path).resolve().as_posix()
            files += [catalog, f"{catalog}.wal"]
        if location.data_path is not None:  # data files outside the lake's folder (#126)
            folders.append(_sandbox_folder(location.data_path))
    else:
        folders.append(_sandbox_folder(location.data_path))
    if isinstance(location, SqliteLakeLocation):
        catalog = Path(location.path).resolve().as_posix()
        files += [catalog, *(f"{catalog}-{suffix}" for suffix in ("wal", "shm", "journal"))]
    return folders, files


class Lake:
    """One open lake. Reads may run concurrently; write transactions run one at a time."""

    def __init__(
        self,
        location: LakeLocation | AnyLakeLocation,
        limits: Limits,
        sources: Sequence[AnySource] = (),
    ) -> None:
        if isinstance(location, LakeLocation):
            location = location.root
        # Location checks come first: nothing is opened for a location that must be refused.
        password = (
            _postgres_password(location) if isinstance(location, PostgresLakeLocation) else ""
        )
        data_path = _data_path(location)
        storage = _storage_credentials(location.storage, data_path)
        if (
            not limits.memory_limit
            or not limits.threads
            or limits.max_concurrent is None
            or limits.max_concurrent < 1
            or limits.queue_size is None
            or limits.queue_size < 0
            or limits.default_timeout_ms is None
            or not 1 <= limits.default_timeout_ms <= MAX_TIMEOUT_MS
        ):
            raise StoreError(
                "STORE_LIMITS_REQUIRED",
                "The lake needs a memory limit, a thread count, a concurrency limit, a queue "
                "size and a default timeout.",
                {},
            )
        attachments: list[FileSourceAttachment | SqliteSourceAttachment | PostgresSourceAttachment]
        attachments = [
            source.root if isinstance(source, SourceAttachment) else source for source in sources
        ]
        # Passwords are read and registered before anything is opened, so no error can show one.
        passwords = {
            source.name: _source_password(source)
            for source in attachments
            if isinstance(source, PostgresSourceAttachment)
        }
        names = [source.name for source in attachments]
        reserved = sorted(set(names) & RESERVED_SOURCE_NAMES)
        if reserved:
            # The contract excludes these names too; generated models do not check `not`.
            raise StoreError(
                "STORE_SOURCE_INVALID",
                f"Source name {reserved[0]} is reserved.",
                {"source": reserved[0]},
            )
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise StoreError(
                "STORE_SOURCE_INVALID",
                f"Source {duplicates[0]} is declared more than once.",
                {"source": duplicates[0]},
            )
        self._limits = limits
        self._admission = Admission(limits.max_concurrent, limits.queue_size)
        try:
            self._connection = duckdb.connect(
                config={
                    "memory_limit": limits.memory_limit,
                    "threads": int(limits.threads),
                    # Secrets saved on this machine are never loaded into a lake.
                    "allow_persistent_secrets": False,
                }
            )
        except (duckdb.Error, ValueError) as error:
            raise StoreError(
                "STORE_LIMITS_REQUIRED",
                "The memory limit or the thread count is not valid.",
                {"duckdb": str(error)},
            ) from error
        self._write_lock = threading.Lock()
        self._maintenance_lock = threading.Lock()
        # DuckDB spills to this folder when a query exceeds the memory limit.
        self._spill = tempfile.mkdtemp(prefix="lake-spill-")
        self._local = threading.local()
        try:
            self._load_extensions()
            # Catalog tables: `<metadata database>[.<schema>]`; the schema name is validated by
            # the contract pattern ([a-z_][a-z0-9_]*) before it reaches SQL.
            self._metadata = METADATA
            if isinstance(location, PostgresLakeLocation) and location.metadata_schema:
                self._metadata = f"{METADATA}.{location.metadata_schema}"
            if storage is not None:
                self._storage_secret(storage, data_path)
            else:
                Path(data_path).mkdir(parents=True, exist_ok=True)
            options = [f"DATA_PATH {_quote(data_path)}"]
            if location.encrypted:
                # Fixed when the lake is created; deletion vectors stay off (written unencrypted).
                # DuckDB writes encrypted files only with httpfs's crypto module loaded (without
                # it, its built-in one is read-only and every write fails, #127).
                self._load_httpfs()
                options.append("ENCRYPTED")
            if isinstance(location, LocalLakeLocation):
                self._attach_local(Path(location.path), location.catalog_path, options)
            elif isinstance(location, SqliteLakeLocation):
                self._attach_sqlite_catalog(location, options)
            else:
                self._attach_postgres(location, password, options)
            # Nothing may load code or reach data the lake did not ask for after this point.
            self._connection.sql("SET autoinstall_known_extensions = false")
            self._connection.sql("SET autoload_known_extensions = false")
            # (source, schema or "", table, column, label) of every column DuckDB cannot carry
            # exactly; the label is how errors name it.
            self._inexact: set[Inexact] = set()
            readable: list[str] = []
            for source in attachments:
                if isinstance(source, SqliteSourceAttachment):
                    readable += self._attach_sqlite(source)
                elif isinstance(source, PostgresSourceAttachment):
                    self._attach_postgres_source(source, passwords[source.name])
                else:
                    readable += self._attach_source(source)
            # After the sources: their extensions add table functions (sqlite_scan, …) to refuse.
            self._table_functions = self._names_of_table_functions()
            self._sandbox(location, readable)
        except BaseException:
            self._connection.close()
            shutil.rmtree(self._spill, ignore_errors=True)
            raise

    def _names_of_table_functions(self) -> frozenset[str]:
        row = self._one(TABLE_FUNCTIONS_SQL, {})
        text = str(row[0]) if row and row[0] is not None else ""
        return frozenset(text.split("|")) - {""} - SHARED_HARMLESS_FUNCTIONS

    def _attach_sqlite(self, source: SqliteSourceAttachment) -> list[str]:
        """Attach a private copy of a SQLite file, read-only.

        The copy (SQLite's backup, consistent even while the file is being written, WAL
        included) is what gets inspected and attached, so a change to the original after the
        lake opens can neither add a view nor an inexact column. Its DECIMAL/NUMERIC columns
        are recorded as inexact.
        """
        path = Path(source.path)
        if not path.is_file():
            raise _source_not_found(source)  # SQLite would create a missing file
        folder = Path(self._spill) / "sources"
        folder.mkdir(exist_ok=True)
        copy = folder / f"{source.name}.sqlite"
        declared = _sqlite_snapshot(source, copy)
        self._load_extension("sqlite")
        self._source_sql(
            source,
            f"ATTACH {_quote(copy.as_posix())} AS {_identifier(source.name)} "
            "(TYPE sqlite, READ_ONLY)",
        )
        for table, column, kind in declared:
            # DuckDB reads every declared type containing DEC or NUM (DECIMAL, NUMERIC,
            # NUMBER, NUM) as floating point, and SQLite stores such values as REAL anyway.
            if re.search(r"DEC|NUM", kind, re.IGNORECASE):
                self._inexact.add(
                    Inexact(source.name, "", table.lower(), column.lower(), f"{table}.{column}")
                )
        return []  # the copy lives in the lake's temporary folder, already allowed

    def _attach_postgres_source(self, source: PostgresSourceAttachment, password: str) -> None:
        """Attach a PostgreSQL database read-only, twice over: DuckDB's READ_ONLY and a
        read-only session. Inexact columns (numeric without precision, money) are recorded."""
        self._load_extension("postgres")
        secret = _identifier(f"source_{source.name}")
        self._source_connect(
            source,
            f"CREATE TEMPORARY SECRET {secret} (TYPE postgres, HOST {_quote(source.host)}, "
            f"PORT {int(source.port)}, DATABASE {_quote(source.database)}, "
            f"USER {_quote(source.user)}, PASSWORD {_quote(password)})",
        )
        connection = (
            f"connect_timeout={POSTGRES_CONNECT_TIMEOUT_S} "
            "options='-c default_transaction_read_only=on'" + _source_tls(source)
        )
        options = ["TYPE postgres", f"SECRET {secret}", "READ_ONLY"]
        if source.schema_:
            options.append(f"SCHEMA {_quote(source.schema_)}")
        self._source_connect(
            source,
            f"ATTACH {_quote(connection)} AS {_identifier(source.name)} ({', '.join(options)})",
        )
        scope = "" if not source.schema_ else f" AND n.nspname = {_quote(source.schema_)}"
        # Every relation DuckDB can scan (tables, views, materialized views, foreign and
        # partitioned tables). Inexact: numeric without precision or above 38 digits (DuckDB
        # reads both as DOUBLE), money, and arrays of either.
        inner = (
            "SELECT n.nspname AS table_schema, c.relname AS table_name, a.attname AS column_name "
            "FROM pg_catalog.pg_attribute a "
            "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relkind IN ('r', 'v', 'm', 'f', 'p') AND a.attnum > 0 "
            "AND NOT a.attisdropped "
            "AND n.nspname NOT IN ('pg_catalog', 'information_schema') "
            "AND n.nspname NOT LIKE 'pg_toast%' AND ("
            "(a.atttypid = 'numeric'::regtype "
            # typmod - 4 = precision << 16 | scale (11 bits; PostgreSQL 15+ allows a negative
            # scale or one above the precision, which DuckDB cannot represent either).
            "AND (a.atttypmod < 0 OR ((a.atttypmod - 4) >> 16) > 38 "
            "OR ((a.atttypmod - 4) & 2047) > ((a.atttypmod - 4) >> 16))) "
            "OR a.atttypid IN ('money'::regtype, '_money'::regtype, '_numeric'::regtype))" + scope
        )
        cursor = self._connection.cursor()
        try:
            # One metadata row: the inexact columns as a list (no row data reaches Python).
            row = cursor.execute(
                "SELECT list([table_schema, table_name, column_name]) "
                "FROM postgres_query($name, $sql)",
                {"name": source.name, "sql": inner},
            ).fetchone()
        except duckdb.Error as error:
            raise StoreError(
                "STORE_CONNECTION_FAILED",
                f"Source {source.name} could not be inspected.",
                {"source": source.name, "duckdb": str(error)},
            ) from None
        finally:
            cursor.close()
        found = row[0] if row and isinstance(row[0], list) else []
        for schema, table, column in found:
            label = f"{table}.{column}" if source.schema_ else f"{schema}.{table}.{column}"
            self._inexact.add(
                Inexact(
                    source.name, str(schema).lower(), str(table).lower(), str(column).lower(), label
                )
            )

    def _source_connect(self, source: PostgresSourceAttachment, sql: str) -> None:
        try:
            self._connection.sql(sql)
        except duckdb.Error as error:
            # `from None`: the DuckDB error is not chained; the message is redacted.
            raise StoreError(
                "STORE_CONNECTION_FAILED",
                f"Source {source.name} could not be reached or refused the credentials.",
                {"source": source.name, "duckdb": str(error)},
            ) from None

    def _attach_source(self, source: FileSourceAttachment) -> list[str]:
        """Attach one file source read-only; returns the files the sandbox must allow.

        For a glob, exactly the files it matches now: a file added later to the same folder,
        or any other file there, stays unreadable.
        """
        path = source.path
        if source.type == "duckdb":
            if not Path(path).is_file():
                raise _source_not_found(source)
            self._source_sql(
                source, f"ATTACH {_quote(path)} AS {_identifier(source.name)} (READ_ONLY)"
            )
            macros = self._one(
                "SELECT (SELECT count(*) FROM duckdb_functions() WHERE database_name = $name "
                "AND NOT internal AND function_type IN ('macro', 'table_macro')) "
                "+ (SELECT count(*) FROM duckdb_views() WHERE database_name = $name "
                "AND NOT internal)",
                {"name": source.name},
            )
            if macros and int(str(macros[0])):
                # A view or macro runs with the lake's connection: it could read lake tables or
                # the lake's catalog database, which a source must never reach.
                raise StoreError(
                    "STORE_SOURCE_INVALID",
                    f"Source {source.name} defines views or macros, which a lake cannot attach.",
                    {"source": source.name},
                )
            return [Path(path).as_posix()]
        matched = self._one("SELECT list(file ORDER BY file) FROM glob($path)", {"path": path})
        matched_files = matched[0] if matched else None
        if not isinstance(matched_files, list) or not matched_files:
            raise _source_not_found(source)
        # DuckDB's readers expand `*`, `?` and `[` in each listed name again: `r[1].parquet`
        # would be read as `r1.parquet` (measured on 2.0.0). Such a name is refused, as #82
        # specifies; a bracket class (`r[[]1].parquet`) would read it literally (#232).
        expanded = next(
            (str(file) for file in matched_files if _GLOB_IN_NAME.search(str(file))), None
        )
        if expanded is not None:
            raise StoreError(
                "STORE_SOURCE_INVALID",
                f"Source {source.name} matches the file {expanded}, whose name has *, ? or [: "
                "DuckDB would read it as a pattern, so another file could be read in its place. "
                "Rename the file.",
                {"source": source.name, "file": expanded},
            )
        # The view lives in its own small database file, attached read-only: nothing can be
        # created in, or dropped from, a source catalog.
        folder = Path(self._spill) / "sources"
        folder.mkdir(exist_ok=True)
        catalog = (folder / f"{source.name}.duckdb").as_posix()
        builder = duckdb.connect(catalog)
        try:
            # The files matched now, listed one by one: the sandbox allows exactly these.
            listed = ", ".join(_quote(str(file)) for file in matched_files)
            if source.type == "parquet":
                reader = f"read_parquet([{listed}], union_by_name = true)"
                replaced = _wide_decimals(source.name, [str(file) for file in matched_files])
            else:
                # Detection never yields floating point: decimal text stays exact text, and the
                # model casts it explicitly (law 11). A DECIMAL(38,9) candidate would round
                # longer fractions silently (measured on 2.0.0).
                # The whole file is sniffed (`sample_size = -1`): with a sample, a decimal after
                # it would be rounded into an integer column inferred from the sample.
                reader = (
                    f"read_csv([{listed}], union_by_name = true, sample_size = -1, "
                    f"auto_type_candidates = {_CSV_TYPE_CANDIDATES})"
                )
                replaced = ""
            builder.sql(f"CREATE VIEW data AS SELECT *{replaced} FROM {reader}")
        except duckdb.Error as error:
            raise StoreError(
                "STORE_SOURCE_INVALID",
                f"Source {source.name} cannot be read as {source.type}.",
                {"source": source.name, "duckdb": str(error)},
            ) from None
        finally:
            builder.close()
        self._source_sql(
            source, f"ATTACH {_quote(catalog)} AS {_identifier(source.name)} (READ_ONLY)"
        )
        return [str(file) for file in matched_files]

    def _source_sql(self, source: FileSourceAttachment | SqliteSourceAttachment, sql: str) -> None:
        try:
            self._connection.sql(sql)
        except duckdb.Error as error:
            raise StoreError(
                "STORE_SOURCE_INVALID",
                f"Source {source.name} could not be attached.",
                {"source": source.name, "duckdb": str(error)},
            ) from None

    def _sandbox(
        self,
        location: AnyLakeLocation,
        readable: Sequence[str] = (),
    ) -> None:
        """Limit file access to the lake's own folders, its catalog file and the declared source
        files, for good (DuckDB cannot undo it)."""
        folders, files = _sandbox_entries(location, self._spill, readable)
        listed = ", ".join(_quote(folder.rstrip("/") + "/") for folder in folders)
        self._connection.sql(f"SET temp_directory = {_quote(self._spill)}")
        self._connection.sql(f"SET allowed_directories = [{listed}]")
        if files:
            self._connection.sql(f"SET allowed_paths = [{', '.join(map(_quote, files))}]")
        self._connection.sql("SET enable_external_access = false")

    def _load_extensions(self) -> None:
        for extension in EXTENSIONS:
            self._load_extension(extension)

    def _attach_local(self, folder: Path, catalog_path: str | None, options: Sequence[str]) -> None:
        folder.mkdir(parents=True, exist_ok=True)
        if catalog_path is None:
            catalog = (folder / "catalog.ducklake").as_posix()
        else:
            Path(catalog_path).parent.mkdir(parents=True, exist_ok=True)
            catalog = Path(catalog_path).as_posix()
        # Paths come from the project configuration, not from query input; quoted as literals.
        self._attach(f"ATTACH {_quote('ducklake:' + catalog)} AS {LAKE} ({', '.join(options)})")

    def _attach(self, sql: str) -> None:
        """Attach the lake; object storage that refuses the credentials or cannot be reached is
        reported without them."""
        try:
            self._connection.sql(sql)
        except duckdb.Error as error:
            raise StoreError(
                "STORE_CATALOG_UNAVAILABLE",
                "The lake could not be opened (its catalog or its data files).",
                {"duckdb": str(error)},
            ) from None

    def _storage_secret(self, storage: _Storage, data_path: str) -> None:
        """A temporary secret for the lake's prefix only; never persisted."""
        settings = storage.settings
        if isinstance(settings, AzureStorage):
            self._load_extension("azure")
            options = [
                "TYPE azure",
                f"CONNECTION_STRING {_quote(storage.key_id)}",
                f"SCOPE {_quote(data_path)}",
            ]
        else:
            self._load_httpfs()
            options = [
                f"TYPE {settings.type}",
                f"KEY_ID {_quote(storage.key_id)}",
                f"SECRET {_quote(storage.secret)}",
                f"USE_SSL {'true' if settings.use_ssl else 'false'}",
                f"SCOPE {_quote(data_path)}",
            ]
            if isinstance(settings, S3Storage):
                options.append(f"REGION {_quote(settings.region)}")
                if settings.url_style is not None:
                    options.append(f"URL_STYLE {_quote(settings.url_style)}")
            if settings.endpoint is not None:
                options.append(f"ENDPOINT {_quote(settings.endpoint)}")
            if isinstance(settings, GcsStorage):
                # The endpoint is reached by path (`host/bucket/key`), as the S3 API of Cloud
                # Storage and local test servers both accept.
                options.append("URL_STYLE 'path'")
        try:
            self._connection.sql(f"CREATE TEMPORARY SECRET lake_storage ({', '.join(options)})")
        except duckdb.Error as error:
            raise StoreError(
                "STORE_CATALOG_UNAVAILABLE",
                "The lake's object storage settings were refused.",
                {"duckdb": str(error)},
            ) from None

    def _load_httpfs(self) -> None:
        """httpfs, for S3 and Google Cloud Storage and for writing encrypted files, with its web
        file systems turned off: it also opens http://, https:// and hf:// addresses as files,
        and the statement check lets web addresses through as ordinary text (#136, Reviewer
        passes 4 and 5). Set after the extension loads: it names its file systems."""
        self._load_extension("httpfs")
        self._connection.sql("SET disabled_filesystems = 'HTTPFileSystem,HuggingFaceFileSystem'")

    def _attach_sqlite_catalog(self, location: SqliteLakeLocation, options: Sequence[str]) -> None:
        """Attach a lake whose catalog is a SQLite file (several processes on one machine)."""
        try:
            Path(location.path).parent.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            # A file where the folder goes, no permission, …: the catalog cannot be reached.
            raise StoreError(
                "STORE_CATALOG_UNAVAILABLE",
                "The lake's SQLite catalog folder cannot be created.",
                {"error": type(error).__name__},
            ) from None
        self._load_extension("sqlite")
        self._attach(
            f"ATTACH {_quote('ducklake:sqlite:' + Path(location.path).as_posix())} AS {LAKE} "
            f"({', '.join(options)})"
        )

    def _attach_postgres(
        self, location: PostgresLakeLocation, password: str, attach_options: Sequence[str]
    ) -> None:
        """Attach a lake whose catalog is in PostgreSQL; the password comes from the environment."""
        self._load_extension("postgres")
        try:
            # A temporary default secret supplies the password to every PostgreSQL connection of
            # this database; DuckDB never shows secret values (duckdb_secrets() redacts them).
            self._connection.sql(
                f"CREATE TEMPORARY SECRET (TYPE postgres, PASSWORD {_quote(password)})"
            )
            options = list(attach_options)
            if location.metadata_schema:
                options.append(f"METADATA_SCHEMA {_quote(location.metadata_schema)}")
            connection = _with_root_cert(location.connection)
            self._connection.sql(
                f"ATTACH {_quote('ducklake:postgres:' + connection)} AS {LAKE} "
                f"({', '.join(options)})"
            )
        except duckdb.Error as error:
            # `from None`: the DuckDB exception is not chained, so a logged traceback cannot show
            # anything the redacted message does not.
            raise StoreError(
                "STORE_CATALOG_UNAVAILABLE",
                "The PostgreSQL catalog could not be reached or refused the credentials.",
                {"duckdb": str(error)},
            ) from None

    def _load_extension(self, extension: str) -> None:
        try:
            self._connection.install_extension(extension)
            self._connection.load_extension(extension)
        except duckdb.Error as error:
            raise StoreError(
                "STORE_EXTENSION_UNAVAILABLE",
                f"The DuckDB extension {extension} could not be installed or loaded.",
                {"extension": extension, "duckdb": str(error)},
            ) from error

    def close(self) -> None:
        self._connection.close()
        shutil.rmtree(self._spill, ignore_errors=True)

    def _cursor(self) -> duckdb.DuckDBPyConnection:
        cursor = self._connection.cursor()
        cursor.sql(f"USE {LAKE}")
        # TIMESTAMPTZ values reach Arrow as instants in UTC whatever the machine's zone;
        # presenting them in a local zone is a later, explicit step.
        cursor.sql("SET TimeZone = 'UTC'")
        # A name in FROM must never resolve to a Python object in scope.
        cursor.sql("SET python_enable_replacements = false")
        return cursor

    def _one(
        self,
        sql: str,
        parameters: dict[str, object],
        cursor: duckdb.DuckDBPyConnection | None = None,
    ) -> tuple[object, ...] | None:
        """One metadata row, on `cursor` (a request's, under its guard, #160) or on a
        short-lived cursor; no unfinished result is left behind."""
        if cursor is not None:
            result = cursor.execute(sql, parameters)
            found: tuple[object, ...] | None = result.fetchone()
            result.fetchone()  # reaches the end of the one-row result: the cursor is free again
            return found
        own = self._cursor()
        try:
            row: tuple[object, ...] | None = own.execute(sql, parameters).fetchone()
        finally:
            own.close()
        return row

    def _snapshot(
        self, snapshot_id: int | None, cursor: duckdb.DuckDBPyConnection | None = None
    ) -> Snapshot | None:
        where = "" if snapshot_id is None else "WHERE snapshot_id = $id"
        parameters = {} if snapshot_id is None else {"id": snapshot_id}
        row = self._one(
            "SELECT snapshot_id, epoch_us(snapshot_time) FROM ducklake_snapshots($lake) "
            f"{where} ORDER BY snapshot_id DESC LIMIT 1",
            {"lake": LAKE, **parameters},
            cursor,
        )
        return None if row is None else _snapshot_of(row)

    def _oldest_snapshot(self, cursor: duckdb.DuckDBPyConnection | None = None) -> Snapshot | None:
        row = self._one(
            "SELECT snapshot_id, epoch_us(snapshot_time) FROM ducklake_snapshots($lake) "
            "ORDER BY snapshot_id LIMIT 1",
            {"lake": LAKE},
            cursor,
        )
        return None if row is None else _snapshot_of(row)

    def _missing_snapshot(
        self, snapshot_id: int, cursor: duckdb.DuckDBPyConnection | None = None
    ) -> StoreError:
        """Why a snapshot cannot be read: expired by maintenance (ids are never reused and every
        id up to the current one was committed, so a missing one among them expired), or never
        existed."""
        oldest = self._oldest_snapshot(cursor)
        current = self._snapshot(None, cursor)
        if oldest is not None and current is not None and 0 <= snapshot_id <= current.id:
            return StoreError(
                "STORE_SNAPSHOT_EXPIRED",
                f"Snapshot {snapshot_id} has expired: the oldest snapshot kept is {oldest.id} "
                f"({oldest.committed_at.isoformat()}); the lake's snapshot retention decides how "
                "long snapshots stay readable.",
                {"snapshot": str(snapshot_id), "oldest": str(oldest.id)},
            )
        return StoreError(
            "STORE_SNAPSHOT_NOT_FOUND",
            f"Snapshot {snapshot_id} does not exist in the lake.",
            {"snapshot": str(snapshot_id)},
        )

    def maintain(
        self, expire_before: dt.datetime, orphans_before: dt.datetime
    ) -> MaintenanceReport:
        """Expire the snapshots committed before `expire_before`, merge small files, rewrite
        files with many deletes, and delete the files no remaining snapshot uses, and orphaned
        files older than `orphans_before` (a writer may still be creating a newer one).

        DuckLake's own `CHECKPOINT` order, step by step so each one is counted: flush inlined
        data, expire, merge, rewrite, expire again (the snapshot merging replaced, when it is
        older than the cutoff), clean up, delete orphans. The current snapshot is never expired.
        Runs alone: one maintenance at a time, after every read and write of this lake ends
        (the writer lock, then every admission slot). Counts are read in SQL; no row reaches
        Python.
        """
        for instant in (expire_before, orphans_before):
            if instant.tzinfo is None:
                raise StoreError(
                    "STORE_PARAMETER_INVALID", "Maintenance cutoffs must be aware instants.", {}
                )
        cutoff = expire_before.astimezone(dt.UTC).isoformat()
        orphans = orphans_before.astimezone(dt.UTC).isoformat()
        deadline = time.monotonic() + self._timeout_ms(None) / 1000
        # One maintenance at a time (two taking part of the slots each would wait for each
        # other), then the writer lock, then every admission slot: no read or write of this lake
        # runs during maintenance (a read pinned to a snapshot being expired would lose its files).
        # The writer lock comes before the slots, as for a transaction, so neither holds what the
        # other waits for (#77).
        if not self._maintenance_lock.acquire(timeout=max(0.0, deadline - time.monotonic())):
            raise StoreError("STORE_TIMEOUT", "Another maintenance of this lake is running.", {})
        try:
            if not self._write_lock.acquire(timeout=max(0.0, deadline - time.monotonic())):
                raise StoreError(
                    "STORE_TIMEOUT", "Maintenance waited too long for the writer ahead.", {}
                )
            taken = 0
            try:
                for _ in range(int(self._limits.max_concurrent or 1)):
                    self._admission.acquire(deadline)
                    taken += 1
                cursor = self._cursor()
                try:
                    report = self._maintain(cursor, cutoff, orphans)
                finally:
                    cursor.close()
            finally:
                for _ in range(taken):
                    self._admission.release()
                self._write_lock.release()
        finally:
            self._maintenance_lock.release()
        return report

    def _maintain(
        self, cursor: duckdb.DuckDBPyConnection, cutoff: str, orphans: str
    ) -> MaintenanceReport:
        lake: dict[str, object] = {"lake": LAKE}
        expire = (
            "SELECT count(*) FROM ducklake_expire_snapshots("
            "$lake, older_than => CAST($cutoff AS TIMESTAMPTZ))"
        )
        compact = (
            "SELECT coalesce(sum(files_processed), 0) || ',' || coalesce(sum(files_created), 0) "
            "FROM {function}($lake)"
        )
        try:
            _single_value(cursor, "SELECT count(*) FROM ducklake_flush_inlined_data($lake)", lake)
            expired = int(_single_value(cursor, expire, {**lake, "cutoff": cutoff}))
            merged, created = _single_value(
                cursor, compact.format(function="ducklake_merge_adjacent_files"), lake
            ).split(",")
            rewritten, rewritten_into = _single_value(
                cursor, compact.format(function="ducklake_rewrite_data_files"), lake
            ).split(",")
            expired += int(_single_value(cursor, expire, {**lake, "cutoff": cutoff}))
            deleted = _single_value(
                cursor,
                "SELECT count(*) FROM ducklake_cleanup_old_files($lake, cleanup_all => true)",
                lake,
            )
            orphaned = _single_value(
                cursor,
                "SELECT count(*) FROM ducklake_delete_orphaned_files("
                "$lake, older_than => CAST($orphans AS TIMESTAMPTZ))",
                {**lake, "orphans": orphans},
            )
            oldest = _snapshot_of(
                _single_value(
                    cursor,
                    "SELECT snapshot_id || ',' || epoch_us(snapshot_time) "
                    "FROM ducklake_snapshots($lake) ORDER BY snapshot_id LIMIT 1",
                    lake,
                ).split(",")
            )
        except duckdb.Error as error:
            raise StoreError(
                "STORE_QUERY_FAILED", "Lake maintenance failed.", {"duckdb": str(error)}
            ) from error
        return MaintenanceReport(
            kind="maintenance_report",
            snapshots_expired=expired,
            files_merged=int(merged),
            files_created=int(created) + int(rewritten_into),
            files_rewritten=int(rewritten),
            files_deleted=int(deleted),
            orphaned_files_deleted=int(orphaned),
            oldest_snapshot=oldest,
        )

    def tables(self) -> set[tuple[str, str]]:
        """(schema, table) of every table of the lake now, lower-cased."""
        return self._tables_at(self.current_snapshot().id)

    def current_snapshot(self) -> Snapshot:
        snapshot = self._snapshot(None)
        if snapshot is None:
            raise StoreError("STORE_SNAPSHOT_NOT_FOUND", "The lake has no snapshot.", {})
        return snapshot

    def _tables_at(
        self, snapshot: int, cursor: duckdb.DuckDBPyConnection | None = None
    ) -> set[tuple[str, str]]:
        """(schema, table) of every lake table alive at `snapshot`, lower-cased."""
        row = self._one(
            f"SELECT string_agg(lower(s.schema_name) || '.' || lower(t.table_name), '|') "
            f"FROM {self._metadata}.ducklake_table AS t "
            f"JOIN {self._metadata}.ducklake_schema AS s USING (schema_id) "
            "WHERE t.begin_snapshot <= $n AND (t.end_snapshot IS NULL OR t.end_snapshot > $n) "
            "AND s.begin_snapshot <= $n AND (s.end_snapshot IS NULL OR s.end_snapshot > $n)",
            {"n": snapshot},
            cursor,
        )
        text = str(row[0]) if row and row[0] is not None else ""
        found: set[tuple[str, str]] = set()
        for item in text.split("|"):
            schema, _, table = item.partition(".")
            if table:
                found.add((schema, table))
        return found

    def _pinned(
        self, tree: dict[str, Any], snapshot: int, cursor: duckdb.DuckDBPyConnection
    ) -> str:
        """The read's SELECT (`_read_tree`) with `AT (VERSION => snapshot)` on every table it
        references, on the read's cursor (under its guard, #160).

        Every base table is pinned, including CTE references (DuckDB binds a CTE with an AT
        clause unchanged). A name that is not a lake table (or the read's own CTE) is refused.
        """
        tables = self._tables_at(snapshot, cursor)
        schemas = {schema for schema, _ in tables}
        ctes = _cte_names(tree)
        for node in _walk(tree):
            if node.get("type") != "BASE_TABLE":
                continue
            qualified = node.get("qualified_name")
            raw = qualified.get("path") if isinstance(qualified, dict) else None
            path = (
                [str(part) for part in raw] if isinstance(raw, list) else [str(node["table_name"])]
            )
            if not _is_lake_table(path, tables, schemas, ctes):
                raise StoreError(
                    "STORE_STATEMENT_INVALID",
                    "A read may reference only tables of the lake (and its own CTEs).",
                    {"table": ".".join(path)},
                )
            node["at_clause"] = {
                "unit": "VERSION",
                "expr": {
                    "class": "CONSTANT",
                    "type": "VALUE_CONSTANT",
                    "alias": "",
                    "query_location": 0,
                    "query_location_length": 0,
                    "literal": {"kind": "INTEGER", "text": str(int(snapshot))},
                },
            }
        rewritten = self._one(
            "SELECT json_deserialize_sql($tree)", {"tree": json.dumps(tree)}, cursor
        )
        return str(rewritten[0]) if rewritten else ""

    def describe(self, sql: str, parameters: Sequence[Parameter] = ()) -> list[tuple[str, str]]:
        """(name, DuckDB type) of each output column of one SELECT, from the binder only.

        Nothing is executed. Sources may be named (writes read them); the statement checks of
        writes apply (no file readers, no quoted file names, no catalog database). Parameters
        are bound as for a write (typically with a `null` value: only their type matters).
        """
        sql = _plain(sql)
        statement = _single_statement(sql, self._table_functions)
        if statement.type != duckdb.StatementType.SELECT:
            raise StoreError(
                "STORE_STATEMENT_INVALID",
                "Only a SELECT can be described.",
                {"statement": statement.type.name},
            )
        bound = _bound(statement, list(parameters))
        typed = _typed(sql, list(parameters))
        self._admission.acquire(time.monotonic() + self._timeout_ms(None) / 1000)
        cursor = None
        try:
            cursor = self._cursor()
            relation = cursor.sql(typed, params=bound) if bound else cursor.sql(typed)
            return [
                (str(name), str(kind))
                for name, kind in zip(relation.columns, relation.types, strict=True)
            ]
        except duckdb.Error as error:
            raise StoreError(
                "STORE_QUERY_FAILED", "DuckDB could not bind the query.", {"duckdb": str(error)}
            ) from None
        finally:
            if cursor is not None:
                cursor.close()
            self._admission.release()

    def read(self, request: ReadRequest) -> pa.RecordBatchReader:
        """Run one SELECT as of `request.snapshot` and return it once, as Arrow batches."""
        return self.start_read(request).reader

    def _timeout_ms(self, requested: int | None) -> int:
        assert self._limits.default_timeout_ms is not None
        return requested if requested is not None else self._limits.default_timeout_ms

    def start_read(self, request: ReadRequest) -> ReadHandle:
        """Like `read`, with a handle that can cancel the read from another thread.

        The read waits for an admission slot, holds it until its stream ends, and is interrupted
        when its timeout (counted from the call, waiting included) expires.
        """
        sql = _plain(request.sql)
        statement = _single_statement(sql, self._table_functions)
        if statement.type != duckdb.StatementType.SELECT:
            raise StoreError(
                "STORE_STATEMENT_INVALID",
                "A read must be one SELECT statement.",
                {"statement": statement.type.name},
            )
        parameters = _bound(statement, request.parameters)
        typed = _typed(sql, request.parameters)
        tree = _read_tree(typed)  # before admission: no catalog needed (#160)
        deadline = time.monotonic() + self._timeout_ms(request.timeout_ms) / 1000
        self._admission.acquire(deadline)
        try:
            cursor = self._cursor()
        except BaseException:
            self._admission.release()
            raise
        guard = Guard(cursor, deadline)
        try:
            # Every query on the lake runs inside the slot (#77), and under the read's limit
            # (#160). The snapshot is checked once admitted: maintenance may have expired it
            # while this read waited behind it (maintenance runs alone, so it cannot expire it
            # from now on).
            if self._snapshot(request.snapshot, cursor) is None:
                raise self._missing_snapshot(request.snapshot, cursor)
            sql = self._pinned(tree, request.snapshot, cursor)
        except BaseException as error:
            guard.stop()
            cursor.close()
            self._admission.release()
            if isinstance(error, duckdb.Error):
                raise guard.error(error) from error
            raise
        # Who owns the cursor: the calling thread while it sets the read up ("setup"), nobody
        # while the stream waits for its first read ("idle") or while the consumer holds a batch
        # ("paused"), the reading thread while DuckDB computes a batch ("computing"). A read
        # nobody owns is closed by the guard when it fires (or by collection of a dropped
        # reader); the owner closes it otherwise. The slot is released exactly once, and before
        # anyone can see the read's error (#77).
        state = threading.Lock()
        phase = "setup"
        released = threading.Event()

        def close() -> None:
            """Called once, by whoever moved the read to "finished"."""
            try:
                guard.stop()
                cursor.close()
            finally:
                self._admission.release()
                released.set()

        def finish() -> None:
            nonlocal phase
            with state:
                already = phase == "finished"
                phase = "finished"
            if already:
                # Closed by another thread: its error is seen only once the slot is free.
                released.wait()
                return
            close()

        def release_if_idle() -> None:
            nonlocal phase
            # Checked and claimed in one step, so the reading thread cannot take the cursor back
            # in between (#159, Reviewer pass 1).
            with state:
                unowned = phase in {"idle", "paused"}
                if unowned:
                    phase = "finished"
            if unowned:
                close()

        guard.on_fire = release_if_idle

        try:
            # Binding is quick and returns before any row is computed; the work runs on the first
            # read of the stream, where a timeout or a cancel can still interrupt it.
            relation = cursor.sql(sql, params=parameters)
            names = list(relation.columns)
            types = [str(kind) for kind in relation.types]
            exact = exact_select(sql, names, types)
            if exact is not None:
                relation = cursor.sql(exact, params=parameters)
            empty = relation.limit(0).to_arrow_table()
            schema = _with_duckdb_types(_utc_schema(empty.schema), types)
        except duckdb.Error as error:
            finish()
            if isinstance(error, duckdb.InterruptException) or guard.reason:
                raise guard.error(error) from error
            raise StoreError(
                "STORE_QUERY_FAILED", "DuckDB could not run the read.", {"duckdb": str(error)}
            ) from error
        except BaseException:
            finish()
            raise
        if guard.reason is not None:
            # The limit expired or a cancel came while the read was being set up.
            finish()
            raise guard.error()
        with state:
            phase = "idle"
            fired = guard.reason is not None
        if fired:
            # It fired between the check above and now, and found the read still being set up.
            release_if_idle()

        def take() -> None:
            """The reading thread takes the cursor back; a read closed meanwhile raises its
            error, once its slot is free."""
            nonlocal phase
            with state:
                closed = phase == "finished"
                if not closed:
                    phase = "computing"
            if closed:
                finish()  # waits until the slot is released
                raise guard.error()

        def batches() -> Iterator[pa.RecordBatch]:
            # The cursor and the admission slot live as long as the stream.
            nonlocal phase
            take()
            try:
                if guard.reason is not None:
                    raise guard.error()
                stream = relation.to_arrow_reader(request.batch_size)
                while True:
                    try:
                        batch = stream.read_next_batch()
                    except StopIteration:
                        return
                    # Checked and paused in one step: a guard that fires before it is seen here,
                    # one that fires after finds the read paused and closes it (#159, Reviewer
                    # pass 1).
                    with state:
                        fired = guard.reason is not None
                        if not fired and phase == "computing":
                            phase = "paused"
                    if fired:
                        raise guard.error()
                    # While the consumer holds this batch, the guard may close the read.
                    yield batch if batch.schema == schema else batch.cast(schema)
                    take()
                    if guard.reason is not None:
                        raise guard.error()
            except (OSError, pa.ArrowException, duckdb.Error) as error:
                if guard.reason is not None or isinstance(error, duckdb.InterruptException):
                    raise guard.error(error) from error
                raise StoreError(
                    "STORE_QUERY_FAILED",
                    "DuckDB failed while streaming the read.",
                    {"duckdb": str(error)},
                ) from error
            finally:
                finish()

        reader = pa.RecordBatchReader.from_batches(schema, batches())
        # A stream dropped before its first batch never runs its generator, so its slot is
        # released when the reader is collected (a started stream releases it when closed).
        weakref.finalize(reader, release_if_idle)
        return ReadHandle(reader=reader, _guard=guard)

    @contextmanager
    def transaction(self) -> Iterator[Transaction]:
        """Statements inside the block commit together to one snapshot, or roll back on error.

        Write transactions in this process run one at a time, so the snapshot reported in the
        result is the one this transaction created.
        """
        if getattr(self._local, "writing", False):
            raise StoreError(
                "STORE_STATEMENT_INVALID",
                "A transaction cannot be opened inside another one in the same thread.",
                {},
            )
        deadline = time.monotonic() + self._timeout_ms(None) / 1000
        # The writer ahead first, then a slot, both under the same limit: a writer that only
        # waits holds no slot, so reads are not blocked behind it (#77). Maintenance takes the
        # writer lock before the slots too, so the two never wait for each other.
        if not self._write_lock.acquire(timeout=max(0.0, deadline - time.monotonic())):
            raise StoreError(
                "STORE_TIMEOUT", "The transaction waited too long for the writer ahead.", {}
            )
        try:
            self._admission.acquire(deadline)
            try:
                yield from self._transaction(deadline)
            finally:
                self._admission.release()
        finally:
            self._write_lock.release()

    def _transaction(self, deadline: float) -> Iterator[Transaction]:
        self._local.writing = True
        try:
            cursor = self._cursor()
        except BaseException:
            self._local.writing = False
            raise
        try:
            # The store's own catalog queries run under the transaction's limit too (#160).
            before, outside = _bounded(
                cursor,
                deadline,
                lambda: (self._snapshot(None, cursor), _outside_inventory(cursor)),
            )
            cursor.begin()
            handle = Transaction(
                cursor,
                self._table_functions,
                self._timeout_ms,
                frozenset(self._inexact),
                deadline,
            )
            try:
                yield handle
                # A statement interrupted inside the block decides the outcome, even when
                # the block caught its error (#77).
                if handle.interrupted is not None:
                    raise handle.interrupted
                if handle.cancelled:
                    raise StoreError("STORE_CANCELLED", "The transaction was cancelled.", {})
                changed_outside, unpinnable = handle.bounded(
                    lambda: (_outside_inventory(cursor) != outside, _unpinnable_objects(cursor))
                )
                if handle.cancelled:
                    # A cancel that came as the checks finished still stops the commit.
                    raise StoreError("STORE_CANCELLED", "The transaction was cancelled.", {})
                if changed_outside:
                    raise StoreError(
                        "STORE_STATEMENT_INVALID",
                        "A transaction may change only tables and schemas of the lake.",
                        {},
                    )
                if unpinnable:
                    raise StoreError(
                        "STORE_STATEMENT_INVALID",
                        "Views and macros are not allowed: a read could not pin the "
                        "tables they use to a snapshot.",
                        {},
                    )
                # The commit runs inside the transaction's limit; once started it is one
                # atomic catalog transaction and is not interrupted.
                if time.monotonic() >= deadline:
                    raise StoreError(
                        "STORE_TIMEOUT", "The transaction exceeded its time limit.", {}
                    )
            except BaseException:
                cursor.rollback()
                raise
            try:
                # The point of no return: the commit and the snapshot read after it are not
                # interrupted, so a committed transaction is never reported as failed (#160).
                _commit(cursor)
            except duckdb.TransactionException as error:
                raise StoreError(
                    "STORE_TRANSACTION_CONFLICT",
                    "Another writer changed the same data first; nothing was written.",
                    {"duckdb": str(error)},
                ) from error
            except duckdb.Error as error:
                raise StoreError(
                    "STORE_QUERY_FAILED",
                    "The transaction could not commit.",
                    {"duckdb": str(error)},
                ) from error
            after = self._snapshot(None)
        finally:
            cursor.close()
            self._local.writing = False
        changed = after is not None and (before is None or after.id != before.id)
        handle.result = CommitResult(kind="commit_result", snapshot=after if changed else None)


def _read_tree(sql: str) -> dict[str, Any]:
    """A read's parse tree, checked for what needs no catalog: SQL that does not parse,
    functions that run SQL given as text, `SUMMARIZE`/`DESCRIBE`/`SHOW`, and table functions
    other than generators (which could read the lake or its files without a snapshot). Parsed
    on a connection of its own, with no lake attached, so it runs before the read is admitted:
    an invalid read gets its own error on a busy lake (#160)."""
    parser = _parser()
    try:
        serialized = parser.execute("SELECT json_serialize_sql($sql)", {"sql": sql}).fetchone()
    finally:
        parser.close()
    tree: dict[str, Any] = json.loads(str(serialized[0])) if serialized else {"error": True}
    if tree.get("error"):
        raise StoreError(
            "STORE_QUERY_FAILED",
            "The SQL could not be parsed.",
            {"duckdb": str(tree.get("error_message", ""))},
        )
    for node in _walk(tree):
        if (
            node.get("class") == "FUNCTION"
            and str(node.get("function_name", "")).lower() in _EXTERNAL_SQL_FUNCTIONS
        ):
            raise StoreError(
                "STORE_STATEMENT_INVALID",
                "This function is not allowed in a read.",
                {"function": str(node.get("function_name"))},
            )
        if node.get("type") == "SHOW_REF":
            # SUMMARIZE / DESCRIBE / SHOW: unpinned, and a quoted target is read as a file.
            raise StoreError(
                "STORE_STATEMENT_INVALID",
                "SUMMARIZE, DESCRIBE and SHOW are not allowed in statements.",
                {},
            )
        if node.get("type") == "TABLE_FUNCTION":
            function = node.get("function")
            name = function.get("function_name") if isinstance(function, dict) else None
            if str(name).lower() not in READ_TABLE_FUNCTIONS:
                raise StoreError(
                    "STORE_STATEMENT_INVALID",
                    "A read may use only the table functions range, generate_series and unnest.",
                    {"function": str(name)},
                )
    return tree


_PARSER_LOCK = threading.Lock()
_PARSER_DATABASE: list[duckdb.DuckDBPyConnection] = []


def _parser() -> duckdb.DuckDBPyConnection:
    """A cursor on the process's one in-memory DuckDB database for parsing only: no lake, no
    extension, no file access. One database for every thread (a cursor each call), so parsing
    adds no DuckDB instance per thread (#160, Reviewer pass 1). The caller closes the cursor."""
    with _PARSER_LOCK:
        if not _PARSER_DATABASE:
            database = duckdb.connect(config={"enable_external_access": False})
            database.execute("SET autoload_known_extensions = false")
            _PARSER_DATABASE.append(database)
        return _PARSER_DATABASE[0].cursor()


def _commit(cursor: duckdb.DuckDBPyConnection) -> None:
    """Commit the write transaction (a function of its own, so tests can stand in a slow one)."""
    cursor.commit()


def _bounded[T](cursor: duckdb.DuckDBPyConnection, deadline: float, work: Callable[[], T]) -> T:
    """`work`, a few queries on `cursor`, interrupted at `deadline` (`STORE_TIMEOUT`)."""
    guard = Guard(cursor, deadline)
    try:
        if guard.reason is not None:
            raise guard.error()
        return work()
    except duckdb.Error as error:
        raise guard.error(error) from error
    finally:
        guard.stop()


def _snapshot_of(row: Sequence[object]) -> Snapshot:
    """A snapshot from (id, microseconds since the epoch): no time-zone library is needed."""
    committed = dt.datetime(1970, 1, 1, tzinfo=dt.UTC) + dt.timedelta(microseconds=int(str(row[1])))
    return Snapshot(kind="snapshot", id=int(str(row[0])), committed_at=committed)


def _unpinnable_objects(cursor: duckdb.DuckDBPyConnection) -> int:
    """Views in the lake and user macros anywhere, read inside the open transaction.

    Both can read lake tables from inside a function call or a view body, where a read's
    snapshot rewrite cannot reach them, so they are not allowed to exist.
    """
    result = cursor.execute(
        "SELECT (SELECT count(*) FROM duckdb_views() "
        "        WHERE lower(database_name) = $lake AND NOT internal) "
        "     + (SELECT count(*) FROM duckdb_functions() "
        "        WHERE NOT internal AND function_type IN ('macro', 'table_macro'))",
        {"lake": LAKE},
    )
    row = result.fetchone()
    result.fetchone()  # reaches the end of the one-row result, so the cursor is free again
    return int(str(row[0])) if row else 0


def _single_value(
    cursor: duckdb.DuckDBPyConnection, sql: str, parameters: dict[str, object]
) -> str:
    """One value of a one-row query on `cursor`, leaving the cursor free for the next statement."""
    result = cursor.execute(sql, parameters)
    row = result.fetchone()
    result.fetchone()  # reaches the end of the one-row result
    return "" if not row or row[0] is None else str(row[0])


def _outside_inventory(cursor: duckdb.DuckDBPyConnection) -> str:
    """Every user object outside the lake: tables, views, sequences, types, macros, secrets."""
    return _single_value(
        cursor,
        "SELECT string_agg(item, '|' ORDER BY item) FROM ("
        " SELECT 'table:' || database_name || '.' || schema_name || '.' || table_name AS item"
        " FROM duckdb_tables() WHERE lower(database_name) NOT IN ($lake, $metadata)"
        " UNION ALL SELECT 'view:' || database_name || '.' || schema_name || '.' || view_name"
        " FROM duckdb_views() WHERE NOT internal AND lower(database_name) NOT IN ($lake, $metadata)"
        " UNION ALL SELECT 'sequence:' || database_name || '.' || schema_name || '.'"
        " || sequence_name"
        " FROM duckdb_sequences() WHERE lower(database_name) NOT IN ($lake, $metadata)"
        " UNION ALL SELECT 'type:' || database_name || '.' || schema_name || '.' || type_name"
        " FROM duckdb_types() WHERE NOT internal AND lower(database_name) NOT IN ($lake, $metadata)"
        " UNION ALL SELECT 'secret:' || name FROM duckdb_secrets()"
        ")",
        {"lake": LAKE, "metadata": METADATA},
    )


def _created_kind_allowed(sql: str, statement: duckdb.Statement) -> bool:
    """CREATE, DROP and ALTER may only target tables and schemas (decided from the tokens)."""
    if statement.type not in {
        duckdb.StatementType.CREATE,
        duckdb.StatementType.DROP,
        duckdb.StatementType.ALTER,
    }:
        return True
    tokens = _tokens(sql)
    ends = [position for position, _ in tokens[1:]] + [len(sql)]
    words = [
        sql[position:end].strip().upper()
        for (position, kind), end in zip(tokens, ends, strict=True)
        if kind == duckdb.token_type.keyword
    ][:6]
    modifiers = {"OR", "REPLACE", "TEMP", "TEMPORARY", "PERSISTENT", "IF", "NOT", "EXISTS"}
    target = next((word for word in words[1:] if word not in modifiers), "")
    return target in {"TABLE", "SCHEMA"}


class Transaction:
    """A write transaction; `result` is set when the block commits."""

    def __init__(
        self,
        cursor: duckdb.DuckDBPyConnection,
        table_functions: frozenset[str],
        timeout_ms: Callable[[int | None], int],
        inexact: frozenset[Inexact] = frozenset(),
        deadline: float = math.inf,
    ) -> None:
        self._cursor = cursor
        # The transaction's own limit: no statement of it runs past it (#160).
        self._deadline = deadline
        self._table_functions = table_functions
        self._inexact = inexact
        self._timeout_ms = timeout_ms
        self._guard: Guard | None = None
        self._lock = threading.Lock()
        self.cancelled = False
        # The first statement interrupted by its time limit or a cancel: the transaction's
        # outcome, even if the block catches the error (#77).
        self.interrupted: StoreError | None = None
        self.result: CommitResult | None = None

    def _interrupted(self, error: StoreError) -> StoreError:
        if error.code in {"STORE_TIMEOUT", "STORE_CANCELLED"} and self.interrupted is None:
            self.interrupted = error
        return error

    def _statement_deadline(self, timeout_ms: int | None) -> float:
        """A statement's deadline: its own limit, capped by the transaction's (#160)."""
        return min(time.monotonic() + self._timeout_ms(timeout_ms) / 1000, self._deadline)

    def bounded[T](self, work: Callable[[], T]) -> T:
        """`work`, the store's own queries on this transaction's cursor, under the transaction's
        limit and its cancel, like a statement (#160)."""
        guard = Guard(self._cursor, self._deadline)
        with self._lock:
            if self.cancelled:
                guard.stop()
                raise StoreError("STORE_CANCELLED", "The transaction was cancelled.", {})
            self._guard = guard
        try:
            if guard.reason is not None:
                raise guard.error()
            return work()
        except duckdb.Error as error:
            raise self._interrupted(guard.error(error)) from error
        finally:
            guard.stop()
            self._guard = None

    def cancel(self) -> None:
        """Cancel from any thread: a running statement is interrupted, and the block rolls back."""
        with self._lock:
            self.cancelled = True
            guard = self._guard
        if guard is not None:
            guard.cancel()

    def value(self, request: StatementRequest) -> str | None:
        """The first column of the first row of one SELECT run inside this transaction, as text.

        For small checks between statements (counts, existence): it sees the transaction's
        uncommitted changes. `None` when there is no row or the value is NULL.
        """
        sql = _plain(request.sql)
        statement = _single_statement(sql, self._table_functions)
        if statement.type != duckdb.StatementType.SELECT:
            raise StoreError(
                "STORE_STATEMENT_INVALID",
                "A transaction value must come from one SELECT.",
                {"statement": statement.type.name},
            )
        parameters = _bound(statement, request.parameters)
        deadline = self._statement_deadline(request.timeout_ms)
        guard = Guard(self._cursor, deadline)
        with self._lock:
            if self.cancelled:
                guard.stop()
                raise StoreError("STORE_CANCELLED", "The transaction was cancelled.", {})
            self._guard = guard
        typed = _typed(sql, request.parameters)
        try:
            inexact = _inexact_column_read(self._cursor, typed, parameters, self._inexact)
            if inexact is not None:
                raise StoreError(
                    "UNSUPPORTED_SOURCE_TYPE",
                    f"Column {inexact} has no exact type in DuckDB (law 11); cast it at the "
                    "source or leave it out.",
                    {"column": inexact},
                )
            result = self._cursor.execute(
                f"SELECT CAST(#1 AS VARCHAR) FROM ({typed}) LIMIT 1", parameters
            )
            row = result.fetchone()
            result.fetchone()  # reaches the end of the result, so the cursor is free again
        except duckdb.Error as error:
            raise self._interrupted(guard.error(error)) from error
        except RuntimeError as error:
            if _user_interrupt(error):
                raise KeyboardInterrupt from error
            raise
        finally:
            guard.stop()
            self._guard = None
        return None if not row or row[0] is None else str(row[0])

    def execute(self, request: StatementRequest) -> None:
        sql = _plain(request.sql)
        statement = _single_statement(sql, self._table_functions)
        if statement.type not in WRITE_STATEMENTS or not _created_kind_allowed(sql, statement):
            raise StoreError(
                "STORE_STATEMENT_INVALID",
                "This statement kind is not allowed in a transaction.",
                {"statement": statement.type.name},
            )
        parameters = _bound(statement, request.parameters)
        deadline = self._statement_deadline(request.timeout_ms)
        guard = Guard(self._cursor, deadline)
        # Checked and published together, so a cancel either sees this guard or is seen here.
        with self._lock:
            if self.cancelled:
                guard.stop()
                raise StoreError("STORE_CANCELLED", "The transaction was cancelled.", {})
            self._guard = guard
        typed = _typed(sql, request.parameters)
        try:
            inexact = _inexact_column_read(self._cursor, typed, parameters, self._inexact)
            if inexact is not None:
                raise StoreError(
                    "UNSUPPORTED_SOURCE_TYPE",
                    f"Column {inexact} has no exact type in DuckDB (law 11); cast it at the "
                    "source or leave it out.",
                    {"column": inexact},
                )
            self._cursor.sql(typed, params=parameters)
        except duckdb.Error as error:
            raise self._interrupted(guard.error(error)) from error
        except RuntimeError as error:
            if _user_interrupt(error):
                raise KeyboardInterrupt from error
            raise
        finally:
            guard.stop()
            self._guard = None

    def load_arrow(
        self,
        schema: str,
        table: str,
        reader: pa.RecordBatchReader,
        columns: Sequence[tuple[str, str]],
        on_interrupt: Callable[[], None] | None = None,
        timeout_ms: int | None = None,
    ) -> int:
        """Create `schema.table` in the lake, inside this transaction, from an Arrow stream: each
        column `(name, DuckDB type)` is the stream's field of that name cast to that type, in
        order. The stream is read once, batch by batch, by DuckDB (no row passes through Python),
        and the number of rows is returned.

        `on_interrupt` is called from another thread when the statement times out or is
        cancelled, and on Ctrl-C when the load runs in the main thread (to close the stream's
        source, so a remote query stops at once even while it sends nothing). Ctrl-C then raises
        `KeyboardInterrupt`. An exception raised by the stream itself propagates unchanged; the
        caller decides what it means.
        """
        names = [name for name, _ in columns]
        if len({name.lower() for name in names}) != len(names):
            # DuckDB resolves names without case: `a` and `A` would read the same field.
            raise StoreError(
                "STORE_STATEMENT_INVALID",
                "Column names must differ in more than letter case.",
                {"columns": ", ".join(names)},
            )
        if not columns or reader.schema.names != names:
            raise StoreError(
                "STORE_STATEMENT_INVALID",
                "The Arrow stream's fields must be exactly the declared columns, in order.",
                {"fields": ", ".join(reader.schema.names), "columns": ", ".join(names)},
            )
        for _, kind in columns:
            if _ARROW_LOAD_TYPE.fullmatch(kind) is None:
                raise StoreError(
                    "STORE_STATEMENT_INVALID", "Not a DuckDB type name.", {"type": kind}
                )
        failure: list[BaseException] = []

        def batches() -> Iterator[pa.RecordBatch]:
            try:
                yield from reader
            except BaseException as error:
                failure.append(error)
                raise

        stream = pa.RecordBatchReader.from_batches(reader.schema, batches())
        view = f"__arrow_{uuid.uuid4().hex}"
        target = f"{_identifier(schema)}.{_identifier(table)}"
        selected = ", ".join(
            f"CAST({_identifier(name)} AS {kind}) AS {_identifier(name)}" for name, kind in columns
        )
        deadline = self._statement_deadline(timeout_ms)
        guard = Guard(self._cursor, deadline)
        guard.on_fire = on_interrupt
        with self._lock:
            if self.cancelled:
                guard.stop()
                raise StoreError("STORE_CANCELLED", "The transaction was cancelled.", {})
            self._guard = guard
        pressed: list[bool] = []
        restore = _on_ctrl_c(on_interrupt, pressed)
        try:
            self._cursor.sql(f"CREATE SCHEMA IF NOT EXISTS {_identifier(schema)}")
            self._cursor.register(view, stream)
            try:
                self._cursor.execute(f"CREATE TABLE {target} AS SELECT {selected} FROM {view}")
            finally:
                self._cursor.unregister(view)
            if guard.reason is not None:
                # Interrupted, even if the statement ended: what it read is not the whole stream.
                raise self._interrupted(guard.error())
            counted = self._cursor.execute(f"SELECT count(*) FROM {target}").fetchone()
            if pressed:
                raise KeyboardInterrupt
        except duckdb.Error as error:
            if pressed:
                raise KeyboardInterrupt from None
            if failure and guard.reason is None:
                raise failure[0] from None
            raise self._interrupted(guard.error(error)) from error
        except StoreError:
            raise
        except Exception as error:
            # The stream's own error: unchanged, unless the statement was interrupted, which is
            # then the reason (closing the source on interrupt makes the stream fail).
            if pressed:
                raise KeyboardInterrupt from None
            if guard.reason is not None:
                raise self._interrupted(guard.error()) from None
            if isinstance(error, RuntimeError) and _user_interrupt(error):
                raise KeyboardInterrupt from error
            raise
        finally:
            restore()
            guard.stop()
            self._guard = None
        if pressed:  # pressed while the load was finishing: never lost
            raise KeyboardInterrupt
        return int(counted[0]) if counted else 0


def _on_ctrl_c(on_interrupt: Callable[[], None] | None, pressed: list[bool]) -> Callable[[], None]:
    """While a load runs in the main thread, Ctrl-C calls `on_interrupt` at once and is noted in
    `pressed`; the load then raises `KeyboardInterrupt` itself. Returns the restoring function.

    Python runs signal handlers only in the main thread, and only when it gets control: DuckDB,
    waiting for a stream blocked on a quiet network read, may not give it back until the next
    rows arrive (7.9 s measured on CI). So the signal is also watched from another thread through
    Python's wakeup file descriptor, which the C-level handler writes to as the signal arrives.
    """
    if on_interrupt is None or threading.current_thread() is not threading.main_thread():
        return lambda: None
    previous = signal.getsignal(signal.SIGINT)
    if previous is signal.SIG_IGN or previous is None:  # Ctrl-C is ignored, or not Python's
        return lambda: None

    once = threading.Lock()

    def interrupt() -> None:
        # Both the handler and the watcher see the signal; the source is closed once.
        with once:
            first = not pressed
            pressed.append(True)
        if first:
            with contextlib.suppress(Exception):
                on_interrupt()

    def handler(number: int, frame: FrameType | None) -> None:
        interrupt()

    receiver, sender = socket.socketpair()
    sender.setblocking(False)
    stop = threading.Event()
    try:
        signal.signal(signal.SIGINT, handler)
        earlier = signal.set_wakeup_fd(sender.fileno(), warn_on_full_buffer=False)
    except ValueError:  # not the main interpreter
        signal.signal(signal.SIGINT, previous)
        receiver.close()
        sender.close()
        return lambda: None

    def watch() -> None:
        while not stop.is_set():
            ready, _, _ = select.select([receiver], [], [], 0.2)
            if not ready:
                continue
            with contextlib.suppress(OSError):
                received = receiver.recv(256)
                if earlier != -1:
                    os.write(earlier, received)  # whoever watched signals before still sees them
                if signal.SIGINT in received:
                    interrupt()

    watcher = threading.Thread(target=watch, name="load-ctrl-c", daemon=True)
    watcher.start()

    def restore() -> None:
        stop.set()
        signal.set_wakeup_fd(earlier)
        signal.signal(signal.SIGINT, previous)
        watcher.join()
        receiver.close()
        sender.close()

    return restore


def _user_interrupt(error: RuntimeError) -> bool:
    """Whether a `RuntimeError` from a statement is Ctrl-C.

    DuckDB's Python client (measured on 2.0) answers Ctrl-C during a statement by interrupting
    it and raising `RuntimeError("Query interrupted")`, consuming the signal. It is given back to
    the caller as `KeyboardInterrupt`, so an interrupted program stops as Python programs do
    (the transaction still rolls back on the way out). `test_ctrl_c_is_a_keyboard_interrupt`
    pins the measured behavior.
    """
    return type(error) is RuntimeError and str(error) == "Query interrupted"


def _inexact_column_read(
    cursor: duckdb.DuckDBPyConnection,
    sql: str,
    parameters: dict[str, str | None],
    inexact: frozenset[Inexact],
) -> str | None:
    """`source.table.column` of an inexact source column the statement reads, or None.

    The columns a statement reads are those DuckDB's plan scans (projections and pushed-down
    filters). A scan is matched by its table name and, when the plan names one, its schema or
    source; when two candidates share a name, any of them counts (refusing beats a lost digit).
    A statement that names none of these sources is not planned twice.
    """
    lowered = sql.lower()
    # Every inexact column of a source the statement names: a SQLite view is expanded by
    # DuckDB, so its scan names the underlying table, not the view.
    candidates = [entry for entry in inexact if entry.source in lowered]
    if not candidates:
        return None
    result = cursor.execute(f"EXPLAIN (FORMAT json) {sql}", parameters)
    row = result.fetchone()
    result.fetchone()  # reaches the end of the one-row result, so the cursor is free again
    plan = json.loads(str(row[1])) if row else []
    for node in _walk(plan):
        info = node.get("extra_info")
        table = info.get("Table") if isinstance(info, dict) else None
        if not isinstance(info, dict) or not isinstance(table, str):
            continue
        scanned = table.lower().replace('"', "").split(".")
        for entry in candidates:
            if scanned[-1] != entry.table:
                continue
            qualifiers = scanned[:-1]
            if qualifiers and not ({entry.schema, entry.source} & set(qualifiers)) and entry.schema:
                continue
            if "Projections" not in info:
                return f"{entry.source}.{entry.label}"  # columns unknown: assume all are read
            used = f"{info.get('Projections')} {info.get('Filters', '')}".lower()
            if re.search(rf"(?<![a-z0-9_]){re.escape(entry.column)}(?![a-z0-9_])", used):
                return f"{entry.source}.{entry.label}"
    return None


type JsonTree = dict[str, object]


def _walk(node: object) -> Iterator[dict[str, object]]:
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item)


def _cte_names(tree: JsonTree) -> set[str]:
    names: set[str] = set()
    for node in _walk(tree):
        cte_map = node.get("cte_map")
        if isinstance(cte_map, dict):
            for entry in cte_map.get("map", []):
                if isinstance(entry, dict) and isinstance(entry.get("key"), str):
                    names.add(entry["key"].lower())
    return names


def _is_lake_table(
    path: list[str], tables: set[tuple[str, str]], schemas: set[str], ctes: set[str]
) -> bool:
    """Whether a FROM name is a lake table alive at the snapshot, or one of the query's CTEs.

    Names that look like files or URLs are refused outright: DuckDB would otherwise fall back to
    reading them as files when no table matches.
    """
    parts = [part.lower() for part in path]
    if any(_SUSPICIOUS.search(part) for part in parts):
        return False
    if len(parts) == 1:
        return parts[0] in ctes or ("main", parts[0]) in tables
    if len(parts) == 2:
        if parts[0] in schemas:
            return (parts[0], parts[1]) in tables
        return parts[0] == LAKE and ("main", parts[1]) in tables
    return len(parts) == 3 and parts[0] == LAKE and (parts[1], parts[2]) in tables


def exact_select(sql: str, names: list[str], types: list[str]) -> str | None:
    """`sql` with every UHUGEINT column cast to decimal text, or None when there is none.

    Arrow has no exact 128-bit unsigned type: DuckDB would wrap large values into
    decimal128(38,0) (the maximum becomes -1). Columns are addressed by position (`#n`), so
    duplicate names are kept as they are and nothing else changes.
    """
    if "UHUGEINT" not in types:
        return None
    columns = ", ".join(
        (f"CAST(#{index} AS VARCHAR)" if kind == "UHUGEINT" else f"#{index}")
        + f" AS {_identifier(name)}"
        for index, (name, kind) in enumerate(zip(names, types, strict=True), start=1)
    )
    return f"SELECT {columns} FROM ({sql})"


def _identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _with_duckdb_types(schema: pa.Schema, types: list[str]) -> pa.Schema:
    """Record each column's DuckDB type in its field metadata (`duckdb_type`).

    Arrow alone cannot tell `HUGEINT` from `DECIMAL(38,0)`; consumers (P1-03) need the type.
    """
    fields = [
        field.with_metadata({**(field.metadata or {}), b"duckdb_type": kind.encode()})
        for field, kind in zip(schema, types, strict=True)
    ]
    return pa.schema(fields, metadata=schema.metadata)


def _utc_schema(schema: pa.Schema) -> pa.Schema:
    """Name every zoned timestamp's zone `UTC` (DuckDB may say `Etc/UTC`); values are unchanged."""
    fields = [
        field.with_type(pa.timestamp(field.type.unit, tz="UTC"))
        if pa.types.is_timestamp(field.type) and field.type.tz is not None
        else field
        for field in schema
    ]
    return pa.schema(fields, metadata=schema.metadata)


_CSV_TYPE_CANDIDATES = "['BOOLEAN', 'BIGINT', 'DATE', 'TIMESTAMP', 'TIMESTAMPTZ', 'VARCHAR']"


def _wide_decimals(source: str, files: Sequence[str]) -> str:
    """` REPLACE (…)` for every top-level Parquet column that holds, at any depth (struct
    field, list element, map key or value), a DECIMAL wider than 38 digits in any of `files`:
    DuckDB reads such a value as DOUBLE, so reading the column fails instead (law 11). Queries
    that do not read it are unaffected. "" when there is none.

    The schemas are read with pyarrow from each file's footer (metadata only, no rows). It
    resolves both Parquet's converted and logical decimal annotations."""
    widest: dict[str, tuple[str, int]] = {}
    for file in files:
        try:
            schema = pq.read_schema(file)
        except (OSError, pa.ArrowException) as error:
            raise StoreError(
                "STORE_SOURCE_INVALID",
                f"Source {source} cannot be read as parquet.",
                {"source": source, "file": file, "duckdb": str(error)},
            ) from None
        for field in schema:
            digits = _widest_decimal(field.type)
            key = field.name.lower()
            if digits > 38 and digits > widest.get(key, ("", 0))[1]:
                widest[key] = (field.name, digits)
    if not widest:
        return ""
    parts = []
    for name, digits in sorted(widest.values()):
        message = (
            f"UNSUPPORTED_SOURCE_TYPE: column {name} of source {source} holds a DECIMAL with "
            f"{digits} digits, which cannot be read exactly."
        )
        parts.append(f"error({_quote(message)}) AS {_identifier(name)}")
    return f" REPLACE ({', '.join(parts)})"


def _widest_decimal(kind: pa.DataType) -> int:
    """The largest DECIMAL precision anywhere inside an Arrow type (0 when there is none)."""
    if pa.types.is_decimal(kind):
        return int(kind.precision)
    if pa.types.is_struct(kind):
        return max((_widest_decimal(kind.field(i).type) for i in range(kind.num_fields)), default=0)
    if pa.types.is_map(kind):
        return max(_widest_decimal(kind.key_type), _widest_decimal(kind.item_type))
    if (
        pa.types.is_list(kind)
        or pa.types.is_large_list(kind)
        or pa.types.is_fixed_size_list(kind)
        or pa.types.is_list_view(kind)
        or pa.types.is_large_list_view(kind)
    ):
        return _widest_decimal(kind.value_type)
    return 0


class Inexact(NamedTuple):
    """A source column DuckDB cannot carry exactly."""

    source: str
    schema: str  # "" for SQLite
    table: str
    column: str
    label: str  # how errors name it, after the source name


type AnySource = (
    SourceAttachment | FileSourceAttachment | SqliteSourceAttachment | PostgresSourceAttachment
)


def _source_password(source: PostgresSourceAttachment) -> str:
    _require_source_tls(source)
    _source_tls(source)  # an unset CA variable is refused before anything is opened
    password = os.environ.get(source.password_env, "")
    if not password:
        raise StoreError(
            "STORE_SECRET_MISSING",
            f"The environment variable {source.password_env} is not set.",
            {"variable": source.password_env, "source": source.name},
        )
    register_secret(password)
    return password


def _require_source_tls(source: PostgresSourceAttachment) -> None:
    """Refuse a PostgreSQL source on another machine unless it is reached with
    `sslmode=verify-full` (#122): the password and the production rows would otherwise travel
    over a connection that does not prove the server is the named host. The same rule as a
    catalog's (`_local_connection`), environment included."""
    if _local_connection({"host": source.host}) or source.sslmode == "verify-full":
        return
    raise StoreError(
        "STORE_TLS_REQUIRED",
        f"Source {source.name} is on another machine and must be reached with "
        "sslmode=verify-full (TLS that checks the server's certificate and name).",
        {"source": source.name, "host": source.host, "sslmode": source.sslmode or "(none)"},
    )


def _source_tls(source: PostgresSourceAttachment) -> str:
    """The source's TLS settings as libpq connection keywords (quoted values)."""
    if source.sslmode is None:
        return ""
    settings = f" sslmode={_libpq_value(source.sslmode)}"
    if source.sslmode == "verify-full":
        root = source.sslrootcert or "system"
        if root != "system":
            match = _VARIABLE.fullmatch(root)
            assert match is not None  # the contract's pattern
            variable = match.group(1)
            root = os.environ.get(variable, "")
            if not root:
                raise StoreError(
                    "STORE_SECRET_MISSING",
                    f"The environment variable {variable} is not set (the certificate "
                    f"authorities of source {source.name}).",
                    {"variable": variable, "source": source.name},
                )
        settings += f" sslrootcert={_libpq_value(root)}"
    return settings


def _libpq_value(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _sqlite_snapshot(source: SqliteSourceAttachment, copy: Path) -> list[tuple[str, str, str]]:
    """Copy the SQLite file to `copy` and return (table, column, declared type) of the copy.

    A file with views is refused: DuckDB turns a SQLite view's SQL into a DuckDB view that runs
    with the lake's connection, where it could call functions the statement guard refuses (and
    run SQL on other sources). Triggers never fire on a read-only attachment.
    """
    uri = Path(source.path).absolute().as_uri() + "?mode=ro"
    failure: StoreError | None = None
    found: list[tuple[str, str, str]] = []
    try:
        original = sqlite3.connect(uri, uri=True)
        target = sqlite3.connect(copy)
        try:
            original.backup(target)
        finally:
            original.close()
        try:
            views = target.execute("SELECT count(*) FROM sqlite_master WHERE type = 'view'")
            row = views.fetchone()
            if row and int(row[0]):
                failure = StoreError(
                    "STORE_SOURCE_INVALID",
                    f"Source {source.name} defines views, which a lake cannot attach.",
                    {"source": source.name},
                )
            tables = target.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
            for (table,) in list(tables):
                for name, kind in target.execute(
                    "SELECT name, type FROM pragma_table_info(?)", (table,)
                ):
                    found.append((str(table), str(name), str(kind or "")))
        finally:
            target.close()
    except sqlite3.Error as error:
        failure = StoreError(
            "STORE_SOURCE_INVALID",
            f"Source {source.name} is not a readable SQLite database.",
            {"source": source.name, "sqlite": str(error)},
        )
    if failure is not None:
        raise failure
    return found


_GLOB_IN_NAME = re.compile(r"[*?\[]")


def _source_not_found(source: FileSourceAttachment | SqliteSourceAttachment) -> StoreError:
    return StoreError(
        "SOURCE_NOT_FOUND",
        f"No file matches the path of source {source.name}.",
        {"source": source.name},
    )


def open_lake(
    location: LakeLocation | AnyLakeLocation,
    limits: Limits,
    sources: Sequence[AnySource] = (),
) -> Lake:
    return Lake(location, limits, sources)
