# `store` contract v1

**Producer:** `store` (`orvex.store`). **Consumers:** `transform` (writes, from P1), `semantic` (reads, from P2), `server`.

How every other module reads and writes analytical data in the lake (DuckLake), without knowing DuckDB.

## Messages (`schema.json`)

| Message | Purpose |
|---|---|
| `LakeLocation` | Where the lake is: `LocalLakeLocation` (`catalog: local`, a DuckDB-file catalog at `<path>/catalog.ducklake` or at `catalog_path`, data files under `<path>/data/`, one process), `SqliteLakeLocation` (`catalog: sqlite`, see below) or `PostgresLakeLocation` (`catalog: postgres`, see below) |
| `StatementRequest` | One SQL statement run inside a write transaction (`CREATE TABLE … AS`, `INSERT … SELECT`, `MERGE`, `UPDATE`, `DELETE`, `DROP`, `ALTER`) |
| `ReadRequest` | One `SELECT` read **once**, as of `snapshot`, in batches of `batch_size` rows |
| `Parameter` | A value bound to `$name`; `value` is text (or `null`) converted to the declared DuckDB type, so exact values never travel as JSON numbers. Values are never concatenated into SQL. `DATE`, `TIMESTAMP` and `TIMESTAMPTZ` values are ISO 8601 (`YYYY-MM-DD`, `YYYY-MM-DDTHH:MM:SS[.ffffff]`, the same with `Z` or `±HH:MM`), or `"infinity"` / `"-infinity"` |
| `Snapshot` | A lake snapshot: `id` and `committed_at` (with offset) |
| `CommitResult` | The one snapshot a committed transaction created, or `null` when it changed nothing |
| `MaintenanceReport` | What one `maintain` call did (below) |
| `StoreError` | `code`, `message`, `details` |
| `ResultJson` | A read result as JSON: `columns` (`name`, DuckDB `type`) and positional `rows`, encoded exactly (below) |
| `SourceAttachment` | A source given to `open_lake` — files (`parquet`, `csv`, `duckdb`) or databases (`sqlite`, `postgres`); readable by name inside write transactions only (below) |

## File sources (`SourceAttachment`)

`open_lake(location, limits, sources)` makes each source a read-only catalog named after its connection, before the file sandbox is set:

| `type` | Tables |
|---|---|
| `parquet`, `csv` | one table `<name>.data`: every file matching `path` (a glob is allowed), columns united by name, types inferred by DuckDB, never as floating point unless the source holds floating point (law 11, #252):
  - **CSV:** inference uses only `BOOLEAN`, `BIGINT`, `DATE`, `TIMESTAMP`, `TIMESTAMPTZ` and `VARCHAR`, and reads every row of every file, not a sample. A decimal column, or an integer beyond `BIGINT`, arrives as its exact text, and the model casts it (`CAST(amount AS DECIMAL(14, 2))`).
  - **Parquet:** some top-level columns hold, at any depth (directly, or in a struct field, list element or map key or value), a `DECIMAL` wider than 38 digits in any matched file. Such a column cannot be read: reading it fails with a message naming `UNSUPPORTED_SOURCE_TYPE`, the source and the column. The other columns stay readable. Each file's Parquet schema is read by DuckDB (`parquet_schema`, footer metadata only), under both of Parquet's decimal annotations; Arrow metadata stored in a file never hides a column.

  A matched file whose name has `*`, `?` or `[` is `STORE_SOURCE_INVALID` (`details.file`): DuckDB's readers would expand it again and read another file (#82) |
| `duckdb` | the file attached `READ_ONLY`: `<name>.<schema>.<table>`, and `<name>.<table>` for schema `main` |

- Only **write transactions** read sources; a read (`ReadRequest`) still sees only the lake (`STORE_STATEMENT_INVALID`).
- The file sandbox gains exactly the declared files: for a glob, the files it matches when the lake opens (a file added later, or any other file in the same folder, stays unreadable).
- Statements still cannot call file readers (`read_parquet`, `read_csv`, …) or functions that bind or run SQL given as text (`json_serialize_plan`, `json_execute_serialized_sql`, `query`, …; a read's parse tree is checked for them too), and a quoted string or name that names an existing file, or that is an object-storage address (`s3://`, `s3a://`, `s3n://`, `gs://`, `gcs://`, `r2://`, `az://`, `azure://`, `abfs://`, `abfss://`, `hf://`, any case), is refused wherever it appears (`FROM '…'`, `MERGE … USING '…'`, `PIVOT '…'`, …), in every quoting form (`'…'`, `E'…'`, `$$…$$`, `$tag$…$tag$` with any tag, `"…"`), and so is a string starting with `file:/` (`file:/p`, `file:///p`, `file://localhost/p`, which DuckDB opens as local files), a statement containing a NUL character (DuckDB opens a path only up to it), and a dotted name whose parts, joined with `.`, name an existing file or match files (`"<dir>/x".parquet`, `x.parquet`; DuckDB reads such a name as a file when no table has it): a model reaches a file only through its connection (`STORE_STATEMENT_INVALID`). A single string constant starting with one of those addresses is therefore refused; web addresses (`https://…`) are not. (Text built from several constants is not checked. It is safe only because no function a statement may call opens a file named by an expression: file readers, functions that run SQL given as text, and every table function but `range`, `generate_series` and `unnest` are refused by name. A table function that shares its name with a scalar or aggregate function is refused in every form, unless it is known to read nothing (`repeat`, `version`): `histogram`, whose table form reads the table or file its first argument names, is refused as an aggregate too.)
- The checks read a statement as DuckDB does: DuckDB's parser first rewrites the 18 characters it takes for spaces beyond ASCII (U+00A0, U+2000–U+200B, U+202F, U+205F, U+2060, U+3000, U+FEFF) as ordinary spaces, outside what its own quote tracker reads as `'…'`, `"…"`, `$tag$…$tag$` or a `--` comment. That tracker knows neither block comments nor backslash escapes, so a quote inside `/* … */` or an escaped quote in `E'…'` changes what it counts as quoted. The store applies the same rewrite, and checks and runs the result, which DuckDB then leaves unchanged (#213).
- DuckLake's own table macros (`set_option`, `set_commit_message`, `merge_adjacent_files`, `options`, `settings`, `current_snapshot`, `last_committed_snapshot`, `snapshots`, `table_info`, `table_changes`, `table_deletions`, `table_insertions`, with or without a qualifier) and table options (`CREATE TABLE … WITH (…)`, `ALTER TABLE … SET (…)`/`RESET (…)`) are refused in statements (`STORE_STATEMENT_INVALID`): the store alone runs maintenance and sets the lake's options, so deletion vectors stay off. A table named like a macro is still usable after `INTO`, `TABLE` or `REFERENCES` (`INSERT INTO settings (a) …`).
- **These checks cost a bounded amount** (#178). A file pattern (`*`, `?`, `[`) is matched by walking the folders it names, as DuckDB expands it; one statement's walks may read at most 10,000 directory entries together, each folder read counting as 50 entries (so at most 200 folders, which bounds the time on a cold disk cache) and each folder that is not there as one, and may take at most 100,000 steps. Each folder is read once per statement: other patterns of the same statement use the kept listing at no cost (#193), so many strings over the same folder do not add up; listings are never kept from one statement to the next. A pattern that cannot be checked within that, or that meets a folder that cannot be listed, counts as naming a file (`STORE_STATEMENT_INVALID`). `details.reason` tells why a string was refused: `names_a_file` (it names, or matches, a file, or is an object-storage address or `file:` URI), `pattern_too_broad` (past the budget) or `pattern_unreadable` (a folder that cannot be listed).
- SQL that DuckDB cannot parse fails with `STORE_QUERY_FAILED` (the parser's message in `details.duckdb`) before any other check: parsing runs nothing.
- A table read at another snapshot (`… AT (VERSION => n)`, `AT (TIMESTAMP => t)`) is refused in every statement (`STORE_STATEMENT_INVALID`, `details.clause` `AT`; #147, decision #182): a model reads the current data, so rows deleted since never come back through it. A read is pinned to its snapshot by the store itself. A name or label `at` stays allowed.
- `SUMMARIZE`, `DESCRIBE` (`DESC`) and `SHOW` are refused wherever DuckDB could read them as a statement: at the start, in a subquery, after a set operator, as an `INSERT` source or after `CREATE … AS` (`STORE_STATEMENT_INVALID`). DuckDB reads a quoted `SUMMARIZE` target as a file, and these reads would not be pinned to their snapshot. The words remain allowed as a column label after `AS` (`SELECT a AS desc`), as part of a qualified name (`t.desc`), and `DESC` as an ordering in `ORDER BY`. Quoted names (`"describe"`) are ordinary names. A read is also refused when DuckDB's parse tree contains such a statement.
- A DuckDB source that defines views or macros is refused (`STORE_SOURCE_INVALID`): they would run with the lake's connection.
- Nothing can be written to a source; a DuckDB source file is never modified.
- A path that matches no file → `SOURCE_NOT_FOUND`, naming the source, when the lake opens. Two sources with the same name → `STORE_SOURCE_INVALID`.

## Database sources (`SqliteSourceAttachment`, `PostgresSourceAttachment`)

| `type` | Attached as | Tables |
|---|---|---|
| `sqlite` | a private copy taken at open, attached with DuckDB's `sqlite` extension, `READ_ONLY` (a missing file is `SOURCE_NOT_FOUND`, never created) | `<name>.<table>` |
| `postgres` | DuckDB `postgres` extension, `READ_ONLY`, and the session set `default_transaction_read_only = on`; password from `password_env` | `<name>.<table>` with `schema`, otherwise `<name>.<schema>.<table>` |

- Nothing can be written to a database source, even when its user could write.
- A SQLite source is copied when the lake opens (SQLite's backup, WAL included) and the copy is attached: later changes to the file do not reach the lake until it is opened again. A SQLite file that defines views is refused (`STORE_SOURCE_INVALID`): DuckDB would run a view's SQL with the lake's connection.
- Functions that run SQL on a source (`postgres_query`, `postgres_execute`, `sqlite_query`, …) and `ATTACH`/`DETACH` are refused in statements (`STORE_STATEMENT_INVALID`).
- **Exact types (law 11):** a column DuckDB cannot carry exactly — PostgreSQL `numeric` without precision, with more than 38 digits or with a negative scale or one above its precision, `money` and arrays of either, in tables, views and materialized views; SQLite columns whose declared type contains `DEC` or `NUM` (`DECIMAL`, `NUMERIC`, `NUMBER`; DuckDB reads them as floating point, and SQLite stores them so) — is refused when a write reads it: `UNSUPPORTED_SOURCE_TYPE` naming `<source>.<table>.<column>`. Columns a write does not read do not matter.
- **TLS (#122).** A PostgreSQL source on a host other than the local machine must have `sslmode: verify-full`. The local machine is decided as for a catalog: the host is `localhost`, `127.0.0.1` or `::1`, `PGHOSTADDR` (when set) is local too, and no `PGSERVICE` is set, since libpq reads both from the environment; otherwise it is refused before anything is opened (`STORE_TLS_REQUIRED`). `sslrootcert` is `system` (the default) or `${env:NAME}`, a variable holding a CA file's path, read by the store. `sslmode: disable` is for the local machine only.
- A connection or credential failure is `STORE_CONNECTION_FAILED` naming the source, with the password redacted; an unset variable (password or CA) is `STORE_SECRET_MISSING`.

## File catalogs (`LocalLakeLocation.catalog_path`, `SqliteLakeLocation`)

- `LocalLakeLocation.catalog_path`, when given, is the DuckDB catalog file instead of `<path>/catalog.ducklake`; data files stay under `<path>/data/`. The store adds exactly that file and its `.wal` to the file sandbox; DuckDB adds the `.wal.checkpoint` and `.wal.recovery` files of each database it attaches.
- `SqliteLakeLocation` keeps the catalog in the SQLite file `path` (DuckDB's official `sqlite` extension), so several processes on one machine can use the lake; data files go under `data_path`. The store adds exactly the catalog file and its `-wal`, `-shm` and `-journal` files to the file sandbox, no folder.
- A file catalog must be on a local disk; the server's loader checks that before it builds the location (`contracts/project/v1`).

## Data files (`data_path`, `storage`, `encrypted`, #126, #127)

- `data_path` is where data files are written: `<path>/data/` by default for `LocalLakeLocation`, otherwise an absolute local folder or object storage: `s3://bucket/prefix/`, `gs://bucket/prefix/` (Google Cloud Storage) or `az://container/prefix/` (Azure Blob Storage).
- An object-storage path cannot contain `.` or `..` segments (`STORE_LOCATION_INVALID`): `s3://bucket/prefix/../` would make the whole bucket the lake's.
- An object-storage path needs `storage` of its kind, and `storage` needs such a path; any other URL (`gcs://`, `azure://`, `abfss://`, …) is refused (`STORE_LOCATION_INVALID`):
  - `s3://` with type `s3`: `region`, `use_ssl`, optional `endpoint` and `url_style`, and the names of the environment variables holding `key_id` and `secret`;
  - `gs://` with type `gcs`: the names of the variables holding an HMAC key (`key_id_env`, `secret_env`), and `endpoint` and `use_ssl`, by default `storage.googleapis.com` and `true` (Cloud Storage's S3-compatible API, reached by path);
  - `az://` with type `azure`: the name of the variable holding a connection string (`connection_string_env`). It must be `key=value` pairs naming an `AccountName`, otherwise `STORE_STATEMENT_INVALID`, without showing it.
- The credentials are read when the lake opens and registered so no error shows them; for Azure, the whole connection string and its `AccountKey` (or `SharedAccessSignature`). They become a **temporary** DuckDB secret (official `httpfs` extension for `s3` and `gcs`, official `azure` extension for `azure`, loaded only for an Azure lake) scoped to the prefix, which always ends with `/` (a `data_path` without it is read as that folder, #147); persistent secrets are never loaded. An unset variable is `STORE_SECRET_MISSING`.
- `use_ssl: false` is refused unless the endpoint is the local machine (`STORE_TLS_REQUIRED`), before anything is opened. An Azure connection string must use HTTPS (`DefaultEndpointsProtocol`, and a `BlobEndpoint` if it has one) unless its `BlobEndpoint` is the local machine.
- The file sandbox allows exactly the data prefix. DuckDB checks remote paths against it too: another prefix of the same bucket stays closed.
- **Known limit (DuckDB 2.0.0, decision #140).** DuckDB checks an `s3://` or `gs://` path against the sandbox as text and resolves `..` afterwards, so `s3://bucket/prefix/../other` passes the check and reaches outside the prefix (measured for both in CI). `az://` paths are resolved before the check and refused (measured). No statement can name a file or an object-storage address (`STORE_STATEMENT_INVALID`), and the lake's connection has DuckDB's web file systems turned off (`disabled_filesystems = 'HTTPFileSystem,HuggingFaceFileSystem'`), so a web address (`http://…`, `https://…`, which the statement check accepts as text) cannot be opened either. Only DuckLake's own paths reach object storage. Give the lake credentials that grant access to its prefix only.
- `encrypted: true` attaches with DuckLake's `ENCRYPTED`; the keys are stored in the catalog. Deletion vectors (`WRITE_DELETION_VECTORS`, written unencrypted) are never enabled.
- Maintenance (expiration, merging, cleanup and orphan deletion) works the same on object storage. Orphan deletion removes every `.parquet` file under the data path that the catalog does not list, so the data folder or prefix must be **dedicated to the lake**.

## PostgreSQL catalog (`PostgresLakeLocation`)

Several processes read and write one lake. `connection` holds libpq `key=value` pairs **without** a password (any `password` key or URI form is refused with `STORE_SECRET_INLINE`); the password is read at open time from the environment variable named in `password_env` and never appears in errors. `metadata_schema` keeps the catalog tables of one lake in their own PostgreSQL schema (default `public`), so one database can hold several lakes. `data_path` is where data files are written.

**TLS.** A catalog on a host other than the local machine must be reached with `sslmode=verify-full`, which encrypts the connection and checks that the server's certificate belongs to the named host; any other mode is refused before anything is opened (`STORE_TLS_REQUIRED`). Local means every host libpq could use is `localhost`, `127.0.0.1`, `::1` or a Unix-socket directory (a host starting with `/`), including `hostaddr`; `PGHOST`/`PGHOSTADDR` count when the connection names no host, and a service (`service=`, `PGSERVICE`) is never local. With `verify-full`, the operating system's authorities are used (`sslrootcert=system`) unless the connection names a CA file. The store is the enforcement point, so every user of this contract gets the rule.

Statements never reference the catalog database itself, nor call functions that run SQL elsewhere (`postgres_query`, `postgres_execute`, `query`, …): `STORE_STATEMENT_INVALID`.

## Read results: Arrow

A read returns an Arrow record-batch stream. Arrow types follow DuckDB types exactly:

| DuckDB | Arrow |
|---|---|
| `DECIMAL(p,s)` | `decimal128(p,s)` |
| `HUGEINT` | `decimal128(38,0)` |
| `UBIGINT` | `uint64` |
| `BIGINT` | `int64` |
| `TIMESTAMPTZ` | `timestamp[us, tz=UTC]` |
| `NULL` (any type) | Arrow null |
| `UHUGEINT` | `utf8` (decimal text; Arrow has no exact unsigned 128-bit type) |

Each Arrow field carries its DuckDB type in the field metadata key `duckdb_type`.

No value passes through a floating-point type unless its DuckDB type is `FLOAT` or `DOUBLE`.

## JSON encoding (`ResultJson`)

Every JSON response that carries data (HTTP API, MCP, exports) uses this encoding.

| DuckDB type | JSON |
|---|---|
| `BOOLEAN` | `true` / `false` |
| `TINYINT`, `SMALLINT`, `INTEGER`, `UTINYINT`, `USMALLINT`, `UINTEGER` | number |
| `BIGINT`, `UBIGINT`, `HUGEINT`, `UHUGEINT` | string of digits, optional leading `-` |
| `DECIMAL(p,s)` | string in plain notation with exactly `s` fraction digits (`"12.50"`, `"0.0000001000"`); negative zero is `"0.00"` |
| `FLOAT`, `DOUBLE` | number (for `FLOAT`, the shortest decimal that reads back as the same 32-bit value); `"NaN"`, `"Infinity"`, `"-Infinity"` as strings |
| `VARCHAR`, `UUID` | string |
| `DATE` | `YYYY-MM-DD`; years outside 1–9999 use astronomical numbering with a sign and more digits (`-0044-03-15`, `29349-01-26`); `"infinity"` / `"-infinity"` |
| `TIMESTAMP` | `YYYY-MM-DDTHH:MM:SS.ffffff` (no offset); same year rules; `"infinity"` / `"-infinity"` |
| `TIMESTAMP WITH TIME ZONE` | `YYYY-MM-DDTHH:MM:SS.ffffffZ` (UTC); same year rules; `"infinity"` / `"-infinity"` |
| `INTERVAL` | ISO 8601 duration `P<months>M<days>DT<seconds>S` (each part may be negative) |
| `BLOB` | base64 |
| any, `NULL` | `null` |

Other types (lists, structs, maps, unions, `TIME`, geometry, …) are refused with `STORE_TYPE_UNSUPPORTED` before any row is produced.

## Time limits, cancellation and admission

`ReadRequest` and `StatementRequest` accept `timeout_ms` (1–3,600,000; default set by the lake). A lake runs a bounded number of statements at once (reads until their stream ends, transactions until they commit or roll back); further requests wait in arrival order in a bounded queue, and are refused with `STORE_BUSY` when it is full. Waiting counts against the timeout. A statement past its time limit is interrupted (`STORE_TIMEOUT`); a read or a transaction can be cancelled from another thread (`STORE_CANCELLED`); an interrupted or cancelled transaction rolls back. Ctrl-C during a statement of a transaction (`execute`, `value`, `load_arrow`) raises Python's `KeyboardInterrupt`, as in any Python program, and the transaction rolls back.

At the edges (#77):
- **Every query of a read runs inside its slot.** The snapshot check and the pinning happen once the read is admitted, under the read's limit (#160).
- **A read that needs no catalog to be refused is refused before admission** (#160): SQL that does not parse (`STORE_QUERY_FAILED`), `SUMMARIZE`/`DESCRIBE`/`SHOW`, functions that run SQL given as text and table functions other than generators (`STORE_STATEMENT_INVALID`) get their own error even on a full lake. A name that is not a lake table needs the snapshot's tables, so on a full lake it gets `STORE_BUSY` or `STORE_TIMEOUT` first.
- **A slot is free before its error is seen.** A read that times out or is cancelled releases its slot before `start_read` raises, or before the stream's next read raises.
- **A paused stream is closed at its limit.** When the limit or a cancel comes while the consumer holds a batch, the read ends at once (cursor closed, slot released), and the stream's next read raises `STORE_TIMEOUT` or `STORE_CANCELLED`. While DuckDB computes a batch, the read is interrupted.
- **A transaction waits for the writer ahead before it takes a slot**, both within its limit, so a waiting writer holds no slot. Maintenance takes the writer lock, then every slot.
- **The commit runs inside the transaction's limit** (the lake's default time limit, counted from `transaction()`): a block that ends past it rolls back with `STORE_TIMEOUT`. The store's own catalog queries around the block (the snapshot and inventory before it, the checks before the commit) run under that limit and the transaction's cancel too (#160). A commit that has started, and the snapshot read right after it, are not interrupted: a committed transaction is never reported as failed. The commit's own duration is bounded by the catalog database, not by the time limit.
- **A statement's limit is capped by its transaction's** (#160): a statement of a transaction (`execute`, `value`, `load_arrow`) is interrupted at the earlier of its own limit and the transaction's.
- **An interrupted statement decides the transaction's outcome.** When a statement of the block fails with `STORE_TIMEOUT` or `STORE_CANCELLED`, the transaction rolls back and leaving the block raises that error, even if the block caught it. A cancel between statements makes leaving the block raise `STORE_CANCELLED`. Nothing is committed in either case.

## Arrow loads (`Transaction.load_arrow`)

`load_arrow(schema, table, reader, columns, on_interrupt=None, timeout_ms=None)` creates `schema.table` in the lake, inside the write transaction, from an Arrow record-batch stream (P1-17, #98):
- `columns` are `(name, DuckDB type)` pairs. The stream's fields must be exactly those names, in order, and no two names may differ only in letter case (`STORE_STATEMENT_INVALID` otherwise).
- Each column is cast to its type (`CREATE TABLE … AS SELECT CAST(…) …`). The stream is read once, batch by batch, by DuckDB, and no row passes through Python.
- It returns the number of rows.
- A type name must be a DuckDB type name with optional numeric arguments (`DECIMAL(18,4)`); anything else is refused before the SQL is built.
- Time limits and cancellation apply as for statements. `on_interrupt` is called from another thread when the statement times out or is cancelled, and on Ctrl-C when the load runs in the main thread, so the caller can close the stream's source (a remote query then stops, even while it sends nothing). Ctrl-C then raises `KeyboardInterrupt`.
- An exception raised by the stream itself propagates unchanged; after a time limit or a cancellation, the store's error is raised instead, even if the statement ended.
- The transaction rolls back on any failure.

Like a Python method, this is part of the contract's Python protocol; it has no JSON message.

## Maintenance (`MaintenanceReport`)

`Lake.maintain(expire_before, orphans_before)` takes two aware instants. It waits for every read and write of the lake to end and runs alone: it takes every admission slot, then the writer lock. It follows DuckLake's own `CHECKPOINT` order, step by step, so that each step is counted:

1. flush inlined data;
2. expire the snapshots committed before `expire_before`;
3. merge adjacent small files;
4. rewrite files with many deleted rows;
5. expire again (the snapshot that merging replaced, when it is older than the cutoff);
6. delete the files no remaining snapshot uses;
7. delete orphaned files in the data folder that are older than `orphans_before` (a newer one may belong to a writer that has not committed yet).

The current snapshot is never expired. Merging never changes what any kept snapshot reads. The report counts:
- `snapshots_expired`;
- `files_merged` and `files_rewritten`, the files processed by steps 3 and 4;
- `files_created`, the files those steps wrote;
- `files_deleted`;
- `orphaned_files_deleted`;
- `oldest_snapshot`, the oldest snapshot kept.

A read pinned to a snapshot that maintenance expired fails with `STORE_SNAPSHOT_EXPIRED`, which names the oldest one kept. Snapshot ids are never reused and every id up to the current one was committed, so a missing id in that range expired. Any other id is `STORE_SNAPSHOT_NOT_FOUND`.

Preconditions:
- **No other process uses the lake.** In this process, `maintain` excludes reads and writes itself. Other processes are excluded by the caller; the server holds the project lock.
- **One data path per lake.** A data folder shared by two lakes would make each delete the other's files as orphans.
- **Partial work.** If a step fails, `maintain` raises, and the steps already done (expired snapshots, merged or deleted files) stay done. None of them changes what a kept snapshot reads.

## Guarantees

- A write transaction commits to exactly one snapshot, or rolls back and creates none.
- A read sees exactly the data of its `snapshot`, whatever is committed afterwards.
- A read's SQL may reference only lake tables (and table functions such as `range`); it may not name another catalog.

## Error codes

| Code | Meaning |
|---|---|
| `STORE_SNAPSHOT_NOT_FOUND` | The requested snapshot does not exist |
| `STORE_SNAPSHOT_EXPIRED` | The requested snapshot existed but was expired by maintenance (a missing id up to the current one) |
| `STORE_STATEMENT_INVALID` | Not exactly one statement, a non-`SELECT` in a read, a statement kind not allowed in a transaction, or a read naming another catalog |
| `STORE_QUERY_FAILED` | DuckDB rejected or failed the statement; its message is in `details` |
| `STORE_PARAMETER_INVALID` | A parameter is missing, extra, or not convertible to its declared type |
| `STORE_LIMITS_REQUIRED` | The lake was opened without an explicit memory limit and thread count |
| `STORE_EXTENSION_UNAVAILABLE` | A DuckDB extension the lake needs could not be installed or loaded |
| `STORE_TRANSACTION_CONFLICT` | Another writer committed a conflicting change first; nothing of this transaction was written |
| `STORE_SECRET_MISSING` | An environment variable the location names is not set: `password_env`, or the object storage's `key_id_env` or `secret_env` (the variable is named, never a value) |
| `STORE_SECRET_INLINE` | The PostgreSQL `connection` contains a password or is a URI |
| `STORE_TIMEOUT` | The statement exceeded its time limit (waiting included); a transaction rolled back |
| `STORE_CANCELLED` | The read or transaction was cancelled; a transaction rolled back |
| `STORE_TLS_REQUIRED` | A PostgreSQL catalog or source on another machine without `sslmode=verify-full`, or object storage on another machine without HTTPS (`use_ssl: false`); `details` names the host or endpoint and the mode found (and the source) |
| `STORE_BUSY` | The lake is running its maximum number of statements and its queue is full |
| `STORE_TYPE_UNSUPPORTED` | A result column has a type with no JSON encoding yet |
| `STORE_CATALOG_UNAVAILABLE` | The lake cannot be opened: its catalog (a DuckDB or SQLite file, or PostgreSQL) cannot be reached or refused the credentials, its data files cannot be reached, or DuckDB refused the object storage settings; DuckDB's message is in `details`, never a credential |
| `STORE_LOCATION_INVALID` | The data location is not a local path or an `s3://` prefix, or an `s3://` path and the storage settings do not come together (new in #126, compatible) |
| `SOURCE_NOT_FOUND` | A declared file source matches no file, or a SQLite source file does not exist, when the lake opens; `details` names the source |
| `STORE_SOURCE_INVALID` | A source the lake cannot attach safely: two sources with the same name, a DuckDB source that defines views or macros, a SQLite source that defines views; `details` names the source |
| `STORE_CONNECTION_FAILED` | A database source (PostgreSQL, SQLite) cannot be reached or refuses the credentials; `details` names the source, the password redacted |
| `UNSUPPORTED_SOURCE_TYPE` | A write reads a source column DuckDB cannot carry exactly (see "Exact types" above); the message names `<source>.<table>.<column>` |

## Compatibility

New optional fields, new `LakeLocation` catalogs and new error codes documented as such are compatible changes within v1.
