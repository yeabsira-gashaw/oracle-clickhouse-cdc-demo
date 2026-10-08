# Oracle to ClickHouse CDC: methods and design

## Objective

This POC demonstrates row-level Oracle change capture and delivery into ClickHouse with a simple local stack. It uses a consistent initial snapshot for existing rows, then streams committed changes through Oracle LogMiner. Kafka buffers the events between source and target.

## Component roles

1. **Oracle Database Free** stores the source table `APP.CUSTOMERS`. Minimal and table-level supplemental logging identify changed values. `ARCHIVELOG` mode retains redo for LogMiner. A common user (`C##DBZUSER`) with `SET CONTAINER` can access the CDB and the `FREEPDB1` PDB.
2. **Debezium Oracle connector** runs inside Kafka Connect. It snapshots `APP.CUSTOMERS`, then mines the Oracle redo/archive logs. Its unwrap single-message transform converts Debezium envelope records into flat row JSON and marks deletes with `__deleted`.
3. **Kafka** is the durable event backbone. Debezium publishes this table to `oracle-cdc.APP.CUSTOMERS`. Connect also stores offsets and schema history in Kafka topics.
4. **ClickHouse Kafka engine** consumes the table topic as `JSONEachRow`. Oracle's unquoted column identifiers arrive uppercase (`ID`, `NAME`, `EMAIL`, `UPDATED_AT`), so the ClickHouse Kafka table uses those exact names.
5. **ClickHouse materialized view** converts the string-form Oracle ID into `UInt64`, converts the epoch-millisecond timestamp to readable text, maps the delete marker, and writes each event into `poc.customer_events`.
6. **ReplacingMergeTree** stores versions ordered by the customer's ID. The Kafka `_offset` becomes the row version. This one-partition demo therefore preserves event order for each key. `FINAL` resolves the latest version in a query.

## Change flow

### Initial snapshot

When the connector starts with `snapshot.mode=initial`, it reads the current source rows and publishes them to Kafka. ClickHouse consumes those records through its Kafka engine table and materialized view. This is why the first ClickHouse query should show the seeded Ada and Grace rows without manual inserts.

### Ongoing insert/update/delete

After an Oracle transaction commits, Debezium mines the corresponding redo and publishes the changed row to Kafka. The Kafka engine passes that JSON record into the materialized view, which writes a new version for that customer. A delete is kept as a tombstone row with `is_deleted=1`; it is not physically removed from ClickHouse. Query live records with `FINAL WHERE is_deleted=0`.

### ClickHouse field conversion

The source table uses unquoted Oracle identifiers. Oracle reports them in uppercase, as shown by a Kafka event sample:

```json
{"ID":"1","NAME":"Ada Lovelace","EMAIL":"ada@example.test","UPDATED_AT":1791448407488}
```

`ID` is serialized as a string because the connector uses `decimal.handling.mode=string`. The ClickHouse view explicitly converts it with `toUInt64OrNull`. `UPDATED_AT` is in epoch milliseconds and becomes a ClickHouse-readable timestamp string. Field names in JSON are case-sensitive for this mapping; changing Oracle column case or converter behavior requires a matching ClickHouse schema/view update.

## Oracle prerequisites and account model

- **ARCHIVELOG mode:** required for LogMiner. Oracle must be mounted to switch from `NOARCHIVELOG` to `ARCHIVELOG`, then opened again. This is a one-time database operation and is documented in the README.
- **Supplemental logging:** the first-init SQL enables database-level minimal supplemental logging and all-column logging for `APP.CUSTOMERS`.
- **Common LogMiner user:** the connector switches its session between the CDB root and PDB. A local-only `APP` account cannot perform that switch; the common `C##DBZUSER` account is granted `SET CONTAINER`, LogMiner privileges, catalog access, and required dynamic-view access.
- **Sample user:** `APP` owns and updates the demo table. Keep its credentials separate from the Debezium account.

## Kafka and ClickHouse behavior

- Kafka runs one KRaft broker with replication factor 1. This keeps the POC self-contained; it does not provide fault tolerance.
- The ClickHouse Kafka table uses a dedicated consumer group. Its materialized view pushes data into a normal MergeTree-family table; clients query that target rather than querying the streaming table.
- The provided uppercase-field migration drops and recreates the Kafka engine table with a new group, truncates the malformed target, and replays retained topic messages from the beginning. It does not modify Oracle or Kafka data.
- The target uses `ReplacingMergeTree(version)` rather than ReplacingMergeTree's delete-aware variant, so delete tombstones remain visible for this teaching/demo query. `FINAL` applies the newest version per ID at read time.
- Kafka topic retention and the local named volumes determine how much history is available for replay. Removing volumes deletes local state.

## Operational workflow

1. Start all services with `docker compose up -d`.
2. Verify Oracle and Kafka health.
3. Enable Oracle `ARCHIVELOG` once for the initialized database.
4. Register the Debezium connector and confirm connector and task states are `RUNNING`.
5. Verify the initial snapshot in ClickHouse.
6. Commit Oracle INSERT/UPDATE/DELETE transactions and query ClickHouse after a short asynchronous delivery delay.

See the repository [README](../README.md) for exact commands, credentials, recovery procedures, and reset instructions.

## Known limits

- Single machine, single Kafka broker, single connector task, single ClickHouse node, single topic partition.
- Plaintext local credentials and unencrypted local service ports are for demonstration only.
- The Oracle Free image is community-built and subject to Oracle Database Free usage limits and license terms.
- The `gvenzl/oracle-free:23-slim` tag can advance to newer Oracle Free releases. The selected Debezium 3.5.0.Final version supports the current Oracle 26ai version banner used in this POC.
- This architecture demonstrates CDC semantics; it is not a production security, availability, or schema-evolution design.
