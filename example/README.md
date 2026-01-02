# PocketBase Replication Example

This example demonstrates how to use `replited` to replicate a PocketBase database in real-time.

## Structure

- **Primary**: PocketBase (8090) + Replited (Sidecar mode)
- **Replica**: PocketBase (8091) + Replited (Sidecar mode)

## Prerequisites

- Docker
- Docker Compose

## Quick Start

1.  **Start Services**:

    ```bash
    docker-compose up --build -d
    ```

2.  **Verify Replication**:

    - Open **Primary Admin UI**: http://127.0.0.1:8090/_/
      - Create an admin account.
      - Create a collection (e.g., `posts`) and add a record.
    - Open **Replica Admin UI**: http://127.0.0.1:8091/_/
      - Log in with the same credentials (it's replicated!).
      - View the `posts` collection. You should see the record created in Primary.

3.  **Check Logs**:
    ```bash
    docker-compose logs -f primary-replited replica-replited
    ```

## Child Process Mode (Recommended for Replica)

This example uses the **Child Process Mode** for the Replica.
In this mode, `replited` acts as the supervisor for the PocketBase process.

```bash
replited replica-sidecar --exec "/usr/local/bin/pocketbase serve ..."
```

**Benefits:**

1.  **Auto-Restore**: If replication fails (e.g. WAL checksum mismatch or Stuck WAL), `replited` can stop PocketBase, delete the corrupt DB, download a fresh snapshot, and restart PocketBase automatically.
2.  **Schema Updates**: (Future) Can restart PocketBase when schema changes are detected to clear in-memory cache.

To support this, the `replica-replited` service uses a custom image (`example/replica.Dockerfile`) that contains both `replited` and `pocketbase` binaries.

## Configuration

- `config/primary.toml`: Configures `replited` to stream changes from `/pb_data/data.db` on port 50051.
- `config/replica.toml`: Configures `replited` to connect to `primary-replited:50051` and apply changes to its local `/pb_data/data.db`.

## Note

The replica is **Read-Only** by definition of SQLite replication. Writing to the replica database will fail or cause divergence.
