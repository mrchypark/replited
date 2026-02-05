# TrailBase Replication Example

This example demonstrates how to use `replited` to replicate a TrailBase database in real-time.

## Structure

- **Primary**: TrailBase (4000) + Replited (Sidecar mode)
- **Replica**: TrailBase (4001) + Replited (Sidecar mode)

## Prerequisites

- Docker
- Docker Compose
- Python 3 (for verification script)

## Quick Start

1.  **Start Services**:

    ```bash
    docker-compose up --build -d
    ```

2.  **Set Admin Password** (first time only):

    TrailBase creates an admin user on first start with a random password.
    Set a known password for testing:

    ```bash
    docker-compose exec primary-trailbase /app/trail user change-password admin@localhost secret
    ```

3.  **Verify Replication**:

    ```bash
    python verify_trailbase.py
    ```

    Or manually:

    - Open **Primary Admin UI**: http://127.0.0.1:4010/_/admin/
      - Log in with `admin@localhost` / `secret`
      - Create a table (e.g., `notes`) and add a record.
    - Open **Replica Admin UI**: http://127.0.0.1:4011/_/admin/
      - Log in with the same credentials (it's replicated!).
      - View the `notes` table. You should see the record created in Primary.

4.  **Check Logs**:
    ```bash
    docker-compose logs -f primary-replited replica-replited
    ```

## Child Process Mode (Recommended for Replica)

This example uses the **Child Process Mode** for the Replica.
In this mode, `replited` acts as the supervisor for the TrailBase process.

```bash
replited replica-sidecar --exec "/app/trail run"
```

**Benefits:**

1.  **Auto-Restore**: If replication fails (e.g. WAL checksum mismatch or Stuck WAL), `replited` can stop TrailBase, delete the corrupt DB, download a fresh snapshot, and restart TrailBase automatically.
2.  **Schema Updates**: (Future) Can restart TrailBase when schema changes are detected to clear in-memory cache.

## Configuration

- `config/primary.toml`: Configures `replited` to stream changes from `/app/traildepot/data/main.db` on port 50051.
- `config/replica.toml`: Configures `replited` to connect to `primary-replited:50051` and apply changes to its local database.

## TrailBase vs PocketBase

| Feature        | PocketBase | TrailBase         |
| -------------- | ---------- | ----------------- |
| Default Port   | 8090       | 4000              |
| Data Directory | `/pb_data` | `/app/traildepot` |
| Database File  | `data.db`  | `data/main.db`    |
| Admin UI       | `/_/`      | `/_/admin/`       |
| Language       | Go         | Rust              |

## Note

The replica is **Read-Only** by definition of SQLite replication. Writing to the replica database will fail or cause divergence.

## Cleanup

```bash
docker-compose down -v
rm -rf data/primary data/replica
```
