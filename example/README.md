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

## Configuration

- `config/primary.toml`: Configures `replited` to stream changes from `/pb_data/data.db` on port 50051.
- `config/replica.toml`: Configures `replited` to connect to `primary-replited:50051` and apply changes to its local `/pb_data/data.db`.

## Note

The replica is **Read-Only** by definition of SQLite replication. Writing to the replica database will fail or cause divergence.
