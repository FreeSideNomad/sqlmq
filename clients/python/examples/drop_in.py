"""Drop-in usage example.

Mirrors a typical tembo-pgmq-python snippet. The only line that differs is
the import; the connection params change because the backend is SQL Server.

Run against a local sqlmq-provisioned database. Bring one up via:

    docker run --rm -d --name sqlmq-demo -p 1433:1433 \\
        -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD='P@ssw0rd1' \\
        mcr.microsoft.com/mssql/server:2022-latest

then apply sql/migrations/V*.sql against a fresh database.
"""

from __future__ import annotations

from sqlmq import PGMQueue


def main() -> None:
    q = PGMQueue(
        host="localhost", port="1433",
        database="sqlmq",
        username="sa", password="P@ssw0rd1",
    )

    q.create_queue("orders")
    msg_id = q.send("orders", {"order_id": 42, "amount": 19.99})
    print(f"sent msg_id={msg_id}")

    msg = q.read("orders")
    assert msg is not None
    print(f"read msg_id={msg.msg_id} payload={msg.message}")

    deleted = q.delete("orders", msg.msg_id)
    print(f"deleted={deleted}")


if __name__ == "__main__":
    main()
