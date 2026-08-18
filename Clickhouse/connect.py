import pathlib

import  clickhouse_connect

config = {
    "host":"localhost",
    "port":8123,
    "username":"default",
    "password":"admin"
}
base_path = pathlib.Path("Clickhouse/sql")
file_path = {
    "create_database": base_path/"create_database.sql",
    "create_table": base_path/"create_table.sql",
    "create_kafka_table": base_path/"create_kafka_table.sql",
    "materialized": base_path/"mv_insert.sql"
    }
with clickhouse_connect.get_client(**config) as client:
    for name,path in file_path.items():
        print(f"Applying {path.name}")
        client.command(path.read_text())
