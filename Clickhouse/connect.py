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
    "create_database": base_path/"create_database.sql"
    }
with clickhouse_connect.get_client(**config) as client:
    create_dabtabase = file_path["create_database"].read_text(encoding="utf-8")
    client.command(create_dabtabase)

