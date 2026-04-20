import os
from contextlib import contextmanager

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


@contextmanager
def snowflake_connection():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )
    try:
        yield conn
    finally:
        conn.close()


def execute_sql(sql: str, params: tuple | None = None):
    with snowflake_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params or ())
            return cur
        finally:
            cur.close()