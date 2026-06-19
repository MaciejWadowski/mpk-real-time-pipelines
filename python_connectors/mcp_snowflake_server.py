#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "mcp",
#   "snowflake-connector-python",
#   "pyyaml",
# ]
# ///
"""
Minimal MCP server for Snowflake.
Credentials are read from config.yml (same format as other python_connectors)
or fall back to SNOWFLAKE_* environment variables.
"""

import asyncio
import json
import os
from pathlib import Path

import snowflake.connector
import yaml
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types


def load_config() -> dict:
    config_file = Path(__file__).parent / "config.yml"
    if config_file.exists():
        with open(config_file) as f:
            cfg = yaml.safe_load(f)
        sf = cfg["snowflake"]
        return {
            "account": sf["account"],
            "user": sf["user"],
            "password": sf["password"],
            "database": sf.get("database", "GTFS_TEST"),
            "warehouse": sf.get("warehouse", "COMPUTE_WH"),
            "role": sf.get("role"),
            "default_schema": sf.get("schema", "SCHEDULE_DATA_MARTS"),
        }
    return {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "database": os.environ.get("SNOWFLAKE_DATABASE", "GTFS_TEST"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "role": os.environ.get("SNOWFLAKE_ROLE"),
        "default_schema": os.environ.get("SNOWFLAKE_SCHEMA", "SCHEDULE_DATA_MARTS"),
    }


def get_connection(cfg: dict):
    params = {
        "user": cfg["user"],
        "password": cfg["password"],
        "account": cfg["account"],
        "database": cfg["database"],
        "warehouse": cfg["warehouse"],
        "network_timeout": 60,
        "client_session_keep_alive": False,
    }
    if cfg.get("role"):
        params["role"] = cfg["role"]
    return snowflake.connector.connect(**params)


cfg = load_config()
server = Server("snowflake-mpk")


@server.list_tools()
async def list_tools():
    return [
        types.Tool(
            name="query",
            description="Execute a SELECT SQL query against Snowflake and return results as JSON. Only SELECT is allowed.",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "The SELECT SQL query to execute"},
                    "limit": {"type": "integer", "description": "Max rows to return (default 100)", "default": 100},
                },
                "required": ["sql"],
            },
        ),
        types.Tool(
            name="list_tables",
            description="List all tables in a Snowflake schema",
            inputSchema={
                "type": "object",
                "properties": {
                    "schema": {
                        "type": "string",
                        "description": f"Schema name (default: {cfg['default_schema']})",
                    }
                },
            },
        ),
        types.Tool(
            name="describe_table",
            description="Show columns and data types for a Snowflake table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Table name (or fully qualified DATABASE.SCHEMA.TABLE)"},
                    "schema": {"type": "string", "description": "Schema name if table is not fully qualified"},
                },
                "required": ["table"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict):
    try:
        if name == "query":
            sql = arguments["sql"].strip()
            limit = int(arguments.get("limit", 100))
            if not sql.upper().startswith("SELECT"):
                return [types.TextContent(type="text", text="Error: only SELECT queries are permitted")]
            if "LIMIT" not in sql.upper():
                sql = f"{sql.rstrip(';')} LIMIT {limit}"
            conn = get_connection(cfg)
            try:
                cur = conn.cursor()
                cur.execute(sql)
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
                result = [dict(zip(cols, row)) for row in rows]
                return [types.TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
            finally:
                conn.close()

        elif name == "list_tables":
            schema = arguments.get("schema", cfg["default_schema"])
            conn = get_connection(cfg)
            try:
                cur = conn.cursor()
                cur.execute(
                    f"SELECT table_name, table_type, row_count, bytes "
                    f"FROM {cfg['database']}.information_schema.tables "
                    f"WHERE table_schema = %s ORDER BY table_name",
                    (schema.upper(),),
                )
                rows = cur.fetchall()
                result = [
                    {"table": r[0], "type": r[1], "rows": r[2], "size_bytes": r[3]}
                    for r in rows
                ]
                return [types.TextContent(type="text", text=json.dumps(result, indent=2))]
            finally:
                conn.close()

        elif name == "describe_table":
            table = arguments["table"].strip()
            schema = arguments.get("schema", cfg["default_schema"])
            if table.count(".") < 2:
                table = f"{cfg['database']}.{schema}.{table}"
            conn = get_connection(cfg)
            try:
                cur = conn.cursor()
                cur.execute(f"DESCRIBE TABLE {table}")
                rows = cur.fetchall()
                result = [{"column": r[0], "type": r[1], "nullable": r[3]} for r in rows]
                return [types.TextContent(type="text", text=json.dumps(result, indent=2))]
            finally:
                conn.close()

        return [types.TextContent(type="text", text=f"Unknown tool: {name}")]

    except Exception as e:
        return [types.TextContent(type="text", text=f"Error: {type(e).__name__}: {e}")]


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
