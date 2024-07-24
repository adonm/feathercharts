#!/usr/bin/env python3
"""
DuckDB FastAPI Server

Usage:
  main.py [--host=<host>] [--port=<port>] [--db=<database>] [--log-level=<level>]

Options:
  --host=<host>        Host to bind the server to [default: 0.0.0.0]
  --port=<port>        Port to run the server on [default: 3000]
  --db=<database>      DuckDB database file [default: :memory:]
  --log-level=<level>  Logging level (debug, info, warning, error, critical) [default: info]

Environment Variables:
  DUCKDB_API_HOST      Host to bind the server to
  DUCKDB_API_PORT      Port to run the server on
  DUCKDB_API_DB        DuckDB database file
  DUCKDB_API_LOG_LEVEL Logging level
"""

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
from pydantic import BaseModel
import ibis
import duckdb
import tempfile
import uvicorn
import logging
from docopt import docopt
import os

app = FastAPI(title="DuckDB API", version="1.0.0")

# Global variables
conn = None

class Query(BaseModel):
    sql: str

class CreateTableRequest(BaseModel):
    name: str
    schema: dict

class PivotRequest(BaseModel):
    table: str
    index: list
    columns: str
    values: str

class OrderRequest(BaseModel):
    table: str
    by: list
    ascending: bool = True

@app.post("/execute", summary="Execute a SQL query")
async def execute_query(query: Query, format: str = "json"):
    """Execute a SQL query and return the results in the specified format."""
    return export_data(query.sql, format)

@app.get("/tables", summary="List all tables")
async def list_tables():
    """Get a list of all tables in the database."""
    return {"tables": ibis.list_tables()}

@app.get("/table/{table_name}", summary="Get table information")
async def get_table_info(table_name: str):
    """Get information about a specific table, including its schema."""
    try:
        table = ibis.table(table_name)
        schema = {col: str(dtype) for col, dtype in table.schema().items()}
        return {"name": table_name, "schema": schema}
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Table {table_name} not found")

@app.post("/create_table", summary="Create a new table")
async def create_table(request: CreateTableRequest):
    """Create a new table with the specified name and schema."""
    try:
        schema = ibis.schema(request.schema)
        ibis.create_table(request.name, schema=schema)
        return {"message": f"Table {request.name} created successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/import", summary="Import data from a file")
async def import_data(table_name: str, file: UploadFile = File(...)):
    """Import data from a file (CSV, JSON, NDJSON, or Parquet) into a new or existing table."""
    try:
        file_extension = file.filename.split('.')[-1].lower()
        if file_extension in ['csv', 'json', 'ndjson', 'parquet']:
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_{file_extension}(?, AUTO_DETECT=TRUE)", [file.file])
            return {"message": f"Data imported to table {table_name} successfully"}
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/pivot", summary="Pivot table data")
async def pivot_table(request: PivotRequest, format: str = "json"):
    """Perform a pivot operation on table data and return results in the specified format."""
    try:
        table = ibis.table(request.table)
        pivoted = table.pivot(request.index, request.columns, request.values)
        return export_data(pivoted.compile(), format)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/order", summary="Order table data")
async def order_table(request: OrderRequest, format: str = "json"):
    """Order table data based on specified columns and return results in the specified format."""
    try:
        table = ibis.table(request.table)
        ordered = table.order_by(request.by, ascending=request.ascending)
        return export_data(ordered.compile(), format)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

def export_data(sql: str, format: str):
    formats = {
        "csv": ("text/csv", "CSV, HEADER"),
        "json": ("application/json", "JSON"),
        "ndjson": ("application/x-ndjson", "NDJSON"),
        "parquet": ("application/octet-stream", "PARQUET")
    }
    if format not in formats:
        raise HTTPException(status_code=400, detail="Unsupported export format")
    
    media_type, duckdb_format = formats[format]
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f".{format}")
    conn.execute(f"COPY ({sql}) TO '{temp_file.name}' (FORMAT {duckdb_format})")
    return FileResponse(temp_file.name, media_type=media_type, filename=f"result.{format}")

if __name__ == "__main__":
    args = docopt(__doc__)
    
    # Use environment variables if available, otherwise use CLI arguments, then defaults
    host = os.getenv('DUCKDB_API_HOST') or args['--host'] or '0.0.0.0'
    port = int(os.getenv('DUCKDB_API_PORT') or args['--port'] or 3000)
    db_file = os.getenv('DUCKDB_API_DB') or args['--db'] or ':memory:'
    log_level_str = os.getenv('DUCKDB_API_LOG_LEVEL') or args['--log-level'] or 'info'
    
    log_level = getattr(logging, log_level_str.upper())
    
    logging.basicConfig(level=log_level)
    logger = logging.getLogger("uvicorn")
    logger.setLevel(log_level)

    logger.info(f"Starting server on {host}:{port}")
    logger.info(f"Using database file: {db_file}")
    logger.info(f"Log level set to: {log_level_str}")

    conn = duckdb.connect(db_file)
    ibis.set_backend('duckdb', conn)

    uvicorn.run(app, host=host, port=port, log_level=log_level_str.lower())