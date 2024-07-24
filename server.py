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

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, UploadFile, File, Request, Depends, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel
import ibis
import tempfile
import uvicorn
import logging
from docopt import docopt
import os
from typing import List, Optional

logger = logging.getLogger("uvicorn")

def initialize_connection(db_file=':memory:'):
    return ibis.connect(f"duckdb://{db_file}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: create the database connection
    db_file = os.getenv('DUCKDB_API_DB') or ':memory:'
    app.state.db = initialize_connection(db_file)
    yield
    # Shutdown: close the database connection
    app.state.db.disconnect()

app = FastAPI(title="DuckDB API", version="1.0.0", lifespan=lifespan)

def get_db(request: Request):
    return request.app.state.db

class SQLQuery(BaseModel):
    sql: str

class CreateTableRequest(BaseModel):
    name: str
    schema: dict

@app.post("/execute", summary="Execute a SQL query")
async def execute_query(query: SQLQuery, format: str = "json", db=Depends(get_db)):
    """Execute a SQL query and return the results in the specified format."""
    return export_data(query.sql, format, db)

@app.get("/tables", summary="List all tables")
async def list_tables(db=Depends(get_db)):
    """Get a list of all tables in the database."""
    return {"tables": db.list_tables()}

@app.get("/table/{table_name}", summary="Get table information")
async def get_table_info(table_name: str, db=Depends(get_db)):
    """Get information about a specific table, including its schema."""
    try:
        table = db.table(table_name)
        schema = {col: str(dtype) for col, dtype in table.schema().items()}
        return {"name": table_name, "schema": schema}
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Table {table_name} not found")

from typing import List, Optional

@app.get("/table/{table_name}/export", summary="Export table data with optional pivot and order")
async def export_table_data(
    table_name: str,
    format: str = Query("json", description="Export format (json, csv, ndjson, parquet)"),
    pivot_index: Optional[List[str]] = Query(None, description="Columns to use as index for pivoting"),
    pivot_columns: Optional[str] = Query(None, description="Column to use for pivot columns"),
    pivot_values: Optional[str] = Query(None, description="Column to use for pivot values"),
    order_by: Optional[List[str]] = Query(None, description="Columns to order by"),
    ascending: bool = Query(True, description="Sort order (True for ascending, False for descending)"),
    db=Depends(get_db)
):
    """
    Export table data with optional pivot and order operations.
    If neither pivot nor order is specified, exports the full table.
    """
    try:
        table = db.table(table_name)
        
        # Apply pivot if pivot parameters are provided
        if pivot_index and pivot_columns and pivot_values:
            table = table.pivot(pivot_index, pivot_columns, pivot_values)
        
        # Apply order if order parameters are provided
        if order_by:
            table = table.order_by(order_by, ascending=ascending)
        
        # Export the data
        return export_data(table.compile(), format, db)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error exporting table {table_name}: {str(e)}")

@app.post("/create_table", summary="Create a new table")
async def create_table(request: CreateTableRequest, db=Depends(get_db)):
    """Create a new table with the specified name and schema."""
    try:
        schema = ibis.schema(request.schema)
        db.create_table(request.name, schema=schema)
        return {"message": f"Table {request.name} created successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/import", summary="Import data from a file")
async def import_data(table_name: str, file: UploadFile = File(...), db=Depends(get_db)):
    """Import data from a file (CSV, JSON, NDJSON, or Parquet) into a new or existing table."""
    try:
        file_extension = file.filename.split('.')[-1].lower()
        if file_extension in ['csv', 'json', 'ndjson', 'parquet']:
            db.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_{file_extension}(?, AUTO_DETECT=TRUE)", [file.file])
            return {"message": f"Data imported to table {table_name} successfully"}
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

def export_data(sql: str, format: str, db):
    logger.info(sql)
    if format == "json":
        try:
            result = db.raw_sql(sql).fetchall()
        except Exception as e:
            # If both methods fail, raise an HTTP exception
            raise HTTPException(status_code=400, detail=f"Failed to execute query: {str(e)}")
        
        # Return the result as-is for FastAPI to handle JSON conversion
        return result

    # For other formats, use the existing logic
    formats = {
        "csv": ("text/csv", "CSV, HEADER"),
        "ndjson": ("application/x-ndjson", "JSON"),
        "parquet": ("application/octet-stream", "PARQUET")
    }
    if format not in formats:
        raise HTTPException(status_code=400, detail="Unsupported export format")
    
    media_type, duckdb_format = formats[format]
    with tempfile.NamedTemporaryFile(delete=False, suffix=f".{format}") as temp_file:
        try:
            db.raw_sql(f"COPY ({sql}) TO '{temp_file.name}' (FORMAT {duckdb_format})")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to export data: {str(e)}")
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
    logger.setLevel(log_level)

    logger.info(f"Starting server on {host}:{port}")
    logger.info(f"Using database file: {db_file}")
    logger.info(f"Log level set to: {log_level_str}")

    uvicorn.run(app, host=host, port=port, log_level=log_level_str.lower())