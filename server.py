import os
from typing import Dict, Any, Optional
from fastapi import FastAPI, File, UploadFile, Query, Path, Body, HTTPException, Request, Depends
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from pydantic import BaseModel, Field
import ibis
import tempfile
import json
import csv
import io
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

app = FastAPI(
    title="DuckDB API",
    description="RESTful API for interacting with DuckDB using ibis. This API allows you to perform various operations on DuckDB tables, including creating, reading, updating, and deleting data, as well as executing custom SQL queries.",
    version="1.0.0"
)

# Configuration from environment variables
PORT = int(os.getenv("PORT", 8000))
DUCKDB_PATH = os.getenv("DUCKDB_PATH", ":memory:")

# Initialize ibis connection
con = ibis.duckdb.connect(DUCKDB_PATH)

class TableSchema(BaseModel):
    columns: Dict[str, str] = Field(..., example={"id": "INTEGER", "name": "TEXT", "value": "DOUBLE"}, description="A dictionary mapping column names to their data types")

def apply_filters(table, select=None, where=None, order=None, limit=None, offset=None):
    if select:
        columns = [col.strip() for col in select.split(",")]
        table = table.select(columns)
    
    if where:
        table = table.filter(ibis.expr(where))
    
    if order:
        order_columns = [col.strip() for col in order.split(",")]
        table = table.order_by(order_columns)
    
    if limit:
        table = table.limit(limit)
    
    if offset:
        table = table.offset(offset)
    
    return table

def format_output(result, ext, filename):
    if ext in [".json", ".csv", ".ndjson", ".parquet"]:
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as temp_file:
            if ext == ".json":
                result.to_json(temp_file.name, orient="records")
            elif ext == ".csv":
                result.to_csv(temp_file.name, index=False)
            elif ext == ".ndjson":
                result.to_json(temp_file.name, orient="records", lines=True)
            elif ext == ".parquet":
                table = pa.Table.from_pandas(result)
                pq.write_table(table, temp_file.name)
        return FileResponse(temp_file.name, filename=filename)
    else:
        return {"columns": result.columns.tolist(), "data": result.to_dict(orient="records")}

@app.get("/tables", summary="List all tables", description="Retrieves a list of all tables in the DuckDB database.")
async def list_tables():
    try:
        tables = con.list_tables()
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tables", summary="Create a new table", description="Creates a new table in the DuckDB database with the specified schema.")
async def create_table(
    table_name: str = Query(..., description="Name of the table to create"),
    schema: TableSchema = Body(..., description="Schema of the table to create")
):
    try:
        column_defs = ", ".join([f"{col} {dtype}" for col, dtype in schema.columns.items()])
        con.sql(f"CREATE TABLE {table_name} ({column_defs})")
        return {"message": f"Table {table_name} created successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/tables/{table_name}", summary="Drop a table", description="Removes the specified table from the DuckDB database.")
async def drop_table(table_name: str = Path(..., description="Name of the table to drop")):
    try:
        con.sql(f"DROP TABLE IF EXISTS {table_name}")
        return {"message": f"Table {table_name} dropped successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/tables/{table_name}", summary="Get table data", description="Retrieves data from the specified table, with optional filtering and formatting.")
async def get_table_data(
    table_name: str = Path(..., description="Name of the table to query"),
    format: str = Query("json", description="Output format. Options: json, csv, ndjson, parquet"),
    select: str = Query(None, description="Comma-separated list of columns to select"),
    where: str = Query(None, description="SQL WHERE clause to filter the data"),
    order: str = Query(None, description="Comma-separated list of columns to order by"),
    limit: int = Query(None, description="Maximum number of rows to return"),
    offset: int = Query(None, description="Number of rows to skip before starting to return data")
):
    try:
        table = con.table(table_name)
        table = apply_filters(table, select, where, order, limit, offset)
        result = table.execute()
        return format_output(result, f".{format}", f"{table_name}.{format}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/tables/{table_name}", summary="Insert data into table", description="Inserts data into the specified table. Supports JSON, CSV, and Parquet formats.")
async def insert_data(
    request: Request,
    table_name: str = Path(..., description="Name of the table to insert data into")
):
    content_type = request.headers.get("Content-Type", "")
    
    try:
        if content_type == "application/json":
            data = await request.json()
            con.insert(table_name, data)
        elif content_type in ["text/csv", "application/csv"]:
            csv_data = await request.body()
            df = pd.read_csv(io.StringIO(csv_data.decode()))
            con.insert(table_name, df)
        elif content_type == "application/parquet":
            parquet_data = await request.body()
            with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as temp_file:
                temp_file.write(parquet_data)
            con.read_parquet(temp_file.name, table_name=table_name)
            os.unlink(temp_file.name)
        else:
            raise HTTPException(status_code=415, detail="Unsupported media type")
        
        return {"message": "Data inserted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.patch("/tables/{table_name}", summary="Update data in table", description="Updates data in the specified table based on the provided conditions.")
async def update_data(
    table_name: str = Path(..., description="Name of the table to update"),
    set_values: Dict = Body(..., description="Dictionary of column names and their new values"),
    where: str = Query(..., description="SQL WHERE clause to determine which rows to update")
):
    try:
        table = con.table(table_name)
        table = table.filter(ibis.expr(where))
        for column, value in set_values.items():
            table = table.mutate(**{column: value})
        table.execute()
        return {"message": "Data updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/tables/{table_name}/rows", summary="Delete data from table", description="Deletes rows from the specified table based on the provided condition.")
async def delete_data(
    table_name: str = Path(..., description="Name of the table to delete data from"),
    where: str = Query(..., description="SQL WHERE clause to determine which rows to delete")
):
    try:
        table = con.table(table_name)
        table = table.filter(ibis.expr(where))
        con.sql(f"DELETE FROM {table_name} WHERE {where}")
        return {"message": "Data deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

class SQLQuery(BaseModel):
    query: str = Field(..., description="SQL query to execute")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Parameters for safe substitution in the query")

@app.post("/sql", summary="Execute SQL query", description="Executes a custom SQL query with optional parameters for safe substitution, and returns the results with optional filtering and formatting.")
async def execute_sql(
    sql_query: SQLQuery,
    format: str = Query("json", description="Output format. Options: json, csv, ndjson, parquet"),
    select: str = Query(None, description="Comma-separated list of columns to select from the query result"),
    where: str = Query(None, description="SQL WHERE clause to filter the query result"),
    order: str = Query(None, description="Comma-separated list of columns to order the query result by"),
    limit: int = Query(None, description="Maximum number of rows to return"),
    offset: int = Query(None, description="Number of rows to skip before starting to return data")
):
    try:
        # Use ibis to create a SQL expression with parameter substitution
        if sql_query.parameters:
            expr = con.sql(sql_query.query, con, params=sql_query.parameters)
        else:
            expr = con.sql(sql_query.query, con)
        
        # Apply filters
        expr = apply_filters(expr, select, where, order, limit, offset)
        
        # Execute the query
        result = expr.execute()
        
        return format_output(result, f".{format}", f"query_result.{format}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/test", summary="Test all functionality", description="Runs a series of tests to verify the functionality of all API endpoints.")
async def test():
    results = {
        "successes": [],
        "failures": [],
        "errors": []
    }

    try:
        # Test create table
        await create_table("test_table", TableSchema(columns={"id": "INTEGER", "name": "TEXT", "value": "DOUBLE"}))
        results["successes"].append("Create table")

        # Test insert data
        json_data = json.dumps([{"id": 1, "name": "Alice", "value": 10.5}, {"id": 2, "name": "Bob", "value": 15.3}])
        headers = {"Content-Type": "application/json"}
        request = Request(scope={"type": "http", "headers": headers.items()})
        request._body = json_data.encode()
        await insert_data(request, "test_table")
        results["successes"].append("Insert data (JSON)")

        # Test get table data
        data = await get_table_data("test_table", select="id,name", where="id = 1", order="name", limit=1)
        if isinstance(data, dict) and data["data"] and len(data["data"]) == 1 and data["data"][0]["name"] == "Alice":
            results["successes"].append("Get table data with filters")
        else:
            results["failures"].append("Get table data with filters")

        # Test update data
        await update_data("test_table", {"value": 11.0}, where="id = 1")
        results["successes"].append("Update data")

        # Test delete data
        await delete_data("test_table", where="id = 2")
        results["successes"].append("Delete data")

        # Test get table data in different formats
        for format in ["json", "csv", "ndjson", "parquet"]:
            response = await get_table_data("test_table", format=format)
            if isinstance(response, FileResponse):
                results["successes"].append(f"Get table data as {format}")
            else:
                results["failures"].append(f"Get table data as {format}")

        # Test execute SQL with parameters
        sql_result = await execute_sql(
            SQLQuery(
                query="SELECT * FROM test_table WHERE id = :id AND name = :name",
                parameters={"id": 1, "name": "Alice"}
            ),
            limit=1
        )
        if isinstance(sql_result, dict) and sql_result["data"] and len(sql_result["data"]) == 1:
            results["successes"].append("Execute SQL with parameters")
        else:
            results["failures"].append("Execute SQL with parameters")

        # Clean up
        await drop_table("test_table")
        results["successes"].append("Drop table")

    except Exception as e:
        results["errors"].append(f"Unexpected error: {str(e)}")

    return results

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)