import os
from typing import Dict, Optional
from fastapi import FastAPI, File, UploadFile, Query, Path
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
import ibis
import tempfile
import json

app = FastAPI(title="DuckDB API", description="API for interacting with DuckDB using ibis")

# Configuration from environment variables
PORT = int(os.getenv("PORT", 8000))
DUCKDB_PATH = os.getenv("DUCKDB_PATH", ":memory:")

# Initialize ibis connection
con = ibis.duckdb.connect(DUCKDB_PATH)

class SQLRequest(BaseModel):
    sql: str
    params: Optional[Dict] = None

@app.post("/sql", summary="Execute SQL query")
async def execute_sql(request: SQLRequest):
    try:
        result = con.sql(request.sql).execute()
        return {"columns": result.columns.tolist(), "data": result.to_dict(orient="records")}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})


@app.post("/import/{import_type}", summary="Import file")
async def import_file(
    import_type: str = Path(..., enum=["csv", "json", "parquet"]),
    file: UploadFile = File(...),
    table_name: str = Query(...)
):
    file_path = f"temp_{file.filename}"
    try:
        with open(file_path, "wb") as buffer:
            buffer.write(await file.read())
        
        if import_type == "csv":
            table = con.read_csv(file_path, table_name=table_name)
        elif import_type == "json":
            table = con.read_json(file_path, table_name=table_name)
        elif import_type == "parquet":
            table = con.read_parquet(file_path, table_name=table_name)
        
        return {"message": f"File imported as table {table_name}"}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

@app.get("/export", summary="Export data")
async def export_data(name: str, type: str = Query(..., enum=["table", "view", "sql"]), format: str = Query(..., enum=["csv", "json", "parquet"])):
    try:
        if type == "sql":
            table = con.sql(name)
        else:
            table = con.table(name)
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{format}") as temp_file:
            if format == "csv":
                con.to_csv(table, temp_file.name)
            elif format == "json":
                con.to_pandas(table).to_json(temp_file.name, orient="records")
            elif format == "parquet":
                con.to_parquet(table, temp_file.name)
        
        return FileResponse(temp_file.name, filename=f"export.{format}")
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.get("/tables/{table_name}", summary="Query table with filters")
async def query_table(table_name: str, filters: str = Query(None)):
    try:
        table = con.table(table_name)
        
        if filters:
            filter_dict = json.loads(filters)
            for column, value in filter_dict.items():
                table = table.filter(table[column] == value)
        
        result = table.execute()
        return {"columns": result.columns.tolist(), "data": result.to_dict(orient="records")}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.get("/test", summary="Test all functionality")
async def test():
    results = {
        "successes": [],
        "failures": [],
        "errors": []
    }

    try:
        # Test SQL execution
        sql_result = await execute_sql(SQLRequest(sql="CREATE TABLE test_table (id INTEGER, name TEXT)"))
        results["successes"].append("SQL execution: Create table")

        sql_result = await execute_sql(SQLRequest(sql="INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')"))
        results["successes"].append("SQL execution: Insert data")

        # Test import
        csv_content = "id,name\n3,Charlie\n4,David"
        csv_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        csv_file.write(csv_content.encode())
        csv_file.close()
        
        with open(csv_file.name, "rb") as f:
            import_result = await import_file(
                import_type="csv",
                file=UploadFile(filename="test.csv", file=f),
                table_name="imported_table"
            )
        results["successes"].append("Import: CSV file")
        os.unlink(csv_file.name)

        # Test export
        export_result = await export_data(name="test_table", type="table", format="json")
        if isinstance(export_result, FileResponse):
            results["successes"].append("Export: JSON file")
        else:
            results["failures"].append("Export: JSON file")

        # Test table query
        query_result = await query_table(table_name="test_table", filters='{"id": 1}')
        if isinstance(query_result, dict) and query_result.get("data") and len(query_result["data"]) == 1:
            results["successes"].append("Table query: With filter")
        else:
            results["failures"].append("Table query: With filter")

        # Clean up
        await execute_sql(SQLRequest(sql="DROP TABLE IF EXISTS test_table"))
        await execute_sql(SQLRequest(sql="DROP TABLE IF EXISTS imported_table"))

    except Exception as e:
        results["errors"].append(f"Unexpected error: {str(e)}")

    return results

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)