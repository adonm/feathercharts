export const openApiSpec = {
    openapi: "3.0.0",
    info: {
        title: "DuckDB REST API with Hono",
        version: "1.0.0",
        description:
            `A REST API for interacting with DuckDB, built using the Hono web framework. This API provides endpoints for querying, exporting, and importing data in DuckDB, an in-process SQL OLAP database management system.
  
  How to run the server:
  1. Ensure Deno is installed on your system.
  2. Set environment variables (optional):
     - PORT: The port number for the server (default: 8000)
     - DB_PATH: Path to the DuckDB database file (default: ":memory:")
  3. Run the server using the following command:
     PORT=8080 DB_PATH=/path/to/db.duck deno run -A server.ts
     
  If environment variables are not set, the server will use default values.`,
    },
    paths: {
        "/query": {
            post: {
                summary: "Execute a SQL query",
                description:
                    "Executes a SQL query using DuckDB. The query is run using the `conn.all()` method, which returns all results as a JSON array. This endpoint is suitable for queries that return a manageable amount of data.",
                requestBody: {
                    required: true,
                    content: {
                        "application/json": {
                            schema: {
                                type: "object",
                                properties: {
                                    sql: { type: "string", description: "SQL query to execute" },
                                    params: {
                                        type: "array",
                                        items: { type: "any" },
                                        description: "Query parameters (optional). These are used for parameterized queries to prevent SQL injection.",
                                    },
                                },
                                required: ["sql"],
                            },
                        },
                    },
                },
                responses: {
                    "200": {
                        description: "Successful response",
                        content: {
                            "application/json": {
                                schema: {
                                    type: "array",
                                    items: { type: "object" },
                                    description: "Query results as a JSON array",
                                },
                            },
                        },
                    },
                    "500": {
                        description: "Error executing query",
                        content: {
                            "application/json": {
                                schema: {
                                    type: "object",
                                    properties: {
                                        error: { type: "string" },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        "/export": {
            post: {
                summary: "Export query results to a file",
                description:
                    "Executes a SQL query and exports the results to a file. This uses DuckDB's `COPY` command to write the query results directly to a file, which is then sent as a response. This method is efficient for large result sets.",
                requestBody: {
                    required: true,
                    content: {
                        "application/json": {
                            schema: {
                                type: "object",
                                properties: {
                                    sql: { type: "string", description: "SQL query to execute" },
                                    params: {
                                        type: "array",
                                        items: { type: "any" },
                                        description: "Query parameters (optional). These are used for parameterized queries to prevent SQL injection.",
                                    },
                                    format: {
                                        type: "string",
                                        enum: ["parquet", "ndjson", "json", "csv"],
                                        default: "json",
                                        description: "Export format (optional, defaults to JSON)",
                                    },
                                },
                                required: ["sql"],
                            },
                        },
                    },
                },
                responses: {
                    "200": {
                        description: "Successful response",
                        content: {
                            "application/octet-stream": {
                                schema: {
                                    type: "string",
                                    format: "binary",
                                    description: "Exported file in the requested format",
                                },
                            },
                        },
                    },
                    "500": {
                        description: "Error exporting data",
                        content: {
                            "application/json": {
                                schema: {
                                    type: "object",
                                    properties: {
                                        error: { type: "string" },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        "/import": {
            post: {
                summary: "Import data from a file",
                description:
                    "Imports data from a file into a new DuckDB table. This endpoint uses DuckDB's built-in functions like `parquet_scan`, `read_csv_auto`, and `read_json_auto` to efficiently read the file and create a table. The import is done in-memory, making it fast but potentially memory-intensive for large files.",
                requestBody: {
                    required: true,
                    content: {
                        "multipart/form-data": {
                            schema: {
                                type: "object",
                                properties: {
                                    file: {
                                        type: "string",
                                        format: "binary",
                                        description: "File to import (parquet, csv, json, or ndjson)",
                                    },
                                    tableName: {
                                        type: "string",
                                        description: "Name of the table to create",
                                    },
                                },
                                required: ["file", "tableName"],
                            },
                        },
                    },
                },
                responses: {
                    "200": {
                        description: "Successful import",
                        content: {
                            "application/json": {
                                schema: {
                                    type: "object",
                                    properties: {
                                        message: { type: "string" },
                                    },
                                },
                            },
                        },
                    },
                    "400": {
                        description: "Bad request (e.g., missing file or table name, unsupported file format)",
                        content: {
                            "application/json": {
                                schema: {
                                    type: "object",
                                    properties: {
                                        error: { type: "string" },
                                    },
                                },
                            },
                        },
                    },
                    "500": {
                        description: "Error importing data",
                        content: {
                            "application/json": {
                                schema: {
                                    type: "object",
                                    properties: {
                                        error: { type: "string" },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        "/test": {
            get: {
                summary: "Test all API functionality",
                description:
                    "Runs a series of tests on all API endpoints. This includes executing a simple query, exporting data, and importing a test file. It's useful for verifying that all parts of the API are functioning correctly.",
                responses: {
                    "200": {
                        description: "Test results",
                        content: {
                            "application/json": {
                                schema: {
                                    type: "array",
                                    items: {
                                        type: "object",
                                        properties: {
                                            test: { type: "string", description: "Name of the test" },
                                            success: { type: "boolean", description: "Whether the test passed" },
                                            error: { type: "string", nullable: true, description: "Error message if the test failed" },
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
    },
};
