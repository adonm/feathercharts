#!/usr/bin/env -S deno run -A --watch
import { Hono } from "@hono/hono";
import { logger } from "@hono/hono/logger";
import { swaggerUI } from "@hono/swagger-ui";
import duckdb from "duckdb";
import { config } from "https://deno.land/x/dotenv@v3.2.2/mod.ts";
import { openApiSpec } from "./openapi.ts";

// Load environment variables
config({ export: true });

// Configure port and database from environment variables or use defaults
const PORT = parseInt(Deno.env.get("PORT") || "8000");
const DB_PATH = Deno.env.get("DB_PATH") || ":memory:";

const app = new Hono();
const db = new duckdb.Database(DB_PATH);
const conn = db.connect();

// Add the logger middleware
app.use("*", logger());

// Redirect root to Swagger UI
app.get("/", (c) => c.redirect("/ui"));

// Add Swagger UI
app.get("/ui", swaggerUI({ url: "/doc" }));

// Serve OpenAPI specification
app.get("/doc", (c) => c.json(openApiSpec));

// Configuration for supported file formats
const formatConfigs = {
    parquet: { command: "PARQUET", contentType: "application/octet-stream", ext: "parquet" },
    ndjson: { command: "JSON", contentType: "application/x-ndjson", ext: "ndjson" },
    json: { command: "JSON, ARRAY true", contentType: "application/json", ext: "json" },
    csv: { command: "CSV", contentType: "text/csv", ext: "csv" },
} as const;

type FormatKey = keyof typeof formatConfigs;

// Patch BigInt to add toJSON method
(BigInt.prototype as { toJSON?: () => string }).toJSON = function () {
    return this.toString();
};

/**
 * Execute a SQL query and return the results as JSON
 *
 * POST /query
 * Request body: { sql: string, params?: any[] }
 * Response: JSON array of query results
 */
app.post("/query", async (c) => {
    const { sql, params = [] } = await c.req.json();
    return new Promise((resolve) => {
        try {
            conn.all(sql, ...params, (err, result) => {
                if (err) {
                    console.error("Error executing query:", err);
                    resolve(c.json({ error: "Failed to execute query" }, 500));
                } else {
                    console.log("Query result:", result);
                    resolve(c.json(result));
                }
            });
        } catch (error) {
            console.error("Error executing query:", error);
            resolve(c.json({ error: "Failed to execute query" }, 500));
        }
    });
});

/**
 * Export query results to a file in the specified format
 *
 * POST /export
 * Request body: { sql: string, params?: any[], format?: string }
 * Response: File download in the specified format (default: JSON)
 */
app.post("/export", async (c) => {
    const { sql, params = [], format = "json" } = await c.req.json();
    const config = formatConfigs[format.toLowerCase() as FormatKey] || formatConfigs.json;
    const tempFilePath = await Deno.makeTempFile();

    return new Promise((resolve) => {
        try {
            conn.exec(`COPY (${sql}) TO '${tempFilePath}' (FORMAT ${config.command})`, ...params, async (err) => {
                if (err) {
                    console.error("Error exporting data:", err);
                    resolve(c.json({ error: "Failed to export data" }, 500));
                } else {
                    try {
                        const result = await Deno.readFile(tempFilePath);
                        c.header("Content-Type", config.contentType);
                        c.header("Content-Disposition", `attachment; filename="result.${config.ext}"`);
                        resolve(c.body(result));
                    } catch (readError) {
                        console.error("Error reading exported file:", readError);
                        resolve(c.json({ error: "Failed to read exported data" }, 500));
                    }
                }
                // Clean up temp file
                try {
                    await Deno.remove(tempFilePath);
                } catch (removeError) {
                    console.error("Error removing temp file:", removeError);
                }
            });
        } catch (error) {
            console.error("Error in export process:", error);
            resolve(c.json({ error: "Failed to initiate export" }, 500));
        }
    });
});

/**
 * Import data from a file into a specified table
 *
 * POST /import
 * Request body: FormData with 'file' (File) and 'tableName' (string)
 * Supported file formats: Parquet, CSV, JSON, NDJSON
 * Response: JSON message confirming import or error details
 */
app.post("/import", async (c) => {
    let tempFilePath: string | undefined;
    try {
        const formData = await c.req.formData();
        const file = formData.get("file") as File;
        const tableName = formData.get("tableName") as string;

        if (!file || !tableName) {
            return c.json({ error: "File and table name are required" }, 400);
        }

        tempFilePath = await Deno.makeTempFile();
        const fileContent = new Uint8Array(await file.arrayBuffer());
        await Deno.writeFile(tempFilePath, fileContent);

        const fileExt = file.name.split(".").pop()?.toLowerCase();
        let importCommand: string;

        switch (fileExt) {
            case "parquet":
                importCommand = `CREATE TABLE ${tableName} AS SELECT * FROM parquet_scan('${tempFilePath}')`;
                break;
            case "csv":
                importCommand = `CREATE TABLE ${tableName} AS SELECT * FROM read_csv_auto('${tempFilePath}')`;
                break;
            case "json":
            case "ndjson":
                importCommand = `CREATE TABLE ${tableName} AS SELECT * FROM read_json_auto('${tempFilePath}')`;
                break;
            default:
                throw new Error("Unsupported file format");
        }

        await new Promise<void>((resolve, reject) => {
            conn.exec(importCommand, (err) => {
                if (err) reject(err);
                else resolve();
            });
        });

        console.log(`Data imported into table ${tableName}`);
        return c.json({ message: `Data imported into table ${tableName}` });
    } catch (error) {
        console.error("Error in import process:", error);
        return c.json({ error: error instanceof Error ? error.message : "Failed to process import" }, 500);
    } finally {
        // Clean up temp file if it exists
        if (tempFilePath) {
            try {
                await Deno.remove(tempFilePath);
            } catch (removeError) {
                console.error("Error removing temp file:", removeError);
            }
        }
    }
});

/**
 * Test endpoint to verify all functionality
 *
 * GET /test
 * Response: JSON with test results
 */
app.get("/test", async (c) => {
    const baseUrl = `http://localhost:${PORT}`;
    const results = [];

    console.log("Starting tests...");

    // Test query endpoint
    try {
        console.log("Testing query endpoint...");
        const queryResp = await fetch(`${baseUrl}/query`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ sql: "SELECT 1 + 1 AS result" }),
        });
        const queryText = await queryResp.text();
        console.log("Query response:", queryText);
        const queryData = JSON.parse(queryText);
        if (!Array.isArray(queryData) || queryData.length === 0 || !("result" in queryData[0])) {
            throw new Error("Unexpected response format");
        }
        results.push({ test: "Query", success: queryData[0].result === 2 });
        console.log("Query test completed.");
    } catch (error) {
        console.error("Query test failed:", error);
        results.push({ test: "Query", success: false, error: error.message });
    }

    // Test export endpoint
    try {
        console.log("Testing export endpoint...");
        const exportResp = await fetch(`${baseUrl}/export`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ sql: "SELECT 1 AS num, 'test' AS str", format: "json" }),
        });
        const exportText = await exportResp.text();
        console.log("Export response:", exportText);
        const exportData = JSON.parse(exportText);
        if (!Array.isArray(exportData) || exportData.length === 0) {
            throw new Error("Unexpected response format");
        }
        results.push({ test: "Export", success: exportData[0].num === 1 && exportData[0].str === "test" });
        console.log("Export test completed.");
    } catch (error) {
        console.error("Export test failed:", error);
        results.push({ test: "Export", success: false, error: error.message });
    }

    // Test import endpoint
    try {
        console.log("Testing import endpoint...");
        const testData = new Blob(['{"num": 1, "str": "test"}'], { type: "application/json" });
        const formData = new FormData();
        formData.append("file", testData, "test.json");
        formData.append("tableName", "test_table");

        const importResp = await fetch(`${baseUrl}/import`, {
            method: "POST",
            body: formData,
        });
        const importResult = await importResp.json();
        results.push({ test: "Import", success: importResult.message && importResult.message.includes("imported") });
        console.log("Import test completed.");

        // Verify imported data
        console.log("Verifying imported data...");
        const verifyResp = await fetch(`${baseUrl}/query`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ sql: "SELECT * FROM test_table" }),
        });
        const verifyText = await verifyResp.text();
        console.log("Verification data:", verifyText);
        const verifyData = JSON.parse(verifyText);
        if (!Array.isArray(verifyData) || verifyData.length === 0) {
            throw new Error("No data returned from verification query");
        }
        results.push({ test: "Import Verification", success: verifyData[0].num == 1 && verifyData[0].str === "test" });
        console.log("Import verification completed.");
    } catch (error) {
        console.error("Import/Verification test failed:", error);
        results.push({ test: "Import/Verification", success: false, error: error.message });
    }

    console.log("All tests completed.");
    return c.json(results);
});

console.log(`Server running on http://localhost:${PORT}`);
console.log(`Using database: ${DB_PATH}`);

Deno.serve({ port: PORT }, app.fetch);
