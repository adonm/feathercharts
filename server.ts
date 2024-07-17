#!/usr/bin/env -S deno run -A --watch
import { Hono } from "@hono/hono";
import duckdb from "duckdb";

const app = new Hono();
const db = new duckdb.Database(":memory:");
const conn = db.connect();

const formatConfigs = {
    parquet: { command: "PARQUET", contentType: "application/octet-stream", ext: "parquet" },
    ndjson: { command: "JSON", contentType: "application/x-ndjson", ext: "ndjson" },
    json: { command: "JSON, ARRAY true", contentType: "application/json", ext: "json" },
} as const;

type FormatKey = keyof typeof formatConfigs;

app.post("/query", async (c) => {
    const { sql, params = [], format = "json" } = await c.req.json();
    const config = formatConfigs[format.toLowerCase() as FormatKey] || formatConfigs.json;
    const tempFilePath = await Deno.makeTempFile();

    try {
        // Execute COPY TO command
        conn.exec(`COPY (${sql}) TO '${tempFilePath}' (FORMAT ${config.command})`, ...params);

        // Read the file content
        const result = await Deno.readFile(tempFilePath);

        // Set headers and return file content
        c.header("Content-Type", config.contentType);
        c.header("Content-Disposition", `attachment; filename="result.${config.ext}"`);
        return c.body(result);
    } catch (error) {
        console.error("Error executing query:", error);
        return c.json({ error: "Failed to execute query" }, 500);
    } finally {
        // Always try to remove the temp file
        try {
            await Deno.remove(tempFilePath);
        } catch (removeError) {
            console.error("Error removing temp file:", removeError);
        }
    }
});

const port = 8000;
console.log(`Server running on http://localhost:${port}`);

Deno.serve({ port }, app.fetch);
