// server.ts

import { Application, Router, send } from "https://deno.land/x/oak@v16.1.0/mod.ts";
import duckdb from "npm:duckdb@1.0.0";

const PORT = parseInt(Deno.env.get("PORT") || "3000");
const DB_FILE = Deno.env.get("DB_FILE") || ":memory:";

let db: duckdb.Database;

// deno-lint-ignore no-explicit-any
function query(sql: string, ...params: any[]): Promise<duckdb.RowData[]> {
  return new Promise((resolve, reject) => {
    db.all(sql, ...params, (err, result) => {
      console.log(sql);
      if (err) reject(err);
      else resolve(result);
    });
  });
}

// deno-lint-ignore no-explicit-any
function exec(sql: string, ...params: any[]): Promise<void> {
  return new Promise((resolve, reject) => {
    db.exec(sql, ...params, (err) => {
      console.log(sql);
      if (err) reject(err);
      else resolve();
    });
  });
}

async function initDB() {
  db = new duckdb.Database(DB_FILE);
  
  console.log(`DuckDB initialized with database: ${DB_FILE}`);

  // Create sample tables
  await exec(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY,
      name VARCHAR,
      email VARCHAR
    )
  `);
  await exec(`
    CREATE TABLE IF NOT EXISTS products (
      id INTEGER PRIMARY KEY,
      name VARCHAR,
      price DECIMAL(10, 2)
    )
  `);
}

await initDB();

function logQuery(sql: string) {
  console.log(`Executing query: ${sql}`);
}

const router = new Router();

router
  .get("/api/tables", async (ctx) => {
    try {
      const result = await query(`SHOW ALL TABLES`);
      ctx.response.body = result;
    } catch (err) {
      ctx.response.status = 500;
      ctx.response.body = { error: err.message };
    }
  })
  .get("/api/:table", async (ctx) => {
    try {
      const tableName = ctx.params.table;
      let sql = `FROM ${tableName}`;
      const params = [];

      if (ctx.request.url.searchParams.toString()) {
        const conditions = Array.from(ctx.request.url.searchParams.entries())
          .map(([key, _value]) => `${key} = ?`)
          .join(" AND ");
        sql += ` WHERE ${conditions}`;
        params.push(...ctx.request.url.searchParams.values());
      }

      logQuery(sql);
      const result = await query(sql, ...params);
      ctx.response.body = result;
    } catch (err) {
      ctx.response.status = 500;
      ctx.response.body = { error: err.message };
    }
  })
  .post("/api/:table", async (ctx) => {
    try {
      const tableName = ctx.params.table;
      const body = await ctx.request.body.json();
      const columns = Object.keys(body).join(", ");
      const placeholders = Object.keys(body).map(() => "?").join(", ");
      const sql = `INSERT INTO ${tableName} (${columns}) VALUES (${placeholders})`;
      const values = Object.values(body);

      logQuery(sql + values);
      await exec(sql, ...values);
      ctx.response.body = { message: "Record inserted successfully" };
    } catch (err) {
      ctx.response.status = 500;
      ctx.response.body = { error: err.message };
    }
  })
  .patch("/api/:table/:id", async (ctx) => {
    try {
      const tableName = ctx.params.table;
      const id = ctx.params.id;
      const body = await ctx.request.body.json();
      const updateSet = Object.keys(body)
        .map((key) => `${key} = ?`)
        .join(", ");
      const sql = `UPDATE ${tableName} SET ${updateSet} WHERE id = ?`;
      const values = [...Object.values(body), id];

      logQuery(sql + values);
      await exec(sql, ...values);
      ctx.response.body = { message: "Record updated successfully" };
    } catch (err) {
      ctx.response.status = 500;
      ctx.response.body = { error: err.message };
    }
  })
  .delete("/api/:table/:id", async (ctx) => {
    try {
      const tableName = ctx.params.table;
      const id = ctx.params.id;
      const sql = `DELETE FROM ${tableName} WHERE id = ?`;

      logQuery(sql);
      await exec(sql, id);
      ctx.response.body = { message: "Record deleted successfully" };
    } catch (err) {
      ctx.response.status = 500;
      ctx.response.body = { error: err.message };
    }
  });

router.get("/(.*)", async (context) => {
  const path = context.params[0];
  await send(context, path, {
    root: `${Deno.cwd()}/html`,
    index: "index.html",
  });
});

const app = new Application();

app.use(async (ctx, next) => {
  try {
    await next();
  } catch (err) {
    ctx.response.status = 500;
    ctx.response.body = { error: err.message };
  }
});

app.use(router.routes());
app.use(router.allowedMethods());

console.log(`Server running on http://localhost:${PORT}`);
await app.listen({ port: PORT });