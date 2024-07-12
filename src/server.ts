// server.ts

import { Hono } from "https://deno.land/x/hono@v4.3.11/mod.ts";
import { renderToString } from "npm:@vue/server-renderer";
import { createSSRApp } from "npm:vue";
import { serialize } from "npm:superjson@2.2.1";
import duckdb from "npm:duckdb@1.0.0";
import { parse } from "npm:@vue/compiler-sfc";

const PORT = parseInt(Deno.env.get("PORT") || "3000");
const DB_FILE = Deno.env.get("DB_FILE") || ":memory:";

let db: duckdb.Database;

function query(sql: string, ...params: unknown[]): Promise<{ success: boolean; data?: any; error?: string }> {
  return new Promise((resolve) => {
    db.all(sql, ...params, (err, result) => {
      console.log(sql);
      if (err) {
        resolve({ success: false, error: err.message });
      } else {
        const { json } = serialize(result);
        resolve({ success: true, data: json });
      }
    });
  });
}

function exec(sql: string, ...params: unknown[]): Promise<{ success: boolean; message?: string; error?: string }> {
  return new Promise((resolve) => {
    db.exec(sql, ...params, (err) => {
      console.log(sql);
      if (err) {
        resolve({ success: false, error: err.message });
      } else {
        resolve({ success: true, message: "Statement executed successfully" });
      }
    });
  });
}

function initDB() {
  db = new duckdb.Database(DB_FILE);
  console.log(`DuckDB initialized with database: ${DB_FILE}`);
}

const app = new Hono();

// JSON endpoints
app.post("/api/query", async (c) => {
  const { sql, params = [] } = await c.req.json();
  const result = await query(sql, ...params);
  return result.success ? c.json(result.data) : c.json({ error: result.error }, 500);
});

app.post("/api/execute", async (c) => {
  const { sql, params = [] } = await c.req.json();
  const result = await exec(sql, ...params);
  return c.json(result);
});

// Serve Vue app
app.get("/", async (c) => {
  const vueSource = await Deno.readTextFile("./src/client.vue");
  const { descriptor } = parse(vueSource);

  const app = createSSRApp({
    template: descriptor.template?.content,
    ...descriptor.script?.content,
  });

  const appContent = await renderToString(app);

  const html = `
    <!DOCTYPE html>
    <html>
      <head>
        <title>DuckDB API Tester</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>
      </head>
      <body>
        <div id="app">${appContent}</div>
        <script>
          const app = Vue.createApp({
            template: ${JSON.stringify(descriptor.template?.content)},
            ${descriptor.script?.content.replace("export default ", "setup: () => (")}
          })
          app.mount('#app')
        </script>
      </body>
    </html>
  `;

  return c.html(html);
});

initDB();
console.log(`Server running on http://localhost:${PORT}`);
Deno.serve({ port: PORT }, app.fetch);
