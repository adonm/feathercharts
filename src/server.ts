import { crypto } from "https://deno.land/std@0.224.0/crypto/mod.ts";
import { Application, Router, send } from "https://deno.land/x/oak@v16.1.0/mod.ts";

const hashQuery = async (str: string): Promise<string> => {
  const hash = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(str));
  return Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, '0')).join('');
};

const fileExists = async (file: string): Promise<boolean> => await Deno.stat(file).then(() => true).catch(() => false);

const getDuckDB = async (): Promise<string> => {
  for (const path of [await new Deno.Command("which", { args: ["duckdb"] }).output().then(({ stdout }) => new TextDecoder().decode(stdout).trim()).catch(() => ""), "/tmp/duckdb"]) {
    if (await fileExists(path)) return path;
  }
  const os = Deno.build.os === "darwin" ? "osx" : "linux", arch = Deno.build.arch === "x86_64" ? "amd64" : Deno.build.arch;
  const url = `https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-${os}-${arch}.zip`;
  const { success } = await new Deno.Command("sh", { args: ["-c", `curl -L ${url} -o /tmp/duckdb.zip && unzip /tmp/duckdb.zip -d /tmp && rm /tmp/duckdb.zip`] }).output();
  if (!success) throw new Error("Failed to install DuckDB.");
  return "/tmp/duckdb";
};

class DuckDB {
  private proc: Deno.ChildProcess;
  private stdin: WritableStreamDefaultWriter<Uint8Array>;
  private stdout: ReadableStreamDefaultReader<Uint8Array>;
  private stderr: ReadableStreamDefaultReader<Uint8Array>;

  constructor(path: string, file: string) {
    this.proc = new Deno.Command(path, { args: [file], stdin: "piped", stdout: "piped", stderr: "piped" }).spawn();
    this.stdin = this.proc.stdin.getWriter();
    this.stdout = this.proc.stdout.getReader();
    this.stderr = this.proc.stderr.getReader();
    [".echo on", ".timer on"].forEach(cmd => this.runCmd(cmd));
    this.initQueryHistoryTable();
  }

  async runCmd(cmd: string): Promise<string> {
    await this.stdin.write(new TextEncoder().encode(cmd + "\n"));
    let out = "", err = "";
    while (true) {
      const { done, value } = await this.stdout.read();
      if (done) break;
      out += new TextDecoder().decode(value);
      if (out.includes("Run Time:")) break;
    }
    while (true) {
      const { done, value } = await this.stderr.read();
      if (done) break;
      err += new TextDecoder().decode(value);
      if (err.includes("\n")) break;
    }
    if (err) throw new Error(err.trim());
    return out.trim();
  }

  async query(sql: string, params: any[] = []): Promise<any> {
    const paramPlaceholders = params.map((_, i) => `$${i + 1}`).join(', ');
    const fullSql = sql.replace('?', paramPlaceholders);
    const result = await this.runCmd(`PREPARE stmt AS ${fullSql}; EXECUTE stmt${params.length > 0 ? ` (${params.join(', ')})` : ''};`);
    return JSON.parse(result);
  }

  async initQueryHistoryTable() {
    await this.runCmd(`
      CREATE TABLE IF NOT EXISTS _query_history (
        query_hash VARCHAR,
        query_text TEXT,
        result_file VARCHAR,
        query_time DOUBLE,
        execution_time TIMESTAMP
      );
    `);
  }

  async saveQueryHistory(queryHash: string, queryText: string, resultFile: string, queryTime: number) {
    const escapedQueryText = queryText.replace(/'/g, "''");
    const cmd = `
      INSERT INTO _query_history (query_hash, query_text, result_file, query_time, execution_time)
      VALUES ('${queryHash}', '${escapedQueryText}', '${resultFile}', ${queryTime}, CURRENT_TIMESTAMP);
    `;
    await this.runCmd(cmd);
  }

  async getAllTables(): Promise<string[]> {
    const result = await this.query("SHOW ALL TABLES;");
    return result.map((row: { name: string }) => row.name);
  }

  async close() {
    await this.stdin.close();
    this.proc.kill();
  }
}

function createRestApi(db: DuckDB) {
  const router = new Router();

  router.get("/api/:table", async (ctx) => {
    const table = ctx.params.table;
    const query = ctx.request.url.searchParams;
    
    let sql = `SELECT * FROM ${table}`;
    const whereClauses = [];
    const params = [];

    for (const [key, value] of query.entries()) {
      if (key !== "order" && key !== "limit" && key !== "offset" && key !== "format") {
        whereClauses.push(`${key} = ?`);
        params.push(value);
      }
    }
    
    if (whereClauses.length > 0) {
      sql += ` WHERE ${whereClauses.join(" AND ")}`;
    }

    const order = query.get("order");
    if (order) {
      sql += ` ORDER BY ${order}`;
    }

    const limit = query.get("limit");
    if (limit) {
      sql += ` LIMIT ${limit}`;
    }
    const offset = query.get("offset");
    if (offset) {
      sql += ` OFFSET ${offset}`;
    }

    const format = query.get("format") || "json";

    try {
      if (format === "parquet") {
        const tempParquetFile = `html/temp_${Date.now()}.parquet`;
        await db.runCmd(`COPY (${sql}) TO '${tempParquetFile}' (FORMAT PARQUET)`);
        const parquetContent = await Deno.readFile(tempParquetFile);
        await Deno.remove(tempParquetFile);
        ctx.response.headers.set("Content-Type", "application/octet-stream");
        ctx.response.headers.set("Content-Disposition", "attachment; filename=result.parquet");
        ctx.response.body = parquetContent;
      } else {
        const result = await db.query(sql, params);
        ctx.response.body = result;
      }
    } catch (error) {
      ctx.response.status = 400;
      ctx.response.body = { error: error.message };
    }
  });

  router.post("/api/:table", async (ctx) => {
    const table = ctx.params.table;
    const body = await ctx.request.body;
    
    const columns = Object.keys(body).join(", ");
    const values = Object.values(body);
    const placeholders = values.map(() => "?").join(", ");

    const sql = `INSERT INTO ${table} (${columns}) VALUES (${placeholders}) RETURNING *`;

    try {
      const result = await db.query(sql, values);
      ctx.response.body = result[0];
    } catch (error) {
      ctx.response.status = 400;
      ctx.response.body = { error: error.message };
    }
  });

  router.put("/api/:table/:id", async (ctx) => {
    const table = ctx.params.table;
    const id = ctx.params.id;
    const body = await ctx.request.body;
    
    const setClauses = Object.keys(body).map(key => `${key} = ?`).join(", ");
    const values = Object.values(body);

    const sql = `UPDATE ${table} SET ${setClauses} WHERE id = ? RETURNING *`;

    try {
      const result = await db.query(sql, [...values, id]);
      ctx.response.body = result[0];
    } catch (error) {
      ctx.response.status = 400;
      ctx.response.body = { error: error.message };
    }
  });

  router.delete("/api/:table/:id", async (ctx) => {
    const table = ctx.params.table;
    const id = ctx.params.id;

    const sql = `DELETE FROM ${table} WHERE id = ? RETURNING *`;

    try {
      const result = await db.query(sql, [id]);
      ctx.response.body = result[0];
    } catch (error) {
      ctx.response.status = 400;
      ctx.response.body = { error: error.message };
    }
  });

  router.post("/api/query", async (ctx) => {
    const query = await ctx.request.body.text();
    const format = ctx.request.url.searchParams.get("format") || "json";

    try {
      const startTime = performance.now();
      const hash = await hashQuery(query);
      const sqlPath = `queries/${hash}.sql`;
      await Deno.writeTextFile(`html/${sqlPath}`, query);

      if (format === "parquet") {
        const parquetPath = `queries/${hash}.parquet`;
        await db.runCmd(`COPY (${query}) TO 'html/${parquetPath}' (FORMAT PARQUET);`);
        const endTime = performance.now();
        const queryTime = (endTime - startTime) / 1000;
        await db.saveQueryHistory(hash, query, parquetPath, queryTime);

        const parquetContent = await Deno.readFile(`html/${parquetPath}`);
        ctx.response.headers.set("Content-Type", "application/octet-stream");
        ctx.response.headers.set("Content-Disposition", `attachment; filename=${hash}.parquet`);
        ctx.response.body = parquetContent;
      } else {
        const output = await db.runCmd(query);
        const endTime = performance.now();
        const queryTime = (endTime - startTime) / 1000;
        await db.saveQueryHistory(hash, query, '', queryTime);
        ctx.response.body = output;
      }
    } catch (error) {
      ctx.response.status = 400;
      ctx.response.body = { error: error.message };
    }
  });

  return router;
}

const main = async () => {
  const dbPath = await getDuckDB().catch((error) => { console.error("Failed to ensure DuckDB:", error); Deno.exit(1); });
  const dbFile = Deno.env.get("DB_FILE") ?? ":memory:";
  const port = parseInt(Deno.env.get("PORT") ?? "") || 3000;
  await Deno.mkdir("html/queries", { recursive: true });
  console.log(`Using database: ${dbFile}\nServer running on http://localhost:${port}`);
  const db = new DuckDB(dbPath, dbFile);

  const app = new Application();
  const restApi = createRestApi(db);

  app.use(restApi.routes());
  app.use(restApi.allowedMethods());

  // Serve static files
  app.use(async (ctx) => {
    try {
      await send(ctx, ctx.request.url.pathname, {
        root: `${Deno.cwd()}/html`,
        index: "index.html",
      });
    } catch {
      ctx.response.status = 404;
      ctx.response.body = "404 Not Found";
    }
  });

  await app.listen({ port });
};

main();