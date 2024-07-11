import { Hono } from "https://deno.land/x/hono@v4.3.11/mod.ts";
import { html } from "https://deno.land/x/hono@v4.3.11/helper.ts";
import { serialize } from "npm:superjson@2.2.1";
import duckdb from "npm:duckdb@1.0.0";

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

async function initDB() {
  db = new duckdb.Database(DB_FILE);
  console.log(`DuckDB initialized with database: ${DB_FILE}`);
  await exec(`CALL dbgen(sf = 0.001);`);
}

const app = new Hono();

// JSON endpoints
app.post("/query", async (c) => {
  const { sql, params = [] } = await c.req.json();
  const result = await query(sql, ...params);
  return result.success ? c.json(result.data) : c.json({ error: result.error }, 500);
});

app.post("/execute", async (c) => {
  const { sql, params = [] } = await c.req.json();
  const result = await exec(sql, ...params);
  return c.json(result);
});

// Main page
app.get("/", (c) => {
  return c.html(html`
<!DOCTYPE html>
<html>
<head>
  <title>DuckDB API Tester</title>
  <link href="https://fonts.googleapis.com/css?family=Roboto:100,300,400,500,700,900" rel="stylesheet">
  <link href="https://cdn.jsdelivr.net/npm/@mdi/font@6.x/css/materialdesignicons.min.css" rel="stylesheet">
  <link href="https://cdn.jsdelivr.net/npm/vuetify@3.3.3/dist/vuetify.min.css" rel="stylesheet">
  <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no, minimal-ui">
</head>
<body>
  <div id="app">
    <v-app>
      <v-main>
        <v-container>
          <h1 class="text-h4 mb-4">DuckDB API Tester</h1>
          <v-textarea v-model="sql" label="Enter SQL" rows="4" class="mb-4"></v-textarea>
          <v-text-field v-model="params" label="Params (comma-separated, optional)" class="mb-4"></v-text-field>
          <v-row class="mb-4">
            <v-col>
              <v-btn color="primary" @click="submit('query')" :disabled="!isReady">Query</v-btn>
            </v-col>
            <v-col>
              <v-btn color="success" @click="submit('execute')" :disabled="!isReady">Execute</v-btn>
            </v-col>
          </v-row>
          <v-alert v-if="error" type="error" class="mb-4">{{ error }}</v-alert>
          <v-alert v-if="message" type="success" class="mb-4">{{ message }}</v-alert>
          <v-data-table
            v-if="tableData.length > 0"
            :headers="tableHeaders"
            :items="tableData"
            :items-per-page="10"
            class="elevation-1"
          ></v-data-table>
          <pre v-if="debugResponse">{{ debugResponse }}</pre>
        </v-container>
      </v-main>
    </v-app>
  </div>

  <script src="https://unpkg.com/vue@3/dist/vue.global.prod.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/vuetify@3.3.3/dist/vuetify.min.js"></script>
  <script>
    const { createApp, ref, computed } = Vue;
    const { createVuetify } = Vuetify;

    const vuetify = createVuetify();

    const app = createApp({
      setup() {
        const sql = ref('');
        const params = ref('');
        const error = ref('');
        const message = ref('');
        const tableData = ref([]);
        const isReady = ref(true);
        const debugResponse = ref('');

        const tableHeaders = computed(() => {
          if (tableData.value.length === 0) return [];
          return Object.keys(tableData.value[0]).map(key => ({
            title: key,
            key: key,
          }));
        });

        async function submit(action) {
          if (!isReady.value) return;
          isReady.value = false;
          error.value = '';
          message.value = '';
          tableData.value = [];
          debugResponse.value = '';

          const paramArray = params.value.split(',').map(p => p.trim()).filter(p => p !== '');
          try {
            const response = await fetch('/' + action, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ sql: sql.value, params: paramArray })
            });
            const data = await response.json();
            debugResponse.value = JSON.stringify(data, null, 2);
            
            if (data.error) {
              error.value = data.error;
            } else if (Array.isArray(data)) {
              tableData.value = data;
              message.value = "Query returned " + data.length + " rows.";
            } else if (typeof data === 'object' && data.message) {
              message.value = data.message;
            } else {
              error.value = 'Unexpected response format';
            }
          } catch (err) {
            error.value = 'Error: ' + err.message;
          } finally {
            isReady.value = true;
          }
        }

        return {
          sql,
          params,
          error,
          message,
          tableData,
          tableHeaders,
          isReady,
          debugResponse,
          submit
        };
      }
    });

    app.use(vuetify);
    app.mount('#app');
  </script>
</body>
</html>
  `);
});

await initDB();
console.log(`Server running on http://localhost:${PORT}`);
Deno.serve({ port: PORT }, app.fetch);
