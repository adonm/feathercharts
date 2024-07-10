import { serveDir } from "https://deno.land/std@0.224.0/http/file_server.ts";

const fileExists = async (filename: string): Promise<boolean> => await Deno.stat(filename).then(() => true).catch(() => false);

const ensureDuckDB = async (): Promise<string> => {
  for (
    const path of [
      await new Deno.Command("which", { args: ["duckdb"] }).output().then(({ stdout }) => new TextDecoder().decode(stdout).trim()).catch(() => ""),
      "/tmp/duckdb",
    ]
  ) {
    if (await fileExists(path)) return path;
  }

  const os = Deno.build.os === "darwin" ? "osx" : "linux";
  const arch = Deno.build.arch === "x86_64" ? "amd64" : Deno.build.arch;
  const url = `https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-${os}-${arch}.zip`;

  const { success } = await new Deno.Command("sh", {
    args: ["-c", `curl -L ${url} -o /tmp/duckdb.zip && unzip /tmp/duckdb.zip -d /tmp && rm /tmp/duckdb.zip`],
  }).output();

  if (!success) throw new Error("Failed to install DuckDB.");
  return "/tmp/duckdb";
};

const handleWebSocket = (ws: WebSocket, dbFilename: string, duckdbPath: string) => {
  const duckdbProcess = new Deno.Command(duckdbPath, { args: [dbFilename, "--json"], stdin: "piped", stdout: "piped", stderr: "piped" }).spawn();
  const stdinWriter = duckdbProcess.stdin.getWriter();

  ws.onmessage = async ({ data }) => {
    if (typeof data === "string") await stdinWriter.write(new TextEncoder().encode(data + "\n"));
  };

  ["stdout", "stderr"].forEach((stream) => {
    (async () => {
      const reader = duckdbProcess[stream as "stdout" | "stderr"].getReader();
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        const content = new TextDecoder().decode(value);
        ws.send(`${stream}:${content}`);
      }
    })();
  });

  ws.onclose = () => {
    stdinWriter.close();
    duckdbProcess.kill();
  };
};

const handleRequest = async (request: Request, dbFilename: string, duckdbPath: string): Promise<Response> => {
  if (request.headers.get("upgrade") === "websocket") {
    const { socket, response } = Deno.upgradeWebSocket(request);
    handleWebSocket(socket, dbFilename, duckdbPath);
    return response;
  }

  // Serve static files from the current directory
  return await serveDir(request, {
    fsRoot: "html",
  });
};

const duckdbPath = await ensureDuckDB().catch((error) => {
  console.error("Failed to ensure DuckDB:", error);
  Deno.exit(1);
});
const dbFilename = Deno.env.get("DB_FILE") ?? ":memory:";
const port = parseInt(Deno.env.get("PORT") ?? "") || 3000;

console.log(`Using database: ${dbFilename}\nServer running on http://localhost:${port}`);

Deno.serve({ port }, (request) => handleRequest(request, dbFilename, duckdbPath));
