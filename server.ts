async function fileExists(filename: string): Promise<boolean> {
  try {
    await Deno.stat(filename);
    return true;
  } catch (error) {
    if (error instanceof Deno.errors.NotFound) {
      return false;
    }
    throw error;
  }
}

async function ensureDuckDB(): Promise<void> {
  if (await fileExists("./duckdb")) {
    console.log("DuckDB found locally.");
    return;
  }

  console.log("DuckDB not found. Installing...");

  const os = Deno.build.os;
  let arch = Deno.build.arch;

  // Convert x86_64 to amd64
  if (arch === "x86_64") {
    arch = "amd64";
  }

  let downloadUrl: string;

  switch (os) {
    case "windows":
      downloadUrl = `https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-windows-${arch}.zip`;
      break;
    case "darwin":
      downloadUrl = "https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-osx-universal.zip";
      break;
    case "linux":
      downloadUrl = `https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-${arch}.zip`;
      break;
    default:
      throw new Error(`Unsupported operating system: ${os}`);
  }

  let installCmd: Deno.Command;

  if (os === "windows") {
    installCmd = new Deno.Command("powershell", {
      args: [
        "-Command",
        `Invoke-WebRequest -Uri ${downloadUrl} -OutFile duckdb.zip; Expand-Archive duckdb.zip .; Remove-Item duckdb.zip`
      ],
    });
  } else {
    installCmd = new Deno.Command("sh", {
      args: [
        "-c",
        `curl -L ${downloadUrl} -o duckdb.zip && unzip duckdb.zip && rm duckdb.zip && chmod +x duckdb`
      ],
    });
  }

  const installProcess = installCmd.spawn();
  const status = await installProcess.status;

  if (status.success) {
    console.log("DuckDB installed successfully.");
  } else {
    throw new Error("Failed to install DuckDB.");
  }
}

function handleWebSocket(ws: WebSocket, dbFilename: string) {
  console.log("WebSocket connection established");

  const duckdbCommand = new Deno.Command("./duckdb", {
    args: [dbFilename],
    stdin: "piped",
    stdout: "piped",
    stderr: "piped",
  });

  const duckdbProcess = duckdbCommand.spawn();

  const stdinWriter = duckdbProcess.stdin.getWriter();

  ws.onmessage = async (event) => {
    if (typeof event.data === "string") {
      const encoder = new TextEncoder();
      await stdinWriter.write(encoder.encode(event.data + "\n"));
    }
  };

  (async () => {
    const decoder = new TextDecoder();
    for await (const chunk of duckdbProcess.stdout) {
      ws.send(JSON.stringify({ type: "stdout", data: decoder.decode(chunk) }));
    }
  })();

  (async () => {
    const decoder = new TextDecoder();
    for await (const chunk of duckdbProcess.stderr) {
      ws.send(JSON.stringify({ type: "stderr", data: decoder.decode(chunk) }));
    }
  })();

  ws.onclose = () => {
    console.log("WebSocket connection closed");
    stdinWriter.close();
    duckdbProcess.kill();
  };
}

async function handleRequest(request: Request, dbFilename: string): Promise<Response> {
  const { pathname } = new URL(request.url);

  if (request.headers.get("upgrade") === "websocket") {
    const { socket, response } = Deno.upgradeWebSocket(request);
    handleWebSocket(socket, dbFilename);
    return response;
  }

  if (pathname === "/") {
    try {
      const html = await Deno.readTextFile("./server.html");
      return new Response(html, {
        headers: { "content-type": "text/html; charset=utf-8" },
      });
    } catch (error) {
      console.error("Error reading HTML file:", error);
      return new Response("Error loading page", { status: 500 });
    }
  }

  return new Response("Not Found", { status: 404 });
}

const port = 8000;

try {
  await ensureDuckDB();

  // Get the database filename from command line arguments, default to ":memory:"
  const dbFilename = Deno.args[0] || ":memory:";

  console.log(`Using database: ${dbFilename}`);
  console.log(`Server running on http://localhost:${port}`);

  await Deno.serve({ port }, (request) => handleRequest(request, dbFilename));
} catch (error) {
  console.error("Failed to start server:", error);
}