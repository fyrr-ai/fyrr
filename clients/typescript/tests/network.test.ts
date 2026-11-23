// The client over real sockets and its default transport, against small local HTTP servers.

import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { createServer as createSocketServer } from "node:net";
import { promisify } from "node:util";
import { gzipSync } from "node:zlib";
import type { AddressInfo } from "node:net";
import { test } from "node:test";

import { Client, FyrrError, nodeTransport, type Transport } from "../src/index.ts";

type Handler = (request: IncomingMessage, response: ServerResponse) => void;

async function serve(handler: Handler): Promise<{ url: string; server: Server; connections: () => number }> {
  let connections = 0;
  const server = createServer(handler);
  server.on("connection", () => connections++);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, server, connections: () => connections };
}

/** The default transport, counting its calls. */
function counting(): { transport: Transport; calls: () => number } {
  let calls = 0;
  return {
    transport: (request) => {
      calls++;
      return nodeTransport(request);
    },
    calls: () => calls,
  };
}

const answer = (response: ServerResponse, body: unknown) => {
  response.setHeader("Content-Type", "application/json");
  response.end(JSON.stringify(body));
};

const WRITE = { upserts: [{ id: "a", text: "t" }] };

test("requests reuse one connection and carry no Origin header", async () => {
  const origins: (string | undefined)[] = [];
  const { url, server, connections } = await serve((request, response) => {
    origins.push(request.headers.origin);
    answer(response, { hits: [] });
  });
  try {
    const client = new Client({ readerUrl: url, apiKey: "k" });
    for (let n = 0; n < 20; n++) await client.query("ns", {});
    assert.equal(connections(), 1);
    assert.deepEqual(origins, new Array(20).fill(undefined));
  } finally {
    server.closeAllConnections();
    server.close();
  }
});

test("a write to a refused connection is retried: it was never sent", async () => {
  const { url, server } = await serve(() => undefined);
  const port = new URL(url).port;
  await new Promise((resolve) => server.close(resolve));
  const counter = counting();
  const client = new Client({
    readerUrl: url,
    writerUrl: `http://127.0.0.1:${port}`,
    apiKey: "k",
    transport: counter.transport,
    maxRetries: 2,
    backoffMs: 1,
  });
  await assert.rejects(client.write("ns", WRITE), Error);
  assert.equal(counter.calls(), 3);
});

test("a connection dropped after the request was sent is retried for a read, not for a write", async () => {
  const { url, server } = await serve((request) => request.socket.destroy());
  try {
    const counter = counting();
    const client = new Client({
      readerUrl: url,
      writerUrl: url,
      apiKey: "k",
      transport: counter.transport,
      maxRetries: 2,
      backoffMs: 1,
    });
    await assert.rejects(client.write("ns", WRITE), Error);
    assert.equal(counter.calls(), 1);
    await assert.rejects(client.query("ns", {}), Error);
    assert.equal(counter.calls(), 1 + 3);
  } finally {
    server.closeAllConnections();
    server.close();
  }
});

test("a request that gets no answer times out: a read is retried, a write is not", async () => {
  const { url, server } = await serve(() => undefined);
  try {
    const counter = counting();
    const client = new Client({
      readerUrl: url,
      writerUrl: url,
      apiKey: "k",
      transport: counter.transport,
      timeoutMs: 100,
      maxRetries: 1,
      backoffMs: 1,
    });
    const timeout = (error: unknown) => error instanceof DOMException && error.name === "TimeoutError";
    await assert.rejects(client.write("ns", WRITE), timeout);
    assert.equal(counter.calls(), 1);
    await assert.rejects(client.query("ns", {}), timeout);
    assert.equal(counter.calls(), 1 + 2);
  } finally {
    server.closeAllConnections();
    server.close();
  }
});

test("a 421 answer is raised, and the request is sent once", async () => {
  // The `fetch` of Node.js repeats a request after a 421 by itself, a write and a deletion too.
  let received = 0;
  const { url, server } = await serve((request, response) => {
    request.resume();
    received++;
    response.statusCode = 421;
    answer(response, { error: "misdirected" });
  });
  try {
    const client = new Client({ readerUrl: url, writerUrl: url, apiKey: "k", maxRetries: 0 });
    const misdirected = (error: unknown) => error instanceof FyrrError && error.status === 421;
    await assert.rejects(client.write("ns", { ...WRITE, conditions: { a: { field: "v", op: "lt", value: 2 } } }), misdirected);
    assert.equal(received, 1);
    await assert.rejects(client.deleteNamespace("ns"), misdirected);
    assert.equal(received, 2);
    await assert.rejects(client.query("ns", {}), misdirected);
    assert.equal(received, 3);
  } finally {
    server.closeAllConnections();
    server.close();
  }
});

test("an integer beyond 2^53 is a bigint in an answer and exact in a request", async () => {
  let body = "";
  const { url, server } = await serve((request, response) => {
    request.setEncoding("utf8");
    request.on("data", (chunk: string) => (body += chunk));
    request.on("end", () =>
      response.end('{"upserted":9007199254740993,"deleted":-9007199254740993,"skipped":7,"ratio":0.5,"huge":1e300,"list":[18446744073709551615]}'),
    );
  });
  try {
    const client = new Client({ readerUrl: url, writerUrl: url, apiKey: "k" });
    const row = { id: "a", text: "t", big: 9007199254740993n, small: 7n, nested: [{ n: -9007199254740993n }] };
    const written = (await client.write("ns", { upserts: [row] })) as unknown as Record<string, unknown>;
    assert.equal(body, '{"upserts":[{"id":"a","text":"t","big":9007199254740993,"small":7,"nested":[{"n":-9007199254740993}]}]}');
    assert.deepEqual(written, {
      upserted: 9007199254740993n,
      deleted: -9007199254740993n,
      skipped: 7,
      ratio: 0.5,
      huge: 1e300,
      list: [18446744073709551615n],
    });
  } finally {
    server.closeAllConnections();
    server.close();
  }
});

test("a compressed answer is refused with a clear error, once", async () => {
  const accepted: (string | undefined)[] = [];
  const { url, server } = await serve((request, response) => {
    accepted.push(request.headers["accept-encoding"]);
    response.setHeader("Content-Encoding", "gzip");
    response.end(gzipSync('{"hits":[]}'));
  });
  try {
    const client = new Client({ readerUrl: url, apiKey: "k" });
    await assert.rejects(client.query("ns", {}), /Content-Encoding gzip/);
    assert.deepEqual(accepted, ["identity"]);
  } finally {
    server.closeAllConnections();
    server.close();
  }
});

test("a 101 answer fails the request instead of hanging it", async () => {
  let received = 0;
  const server = createSocketServer((socket) => {
    socket.on("data", () => {
      received++;
      socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: x\r\nConnection: Upgrade\r\n\r\n");
    });
    socket.on("error", () => undefined);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    const client = new Client({ readerUrl: url, apiKey: "k", timeoutMs: 5000 });
    await assert.rejects(client.query("ns", {}), /switch protocols/);
    assert.equal(received, 1);
  } finally {
    server.close();
  }
});

test("NODE_DEBUG=http does not print the key", async () => {
  const secret = "fyrr_key_debug_secret";
  const { url, server } = await serve((request, response) => {
    request.resume();
    answer(response, { hits: [] });
  });
  try {
    const entry = new URL("../src/index.ts", import.meta.url).href;
    const script = `import { Client } from ${JSON.stringify(entry)};
      await new Client({ readerUrl: ${JSON.stringify(url)}, apiKey: ${JSON.stringify(secret)} }).query("ns", {});`;
    const { stdout, stderr } = await promisify(execFile)(
      process.execPath,
      ["--input-type=module", "-e", script],
      { env: { ...process.env, NODE_DEBUG: "http" } },
    );
    assert.ok(stderr.includes("HTTP"), "the debug output is on"); // so the check below can fail
    assert.ok(!(stdout + stderr).includes(secret), stderr);
  } finally {
    server.closeAllConnections();
    server.close();
  }
});
