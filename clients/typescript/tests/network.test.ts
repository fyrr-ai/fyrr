// The client over real sockets: Node's own `fetch`, against small local HTTP servers.

import assert from "node:assert/strict";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";

import { Client } from "../src/index.ts";

type Handler = (request: IncomingMessage, response: ServerResponse) => void;

async function serve(handler: Handler): Promise<{ url: string; server: Server; connections: () => number }> {
  let connections = 0;
  const server = createServer(handler);
  server.on("connection", () => connections++);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, server, connections: () => connections };
}

/** A `fetch` that counts its calls. */
function counting(): { fetch: typeof fetch; calls: () => number } {
  let calls = 0;
  return {
    fetch: ((...args: Parameters<typeof fetch>) => {
      calls++;
      return fetch(...args);
    }) as typeof fetch,
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
    // The pool may open a second connection while the first is still handing back its socket.
    assert.ok(connections() <= 2, `${connections()} connections for 20 requests`);
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
    fetch: counter.fetch,
    maxRetries: 2,
    backoffMs: 1,
  });
  await assert.rejects(client.write("ns", WRITE), TypeError);
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
      fetch: counter.fetch,
      maxRetries: 2,
      backoffMs: 1,
    });
    await assert.rejects(client.write("ns", WRITE), TypeError);
    assert.equal(counter.calls(), 1);
    await assert.rejects(client.query("ns", {}), TypeError);
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
      fetch: counter.fetch,
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
