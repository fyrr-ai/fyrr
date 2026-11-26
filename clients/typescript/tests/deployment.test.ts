// The client against a real `fyrr writer serve` and `fyrr reader serve` on local storage.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, test } from "node:test";

import { Client, FyrrError, type WriteRequest } from "../src/index.ts";
import { Deployment } from "./deployment.ts";

let deployment: Deployment;

before(async () => {
  deployment = await Deployment.start();
});
after(() => deployment.stop());

function admin(): Client {
  return new Client({
    readerUrl: deployment.readerUrl,
    writerUrl: deployment.writerUrl,
    apiKey: deployment.adminKey,
  });
}

const freshNamespace = () => `ts-client-${randomUUID().slice(0, 12)}`;

// A row that carries its own vector makes no embedding call.
const vector = (n: number) => [n, ...new Array<number>(1023).fill(0)];

function rows(count: number): WriteRequest {
  return {
    upserts: Array.from({ length: count }, (_, n) => ({
      id: `doc-${String(n).padStart(2, "0")}`,
      text: `Document number ${n}`,
      vector: vector(n + 1),
    })),
  };
}

async function collect<T>(items: AsyncIterable<T>): Promise<T[]> {
  const all: T[] = [];
  for await (const item of items) all.push(item);
  return all;
}

test("write, query, describe and list", async () => {
  const client = admin();
  const namespace = freshNamespace();
  assert.equal((await client.write(namespace, rows(3))).upserted, 3);
  const found = await client.query(namespace, { query: "document", consistency: "strong" });
  assert.deepEqual(new Set(found.hits.map((hit) => hit.root_id)), new Set(["doc-00", "doc-01", "doc-02"]));
  assert.equal((await client.describeNamespace(namespace)).namespace, namespace);
  assert.ok((await client.listNamespaces(namespace)).namespaces.includes(namespace));
});

test("iterDocuments pages through every document", async () => {
  const client = admin();
  const namespace = freshNamespace();
  await client.write(namespace, rows(7));
  const every = await collect(client.iterDocuments(namespace, { pageSize: 3, consistency: "strong" }));
  assert.deepEqual(
    every.map((hit) => hit.root_id),
    Array.from({ length: 7 }, (_, n) => `doc-${String(n).padStart(2, "0")}`),
  );
  const some = await collect(
    client.iterDocuments(namespace, {
      filter: { field: "root_id", op: "gte", value: "doc-05" },
      pageSize: 1,
      consistency: "strong",
    }),
  );
  assert.deepEqual(
    some.map((hit) => hit.root_id),
    ["doc-05", "doc-06"],
  );
  assert.deepEqual(await collect(client.iterNamespaces(namespace)), [namespace]);
});

test("an error answer is raised with its status and code", async () => {
  // A key with the `read` role only may not list namespaces.
  const client = new Client({ readerUrl: deployment.readerUrl, apiKey: deployment.readKey });
  await assert.rejects(
    client.listNamespaces(),
    (error: unknown) => error instanceof FyrrError && error.status === 403 && error.code === "forbidden",
  );
});

test("a conditional write applies or skips", async () => {
  const client = admin();
  const namespace = freshNamespace();
  await client.write(namespace, { settings: { attributes: { version: "int" } } });
  const row = (text: string) => ({ id: "a", text, version: 1, vector: vector(1) });
  await client.write(namespace, { upserts: [row("one")] });
  const stale: WriteRequest = {
    upserts: [row("two")],
    conditions: { a: { field: "version", op: "lt", value: { row: "version" } } },
  };
  assert.equal((await client.write(namespace, stale)).skipped, 1);
});

test("deleting a namespace the writer holds nothing of is a no-op", async () => {
  // A namespace is named by its path, and a deletion that finds nothing to remove is done: it is
  // what lets an interrupted deletion be run again. `fyrr writer serve` deletes the namespace it
  // is configured for, `main` here, only after the lease of its writer has expired, which takes a
  // minute: that deletion is not tested.
  assert.deepEqual(await admin().deleteNamespace(freshNamespace()), { deleted: true });
});
