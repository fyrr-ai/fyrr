// Retry, backoff, key and paging rules, against a transport that answers as scripted.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { inspect } from "node:util";
import { test, type TestContext } from "node:test";

import {
  Client,
  FyrrError,
  type Transport,
  type TransportRequest,
  type TransportResponse,
  type WriteRequest,
} from "../src/index.ts";

const READER = "http://reader.test";
const WRITER = "http://writer.test";

type Answer = TransportResponse | (() => never);

/** Answers each request with the next of `answers`; a function answer throws. */
function script(...answers: Answer[]): { transport: Transport; seen: TransportRequest[] } {
  const seen: TransportRequest[] = [];
  const pending = [...answers];
  const transport: Transport = async (request) => {
    seen.push(request);
    const answer = pending.shift();
    if (answer === undefined) throw new Error("no answer left");
    return typeof answer === "function" ? answer() : answer;
  };
  return { transport, seen };
}

/** Records the waits instead of making them. */
function noSleep(t: TestContext): number[] {
  const waits: number[] = [];
  t.mock.method(globalThis, "setTimeout", ((callback: () => void, ms: number) => {
    waits.push(ms);
    callback();
    return 0;
  }) as unknown as typeof setTimeout);
  return waits;
}

const json = (body: unknown, status = 200, headers: { "Retry-After"?: string } = {}): TransportResponse => ({
  status,
  retryAfter: headers["Retry-After"],
  text: JSON.stringify(body),
});

const found = () => json({ hits: [] });
const ok = () => json({ upserted: 1, deleted: 0 });

const withCode = (code: string, syscall = "connect") => () => {
  throw Object.assign(new Error(code), { code, syscall });
};
const refused = withCode("ECONNREFUSED");
const noRoute = withCode("EHOSTUNREACH");
const reset = withCode("ECONNRESET");
const timedOut = () => {
  throw new DOMException("The operation timed out", "TimeoutError");
};

function client(fake: { transport: Transport }, options: { maxRetries?: number; backoffMs?: number } = {}) {
  return new Client({ readerUrl: READER, writerUrl: WRITER, apiKey: "k", transport: fake.transport, ...options });
}

const WRITE: WriteRequest = { upserts: [{ id: "a", text: "t" }] };

const changes: [string, (client: Client) => Promise<unknown>][] = [
  ["write", (c) => c.write("ns", WRITE)],
  ["deleteNamespace", (c) => c.deleteNamespace("ns")],
];

for (const status of [408, 429, 500, 503, 504]) {
  test(`a read is retried on ${status}`, async (t) => {
    const waits = noSleep(t);
    const fake = script(json({ error: "busy" }, status), found());
    assert.deepEqual((await client(fake).query("ns", {})).hits, []);
    assert.equal(fake.seen.length, 2);
    assert.equal(waits.length, 1);
  });
}

test("a read is retried after a connection error and a timeout", async (t) => {
  const waits = noSleep(t);
  const fake = script(refused, timedOut, reset, found());
  assert.deepEqual((await client(fake).query("ns", {})).hits, []);
  assert.equal(waits.length, 3);
});

for (const [name, send] of changes) {
  test(`${name} is retried only when it was not sent`, async (t) => {
    const waits = noSleep(t);
    const unreachable = () => {
      throw new Error("connect failed", {
        cause: new AggregateError(
          [
            Object.assign(new Error("a"), { code: "ECONNREFUSED", syscall: "connect" }),
            Object.assign(new Error("b"), { code: "ENETUNREACH", syscall: "connect" }),
          ],
          "",
        ),
      });
    };
    const fake = script(refused, noRoute, unreachable, ok());
    await send(client(fake, { maxRetries: 3 }));
    assert.equal(fake.seen.length, 4);
    assert.equal(waits.length, 3);
  });

  const mayHaveApplied: [string, Answer][] = [
    ["503 outcome_unknown", json({ error: "outcome_unknown" }, 503)],
    ["429", json({ error: "rate_limited" }, 429, { "Retry-After": "1" })],
    ["a timeout", timedOut],
    ["a reset connection", reset],
    // Linux reports these from `read` on a live connection, after the request was written.
    ["EHOSTUNREACH from read", withCode("EHOSTUNREACH", "read")],
    ["ENETUNREACH from read", withCode("ENETUNREACH", "read")],
  ];
  for (const [what, answer] of mayHaveApplied) {
    test(`${name} is not retried after ${what}`, async (t) => {
      const waits = noSleep(t);
      const fake = script(answer, ok());
      await assert.rejects(send(client(fake)));
      assert.equal(fake.seen.length, 1);
      assert.deepEqual(waits, []);
    });
  }
}

test("a read is retried after an unreachable host reported by read", async (t) => {
  const waits = noSleep(t);
  const fake = script(withCode("EHOSTUNREACH", "read"), found());
  await client(fake).query("ns", {});
  assert.equal(waits.length, 1);
});

test("a request that is wrong is not retried, even a read", async (t) => {
  const waits = noSleep(t);
  const invalid = () => {
    throw Object.assign(new TypeError("Invalid character in header content"), { code: "ERR_INVALID_CHAR" });
  };
  const fake = script(invalid, found());
  await assert.rejects(client(fake).query("ns", {}), TypeError);
  assert.equal(fake.seen.length, 1);
  assert.deepEqual(waits, []);
});

test("a read is not retried after a certificate error", async (t) => {
  const waits = noSleep(t);
  for (const code of ["DEPTH_ZERO_SELF_SIGNED_CERT", "CERT_HAS_EXPIRED", "ERR_TLS_CERT_ALTNAME_INVALID", "UNABLE_TO_VERIFY_LEAF_SIGNATURE"]) {
    const fake = script(withCode(code, "connect"), found());
    await assert.rejects(client(fake).query("ns", {}), Error);
    assert.equal(fake.seen.length, 1, code);
  }
  assert.deepEqual(waits, []);
});

/** The cases that the Python client tests too: `clients/retry-cases.json`. */
const shared = JSON.parse(readFileSync(new URL("../../retry-cases.json", import.meta.url), "utf8")) as {
  conditions: Record<string, WriteRequest["conditions"] | null>;
  cases: { conditions: string; answers: string[]; requests: number; outcome: string }[];
};
const sharedAnswers: Record<string, () => Answer> = {
  refused: () => refused,
  ok,
  "429": () => json({ error: "rate_limited" }, 429),
  "503 outcome_unknown": () => json({ error: "outcome_unknown" }, 503),
};

test("a write is retried as the shared cases say", async (t) => {
  noSleep(t);
  for (const sharedCase of shared.cases) {
    const request: WriteRequest = { ...WRITE };
    const conditions = shared.conditions[sharedCase.conditions];
    if (conditions !== null) request.conditions = conditions;
    const fake = script(...sharedCase.answers.map((name) => sharedAnswers[name]!()));
    const outcome = await client(fake)
      .write("ns", request)
      .then(() => "ok")
      .catch((error: unknown) =>
        error instanceof FyrrError ? `${error.status} ${error.code}` : "connection refused",
      );
    assert.equal(fake.seen.length, sharedCase.requests, JSON.stringify(sharedCase));
    assert.equal(outcome, sharedCase.outcome, JSON.stringify(sharedCase));
  }
});

test("an answer that is not JSON is a FyrrError", async () => {
  const fake = script({ status: 200, retryAfter: undefined, text: "<html>" });
  await assert.rejects(
    client(fake).query("ns", {}),
    (error: unknown) => error instanceof FyrrError && error.status === 200 && error.code === "invalid_json",
  );
});

test("other errors are final", async (t) => {
  const waits = noSleep(t);
  const fake = script(json({ error: "invalid_argument" }, 400));
  await assert.rejects(
    client(fake).query("ns", {}),
    (error: unknown) => error instanceof FyrrError && error.status === 400 && error.code === "invalid_argument",
  );
  assert.deepEqual(waits, []);
});

test("a redirect is an error, not followed", async () => {
  const fake = script({ status: 301, retryAfter: undefined, text: "" });
  await assert.rejects(
    client(fake).query("ns", {}),
    (error: unknown) => error instanceof FyrrError && error.status === 301,
  );
});

test("Retry-After is honoured until the cap", async (t) => {
  const waits = noSleep(t);
  const fake = script(
    json({ error: "deadline_exceeded" }, 408, { "Retry-After": "7" }),
    json({ error: "busy" }, 503, { "Retry-After": "100000" }),
    found(),
  );
  await assert.rejects(
    client(fake).query("ns", {}),
    (error: unknown) => error instanceof FyrrError && error.retryAfterMs === 100_000_000,
  );
  assert.deepEqual(waits, [7000]);
  assert.equal(fake.seen.length, 2);
});

test("Retry-After is read as an HTTP date too", async (t) => {
  const waits = noSleep(t);
  const date = new Date(Date.now() + 5000).toUTCString();
  const fake = script(json({ error: "busy" }, 503, { "Retry-After": date }), found());
  await client(fake).query("ns", {});
  const [wait] = waits;
  assert.ok(wait !== undefined && wait > 3000 && wait <= 5000, String(wait));
});

test("the backoff doubles, is jittered and gives up", async (t) => {
  const waits = noSleep(t);
  const fake = script(...Array.from({ length: 3 }, () => json({ error: "busy" }, 503)));
  await assert.rejects(
    client(fake, { maxRetries: 2, backoffMs: 1000 }).query("ns", {}),
    (error: unknown) => error instanceof FyrrError && error.status === 503,
  );
  assert.equal(fake.seen.length, 3);
  const [first, second] = waits;
  assert.ok(first !== undefined && first >= 500 && first <= 1000, String(first));
  assert.ok(second !== undefined && second >= 1000 && second <= 2000, String(second));
});

test("a request has the route, the key and the JSON body of its call", async () => {
  const fake = script(json({ namespaces: [] }), found(), ok(), json({ deleted: true }));
  const c = new Client({
    readerUrl: `${READER}//`,
    writerUrl: `${WRITER}/`,
    apiKey: " key\n",
    transport: fake.transport,
  });
  await c.listNamespaces("a&b");
  await c.query("a/b", { query: "x" });
  await c.write("ns", WRITE);
  await c.deleteNamespace("ns");
  assert.deepEqual(
    fake.seen.map(({ url, method, body }) => [method, url, body]),
    [
      ["GET", `${READER}/v1/namespaces?prefix=a%26b`, undefined],
      ["POST", `${READER}/v1/namespaces/a%2Fb/query`, '{"query":"x"}'],
      ["POST", `${WRITER}/v1/namespaces/ns/documents`, JSON.stringify(WRITE)],
      ["DELETE", `${WRITER}/v1/namespaces/ns`, '{"namespace":"ns"}'],
    ],
  );
  for (const { headers } of fake.seen) {
    assert.equal(headers.Authorization, "Bearer key");
  }
});

test("a key is stripped, checked and never shown", () => {
  const secret = "fyrr_key_secret";
  const fake = script();
  const c = new Client({ readerUrl: READER, apiKey: `${secret}\n`, transport: fake.transport });
  for (const shown of [inspect(c, { showHidden: true, depth: 5 }), JSON.stringify(c), String(c)]) {
    assert.ok(!shown.includes(secret), shown);
  }
  for (const bad of ["", "  ", `${secret}\x00`, `${secret} two`, "kéy"]) {
    assert.throws(
      () => new Client({ readerUrl: READER, apiKey: bad }),
      (error: unknown) =>
        error instanceof TypeError && !error.message.includes(secret) && !error.message.includes("kéy"),
    );
  }
});

test("a key is not in the error of a failed request", async () => {
  const secret = "fyrr_key_secret";
  const fake = script(json({ error: "unauthorized" }, 401));
  const c = new Client({ readerUrl: READER, apiKey: secret, transport: fake.transport });
  await assert.rejects(c.query("ns", {}), (error: unknown) => !inspect(error, { depth: 6 }).includes(secret));
});

test("a SCIM error has its type as the code, or none", async () => {
  const typed = script(json({ schemas: [], status: "404", scimType: "notFound" }, 404));
  await assert.rejects(
    client(typed).deleteNamespace("ns"),
    (error: unknown) => error instanceof FyrrError && error.code === "notFound",
  );
  const untyped = script(json({ schemas: [], status: "403", detail: "no" }, 403));
  await assert.rejects(
    client(untyped).deleteNamespace("ns"),
    (error: unknown) => error instanceof FyrrError && error.code === undefined && error.message === "403",
  );
});

test("a missing writer URL refuses the writer routes", async () => {
  const c = new Client({ readerUrl: READER, apiKey: "k", transport: script().transport });
  await assert.rejects(c.write("ns", WRITE), /writerUrl/);
  await assert.rejects(c.deleteNamespace("ns"), /writerUrl/);
});

test("a URL or a number that is wrong is refused at construction", () => {
  const options = { readerUrl: READER, apiKey: "k" };
  for (const readerUrl of ["", "reader.test", "ftp://reader.test", "/v1"]) {
    assert.throws(() => new Client({ ...options, readerUrl }), TypeError);
  }
  assert.throws(() => new Client({ ...options, writerUrl: "nope" }), TypeError);
  for (const readerUrl of ["http://user:pass@reader.test", "http://reader.test?a=1", "http://reader.test/#top", "http://reader.test?"]) {
    assert.throws(
      () => new Client({ ...options, readerUrl }),
      (error: unknown) => error instanceof TypeError && !error.message.includes("pass"),
    );
  }
  assert.doesNotThrow(() => new Client({ ...options, readerUrl: "http://reader.test:8090/base/" }));
  for (const bad of [{ timeoutMs: 0 }, { maxRetries: -1 }, { maxRetries: 1.5 }, { backoffMs: Number.NaN }]) {
    assert.throws(() => new Client({ ...options, ...bad }), RangeError);
  }
});

/** A server that lists names by prefix and refuses more than `limit` (10,000 in fyrr). */
function listing(existing: string[], limit: number): { transport: Transport } {
  const transport: Transport = async (request) => {
    const url = new URL(request.url);
    assert.equal(url.pathname, "/v1/namespaces"); // the listing alone, no report
    const matching = existing.filter((name) => name.startsWith(url.searchParams.get("prefix") ?? ""));
    return matching.length > limit ? json({ error: "too_many_namespaces" }, 400) : json({ namespaces: matching });
  };
  return { transport };
}

async function collect<T>(items: AsyncIterable<T>): Promise<T[]> {
  const all: T[] = [];
  for await (const item of items) all.push(item);
  return all;
}

test("iterNamespaces lists each first character above the listing limit", async () => {
  const existing = ["a1", "a2", "a3", "ab", "b", "c"];
  assert.deepEqual(await collect(client(listing(existing, 4)).iterNamespaces()), existing);
  assert.deepEqual(await collect(client(listing(existing, 10)).iterNamespaces()), existing);
});

test("iterNamespaces raises when a first character still overflows", async () => {
  // The name "a" would be listed by no narrower prefix: no complete list can be promised.
  const c = client(listing(["a", "a1", "a2", "b"], 2));
  await assert.rejects(
    collect(c.iterNamespaces()),
    (error: unknown) => error instanceof FyrrError && error.code === "too_many_namespaces",
  );
  assert.deepEqual(await collect(c.iterNamespaces("a1")), ["a1"]);
});

test("iterNamespaces raises other errors", async () => {
  const fake = script(json({ error: "forbidden" }, 403));
  await assert.rejects(
    collect(client(fake).iterNamespaces()),
    (error: unknown) => error instanceof FyrrError && error.code === "forbidden",
  );
});

test("iterDocuments continues after the last hit of each page", async () => {
  const hit = (root: string) => ({ source_id: "s", root_id: root });
  const fake = script(json({ hits: [hit("a"), hit("b")] }), json({ hits: [hit("c")] }), json({ hits: [] }));
  const roots = (await collect(client(fake).iterDocuments("ns", { pageSize: 2, consistency: "strong" }))).map(
    (found) => found.root_id,
  );
  assert.deepEqual(roots, ["a", "b", "c"]);
  const bodies = fake.seen.map(({ body }) => JSON.parse(String(body)) as Record<string, unknown>);
  assert.equal(bodies[0]?.filter, undefined);
  assert.deepEqual(bodies[0], {
    order_by: [
      { field: "source_id", direction: "asc" },
      { field: "root_id", direction: "asc" },
    ],
    output: "metadata",
    limit: 2,
    consistency: "strong",
  });
  assert.deepEqual(bodies[1]?.filter, {
    or: [
      { field: "source_id", op: "gt", value: "s" },
      {
        and: [
          { field: "source_id", op: "eq", value: "s" },
          { field: "root_id", op: "gt", value: "b" },
        ],
      },
    ],
  });
});

test("iterDocuments keeps the filter of the caller on every page", async () => {
  const filter = { field: "root_id", op: "gte", value: "a" } as const;
  const fake = script(json({ hits: [{ source_id: "s", root_id: "a" }] }), json({ hits: [] }));
  await collect(client(fake).iterDocuments("ns", { filter }));
  const second = JSON.parse(String(fake.seen[1]?.body)) as { filter: { and: unknown[] } };
  assert.deepEqual(second.filter.and[0], filter);
  assert.equal(second.filter.and.length, 2);
});
