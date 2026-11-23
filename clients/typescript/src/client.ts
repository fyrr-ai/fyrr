// Client of the fyrr HTTP API, for server runtimes: it uses `node:http` and `node:https`.

import http from "node:http";
import https from "node:https";

import type {
  Consistency,
  DeleteNamespaceResponse,
  Filter,
  NamespaceList,
  NamespaceReport,
  QueryRequest,
  SearchHit,
  SearchOutput,
  WriteRequest,
  WriteResponse,
} from "./models.ts";

// Longer than the 10 s default deadline of a read (`deadline_ms`), so the server answers
// `408 deadline_exceeded` before the client gives up. A request with a longer `deadline_ms`
// needs a longer `timeoutMs`.
export const DEFAULT_TIMEOUT_MS = 30_000;
// The server asks for 60 s after a `429`; a larger `Retry-After` is raised, not waited for.
const MAX_RETRY_AFTER_MS = 120_000;
const MAX_BACKOFF_MS = 30_000;
// A request that failed with one of these codes in one of these system calls never reached the
// server. The code alone is not enough: Linux also reports EHOSTUNREACH and ENETUNREACH from
// `read` on a live connection, after the request was written.
const UNSENT_CODES = new Set(["ECONNREFUSED", "ENOTFOUND", "EAI_AGAIN", "EHOSTUNREACH", "ENETUNREACH"]);
const UNSENT_CALLS = new Set(["connect", "getaddrinfo"]);
// A request that is wrong, not unlucky: a repeat fails the same way.
const PERMANENT_CODES = new Set([
  "ERR_INVALID_URL",
  "ERR_INVALID_PROTOCOL",
  "ERR_INVALID_CHAR",
  "ERR_INVALID_HTTP_TOKEN",
  "ERR_UNESCAPED_CHARACTERS",
  "ERR_HTTP_INVALID_HEADER_VALUE",
]);
// The first character of a namespace name, in name order.
const FIRST_CHARACTERS = "0123456789abcdefghijklmnopqrstuvwxyz";

/** An error answer: `{"error": "<code>"}` of the API, or a SCIM error. */
export class FyrrError extends Error {
  readonly status: number;
  readonly code: string | undefined;
  /** The `Retry-After` of the answer, in milliseconds. */
  readonly retryAfterMs: number | undefined;

  constructor(status: number, code?: string, retryAfterMs?: number) {
    super(code === undefined ? String(status) : `${status} ${code}`);
    this.name = "FyrrError";
    this.status = status;
    this.code = code;
    this.retryAfterMs = retryAfterMs;
  }
}

export interface ClientOptions {
  /** Base URL of the reader: queries, the namespace list and the report. */
  readerUrl: string;
  /**
   * Base URL of the administration listener of `fyrr writer serve`: writes and namespace
   * deletion. Leave it out for a reader-only deployment.
   */
  writerUrl?: string;
  /** An API key or an OIDC access token. */
  apiKey: string;
  /** Per attempt, in milliseconds. Default 30,000. */
  timeoutMs?: number;
  /** Retries after the first attempt. Default 3. */
  maxRetries?: number;
  /** The first wait before a retry, in milliseconds; it doubles with each attempt. Default 500. */
  backoffMs?: number;
  /** Sends one request, once, instead of `node:http`: for a proxy of your own, or a test. */
  transport?: Transport;
}

export interface TransportRequest {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: string | undefined;
  timeoutMs: number;
}

export interface TransportResponse {
  status: number;
  /** The `Retry-After` header, as sent. */
  retryAfter: string | undefined;
  text: string;
}

/**
 * Sends a request at most once and answers with the whole response, or throws. It never repeats
 * a request: the retry policy of the client does.
 */
export type Transport = (request: TransportRequest) => Promise<TransportResponse>;

export interface IterDocumentsOptions {
  filter?: Filter;
  /** Documents per query, at most 1000. Default 1000. */
  pageSize?: number;
  consistency?: Consistency;
}

// "all": a read, repeated on every failure the policy names. "unsent": a change, repeated only
// when it provably did not reach the server (a failed connect). "none": never repeated.
type Retry = "all" | "unsent" | "none";

interface Route {
  service: "reader" | "writer";
  method: "GET" | "POST" | "DELETE";
  path: string;
  body?: unknown;
  retry: Retry;
}

/**
 * A client of the fyrr HTTP API. Connections are reused: the clients of a process share
 * keep-alive agents, so there is nothing to close.
 *
 * A read is retried on a connection error, a timeout and 408, 429 and 5xx, up to `maxRetries`
 * times, after `backoffMs` doubling each time (jittered), or after the server's `Retry-After`.
 * A write and a namespace deletion may have applied whatever the answer says, so they are
 * retried only when the connection could not be made (nothing was sent); a write with
 * `conditions` is never retried. Repeat those yourself when you know that it is safe.
 */
export class Client {
  // Private fields stay out of `console.log`, `util.inspect` and `JSON.stringify`: the key too.
  readonly #urls: { reader: string; writer: string | undefined };
  readonly #authorization: string;
  readonly #transport: Transport;
  readonly #timeoutMs: number;
  readonly #maxRetries: number;
  readonly #backoffMs: number;

  constructor(options: ClientOptions) {
    this.#urls = {
      reader: checkedUrl("readerUrl", options.readerUrl),
      writer: options.writerUrl === undefined ? undefined : checkedUrl("writerUrl", options.writerUrl),
    };
    this.#authorization = `Bearer ${checkedKey(options.apiKey)}`;
    this.#transport = options.transport ?? nodeTransport;
    this.#timeoutMs = checkedNumber("timeoutMs", options.timeoutMs ?? DEFAULT_TIMEOUT_MS, 1);
    this.#maxRetries = checkedNumber("maxRetries", options.maxRetries ?? 3, 0, true);
    this.#backoffMs = checkedNumber("backoffMs", options.backoffMs ?? 500, 0);
  }

  /** One call: the names that start with `prefix`, or `too_many_namespaces` above 10,000. */
  listNamespaces(prefix?: string): Promise<NamespaceList> {
    const query = prefix ? `?${new URLSearchParams({ prefix })}` : "";
    return this.#send({ service: "reader", method: "GET", path: `/v1/namespaces${query}`, retry: "all" });
  }

  /**
   * Every name that starts with `prefix`, in name order.
   *
   * With more than 10,000 names the API refuses the listing, and the helper lists each first
   * character instead. A prefix that still matches more than 10,000 names raises the server's
   * `too_many_namespaces`: the name equal to that prefix could not be told from its longer
   * neighbours, so no complete list can be promised. Pass a longer `prefix`.
   */
  async *iterNamespaces(prefix = ""): AsyncGenerator<string, void, undefined> {
    let names: string[];
    try {
      names = (await this.listNamespaces(prefix)).namespaces;
    } catch (error) {
      if (!(error instanceof FyrrError) || error.code !== "too_many_namespaces" || prefix) throw error;
      for (const first of FIRST_CHARACTERS) {
        yield* this.iterNamespaces(first);
      }
      return;
    }
    yield* names;
  }

  describeNamespace(namespace: string): Promise<NamespaceReport> {
    return this.#send({ service: "reader", method: "GET", path: namespacePath(namespace), retry: "all" });
  }

  deleteNamespace(namespace: string): Promise<DeleteNamespaceResponse> {
    return this.#send({
      service: "writer",
      method: "DELETE",
      path: namespacePath(namespace),
      body: { namespace },
      retry: "unsent",
    });
  }

  query(namespace: string, request: QueryRequest): Promise<SearchOutput> {
    return this.#send({
      service: "reader",
      method: "POST",
      path: `${namespacePath(namespace)}/query`,
      body: request,
      retry: "all",
    });
  }

  write(namespace: string, request: WriteRequest): Promise<WriteResponse> {
    return this.#send({
      service: "writer",
      method: "POST",
      path: `${namespacePath(namespace)}/documents`,
      body: request,
      // A write that sets `conditions` tests the stored documents when it is applied, so a
      // repeat after a lost answer can decide differently: the caller decides.
      retry: request.conditions ? "none" : "unsent",
    });
  }

  /** Every document the key may read that passes `filter`, once, by the document key. */
  async *iterDocuments(
    namespace: string,
    options: IterDocumentsOptions = {},
  ): AsyncGenerator<SearchHit, void, undefined> {
    const { filter, pageSize = 1000, consistency } = options;
    let after: SearchHit | undefined;
    for (;;) {
      const { hits } = await this.query(namespace, scanRequest(after, filter, pageSize, consistency));
      yield* hits;
      after = hits.at(-1);
      if (after === undefined) return;
    }
  }

  async #send<T>(route: Route): Promise<T> {
    const base = this.#urls[route.service];
    if (base === undefined) throw new Error(`this client has no ${route.service}Url`);
    const headers: Record<string, string> = { Authorization: this.#authorization };
    let body: string | undefined;
    if (route.body !== undefined) {
      headers["Content-Type"] = "application/json";
      body = JSON.stringify(route.body, serialize);
    }
    for (let attempt = 0; ; attempt++) {
      let response: TransportResponse | undefined;
      let failure: unknown;
      try {
        response = await this.#transport({
          url: base + route.path,
          method: route.method,
          headers,
          body,
          timeoutMs: this.#timeoutMs,
        });
      } catch (error) {
        failure = error;
      }
      const delay = this.#retryDelay(route.retry, attempt, response, failure);
      if (delay === undefined) {
        if (response === undefined) throw failure;
        return decode(response) as T;
      }
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  /** Milliseconds to wait before the next attempt, or undefined when the outcome is final. */
  #retryDelay(
    retry: Retry,
    attempt: number,
    response: TransportResponse | undefined,
    failure: unknown,
  ): number | undefined {
    if (retry === "none" || attempt >= this.#maxRetries) return undefined;
    if (response === undefined) {
      if (isPermanent(failure) || (retry === "unsent" && !isUnsent(failure))) return undefined;
    } else if (retry !== "all" || !(response.status === 408 || response.status === 429 || response.status >= 500)) {
      // An answer to a change does not prove that it was not applied: a proxy may send a 429 or
      // a 5xx after the writer took the request. Only a read repeats on one.
      return undefined;
    }
    const wait = response && retryAfterMs(response.retryAfter);
    if (wait !== undefined) {
      // A longer wait is the caller's to make: raise the answer, which has `retryAfterMs`.
      return wait <= MAX_RETRY_AFTER_MS ? wait : undefined;
    }
    const delay = Math.min(this.#backoffMs * 2 ** attempt, MAX_BACKOFF_MS);
    return delay / 2 + Math.random() * (delay / 2);
  }
}

function checkedUrl(name: string, value: string): string {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new TypeError(`${name} must be an http or https URL`);
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new TypeError(`${name} must be an http or https URL`);
  }
  // A password in a URL would be shown by an error or a log; the key goes in `apiKey`.
  if (url.username || url.password || value.includes("?") || value.includes("#")) {
    throw new TypeError(`${name} must be a base URL without credentials, query or fragment`);
  }
  return value.replace(/\/+$/, "");
}

/** The key without the newline of the file it was read from; never echoed in an error. */
function checkedKey(apiKey: string): string {
  const key = apiKey.trim();
  if (!/^[\x21-\x7e]+$/.test(key)) {
    throw new TypeError("apiKey must be one token of printable ASCII, with no spaces");
  }
  return key;
}

function checkedNumber(name: string, value: number, min: number, integer = false): number {
  if (!Number.isFinite(value) || value < min || (integer && !Number.isInteger(value))) {
    throw new RangeError(`${name} must be a ${integer ? "whole " : ""}number of at least ${min}`);
  }
  return value;
}

/** Whether the connection could not be made, so the request was never sent. */
function isUnsent(failure: unknown): boolean {
  if (!(failure instanceof Error)) return false;
  const { code, syscall } = failure as { code?: unknown; syscall?: unknown };
  if (typeof code === "string" && UNSENT_CODES.has(code) && typeof syscall === "string" && UNSENT_CALLS.has(syscall)) {
    return true;
  }
  // An error may wrap the system error as its `cause`; several addresses make an AggregateError.
  if (failure instanceof AggregateError && failure.errors.length > 0 && failure.errors.every(isUnsent)) {
    return true;
  }
  return isUnsent(failure.cause);
}

function isPermanent(failure: unknown): boolean {
  const code = (failure as { code?: unknown } | null)?.code;
  return typeof code === "string" && PERMANENT_CODES.has(code);
}

function retryAfterMs(header: string | undefined): number | undefined {
  const value = header?.trim();
  if (!value) return undefined;
  if (/^\d+(\.\d+)?$/.test(value)) return Number(value) * 1000;
  const date = Date.parse(value);
  return Number.isNaN(date) ? undefined : Math.max(0, date - Date.now());
}

function decode(response: TransportResponse): unknown {
  let body: unknown;
  let parsed = true;
  try {
    body = JSON.parse(response.text, revive as Parameters<typeof JSON.parse>[1]);
  } catch {
    parsed = false;
  }
  if (response.status >= 200 && response.status < 300) {
    if (!parsed) throw new FyrrError(response.status, "invalid_json");
    return body;
  }
  let code: unknown;
  if (typeof body === "object" && body !== null) {
    const { error, scimType } = body as { error?: unknown; scimType?: unknown };
    code = error || scimType;
  }
  throw new FyrrError(response.status, typeof code === "string" ? code : undefined, retryAfterMs(response.retryAfter));
}

/** An integer that a `number` cannot hold exactly is a `bigint`; the text of the answer says. */
function revive(_key: string, value: unknown, context?: { source?: string }): unknown {
  if (
    typeof value === "number" &&
    !Number.isSafeInteger(value) &&
    context?.source !== undefined &&
    /^-?\d+$/.test(context.source)
  ) {
    return BigInt(context.source);
  }
  return value;
}

/** A `bigint` is written as the integer it is: `JSON.stringify` alone throws on it. */
function serialize(_key: string, value: unknown): unknown {
  return typeof value === "bigint" ? (JSON as unknown as RawJson).rawJSON(value.toString()) : value;
}

interface RawJson {
  rawJSON(text: string): unknown;
}

const agents = {
  "http:": new http.Agent({ keepAlive: true }),
  "https:": new https.Agent({ keepAlive: true }),
};

/** One request over `node:http` or `node:https`, with no redirect and no retry of its own. */
export const nodeTransport: Transport = (request) =>
  new Promise((resolve, reject) => {
    const url = new URL(request.url);
    const protocol = url.protocol === "https:" ? "https:" : "http:";
    const signal = AbortSignal.timeout(request.timeoutMs);
    const headers = { ...request.headers };
    if (request.body !== undefined) headers["Content-Length"] = String(Buffer.byteLength(request.body));
    // A timeout destroys the request with an `AbortError`; the caller gets the `TimeoutError`.
    const fail = (error: unknown) => reject(signal.aborted ? signal.reason : error);
    const outgoing = (protocol === "https:" ? https : http).request(
      url,
      { method: request.method, headers, agent: agents[protocol], signal },
      (response) => {
        response.setEncoding("utf8");
        let text = "";
        response.on("data", (chunk: string) => (text += chunk));
        response.on("error", fail);
        response.on("close", () => {
          if (!response.complete) return fail(new Error("the connection closed before the end of the answer"));
          const retryAfter = response.headers["retry-after"];
          resolve({ status: response.statusCode ?? 0, retryAfter, text });
        });
      },
    );
    outgoing.on("error", fail);
    outgoing.end(request.body);
  });

function namespacePath(namespace: string): string {
  return `/v1/namespaces/${encodeURIComponent(namespace)}`;
}

/** The next page of a scan by the document key (docs/mcp-tools.md, Paging and scanning). */
function scanRequest(
  after: SearchHit | undefined,
  filter: Filter | undefined,
  pageSize: number,
  consistency: Consistency | undefined,
): QueryRequest {
  const conditions: Filter[] = filter ? [filter] : [];
  if (after !== undefined) {
    const source = after.source_id;
    const root = after.root_id;
    conditions.push({
      or: [
        { field: "source_id", op: "gt", value: source },
        {
          and: [
            { field: "source_id", op: "eq", value: source },
            { field: "root_id", op: "gt", value: root },
          ],
        },
      ],
    });
  }
  const request: QueryRequest = {
    order_by: [
      { field: "source_id", direction: "asc" },
      { field: "root_id", direction: "asc" },
    ],
    output: "metadata",
    limit: pageSize,
  };
  if (conditions.length > 0) request.filter = conditions.length === 1 ? conditions[0] : { and: conditions };
  if (consistency) request.consistency = consistency;
  return request;
}
