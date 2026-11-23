// Generate src/models.ts from schemas/openapi.json.
//
//   node tools/generate-models.ts          # rewrite models.ts
//   node tools/generate-models.ts --check  # fail when models.ts is stale
//
// The output is interfaces and type aliases, so a request is a plain object and a response is the
// decoded JSON.

import { readFileSync, writeFileSync } from "node:fs";

const SPEC = new URL("../../../schemas/openapi.json", import.meta.url);
const OUTPUT = new URL("../src/models.ts", import.meta.url);
const PREFIX = "#/components/schemas/";
const SCALARS: Record<string, string> = {
  string: "string",
  integer: "number",
  number: "number",
  boolean: "boolean",
};

const SMALL_INTEGERS = new Set(["uint8", "uint16", "uint32", "int8", "int16", "int32"]);

const HEADER = `\
// Typed requests and responses of the fyrr HTTP API.
//
// Generated from schemas/openapi.json by tools/generate-models.ts. Do not edit.
`;

type Schema = {
  $ref?: string;
  anyOf?: Schema[];
  oneOf?: Schema[];
  const?: unknown;
  enum?: unknown[];
  type?: string | string[];
  format?: string;
  items?: Schema;
  properties?: Record<string, Schema>;
  required?: string[];
  additionalProperties?: boolean | Schema;
  description?: string;
};

class Generator {
  // Declarations in definition order; an inline object is defined before the type that uses it.
  readonly declarations = new Map<string, string>();

  readonly schemas: Record<string, Schema>;

  constructor(schemas: Record<string, Schema>) {
    this.schemas = schemas;
  }

  run(): string {
    for (const [name, schema] of Object.entries(this.schemas)) {
      this.define(name, schema);
    }
    return `${HEADER}\n${[...this.declarations.values()].join("\n\n")}\n`;
  }

  private define(name: string, schema: Schema): void {
    if (isObject(schema)) {
      this.defineInterface(name, schema);
      return;
    }
    const type = this.expression(schema, name);
    this.declarations.set(name, `${comment(schema.description, "")}export type ${name} = ${type};`);
  }

  private defineInterface(name: string, schema: Schema): void {
    const required = new Set(schema.required ?? []);
    const lines: string[] = [];
    for (const [key, value] of Object.entries(schema.properties ?? {})) {
      const type = this.expression(value, name + camel(key));
      lines.push(
        `${comment(value.description, "  ")}  ${member(key)}${required.has(key) ? "" : "?"}: ${type};`,
      );
    }
    if (schema.additionalProperties === true) {
      lines.push("  [key: string]: unknown;");
    }
    this.declarations.set(
      name,
      `${comment(schema.description, "")}export interface ${name} {\n${lines.join("\n")}\n}`,
    );
  }

  /** The TypeScript type of a schema; an inline object becomes an interface named after `context`. */
  private expression(schema: Schema | true, context: string): string {
    if (schema === true || Object.keys(schema).length === 0) return "unknown";
    if (schema.$ref !== undefined) return schema.$ref.slice(PREFIX.length);
    const members = schema.anyOf ?? schema.oneOf;
    if (members !== undefined) return this.union(members, context);
    if (schema.const !== undefined) return JSON.stringify(schema.const);
    if (schema.enum !== undefined) return schema.enum.map((item) => JSON.stringify(item)).join(" | ");
    const kinds = Array.isArray(schema.type) ? schema.type : [schema.type];
    const base = kinds.filter((kind) => kind !== "null");
    if (base.length !== 1 || base[0] === undefined) {
      throw new Error(`unsupported schema: ${JSON.stringify(schema)}`);
    }
    const kind = base[0];
    let result: string;
    if (kind === "array") {
      const item = schema.items === undefined ? "unknown" : this.expression(schema.items, `${context}Item`);
      result = item.includes(" | ") ? `(${item})[]` : `${item}[]`;
    } else if (kind === "object") {
      result = this.objectType(schema, context);
    } else {
      // A 64-bit integer may be beyond what a `number` holds: the client then gives a `bigint`.
      const scalar = kind === "integer" && !SMALL_INTEGERS.has(schema.format ?? "") ? "number | bigint" : SCALARS[kind];
      if (scalar === undefined) throw new Error(`unsupported type: ${kind}`);
      result = scalar;
    }
    return kinds.includes("null") ? `${result} | null` : result;
  }

  private objectType(schema: Schema, context: string): string {
    if (isObject(schema)) {
      this.defineInterface(context, schema);
      return context;
    }
    const extra = schema.additionalProperties ?? true;
    return `Record<string, ${extra === true ? "unknown" : this.expression(extra as Schema, context)}>`;
  }

  private union(members: Schema[], context: string): string {
    const types: string[] = [];
    for (const member of members) {
      const type =
        member.type === "null"
          ? "null"
          : isObject(member)
            ? this.inlineObject(member, context)
            : this.expression(member, context);
      if (!types.includes(type)) types.push(type);
    }
    // `null` last, as the properties of the schema put it.
    return [...types.filter((type) => type !== "null"), ...types.filter((type) => type === "null")].join(" | ");
  }

  private inlineObject(member: Schema, context: string): string {
    const properties = member.properties ?? {};
    const first = Object.keys(properties)[0];
    if (first === undefined) throw new Error(`inline object without members in ${context}`);
    const tag = properties[first]?.const ?? first;
    const name = context + camel(String(tag));
    if (this.declarations.has(name)) throw new Error(`inline interface name collides: ${name}`);
    this.defineInterface(name, member);
    return name;
  }
}

/** An object with known members: an interface. One that allows other members gets an index signature. */
function isObject(schema: Schema): boolean {
  return (
    schema.type === "object" &&
    schema.properties !== undefined &&
    typeof schema.additionalProperties !== "object"
  );
}

function camel(text: string): string {
  return text
    .replaceAll("-", "_")
    .split("_")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join("");
}

function member(key: string): string {
  return /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(key) ? key : JSON.stringify(key);
}

function comment(text: string | undefined, indent: string): string {
  if (text === undefined || text === "") return "";
  const lines = text.replaceAll("*/", "*\\/").split("\n");
  if (lines.length === 1) return `${indent}/** ${lines[0]} */\n`;
  return `${indent}/**\n${lines.map((line) => `${indent} * ${line}`.trimEnd()).join("\n")}\n${indent} */\n`;
}

function generate(): string {
  const spec = JSON.parse(readFileSync(SPEC, "utf8")) as { components: { schemas: Record<string, Schema> } };
  return new Generator(spec.components.schemas).run();
}

const text = generate();
if (process.argv.includes("--check")) {
  if (readFileSync(OUTPUT, "utf8") !== text) {
    console.error("src/models.ts is stale: run `npm run generate` in clients/typescript");
    process.exit(1);
  }
} else {
  writeFileSync(OUTPUT, text);
}
