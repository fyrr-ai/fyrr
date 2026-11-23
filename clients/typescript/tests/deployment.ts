// A local `file://` deployment for a test file, started by examples/guides/start.

import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";

const ROOT = new URL("../../../", import.meta.url).pathname;
// The guides' start script exports these. The script then waits for the end of its standard
// input, and stops fyrr and removes its directory.
const START =
  "source examples/guides/start || exit 1; " +
  'printf "%s\\n" "$READER" "$WRITER" "$ADMIN_KEY" "$READ_KEY"; ' +
  "read -r _; fyrr_stop";

export class Deployment {
  readonly readerUrl: string;
  readonly writerUrl: string;
  // Keys are not enumerable: a failed assertion prints the object, and CI logs are public.
  declare readonly adminKey: string;
  declare readonly readKey: string;
  readonly #process: ChildProcess;

  constructor(process: ChildProcess, values: string[]) {
    const [readerUrl, writerUrl, adminKey, readKey] = values as [string, string, string, string];
    this.readerUrl = readerUrl;
    this.writerUrl = writerUrl;
    Object.defineProperty(this, "adminKey", { value: adminKey });
    Object.defineProperty(this, "readKey", { value: readKey });
    this.#process = process;
  }

  static async start(): Promise<Deployment> {
    if (spawnSync("bash", ["-c", "command -v fyrr"]).status !== 0) {
      throw new Error("no `fyrr` on PATH: build it with `cargo build -p fyrr` and add target/debug");
    }
    const child = spawn("bash", ["-c", START], { cwd: ROOT, stdio: ["pipe", "pipe", "inherit"] });
    const values: string[] = [];
    const lines = createInterface({ input: child.stdout! });
    for await (const line of lines) {
      values.push(line.trim());
      if (values.length === 4) break;
    }
    if (values.length < 4 || values.some((value) => !value)) {
      child.stdin!.end();
      throw new Error("fyrr did not start; see its log above");
    }
    return new Deployment(child, values);
  }

  async stop(): Promise<void> {
    const exited = new Promise((resolve) => this.#process.once("exit", resolve));
    this.#process.stdin!.end();
    await exited;
  }
}
