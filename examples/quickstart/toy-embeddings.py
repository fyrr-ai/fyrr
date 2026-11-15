"""A toy OpenAI-compatible embeddings service for trying fyrr locally.

It hashes each word into one of 1024 dimensions and normalizes the result,
so texts that share words get similar vectors. It is not a semantic model:
use a real embeddings endpoint for anything beyond the quickstart, and
publish into a fresh namespace when you switch, because a namespace is
bound to the model that embedded it.

    python3 examples/quickstart/toy-embeddings.py [PORT]    # default 8089, loopback only
"""

import hashlib
import json
import math
import re
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

MODEL = "toy-hash-1024"
DIMENSIONS = 1024


def embed(text):
    vector = [0.0] * DIMENSIONS
    for word in re.findall(r"\w+", text.lower()):
        digest = hashlib.sha256(word.encode()).digest()
        vector[int.from_bytes(digest[:4], "big") % DIMENSIONS] += 1.0
    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        vector[0], norm = 1.0, 1.0
    return [value / norm for value in vector]


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def reply(self, status, value):
        body = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/v1/models", "/v1/embeddings/models"):
            self.reply(200, {"data": [{"id": MODEL}]})
        else:
            self.reply(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/v1/embeddings":
            self.reply(404, {"error": "not found"})
            return
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        inputs = request["input"]
        inputs = [inputs] if isinstance(inputs, str) else inputs
        data = [{"index": i, "embedding": embed(text)} for i, text in enumerate(inputs)]
        self.reply(200, {"model": MODEL, "data": data})


port = int(sys.argv[1]) if len(sys.argv) > 1 else 8089
print(f"toy embeddings at http://127.0.0.1:{port}/v1", flush=True)
HTTPServer(("127.0.0.1", port), Handler).serve_forever()
