"""The one synthetic Slack transport, shared by unit and real-worker tests."""

import copy
import json
import sys
import threading
from datetime import UTC, datetime
from email.utils import format_datetime
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

# Run as a script, the fake finds `fake_http` next to the test packages.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fake_http import CONTROL_PATH, JsonHandler, Serving, WorldAdapter


def stamp(value):
    return f"{value // 1_000_000}.{value % 1_000_000:06}"


def micros(value):
    a, b = value.split(".")
    return int(a) * 1_000_000 + int(b)


class FakeSlack:
    def __init__(self):
        self.origin = "https://slack.com"
        self.token = "xoxp-synthetic-only"
        self.workspace = "T001"
        self.clock = 1767225600_000000
        self.channels, self.messages, self.users, self.members = {}, {}, {}, {}
        self.requests, self.sleeps, self.hooks, self.faults = [], [], [], []
        self.page_size = 2
        self.user("U001")
        self.channel("C001", "platform")
        self._lock = threading.RLock()

    def user(self, identity, **values):
        self.users[identity] = {
            "id": identity,
            "team_id": self.workspace,
            "deleted": False,
            "is_restricted": False,
            "is_ultra_restricted": False,
            "is_bot": False,
            "profile": {"display_name": "Fixture author"},
            **values,
        }

    def channel(self, identity, name, private=False, shared=False, **values):
        self.channels[identity] = {
            "id": identity,
            "name": name,
            "is_private": private,
            "is_archived": False,
            "is_shared": shared,
            "is_ext_shared": shared,
            "is_org_shared": False,
            "is_im": False,
            "is_mpim": False,
            **values,
        }
        self.messages.setdefault(identity, [])
        self.members.setdefault(identity, ["U001"])

    def set_members(self, channel, members):
        self.members[channel] = members

    def post(
        self,
        channel="C001",
        text="Body.",
        *,
        seconds=1,
        ts=None,
        user="U001",
        thread=None,
        bot=False,
        **values,
    ):
        if ts is None:
            self.clock += seconds * 1_000_000
            ts = stamp(self.clock)
        else:
            self.clock = max(self.clock, micros(ts))
        body = {"type": "message", "ts": ts, "text": text, "user": user, **values}
        if thread:
            body["thread_ts"] = thread
        if bot:
            body["bot_id"] = "B001"
            body["subtype"] = "bot_message"
        self.messages[channel].append(body)
        return body

    def edit(self, channel, ts, text):
        self.clock += 1_000_000
        body = next(item for item in self.messages[channel] if item["ts"] == ts)
        body.update(text=text, edited={"ts": stamp(self.clock)})

    def remove(self, channel, ts):
        self.messages[channel] = [item for item in self.messages[channel] if item["ts"] != ts]

    def fail(self, method, code="internal_error", times=1, status=200, headers=None):
        self.faults.append([method, code, times, status, headers or {}])

    def after(self, match, change):
        self.hooks.append((match, change))

    def attach(self, transport):
        transport.mount(self.origin, WorldAdapter(self.answer_request))
        return transport

    def answer_request(self, request):
        url = urlsplit(request.url)
        params = {key: values[0] for key, values in parse_qs(url.query).items()}
        return self.handle(
            url.path.removeprefix("/api/"), params, request.headers.get("Authorization")
        )

    def _page(self, entries, params, key):
        offset = int(params.get("cursor") or 0)
        following = offset + self.page_size
        return {
            "ok": True,
            key: entries[offset:following],
            "has_more": following < len(entries),
            "response_metadata": {
                "next_cursor": str(following) if following < len(entries) else ""
            },
        }

    def handle(self, method, params, token):
        with self._lock:
            self.requests.append((method, dict(params)))
            if token != "Bearer " + self.token:
                return 200, {"ok": False, "error": "invalid_auth"}, {}
            fault = next((item for item in self.faults if item[0] == method and item[2]), None)
            if fault:
                fault[2] -= 1
                status, body, headers = fault[3], {"ok": False, "error": fault[1]}, fault[4]
            else:
                status, body, headers = 200, self._answer(method, params), {}
            body = copy.deepcopy(body)
            hook = next((item for item in self.hooks if item[0](method, params)), None)
            if hook:
                self.hooks.remove(hook)
                hook[1]()
            date = datetime.fromtimestamp(self.clock // 1_000_000 + 1, UTC)
            return status, body, {"Date": format_datetime(date, usegmt=True), **headers}

    def _answer(self, method, params):
        if method == "auth.test":
            return {
                "ok": True,
                "team_id": self.workspace,
                "user_id": "U001",
                "url": "https://synthetic.slack.com/",
            }
        if method == "users.list":
            return self._page(list(self.users.values()), params, "members")
        if method == "users.info":
            user = self.users.get(params.get("user"))
            return {"ok": True, "user": user} if user else {"ok": False, "error": "user_not_found"}
        if method == "conversations.list":
            return self._page(list(self.channels.values()), params, "channels")
        channel = params.get("channel")
        if channel not in self.channels:
            return {"ok": False, "error": "channel_not_found"}
        if method == "conversations.info":
            return {"ok": True, "channel": self.channels[channel]}
        if method == "conversations.members":
            return self._page(self.members[channel], params, "members")
        messages = copy.deepcopy(self.messages[channel])
        for item in messages:
            replies = [
                reply
                for reply in messages
                if reply.get("thread_ts") == item["ts"] and reply["ts"] != item["ts"]
            ]
            if replies:
                item.update(reply_count=len(replies), thread_ts=item["ts"])
        if method == "conversations.history":
            low, high = micros(params["oldest"]), micros(params["latest"])
            messages = [
                item
                for item in messages
                if low <= micros(item["ts"]) <= high
                and item.get("thread_ts", item["ts"]) == item["ts"]
            ]
            messages.sort(key=lambda item: item["ts"], reverse=True)
        elif method == "conversations.replies":
            ts = params["ts"]
            messages = [
                item for item in messages if item["ts"] == ts or item.get("thread_ts") == ts
            ]
            if not any(item["ts"] == ts for item in messages):
                return {"ok": False, "error": "thread_not_found"}
            messages.sort(key=lambda item: item["ts"])
        else:
            return {"ok": False, "error": "unknown_method"}
        return self._page(messages, params, "messages")


class FakeServer(Serving):
    def __init__(self, world):
        self.world = world

        class Handler(JsonHandler):
            def do_GET(self):
                self.answer()

            def do_POST(self):
                if self.path.startswith(CONTROL_PATH):
                    params = json.loads(self.read_body() or b"{}")
                    operation = self.path.removeprefix(CONTROL_PATH)
                    if operation == "advance":
                        world.clock += params["seconds"] * 1_000_000
                        result = None
                    elif operation == "delete_channel":
                        world.channels.pop(params["channel"], None)
                        result = None
                    else:
                        result = getattr(world, operation)(**params)
                    self.send_json(200, {"result": result})
                else:
                    self.answer()

            def answer(self):
                url = urlsplit(self.path)
                params = {key: values[0] for key, values in parse_qs(url.query).items()}
                self.send_json(
                    *world.handle(
                        url.path.removeprefix("/api/"), params, self.headers.get("Authorization")
                    )
                )

        super().__init__(Handler)


if __name__ == "__main__":
    fake = FakeServer(FakeSlack())
    print(json.dumps({"endpoint": fake.endpoint}), flush=True)
    try:
        sys.stdin.read()
    finally:
        fake.close()
