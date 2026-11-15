"""The single synthetic Jira transport, shared by U and real-worker S paths."""

import copy
import io
import json
import re
import threading
from datetime import UTC, datetime
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import requests
from requests.adapters import BaseAdapter


def adf(body):
    return {
        "type": "doc",
        "version": 1,
        "content": [{"type": "paragraph", "content": [{"type": "text", "text": body}]}],
    }


class FakeJira:
    def __init__(self):
        self.token = "jira-synthetic-only"
        self.cloud = "11111111-1111-4111-8111-111111111111"
        self.site = "https://synthetic.atlassian.net"
        self.clock = 1767225600
        self.page_size = 2
        self.requests = []
        self.sleeps = []
        self.hooks = []
        self.faults = []
        self.projects = {}
        self.issues = {}
        self.comments = {}
        self.users = {}
        self.groups = {}
        self.roles = {}
        self.schemes = {}
        self.levels = {}
        self.applications = []
        self.user("U001")
        self.group("g1", ["U001"])
        self.project(1)

    def date(self):
        return datetime.fromtimestamp(self.clock, UTC).isoformat()

    def user(self, identity, **attrs):
        self.users[identity] = {
            "accountId": identity,
            "active": True,
            "accountType": "atlassian",
            "displayName": "Fixture author",
            **attrs,
        }

    def group(self, identity, members):
        self.groups[identity] = {
            "groupId": identity,
            "name": "Group " + identity,
            "members": members,
        }

    def project(self, identity, **attrs):
        self.projects[identity] = {
            "id": str(identity),
            "key": f"P{identity}",
            "name": f"Project {identity}",
            "projectTypeKey": "software",
            "simplified": False,
            "archived": False,
            "lead": self.users["U001"],
            **attrs,
        }
        self.schemes.setdefault(
            identity,
            [{"permission": "BROWSE_PROJECTS", "holder": {"type": "group", "value": "g1"}}],
        )
        self.roles.setdefault(
            identity,
            {
                10: {
                    "id": 10,
                    "name": "Readers",
                    "actors": [
                        {"type": "atlassian-user-role-actor", "actorUser": {"accountId": "U001"}}
                    ],
                }
            },
        )
        self.levels.setdefault(identity, {})

    def issue(self, identity, project=1, **fields):
        self.clock += 1
        self.issues[identity] = {
            "id": str(identity),
            "key": f"{self.projects[project]['key']}-{identity}",
            "fields": {
                "project": {"id": str(project)},
                "summary": f"Issue {identity}",
                "description": adf("Description."),
                "issuetype": {"id": "1", "name": "Task"},
                "labels": [],
                "status": {"name": "Open"},
                "reporter": copy.deepcopy(self.users["U001"]),
                "assignee": None,
                "created": self.date(),
                "updated": self.date(),
                "security": None,
                **fields,
            },
        }
        self.comments.setdefault(identity, [])
        return self.issues[identity]

    def comment(self, issue, identity, body="Comment.", **attrs):
        self.clock += 1
        record = {
            "id": str(identity),
            "body": adf(body),
            "author": copy.deepcopy(self.users["U001"]),
            "created": self.date(),
            "updated": self.date(),
            "self": f"{self.site}/rest/api/3/issue/{issue}/comment/{identity}",
            **attrs,
        }
        self.comments[issue].append(record)
        self.issues[issue]["fields"]["updated"] = self.date()
        return record

    def edit(self, identity, **fields):
        self.clock += 1
        self.issues[identity]["fields"].update(fields, updated=self.date())
        project = int(self.issues[identity]["fields"]["project"]["id"])
        self.issues[identity]["key"] = f"{self.projects[project]['key']}-{identity}"

    def edit_comment(self, issue, identity, **attrs):
        self.clock += 1
        record = next(v for v in self.comments[issue] if int(v["id"]) == identity)
        record.update(attrs, updated=self.date())
        self.issues[issue]["fields"]["updated"] = self.date()

    def delete(self, identity):
        self.issues.pop(identity, None)
        self.comments.pop(identity, None)

    def delete_comment(self, issue, identity):
        self.comments[issue] = [v for v in self.comments[issue] if int(v["id"]) != identity]

    def after(self, match, action):
        self.hooks.append((match, action))

    def fail(self, path, status=503, times=1, headers=None):
        self.faults.append([path, status, times, headers or {}])

    def attach(self, transport):
        transport.mount("https://api.atlassian.com", Adapter(self))
        transport.mount(self.site, Adapter(self))
        return transport

    def page(self, values, params, key="values"):
        start = int(params.get("startAt", 0))
        end = start + self.page_size
        return {
            key: values[start:end],
            "startAt": start,
            "total": len(values),
            "isLast": end >= len(values),
            "maxResults": self.page_size,
        }

    def search(self, body):
        query = body["jql"]
        values = []
        for identity, issue in sorted(self.issues.items()):
            fields = issue["fields"]
            project = int(fields["project"]["id"])
            if project not in self.projects or self.projects[project]["archived"]:
                continue
            selectors = re.findall(r"project\s+(?:in\s*\(([^)]*)\)|=\s*(\d+))", query, re.I)
            if any(project not in [int(v) for v in (a or b).split(",")] for a, b in selectors):
                continue
            ids = re.findall(r"\bid\s*=\s*(\d+)", query)
            if ids and identity != int(ids[-1]):
                continue
            id_sets = re.findall(r"\bid\s+in\s*\(([^)]*)\)", query, re.I)
            if id_sets and identity not in {int(v) for v in id_sets[-1].split(",")}:
                continue
            if re.search(r"\blevel\s+IS\s+EMPTY", query, re.I) and fields.get("security"):
                continue
            holders = re.findall(
                r"\b(reporter|assignee|cf\[(\d+)\])\s+IS\s+NOT\s+EMPTY", query, re.I
            )
            if holders and not any(
                fields.get("customfield_" + cf if cf else name.lower()) for name, cf in holders
            ):
                continue
            levels = re.findall(r"\blevel\s*=\s*(\d+)", query)
            if levels and (
                not fields["security"] or int(fields["security"]["id"]) != int(levels[-1])
            ):
                continue
            types = re.findall(r"issuetype in \(([^)]*)\)", query)
            if types and int(fields["issuetype"]["id"]) not in map(int, types[-1].split(",")):
                continue
            labels = re.findall(r"labels in \(([^)]*)\)", query)
            if labels and not set(fields["labels"]) & set(json.loads("[" + labels[-1] + "]")):
                continue
            dates = re.findall(r'updated\s*(>=|<=)\s*"([^"]+)"', query)
            updated = int(datetime.fromisoformat(fields["updated"]).timestamp())
            if any(
                updated
                < int(datetime.strptime(d, "%Y-%m-%d %H:%M").replace(tzinfo=UTC).timestamp())
                if op == ">="
                else updated
                > int(datetime.strptime(d, "%Y-%m-%d %H:%M").replace(tzinfo=UTC).timestamp())
                for op, d in dates
            ):
                continue
            if "status != Closed" in query and fields["status"]["name"] == "Closed":
                continue
            values.append(issue)
        # A native key token contains the immutable ID, never an array offset.
        marker = int(body.get("nextPageToken", 0))
        values = [v for v in values if int(v["id"]) > marker]
        selected = values[: self.page_size]
        last = len(values) <= self.page_size
        return {
            "issues": selected,
            "isLast": last,
            **({"nextPageToken": selected[-1]["id"]} if not last else {}),
        }

    def answer(self, path, params, body):
        if path == "/_edge/tenant_info":
            return {"cloudId": self.cloud}
        if path == "/serverInfo":
            return {"deploymentType": "Cloud", "baseUrl": self.site}
        if path == "/search/jql":
            return self.search(body)
        parts = path.strip("/").split("/")
        if parts[0] == "project":
            identity = int(parts[1])
            project = self.projects.get(identity)
            if project is None:
                raise KeyError
            if len(parts) == 2:
                return project
            if parts[2] == "permissionscheme":
                return {"id": identity, "permissions": self.schemes[identity]}
            if parts[2] == "issuesecuritylevelscheme":
                return {"id": identity, "levels": [{"id": str(i)} for i in self.levels[identity]]}
            if parts[2] == "role":
                if len(parts) == 3:
                    return {
                        v["name"]: f"{self.site}/rest/api/3/project/{identity}/role/{i}"
                        for i, v in self.roles[identity].items()
                    }
                return self.roles[identity][int(parts[3])]
        if parts[0] == "issue":
            identity = int(parts[1])
            if identity not in self.issues:
                raise KeyError
            if len(parts) == 2:
                return self.issues[identity]
            if parts[2] == "comment":
                return self.page(self.comments[identity], params, "comments")
        if path == "/users/search":
            start = int(params.get("startAt", 0))
            return list(self.users.values())[start : start + self.page_size]
        if path == "/user":
            return self.users[params["accountId"]]
        if path == "/group/member":
            return self.page(
                [self.users[user] for user in self.groups[params["groupId"]]["members"]], params
            )
        if path == "/group/bulk":
            values = [
                {k: v for k, v in group.items() if k != "members"}
                for group in self.groups.values()
                if not params.get("groupName") or group["name"] == params["groupName"]
            ]
            return self.page(values, params)
        if path == "/applicationrole":
            return self.applications
        if path == "/issuesecurityschemes/level/member":
            project = int(params["schemeId"])
            level = int(params["levelId"]) if params.get("levelId") else None
            return self.page(
                [
                    {
                        "id": str(i + 1),
                        "issueSecuritySchemeId": str(project),
                        "issueSecurityLevelId": str(member_level),
                        "holder": h,
                    }
                    for i, (member_level, h) in enumerate(
                        (
                            (member_level, h)
                            for member_level, holders in self.levels[project].items()
                            for h in holders
                            if level is None or member_level == level
                        )
                    )
                ],
                params,
            )
        raise KeyError

    def handle(self, method, url, params, body, auth):
        path = urlsplit(url).path
        if "/rest/api/3" in path:
            path = path.split("/rest/api/3", 1)[1]
        self.requests.append((method, path, copy.deepcopy(body or params)))
        if path != "/_edge/tenant_info" and auth != "Bearer " + self.token:
            return 401, {}, {}
        fault = next((v for v in self.faults if v[0] == path and v[2]), None)
        if fault:
            fault[2] -= 1
            return fault[1], {}, fault[3]
        try:
            result = copy.deepcopy(self.answer(path, params, body))
        except KeyError:
            return 404, {}, {}
        hook = next((v for v in self.hooks if v[0](method, path, params, body)), None)
        if hook:
            self.hooks.remove(hook)
            hook[1]()
        return (
            200,
            result,
            {"Date": format_datetime(datetime.fromtimestamp(self.clock, UTC), usegmt=True)},
        )


class Adapter(BaseAdapter):
    def __init__(self, world):
        self.world = world

    def send(self, request, **kwargs):
        query = {k: v[0] for k, v in parse_qs(urlsplit(request.url).query).items()}
        body = json.loads(request.body) if request.body else None
        status, result, headers = self.world.handle(
            request.method, request.url, query, body, request.headers.get("Authorization")
        )
        response = requests.Response()
        response.status_code = status
        response.headers = headers
        response.url = request.url
        response.raw = io.BytesIO(json.dumps(result).encode())
        return response

    def close(self):
        pass


class FakeServer:
    def __init__(self):
        world = FakeJira()
        self.world = world

        class Handler(BaseHTTPRequestHandler):
            def date_time_string(self, timestamp=None):
                return format_datetime(datetime.fromtimestamp(world.clock, UTC), usegmt=True)

            def log_message(self, *args):
                pass

            def do_GET(self):
                self.answer()

            def do_POST(self):
                if self.path.startswith("/fake/"):
                    body = json.loads(
                        self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}"
                    )
                    operation = self.path.removeprefix("/fake/")
                    if operation == "advance":
                        world.clock += body["seconds"]
                        result = None
                    else:
                        result = getattr(world, operation)(**body)
                    self.send(200, {"result": result}, {})
                else:
                    self.answer()

            def answer(self):
                params = {k: v[0] for k, v in parse_qs(urlsplit(self.path).query).items()}
                body = json.loads(
                    self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"null"
                )
                self.send(
                    *world.handle(
                        self.command, self.path, params, body, self.headers.get("Authorization")
                    )
                )

            def send(self, status, body, headers):
                raw = json.dumps(body).encode()
                self.send_response(status)
                for key, value in headers.items():
                    if key.lower() != "date":
                        self.send_header(key, value)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


if __name__ == "__main__":
    import sys

    fake = FakeServer()
    print(json.dumps({"endpoint": fake.endpoint}), flush=True)
    try:
        sys.stdin.read()
    finally:
        fake.close()
