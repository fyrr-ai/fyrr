"""Browse/security/section policy algebra, flattened into stable audience groups."""

import hashlib
import json
import re
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from ..connector import SourceGroup
from .client import MAX_ENTRIES, number
from .members import Directory, account_id, user_id

DYNAMIC = {"reporter", "assignee", "userCF", "groupCF"}


def field_id(holder):
    value = holder.get("value") or holder.get("parameter")
    if isinstance(value, str) and value.isdigit():
        value = "customfield_" + value
    if not isinstance(value, str) or not re.fullmatch(r"customfield_\d+", value):
        raise ValueError("jira_custom_field_holder_invalid")
    return value


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def container(cloud, project):
    return f"jira:{cloud}:project:{project}"


def policy_id(cloud, project, level=None, target=None):
    suffix = digest([target]) if target else "none"
    return f"atlassian.net:{cloud}:policy:{project}:{level or 'none'}:{suffix}"


def policy_token(cloud, project, level=None, target=None):
    return "grp:" + policy_id(cloud, project, level, target)


@dataclass
class PolicyGraph:
    client: object
    settings: object
    directory: Directory
    rules: dict = field(default_factory=dict)
    levels: dict = field(default_factory=dict)
    static: dict = field(default_factory=dict)
    level_static: dict = field(default_factory=dict)
    groups: dict = field(default_factory=dict)
    roles: dict = field(default_factory=dict)
    associations: dict = field(default_factory=dict)
    raw_projects: dict = field(default_factory=dict)
    audiences: dict = field(default_factory=dict)
    edges: int = 0

    def group(self, identity):
        identity = account_id(identity)
        if identity not in self.groups:
            items = self.client.snapshot(
                "/group/member",
                "values",
                params={"groupId": identity, "includeInactiveUsers": "true"},
            )
            seen = set()
            for item in items:
                user = account_id(item.get("accountId"))
                if user in seen:
                    raise ValueError("jira_group_members_changed")
                seen.add(user)
                if user not in self.directory.accounts:
                    raise ValueError("jira_group_directory_incomplete")
            self.edges += len(seen)
            if self.edges > MAX_ENTRIES:
                raise ValueError("jira_permission_edge_limit")
            self.groups[identity] = {u for u in seen if self.directory.active(u)}
        return self.groups[identity]

    def resolve_group(self, value, name=False):
        if name:
            if (
                not isinstance(value, str)
                or not value
                or len(value.encode()) > 256
                or any(ord(c) < 32 for c in value)
            ):
                raise ValueError("jira_group_name_invalid")
            identity = value
        else:
            identity = account_id(value)
        args = {"groupName": identity} if name else {"groupId": identity}
        items = self.client.snapshot("/group/bulk", "values", params=args)
        matching = [
            item
            for item in items
            if (item.get("name") == value if name else item.get("groupId") == identity)
        ]
        if len(matching) != 1:
            raise ValueError("jira_group_identity_unproven")
        return account_id(matching[0].get("groupId"))

    def role(self, project, identity):
        identity = number(identity)
        key = (project, identity)
        if key not in self.roles:
            body = self.client.call(f"/project/{project}/role/{identity}")
            if number(body.get("id")) != identity or not isinstance(body.get("actors"), list):
                raise ValueError("jira_role_identity_mismatch")
            self.edges += len(body["actors"])
            if self.edges > MAX_ENTRIES:
                raise ValueError("jira_permission_edge_limit")
            members = set()
            for actor in body["actors"]:
                if actor.get("type") == "atlassian-user-role-actor":
                    user = account_id(actor.get("actorUser", {}).get("accountId"))
                    if user not in self.directory.accounts:
                        raise ValueError("jira_role_directory_incomplete")
                    if self.directory.active(user):
                        members.add(user)
                elif actor.get("type") == "atlassian-group-role-actor":
                    group = actor.get("actorGroup", {})
                    identity_group = group.get("groupId")
                    if not identity_group:
                        identity_group = self.resolve_group(group.get("name"), True)
                    members.update(self.group(identity_group))
                else:
                    raise ValueError("jira_role_actor_unknown")
            self.roles[key] = members
        return self.roles[key]

    def logged_in(self, application=None):
        apps = self.client.call("/applicationrole")
        if not isinstance(apps, list):
            raise ValueError("jira_application_roles_invalid")
        values = set()
        found = application is None
        for app in apps:
            if application and app.get("key") != application:
                continue
            found = True
            groups = app.get("groupDetails")
            if not isinstance(groups, list):
                raise ValueError("jira_application_groups_unproven")
            for group in groups:
                values.update(self.group(group.get("groupId")))
        if not found:
            raise ValueError("jira_application_identity_unproven")
        if application is None:
            self.audiences["atlassian.net:" + self.settings.cloud_id + ":licensed"] = set(values)
        return values

    def holders(self, project, holders, issue=None):
        if not isinstance(holders, list) or len(holders) > MAX_ENTRIES:
            raise ValueError("jira_holders_invalid")
        values = set()
        for holder in holders:
            if not isinstance(holder, dict) or not isinstance(holder.get("type"), str):
                raise ValueError("jira_holder_invalid")
            kind = holder["type"]
            parameter = holder.get("value") or holder.get("parameter")
            if kind == "user":
                user = account_id(parameter)
                if self.directory.active(user):
                    values.add(user)
            elif kind == "group":
                identity = holder.get("value") or self.resolve_group(holder.get("parameter"), True)
                values.update(self.group(identity))
            elif kind == "projectRole":
                values.update(self.role(project, parameter))
            elif kind == "projectLead":
                user = self.raw_projects[project].get("lead", {}).get("accountId")
                if user and self.directory.active(user):
                    values.add(user)
            elif kind == "applicationRole":
                if parameter in ("", None):
                    values.update(self.logged_in())
                else:
                    values.update(self.logged_in(parameter))
            elif kind in ("anyone", "public"):
                if issue is None and holders is self.rules.get(project):
                    raise ValueError("jira_public_browse_refused")
            elif kind in DYNAMIC:
                if issue is None:
                    continue
                fields = issue["fields"]
                if kind in ("reporter", "assignee"):
                    body = fields.get(kind)
                    entries = [body] if body else []
                else:
                    body = fields.get(field_id(holder))
                    entries = body if isinstance(body, list) else [body] if body else []
                for item in entries:
                    if not isinstance(item, dict):
                        raise ValueError("jira_dynamic_holder_invalid")
                    if kind == "groupCF":
                        values.update(self.group(item.get("groupId")))
                    else:
                        user = account_id(item.get("accountId"))
                        if self.directory.active(user):
                            values.add(user)
            # Unsupported legitimate holder kinds contribute nobody.
        return values

    def project(self, project):
        if project in self.rules:
            return
        raw = self.client.call(f"/project/{project}")
        if number(raw.get("id")) != project:
            raise ValueError("jira_project_identity_mismatch")
        self.raw_projects[project] = raw
        scheme = self.client.call(
            f"/project/{project}/permissionscheme", params={"expand": "permissions"}
        )
        security = self.client.call(f"/project/{project}/issuesecuritylevelscheme")
        scheme_id = number(scheme.get("id"))
        security_id = number(security.get("id"))
        grants = scheme.get("permissions")
        if not isinstance(grants, list) or not isinstance(security.get("levels"), list):
            raise ValueError("jira_policy_catalog_invalid")
        rules = [g["holder"] for g in grants if g.get("permission") == "BROWSE_PROJECTS"]
        self.rules[project] = rules
        self.static[project] = self.holders(project, rules)
        levels = {number(item["id"]): [] for item in security["levels"]}
        # scheme and level filters are inclusive, not an intersection: query one scheme.
        items = (
            self.client.snapshot(
                "/issuesecurityschemes/level/member",
                "values",
                params={"schemeId": security_id, "expand": "holder"},
            )
            if levels
            else []
        )
        for item in items:
            if number(item.get("issueSecuritySchemeId")) != security_id:
                raise ValueError("jira_security_scheme_mismatch")
            level = number(item.get("issueSecurityLevelId"))
            if level not in levels:
                raise ValueError("jira_security_level_mismatch")
            levels[level].append(item["holder"])
        self.levels[project] = levels
        for level, holders in levels.items():
            self.level_static[(project, level)] = self.holders(project, holders)
        self.associations[project] = (scheme_id, security_id)

    def target(self, project, visibility):
        if not isinstance(visibility, dict):
            return None
        kind = visibility.get("type")
        identity = visibility.get("identifier")
        if kind == "group":
            return (
                "group",
                self.resolve_group(identity)
                if identity
                else self.resolve_group(visibility.get("value"), True),
            )
        if kind == "role":
            if identity:
                return ("role", str(number(identity)))
            catalog = self.client.call(f"/project/{project}/role")
            url = catalog.get(visibility.get("value")) if isinstance(catalog, dict) else None
            path = urlsplit(url or "").path
            match = re.fullmatch(r"/rest/api/3/project/" + str(project) + r"/role/(\d+)", path)
            if not match:
                raise ValueError("jira_role_identity_unproven")
            return ("role", str(number(match.group(1))))
        return None

    def audience(self, project, level=None, target=None):
        self.project(project)
        values = set(self.static[project])
        if level is not None:
            values.intersection_update(self.level_static.get((project, level), set()))
        if target:
            kind, identity = target
            values.intersection_update(
                self.group(identity) if kind == "group" else self.role(project, identity)
            )
        key = policy_id(self.settings.cloud_id, project, level, target)
        self.audiences[key] = values
        return values

    def issue_readers(self, issue):
        project = number(issue["fields"]["project"]["id"])
        self.project(project)
        browse = self.holders(project, self.rules[project], issue)
        security = issue["fields"].get("security")
        level = number(security["id"]) if security else None
        if level is not None:
            browse.intersection_update(
                self.holders(project, self.levels[project].get(level, []), issue)
            )
        static = self.audience(project, level)
        return browse - static

    def holder_type_hash(self):
        # Custom-field selectors are part of the dynamic holder's definition.
        # One constant-size hash avoids trusting content descriptors on changes.
        def types(holders):
            return sorted(
                {
                    (h["type"], field_id(h) if h["type"] in ("userCF", "groupCF") else "")
                    for h in holders
                    if h["type"] in DYNAMIC
                }
            )

        return digest(
            [
                sorted((p, types(holders)) for p, holders in self.rules.items()),
                sorted(
                    (p, level, types(holders))
                    for p, levels in self.levels.items()
                    for level, holders in levels.items()
                ),
            ]
        )

    def membership_hash(self):
        return digest(
            [
                sorted((p, sorted(v)) for p, v in self.static.items()),
                sorted((str(k), sorted(v)) for k, v in self.level_static.items()),
                sorted((str(k), sorted(v)) for k, v in self.groups.items()),
                sorted((str(k), sorted(v)) for k, v in self.roles.items()),
                self.rules,
                self.levels,
            ]
        )

    def finish(self):
        for project, (scheme, security) in self.associations.items():
            fresh_scheme = self.client.call(
                f"/project/{project}/permissionscheme", params={"expand": "permissions"}
            )
            fresh_security = self.client.call(f"/project/{project}/issuesecuritylevelscheme")
            if (
                number(fresh_scheme.get("id")) != scheme
                or number(fresh_security.get("id")) != security
                or [
                    g["holder"]
                    for g in fresh_scheme.get("permissions", [])
                    if g.get("permission") == "BROWSE_PROJECTS"
                ]
                != self.rules[project]
                or {number(i["id"]) for i in fresh_security.get("levels", [])}
                != set(self.levels[project])
            ):
                raise ValueError("jira_policy_association_changed")
        token_counts = {user: 1 for user in self.directory.accounts}
        groups = []
        edges = 0
        for identity, values in sorted(self.audiences.items()):
            if len(values) > 16384:
                raise ValueError("jira_policy_member_limit")
            edges += len(values)
            if edges > MAX_ENTRIES:
                raise ValueError("jira_policy_edge_limit")
            for user in values:
                token_counts[user] += 1
                if token_counts[user] > 5000:
                    raise ValueError("jira_subject_token_limit_5000")
            groups.append(
                SourceGroup(
                    id=identity,
                    members=tuple(
                        "usr:" + user_id(self.settings.cloud_id, u) for u in sorted(values)
                    ),
                )
            )
        return tuple(groups)
