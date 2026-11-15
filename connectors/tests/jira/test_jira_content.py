"""Fixed Cloud issue acquisition through the connector batch boundary."""


def test_jira_fixed_issue_comments_metadata_and_no_acl(jira, jira_index):
    jira.issue(101)
    jira.comment(101, 201, "Comment body.")
    jira.comment(101, 202, "Restricted.", visibility={"type": "group", "identifier": "g1"})
    changes = jira_index.sync()
    item = next(iter(changes.values()))
    assert [section["text"] for section in item["sections"]] == [
        "Issue 101",
        "Description.",
        "Open",
        "Task",
        "Fixture author",
        "Comment body.",
    ]
    assert item["readers"] == ["grp:staff"]
    assert item["metadata"]["author"] == "Fixture author"
    assert item["metadata"]["created_at"] and item["metadata"]["updated_at"]
    assert item["sections"][-1]["location"]["kind"] == "comment"
    assert not any(
        path.startswith(("/group", "/users", "/permissionscheme", "/applicationrole"))
        or "/role" in path
        for _, path, _ in jira.requests
    )
    assert jira_index.sync() == {}
    assert jira_index.listing() == set(changes)


def test_jira_content_golden(jira):
    from pathlib import Path

    from jira_kit import Index

    from fyrr_connectors import testkit
    from fyrr_connectors.jira import Jira
    from fyrr_connectors.provider import provider_info

    jira.issue(101)
    jira.comment(101, 201, "Golden body.")
    info = provider_info(
        name="jira", connector=Jira, min_sdk_version="0.1.0", connection_types=("fixed",)
    )
    result = testkit.run_sync(
        info, "content", permissions="fixed", settings=Index(jira).settings, scope=Index(jira).scope
    )
    testkit.assert_golden(result, Path(__file__).parent / "golden/content.json")


def test_jira_fixed_http_gateway_real_requests_and_null_fields(monkeypatch):
    import requests
    from fake_jira import FakeServer
    from jira_kit import Index

    from fyrr_connectors.jira import client

    fake = FakeServer()
    try:
        monkeypatch.setattr(client, "session", requests.Session)
        monkeypatch.setenv("FYRR_TEST_JIRA_TOKEN", fake.world.token)
        fake.world.issue(
            101,
            summary="fixture document 0",
            description=None,
            status=None,
            issuetype={"id": "1"},
            reporter=None,
        )
        index = Index(fake.world)
        index.settings["api_endpoint"] = fake.endpoint
        index.sync()
        assert len(index.published) == 1
        fake.world.comment(101, 201, "fixture document 3")
        index.sync()
        fake.world.delete_comment(101, 201)
        fake.world.clock += 43201
        index.sync()
        fake.world.edit(101, description="oversized synthetic text " * 8)
        index.sync()
        assert index.listing() == set(index.published)
    finally:
        fake.close()
