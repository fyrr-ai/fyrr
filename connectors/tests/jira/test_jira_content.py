"""Fixed Cloud issue acquisition through the connector batch boundary."""


def test_jira_content_golden(jira):
    from pathlib import Path

    from jira_kit import Index

    from fyrr_connectors import testkit
    from fyrr_connectors.jira import Jira
    from fyrr_connectors.provider import provider_info

    paragraph = [
        {
            "type": "text",
            "text": "approved design",
            "marks": [{"type": "link", "attrs": {"href": "https://docs.invalid/d"}}],
        },
        {"type": "text", "text": " and "},
        {"type": "inlineCard", "attrs": {"url": "https://github.invalid/o/r/pull/7"}},
    ]
    jira.issue(
        101,
        description={
            "type": "doc",
            "version": 1,
            "content": [{"type": "paragraph", "content": paragraph}],
        },
        parent={"key": "P1-100", "fields": {"summary": "Not shown"}},
        issuelinks=[
            {
                "type": {"inward": "is blocked by", "outward": "blocks"},
                "outwardIssue": {"key": "P1-140"},
            },
            {
                "type": {"inward": "is blocked by", "outward": "blocks"},
                "inwardIssue": {"key": "P1-88"},
            },
        ],
    )
    jira.comment(101, 201, "Golden body.")
    info = provider_info(
        name="jira", connector=Jira, min_sdk_version="0.1.0", connection_types=("fixed",)
    )
    result = testkit.run_sync(
        info, "content", permissions="fixed", settings=Index(jira).settings, scope=Index(jira).scope
    )
    testkit.assert_golden(result, Path(__file__).parent / "golden/content.json")


def test_jira_comments_name_their_author(jira, jira_index):
    jira.issue(101)
    jira.comment(101, 201, "Staff.")
    jira.comment(101, 202, "Customer.", author={"displayName": "Cora", "accountType": "customer"})
    del jira.comment(101, 203, "Anonymous.")["author"]
    sections = jira_index.sync()["jira:11111111-1111-4111-8111-111111111111:issue:101"]["sections"]
    assert [s["text"] for s in sections if s["location"]["kind"] == "comment"] == [
        "Fixture author:\nStaff.",
        "Cora (customer):\nCustomer.",
        "unknown:\nAnonymous.",
    ]


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


def test_jira_moved_issue_keeps_its_root_and_gets_the_new_key(jira, jira_index):
    jira.issue(101, summary="Login fails")
    first = jira_index.sync()
    root = "jira:11111111-1111-4111-8111-111111111111:issue:101"
    jira.edit(101)
    jira.issues[101]["key"] = "NEW-7"
    second = jira_index.sync()
    assert list(second) == [root]
    assert second[root]["attributes"] == ["jira.key=NEW-7"]
    assert second[root]["sections"][0]["text"] == "NEW-7 Login fails"
    assert second[root]["source_revision"] != first[root]["source_revision"]


def test_jira_links_are_not_published_with_synced_permissions(jira):
    # Jira hides a link from a reader who may not see the linked issue.
    import json

    from jira_kit import Index

    jira.issue(
        101,
        parent={"key": "P1-100"},
        issuelinks=[{"type": {"outward": "blocks"}, "outwardIssue": {"key": "PRIVATE-7"}}],
    )
    sections = Index(jira, mode="sync").sync()[
        "jira:11111111-1111-4111-8111-111111111111:issue:101"
    ]
    assert "PRIVATE-7" not in json.dumps(sections) and "P1-100" not in json.dumps(sections)
