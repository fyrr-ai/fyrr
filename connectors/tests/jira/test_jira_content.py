"""Fixed Cloud issue acquisition through the connector batch boundary."""


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


def test_jira_issue_key_is_an_attribute_and_opens_the_summary(jira, jira_index):
    jira.issue(101, summary="Login fails")
    change = jira_index.sync()["jira:11111111-1111-4111-8111-111111111111:issue:101"]
    assert change["attributes"] == ["jira.key=P1-101"]
    summary = change["sections"][0]
    assert summary["text"] == "P1-101 Login fails"
    assert summary["location"]["label"] == "summary"
    assert change["metadata"]["title"] == "Login fails"


def test_jira_key_that_cannot_be_an_attribute_is_omitted():
    from fyrr_connectors.jira.render import key_attributes

    assert key_attributes({"key": "P1-101"}) == ("jira.key=P1-101",)
    assert key_attributes({"key": "P1-\n101"}) == ()


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
