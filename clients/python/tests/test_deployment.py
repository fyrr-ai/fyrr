"""The clients against a real `fyrr writer serve` and `fyrr reader serve` on local storage."""

from __future__ import annotations

import asyncio
import uuid

import pytest

from fyrr_client import AsyncClient, Client, FyrrError
from fyrr_client.models import WriteRequest

from .conftest import Deployment


def fresh_namespace() -> str:
    return f"py-client-{uuid.uuid4().hex[:12]}"


def rows(count: int) -> WriteRequest:
    # A row that carries its own vector makes no embedding call.
    return {
        "upserts": [
            {
                "id": f"doc-{n:02}",
                "text": f"Document number {n}",
                "vector": [float(n + 1)] + [0.0] * 1023,
            }
            for n in range(count)
        ]
    }


def test_write_query_describe_list_delete(deployment: Deployment) -> None:
    namespace = fresh_namespace()
    with Client(
        deployment.reader_url, deployment.writer_url, api_key=deployment.admin_key
    ) as client:
        assert client.write(namespace, rows(3))["upserted"] == 3
        found = client.query(namespace, {"query": "document", "consistency": "strong"})
        assert {hit["root_id"] for hit in found["hits"]} == {"doc-00", "doc-01", "doc-02"}
        assert client.describe_namespace(namespace)["namespace"] == namespace
        assert namespace in client.list_namespaces(namespace)["namespaces"]
        assert client.delete_namespace(namespace)["deleted"] is True
        assert namespace not in client.list_namespaces(namespace)["namespaces"]


def test_iter_documents_pages_through_every_document(deployment: Deployment) -> None:
    namespace = fresh_namespace()
    with Client(
        deployment.reader_url, deployment.writer_url, api_key=deployment.admin_key
    ) as client:
        client.write(namespace, rows(7))
        every = [
            hit["root_id"]
            for hit in client.iter_documents(namespace, page_size=3, consistency="strong")
        ]
        assert every == [f"doc-{n:02}" for n in range(7)]
        some = client.iter_documents(
            namespace,
            filter={"field": "root_id", "op": "gte", "value": "doc-05"},
            page_size=1,
            consistency="strong",
        )
        assert [hit["root_id"] for hit in some] == ["doc-05", "doc-06"]
        assert list(client.iter_namespaces(namespace)) == [namespace]


def test_error_answers_raise_with_status_and_code(deployment: Deployment) -> None:
    # A key with the `read` role only may not list namespaces.
    with (
        Client(deployment.reader_url, api_key=deployment.read_key) as client,
        pytest.raises(FyrrError) as raised,
    ):
        client.list_namespaces()
    assert (raised.value.status, raised.value.code) == (403, "forbidden")


def test_a_conditional_write_applies_or_skips(deployment: Deployment) -> None:
    namespace = fresh_namespace()
    with Client(
        deployment.reader_url, deployment.writer_url, api_key=deployment.admin_key
    ) as client:
        client.write(namespace, {"settings": {"attributes": {"version": "int"}}})
        first: WriteRequest = {
            "upserts": [{"id": "a", "text": "one", "version": 1, "vector": [1.0] + [0.0] * 1023}]
        }
        client.write(namespace, first)
        stale: WriteRequest = {
            "upserts": [{"id": "a", "text": "two", "version": 1, "vector": [1.0] + [0.0] * 1023}],
            "conditions": {"a": {"field": "version", "op": "lt", "value": {"row": "version"}}},
        }
        assert client.write(namespace, stale).get("skipped") == 1


def test_async_client_covers_the_same_routes(deployment: Deployment) -> None:
    namespace = fresh_namespace()

    async def scenario() -> None:
        async with AsyncClient(
            deployment.reader_url, deployment.writer_url, api_key=deployment.admin_key
        ) as client:
            assert (await client.write(namespace, rows(5)))["upserted"] == 5
            found = await client.query(namespace, {"query": "document", "consistency": "strong"})
            assert len(found["hits"]) == 5
            ids = [
                hit["root_id"]
                async for hit in client.iter_documents(namespace, page_size=2, consistency="strong")
            ]
            assert ids == [f"doc-{n:02}" for n in range(5)]
            assert (await client.describe_namespace(namespace))["namespace"] == namespace
            assert [name async for name in client.iter_namespaces(namespace)] == [namespace]
            await client.delete_namespace(namespace)
            with pytest.raises(FyrrError):
                await client.describe_namespace(namespace)

    asyncio.run(scenario())
