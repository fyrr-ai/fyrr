"""Pin the public site Cloud identity and the authenticated scoped gateway."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Identity:
    cloud: str
    site: str

    @classmethod
    def read(cls, client, settings):
        tenant = client.call("/_edge/tenant_info", public=True)
        info = client.call("/serverInfo")
        if (
            not isinstance(tenant, dict)
            or tenant.get("cloudId") != settings.cloud_id
            or not isinstance(info, dict)
            or info.get("deploymentType") != "Cloud"
            or info.get("baseUrl", "").rstrip("/") != settings.site_url.rstrip("/")
        ):
            raise ValueError("jira_tenant_identity_mismatch")
        return cls(settings.cloud_id, settings.site_url.rstrip("/"))
