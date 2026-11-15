"""The `settings` and `scope` tables of a Google Drive source."""

from ..config import ConfigModel, Secret, secret, setting

#: Scope extension -> the Google-native MIME type it selects.
MIME_TYPES = {
    "gdoc": "application/vnd.google-apps.document",
    "gsheet": "application/vnd.google-apps.spreadsheet",
    "gslides": "application/vnd.google-apps.presentation",
}
ALL_DRIVES = "*"


class Settings(ConfigModel):
    service_account: Secret = secret(
        description="Service-account key JSON with domain-wide delegation for drive.readonly"
    )
    admin_email: str | None = setting(
        description="Directory administrator the sync impersonates; required for sync, "
        "unused for fixed",
        default=None,
    )
    api_endpoint: str = setting(
        description="Google API base URL; change it only for a test fake or a proxy",
        default="https://www.googleapis.com",
        pattern="^https?://[^/?#]+$",
    )


class Scope(ConfigModel):
    my_drive: bool = setting(description="Files the owners below own in My Drive", default=True)
    shared_drives: list[str] = setting(
        description='Shared-drive IDs, or "*" for every drive the owners can open',
        default_factory=lambda: [ALL_DRIVES],
    )
    shared_with_me: bool = setting(
        description="Files in the owners' Drive that someone else owns; needs fixed "
        "permissions, a sync connection is refused",
        default=False,
    )
    owners: list[str] = setting(
        description="User e-mails whose Drive is read; empty means every active user "
        "(sync only, a fixed connection lists them)",
        default_factory=list,
    )
    folders: list[str] = setting(
        description="Folder IDs; only files beneath them are in scope. Empty: no restriction",
        default_factory=list,
    )
    extensions: list[str] = setting(
        description="Google-native types to export: gdoc, gsheet, gslides",
        default_factory=lambda: list(MIME_TYPES),
        min_items=1,
    )
