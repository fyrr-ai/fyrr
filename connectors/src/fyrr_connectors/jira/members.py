"""Complete account-ID directory and opt-in corporate e-mail evidence."""

import re
from dataclasses import dataclass

from ..connector import EmailVerification, SourceUser


def account_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9:_-]{1,256}", value):
        raise ValueError("jira_account_id_invalid")
    return value


def user_id(cloud, account):
    return "atlassian.net:" + cloud + ":" + account_id(account)


@dataclass
class Directory:
    accounts: dict

    @classmethod
    def read(cls, client):
        accounts = {}
        for body in client.snapshot("/users/search", "values"):
            if not isinstance(body, dict) or type(body.get("active")) is not bool:
                raise ValueError("jira_directory_invalid")
            identity = account_id(body.get("accountId"))
            if identity in accounts:
                raise ValueError("jira_directory_changed")
            accounts[identity] = body
        return cls(accounts)

    def active(self, identity):
        body = self.accounts.get(identity)
        return bool(body and body["active"] and body.get("accountType") == "atlassian")

    def users(self, settings):
        result = []
        for identity, body in sorted(self.accounts.items()):
            address = body.get("emailAddress")
            approved = (
                isinstance(address, str)
                and len(address.encode()) <= 320
                and address.count("@") == 1
                and address.rsplit("@", 1)[1].lower() in settings.corporate_domains
                and not any(c.isspace() or ord(c) < 32 for c in address)
            )
            result.append(
                SourceUser(
                    id=user_id(settings.cloud_id, identity),
                    active=self.active(identity),
                    email=address if approved else None,
                    email_verification=EmailVerification.VERIFIED_CORPORATE
                    if approved
                    else EmailVerification.UNVERIFIED,
                )
            )
        return tuple(result)
