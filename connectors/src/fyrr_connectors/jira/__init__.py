"""Bundled Jira provider; configuration discovery does not load its client."""

__all__ = ["Jira"]


def __getattr__(name):
    if name == "Jira":
        from .connector import Jira

        return Jira
    raise AttributeError(name)
