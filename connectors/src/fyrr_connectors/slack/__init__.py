"""Bundled Slack provider; configuration discovery does not load its client."""

__all__ = ["Slack"]


def __getattr__(name):
    if name == "Slack":
        from .connector import Slack

        return Slack
    raise AttributeError(name)
