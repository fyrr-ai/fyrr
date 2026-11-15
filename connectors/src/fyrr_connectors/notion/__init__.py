"""Bundled Notion provider; configuration discovery does not load its client."""

__all__ = ["Notion"]


def __getattr__(name):
    if name == "Notion":
        from .connector import Notion

        return Notion
    raise AttributeError(name)
