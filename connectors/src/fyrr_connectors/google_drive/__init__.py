"""Bundled GoogleDrive provider; configuration discovery does not load its client."""

__all__ = ["GoogleDrive"]


def __getattr__(name):
    if name == "GoogleDrive":
        from .connector import GoogleDrive

        return GoogleDrive
    raise AttributeError(name)
