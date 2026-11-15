"""Bundled GitHub provider; configuration discovery does not load its client."""

__all__ = ["GitHub"]


def __getattr__(name):
    if name == "GitHub":
        from .connector import GitHub

        return GitHub
    raise AttributeError(name)
