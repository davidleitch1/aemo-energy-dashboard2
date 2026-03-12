"""Prices tab package — extracted from gen_dash.py."""


def create_prices_tab(dashboard):
    """Lazy import wrapper to avoid triggering config at import time."""
    from .prices_tab import create_prices_tab as _create
    return _create(dashboard)


__all__ = ["create_prices_tab"]
