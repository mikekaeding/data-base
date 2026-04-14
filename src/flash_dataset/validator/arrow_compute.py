"""Typed wrappers around dynamic pyarrow.compute functions."""

from __future__ import annotations

from collections.abc import Callable
from typing import cast as typing_cast

import pyarrow.compute as pyarrow_compute

ArrowFunction = Callable[..., object]


def _call(name: str, *args: object) -> object:
    return typing_cast(ArrowFunction, getattr(pyarrow_compute, name))(*args)


def add(left: object, right: object) -> object:
    """Return the Arrow add kernel result."""
    return _call("add", left, right)


def and_kleene(left: object, right: object) -> object:
    """Return the Arrow three-valued logical-and kernel result."""
    return _call("and_kleene", left, right)


def binary_length(value: object) -> object:
    """Return the Arrow binary_length kernel result."""
    return _call("binary_length", value)


def cast(value: object, data_type: object) -> object:
    """Return the Arrow cast kernel result."""
    return _call("cast", value, data_type)


def equal(left: object, right: object) -> object:
    """Return the Arrow equal kernel result."""
    return _call("equal", left, right)


def fill_null(value: object, replacement: object) -> object:
    """Return the Arrow fill_null kernel result."""
    return _call("fill_null", value, replacement)


def greater(left: object, right: object) -> object:
    """Return the Arrow greater-than kernel result."""
    return _call("greater", left, right)


def invert(value: object) -> object:
    """Return the Arrow boolean invert kernel result."""
    return _call("invert", value)


def is_null(value: object) -> object:
    """Return the Arrow is_null kernel result."""
    return _call("is_null", value)


def not_equal(left: object, right: object) -> object:
    """Return the Arrow not_equal kernel result."""
    return _call("not_equal", left, right)


def or_kleene(left: object, right: object) -> object:
    """Return the Arrow three-valued logical-or kernel result."""
    return _call("or_kleene", left, right)


def subtract(left: object, right: object) -> object:
    """Return the Arrow subtract kernel result."""
    return _call("subtract", left, right)
