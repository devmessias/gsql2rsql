"""Utility functions for the transpiler."""

import hashlib
from typing import Any


def fnv_hash(*args: Any) -> int:
    """
    Compute FNV-1a hash for the given arguments.
    Used for generating deterministic hash codes.
    """
    hasher = hashlib.md5(usedforsecurity=False)
    for arg in args:
        hasher.update(str(arg).encode("utf-8"))
    return int(hasher.hexdigest(), 16) & 0xFFFFFFFF


def change_indentation(text: str, level: int, indent_str: str = "  ") -> str:
    """
    Change the indentation of text by a given level.

    Args:
        text: The text to indent.
        level: Number of indentation levels to add.
        indent_str: The string to use for each indentation level.

    Returns:
        The indented text.
    """
    indent = indent_str * level
    lines = text.split("\n")
    return "\n".join(f"{indent}{line}" if line else line for line in lines)


def is_subclass_of_generic(cls: type, generic_base: type) -> bool:
    """
    Check if a class is a subclass of a generic base type.

    Args:
        cls: The class to check.
        generic_base: The generic base type.

    Returns:
        True if cls is a subclass of generic_base.
    """
    try:
        if hasattr(cls, "__orig_bases__"):
            for base in cls.__orig_bases__:
                origin = getattr(base, "__origin__", base)
                if origin is generic_base:
                    return True
        return issubclass(cls, generic_base)
    except TypeError:
        return False
