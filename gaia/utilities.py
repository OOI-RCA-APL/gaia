import inspect
import os
from typing import Optional


def rel(path: str) -> str:
    """
    Convert a relative path from the current module to an absolute path.

    :param path: A relative path from the current module.
    :return: The resolved absolute path.
    """
    return os.path.realpath(os.path.join(_get_module_path(), path))


def proj(path: Optional[str] = None) -> str:
    """
    Convert a relative path from project root to an absolute path. The project root is considered
    the nearest parent directory of the current module that contains a "pyproject.toml" file.

    :param path: A relative path from the project root.
    :return: The resolved absolute path.
    """
    current = _get_module_path()
    root = None

    while current:
        if os.path.isfile(os.path.join(current, "pyproject.toml")):
            root = current
            break

        current = os.path.dirname(current)

    assert root
    if path is not None:
        root = os.path.join(root, path)

    return os.path.realpath(root)


def _get_module_path() -> str:
    caller = inspect.stack()[2]
    module = inspect.getmodule(caller[0])
    assert module is not None
    assert module.__file__ is not None
    return os.path.dirname(module.__file__)
