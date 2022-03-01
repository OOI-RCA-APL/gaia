from typing import Any, Callable

from fastapi import APIRouter
from fastapi.types import DecoratedCallable


class Router(APIRouter):
    """
    A FastAPI router that doesn't care about trailing slashes.
    """

    def api_route(
        self,
        path: str,
        *,
        include_in_schema: bool = True,
        **kwargs: Any,
    ) -> Callable[[DecoratedCallable], DecoratedCallable]:
        # Strip trailing slashes.
        path = path.rstrip("/")
        # Create an alternate path with a single trailing slash.
        alternate = path + "/"

        # Register the path with no trailing slash.
        add_route = super().api_route(path, include_in_schema=include_in_schema, **kwargs)
        # Registers the path with a trailing slash. Don't include this one in the schema.
        add_alternate_route = super().api_route(alternate, include_in_schema=False, **kwargs)

        def decorator(target: DecoratedCallable) -> DecoratedCallable:
            add_alternate_route(target)
            return add_route(target)

        return decorator
