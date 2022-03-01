from typing import Dict, Generic, Optional, TypeVar, Union, cast

from fastapi.responses import JSONResponse
from pydantic.main import BaseModel
from starlette.background import BackgroundTask

T = TypeVar("T")


class TypedJSONResponse(JSONResponse, Generic[T]):
    """
    JSON response that restricts the contained data to a specific model type.
    """

    def __init__(
        self,
        content: T,
        status_code: int = 200,
        headers: Optional[Dict[str, str]] = None,
        media_type: Optional[str] = None,
        background: Optional[BackgroundTask] = None,
    ) -> None:
        super().__init__(
            content=content.dict() if isinstance(content, BaseModel) else content,
            status_code=status_code,
            headers=cast(Dict[str, str], headers),
            media_type=cast(str, media_type),
            background=cast(BackgroundTask, background),
        )


TypedJSONResult = Union[TypedJSONResponse[T], T]
"""
Combined type for either a typed JSON response of a given data type or just the data itself.
"""
