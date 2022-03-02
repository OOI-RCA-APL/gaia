from typing import Any, Optional

from pydantic import BaseSettings as PydanticBaseSettings
from pydantic import ValidationError
from pydantic.typing import StrPath


class BaseSettings(PydanticBaseSettings):
    """
    Base Pydantic settings with a more readable error output when validation fails.
    """

    def __init__(
        __pydantic_self__,
        _env_file: Optional[StrPath] = None,
        _env_file_encoding: Optional[str] = None,
        _secrets_dir: Optional[StrPath] = None,
        **values: Any,
    ) -> None:
        try:
            super().__init__(
                _env_file=_env_file,
                _env_file_encoding=_env_file_encoding,
                _secrets_dir=_secrets_dir,
                **values,
            )
        except ValidationError as exception:
            config = __pydantic_self__.__config__
            prefix = config.env_prefix

            message = []
            message.append("Environment variable validation failed: ")
            message.append("{ ")
            message.append(
                ", ".join(
                    [
                        f"{prefix}{str(error['loc'][0]).upper()}: {error['msg']}"
                        for error in exception.errors()
                    ]
                )
            )
            message.append(" }")
            raise EnvironmentError("".join(message))
