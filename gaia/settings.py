from pathlib import Path
from typing import Any, Optional, Union

from pydantic import BaseSettings as PydanticBaseSettings
from pydantic import ValidationError


class BaseSettings(PydanticBaseSettings):
    """
    Base Pydantic settings with a more readable error output when validation fails.
    """

    def __init__(
        __pydantic_self__,
        _env_file: Union[Path, str, None] = None,
        _env_file_encoding: Optional[str] = None,
        _secrets_dir: Union[Path, str, None] = None,
        **values: Any,
    ) -> None:
        try:
            super().__init__(
                _env_file=_env_file,  # type: ignore
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
