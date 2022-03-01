import warnings
from datetime import timedelta
from typing import Any, Dict, Iterable, Optional, cast

from fastapi import HTTPException, Request, status
from fastapi.security.oauth2 import OAuth2PasswordBearer
from fastapi.security.utils import get_authorization_scheme_param
from passlib.context import CryptContext

# Ignore deprecation warnings from "jose".
with warnings.catch_warnings():
    warnings.filterwarnings("ignore")
    from jose import JWTError, jwt


class OAuth2BearerToken(OAuth2PasswordBearer):
    """
    Extends the OAuth2PasswordBearer class to read the bearer tokens from either the `Authorization`
    header or `Authorization` cookie.
    """

    async def __call__(self, request: Request) -> Optional[str]:
        header_authorization: str = request.headers.get("Authorization", "")
        cookie_authorization: str = request.cookies.get("Authorization", "")

        header_scheme, header_token = get_authorization_scheme_param(header_authorization)
        cookie_scheme, cookie_token = get_authorization_scheme_param(cookie_authorization)

        if header_scheme.lower() == "bearer":
            return header_token
        if cookie_scheme.lower() == "bearer":
            return cookie_token

        if self.auto_error:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not authenticated",
            )

        return None


class AuthManager:
    _crypt_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

    def __init__(
        self,
        access_token_secret: str,
        access_token_lifetime: timedelta,
        access_token_encoding_algorithm: str = "HS256",
        access_token_decoding_algorithms: Optional[Iterable[str]] = None,
    ):
        self.access_token_secret = access_token_secret
        self.access_token_lifetime = access_token_lifetime
        self.access_token_encoding_algorithm = access_token_encoding_algorithm
        self.access_token_decoding_algorithms = (
            list(access_token_decoding_algorithms)
            if access_token_decoding_algorithms is not None
            else [access_token_encoding_algorithm]
        )

    def hash_password(self, password: str) -> str:
        return cast(str, self._crypt_context.hash(password))

    def verify_password(self, password: str, password_hash: str) -> bool:
        return cast(bool, self._crypt_context.verify(password, password_hash))

    def encode_access_token(self, claims: Dict[str, Any]) -> str:
        return cast(
            str,
            jwt.encode(
                claims,
                self.access_token_secret,
                self.access_token_encoding_algorithm,
            ),
        )

    def decode_access_token(
        self,
        access_token: str,
        options: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        try:
            return cast(
                Dict[str, Any],
                jwt.decode(
                    token=access_token,
                    key=self.access_token_secret,
                    algorithms=self.access_token_decoding_algorithms,
                    options=options,
                ),
            )
        except JWTError:
            return None
