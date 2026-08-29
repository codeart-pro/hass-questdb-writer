"""Persistent pure-Python ILP/HTTP transport for QuestDB."""

from __future__ import annotations

import base64
import http.client
import ssl
from types import TracebackType
from typing import Final

WRITE_PATH: Final = "/write?precision=n"
MAX_ERROR_BODY_BYTES: Final = 16 * 1024


class IlpTransportError(Exception):
    """Base class for classified transport failures."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool,
        delivery_uncertain: bool,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.delivery_uncertain = delivery_uncertain
        self.status_code = status_code


class PermanentIlpError(IlpTransportError):
    """A request that must not be retried without changing data or config."""


class RetryableIlpError(IlpTransportError):
    """A temporary or uncertain failure that may be retried from the spool."""


class AuthenticationIlpError(PermanentIlpError):
    """QuestDB rejected the configured credentials."""


class IlpHttpTransport:
    """Send complete ILP batches over one reusable HTTP connection."""

    def __init__(
        self,
        host: str,
        port: int,
        *,
        use_tls: bool,
        timeout_seconds: float,
        username: str | None = None,
        password: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        if not host:
            raise ValueError("host must not be empty")
        if not 1 <= port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if (username is None) != (password is None):
            raise ValueError("username and password must be provided together")
        if ssl_context is not None and not use_tls:
            raise ValueError("ssl_context requires use_tls=True")

        self._host = host
        self._port = port
        self._use_tls = use_tls
        self._timeout_seconds = timeout_seconds
        self._ssl_context = ssl_context
        self._connection: http.client.HTTPConnection | None = None
        self._headers = {"Content-Type": "text/plain; charset=utf-8"}
        if username is not None and password is not None:
            credentials = base64.b64encode(
                f"{username}:{password}".encode("utf-8")
            ).decode("ascii")
            self._headers["Authorization"] = f"Basic {credentials}"

    def _make_connection(self) -> http.client.HTTPConnection:
        if self._use_tls:
            return http.client.HTTPSConnection(
                self._host,
                self._port,
                timeout=self._timeout_seconds,
                context=self._ssl_context,
            )
        return http.client.HTTPConnection(
            self._host, self._port, timeout=self._timeout_seconds
        )

    def _get_connection(self) -> http.client.HTTPConnection:
        if self._connection is None:
            self._connection = self._make_connection()
        return self._connection

    def close(self) -> None:
        """Close the current connection; safe to call repeatedly."""
        connection, self._connection = self._connection, None
        if connection is not None:
            connection.close()

    def send_batch(self, payload: bytes) -> None:
        """Send one complete single-table ILP batch.

        A successful return confirms an HTTP 2xx response. A network exception
        is conservatively marked delivery-uncertain because the server may have
        committed the request before the response was lost.
        """
        if not isinstance(payload, bytes) or not payload:
            raise ValueError("payload must be non-empty bytes")
        if not payload.endswith(b"\n"):
            raise ValueError("ILP payload must end with a newline")

        connection = self._get_connection()
        try:
            connection.request(
                "POST", WRITE_PATH, body=payload, headers=self._headers
            )
            response = connection.getresponse()
            body = response.read(MAX_ERROR_BODY_BYTES + 1)
        except (OSError, TimeoutError, http.client.HTTPException) as exc:
            self.close()
            raise RetryableIlpError(
                f"QuestDB request failed: {type(exc).__name__}",
                retryable=True,
                delivery_uncertain=True,
            ) from exc

        if 200 <= response.status < 300:
            if len(body) > MAX_ERROR_BODY_BYTES:
                self.close()
            return

        truncated = len(body) > MAX_ERROR_BODY_BYTES
        if truncated:
            body = body[:MAX_ERROR_BODY_BYTES]
            self.close()
        detail = body.decode("utf-8", errors="replace").strip()
        message = f"QuestDB returned HTTP {response.status}"
        if detail:
            message = f"{message}: {detail}"

        if response.status in (401, 403):
            raise AuthenticationIlpError(
                message,
                retryable=False,
                delivery_uncertain=False,
                status_code=response.status,
            )
        if response.status in (408, 425, 429):
            if response.status == 408:
                self.close()
            raise RetryableIlpError(
                message,
                retryable=True,
                delivery_uncertain=response.status == 408,
                status_code=response.status,
            )
        if 500 <= response.status < 600:
            self.close()
            raise RetryableIlpError(
                message,
                retryable=True,
                delivery_uncertain=True,
                status_code=response.status,
            )
        raise PermanentIlpError(
            message,
            retryable=False,
            delivery_uncertain=False,
            status_code=response.status,
        )

    def __enter__(self) -> IlpHttpTransport:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
