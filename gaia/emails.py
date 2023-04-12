from abc import ABC, abstractmethod
from email.message import EmailMessage
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union, cast

from aiosmtplib import SMTPResponse
from email_validator import validate_email


class EmailManager(ABC):
    """
    Base class for all email managers.
    """

    @abstractmethod
    async def send(
        self,
        *,
        subject: str,
        body: str,
        recipients: Union[str, Sequence[str]],
        subtype: str = "plain",
    ) -> Any:
        """
        Send an email.

        :param subject: The subject of the email.
        :param content: The body of the email.
        :param recipients: Email addresses to send the email to.
        :param subtype: Content subtype for the body. Usually "plain" or "html". Defaults to "plain".
        """
        ...


class SMTPEmailManager(EmailManager):
    """
    Send emails via an external SMTP server.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        use_starttls: bool = False,
        address: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        """
        Create a new SMTP manager with the provided settings.

        :param host: The hostname of the SMTP server to connect to.
        :param port: The port of the SMTP server to connect to.
        :param use_starttls: If `True` use STARTTLS to send emails.
        :param address: The email address to send emails from.
        :param username: The username to sign into the SMTP server with.
        :param password: The password to sign into the SMTP server with.
        """
        super().__init__()
        self._host = host
        self._port = port
        self._use_starttls = use_starttls
        self._address = address
        self._username = username
        self._password = password

    async def send(
        self,
        *,
        subject: str,
        body: str,
        recipients: Union[str, Sequence[str]],
        subtype: str = "plain",
    ) -> Tuple[EmailMessage, Dict[str, SMTPResponse], str]:
        import aiosmtplib

        recipients = self._normalize_recipients(recipients)
        if not recipients:
            return EmailMessage(), {}, ""

        message = EmailMessage()
        message.set_content(body, subtype=subtype)

        message["To"] = ",".join(recipients)
        if self._address is not None:
            message["From"] = self._address
        message["Subject"] = subject

        errors, log = await aiosmtplib.send(
            message=message,
            sender=self._address,
            recipients=recipients,
            hostname=self._host,
            port=self._port,
            username=self._username,
            password=self._password,
            start_tls=self._use_starttls,
        )

        return message, errors, log

    @classmethod
    def _normalize_recipients(cls, recipients: Union[str, Sequence[str]]) -> List[str]:
        if isinstance(recipients, str):
            recipients = recipients.strip().split(",")

        recipients = [
            normalize_email_address(recipient)
            for recipient in recipients
            if validate_email_address(recipient)
        ]

        return recipients


def normalize_email_address(email: str) -> str:
    return cast(str, validate_email(email.strip(), check_deliverability=False).email)


def validate_email_address(value: str) -> bool:
    try:
        normalize_email_address(value.strip())
        return True
    except:
        return False
