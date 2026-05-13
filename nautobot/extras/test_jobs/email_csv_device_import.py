"""
JOBS_ROOT job: poll IMAP for CSV attachments, map rows to Nautobot Device CSV columns, import Devices.

**Deploying:** copy this file into your configured ``JOBS_ROOT`` (see ``NAUTOBOT_JOBS_ROOT`` / ``JOBS_ROOT`` in
settings), restart Celery workers, run ``nautobot-server post_upgrade`` so Job records refresh, then enable the Job in
the UI under **Jobs**.

**Scheduling:** use **Jobs > Scheduled Jobs** to run this Job on an interval or cron expression (Celery Beat must be
running).

**Credentials:** assign an ``extras.Secret`` for the IMAP password in the Job form, or set
``NAUTOBOT_EMAIL_CSV_IMPORT_IMAP_PASSWORD`` in the worker environment when no Secret is selected.
"""

from __future__ import annotations

import codecs
import contextlib
import csv
import email
import imaplib
import io
import logging
import os
import re
from email.message import Message
from io import BytesIO
from typing import Iterable

from django.core.exceptions import PermissionDenied
from django.db import transaction

from nautobot.apps.jobs import (
    BooleanVar,
    DryRunVar,
    IntegerVar,
    Job,
    ObjectVar,
    RunJobTaskFailed,
    StringVar,
    TextVar,
    register_jobs,
)
from nautobot.core.api.parsers import NautobotCSVParser
from nautobot.core.api.utils import get_serializer_for_model
from nautobot.core.exceptions import AbortTransaction
from nautobot.dcim.models import Device
from nautobot.extras.models import Role, Secret, Status

logger = logging.getLogger(__name__)

name = "Email CSV device import"

# Environment variable used when no IMAP password Secret is selected.
IMAP_PASSWORD_ENV = "NAUTOBOT_EMAIL_CSV_IMPORT_IMAP_PASSWORD"


def decode_csv_attachment_bytes(raw: bytes) -> str:
    """Decode attachment bytes as UTF-8, stripping a BOM if present."""
    if not raw:
        return ""
    # utf-8-sig handles BOM; still normalize through same path ImportObjects uses for streams
    decoded = raw.decode("utf-8-sig")
    return decoded


def vendor_row_to_nautobot_flat_dict(
    row: dict[str, str],
    *,
    default_status_name: str,
    default_role_name: str,
    col_serial: str,
    col_manufacturer: str,
    col_device_type_model: str,
    col_location_name: str,
    col_location_parent_name: str | None,
    col_status_name: str | None,
    col_role_name: str | None,
    col_name: str | None,
) -> dict[str, str]:
    """
    Map a vendor CSV row (string values) to flat keys understood by NautobotCSVParser for Device.

    If the row already contains ``device_type__model`` (any ``__`` key), it is treated as pre-normalized:
    only missing ``status__name`` / ``role__name`` are filled from defaults / optional columns.
    """
    out: dict[str, str] = {}

    def _get(col: str | None) -> str:
        if not col:
            return ""
        v = row.get(col)
        return (v or "").strip()

    pre_normalized = any("__" in (k or "") for k in row)

    if pre_normalized:
        for k, v in row.items():
            if k and v is not None and str(v).strip() != "":
                out[k] = str(v).strip()
    else:
        serial = _get(col_serial)
        manufacturer = _get(col_manufacturer)
        model = _get(col_device_type_model)
        location_name = _get(col_location_name)
        if not serial:
            raise ValueError("serial is required")
        if not manufacturer:
            raise ValueError("manufacturer is required")
        if not model:
            raise ValueError("device_type (model) is required")
        if not location_name:
            raise ValueError("location is required")
        out["serial"] = serial
        out["device_type__manufacturer__name"] = manufacturer
        out["device_type__model"] = model
        out["location__name"] = location_name
        if col_location_parent_name and _get(col_location_parent_name):
            out["location__parent__name"] = _get(col_location_parent_name)

        if col_name and _get(col_name):
            out["name"] = _get(col_name)

    status_name = _get(col_status_name) if col_status_name else ""
    role_name = _get(col_role_name) if col_role_name else ""
    out.setdefault("status__name", status_name or default_status_name)
    out.setdefault("role__name", role_name or default_role_name)

    return out


def normalized_rows_to_csv_bytes(rows: Iterable[dict[str, str]]) -> bytes:
    """Build a UTF-8 CSV document (with BOM) from flat row dicts."""
    rows = list(rows)
    if not rows:
        return codecs.BOM_UTF8
    fieldnames: list[str] = []
    seen: set[str] = set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        writer.writerow({k: r.get(k, "") for k in fieldnames})
    return codecs.BOM_UTF8 + buf.getvalue().encode("utf-8")


def extract_csv_attachments_from_message(msg: Message, *, max_bytes: int) -> list[tuple[str | None, bytes]]:
    """Return (filename_or_none, raw_bytes) for each CSV part."""
    found: list[tuple[str | None, bytes]] = []
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue
            filename = part.get_filename()
            ctype = part.get_content_type()
            is_csv = ctype == "text/csv" or (filename and filename.lower().endswith(".csv"))
            if not is_csv:
                continue
            raw = part.get_payload(decode=True)
            if raw is None:
                continue
            if len(raw) > max_bytes:
                raise ValueError(f"Attachment exceeds max size ({len(raw)} > {max_bytes})")
            found.append((filename, raw))
    else:
        raw = msg.get_payload(decode=True)
        if raw and len(raw) <= max_bytes:
            found.append((msg.get_filename(), raw))
    return found


def _msg_set(msg_id: bytes | str) -> str:
    """IMAP message sequence set as str."""
    return msg_id.decode("ascii") if isinstance(msg_id, bytes) else str(msg_id)


def _sender_allowed(from_header: str, allowlist: str) -> bool:
    if not allowlist.strip():
        return True
    emails = {e.strip().lower() for e in allowlist.split(",") if e.strip()}
    if not emails:
        return True
    # crude parse: extract addr-spec-ish tokens
    candidates = re.findall(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", from_header or "")
    lowered = {c.lower() for c in candidates}
    return bool(lowered & emails)


class EmailCsvDeviceImportJob(Job):
    """Poll IMAP for unread mail with CSV attachments and create Device records."""

    imap_host = StringVar(description="IMAP server hostname", required=True)
    imap_port = IntegerVar(default=993, description="IMAP port (993 for SSL)")
    imap_use_ssl = BooleanVar(default=True, description="Use IMAP4_SSL (disable for cleartext / custom setups)")
    imap_mailbox = StringVar(default="INBOX", description="Mailbox folder name")
    imap_search_criteria = StringVar(
        default="UNSEEN",
        description='IMAP SEARCH criteria (e.g. "UNSEEN" or \'UNSEEN FROM "sender@example.com"\')',
    )
    imap_username = StringVar(description="IMAP login username", required=True)
    imap_password_secret = ObjectVar(
        model=Secret,
        required=False,
        description=f"IMAP password via Secret provider; if unset, use env {IMAP_PASSWORD_ENV} on the worker",
    )

    sender_allowlist = TextVar(
        required=False,
        description="Optional comma-separated sender e-mail addresses to accept (empty = any)",
    )
    max_messages = IntegerVar(default=25, description="Maximum number of messages to process in one run")
    max_attachment_bytes = IntegerVar(default=5_000_000, description="Maximum CSV attachment size in bytes")

    default_status = ObjectVar(
        model=Status,
        required=True,
        description="Default Status when the CSV row omits status__name",
        query_params={"content_types": "dcim.device"},
    )
    default_role = ObjectVar(
        model=Role,
        required=True,
        description="Default Role when the CSV row omits role__name",
        query_params={"content_types": "dcim.device"},
    )

    col_serial = StringVar(default="serial", description="Vendor CSV column name for device serial")
    col_manufacturer = StringVar(default="manufacturer", description="Vendor CSV column for manufacturer name")
    col_device_type_model = StringVar(
        default="device_type",
        description="Vendor CSV column for device type model string (not the Nautobot UUID)",
    )
    col_location_name = StringVar(
        default="location",
        description="Vendor CSV column for Location name (mapped to location__name)",
    )
    col_location_parent_name = StringVar(
        required=False,
        description="Optional vendor column for parent Location name (location__parent__name)",
    )
    col_status_name = StringVar(
        required=False,
        description="Optional vendor column for Status name (overrides default when present)",
    )
    col_role_name = StringVar(
        required=False,
        description="Optional vendor column for Role name (overrides default when present)",
    )
    col_name = StringVar(required=False, description="Optional vendor column mapped to Device name")

    roll_back_if_error = BooleanVar(
        default=True,
        description="If any row fails validation, roll back the entire import for that CSV attachment",
    )
    skip_existing_serial = BooleanVar(
        default=True,
        description="Skip CSV rows whose serial already exists on a Device",
    )
    mark_seen_on_success = BooleanVar(
        default=True,
        description=r"Set \Seen on the message after the attachment imports successfully",
    )
    move_to_mailbox = StringVar(
        required=False,
        description="If set, COPY the message to this mailbox after success, then delete from the source mailbox",
    )
    dryrun = DryRunVar()

    class Meta:
        name = "Import Devices from e-mail CSV attachments"
        description = (
            "Connect to IMAP, find messages matching the search, read .csv attachments, map columns to Device fields, "
            "and create Devices using the same CSV validation path as the built-in Import Objects job."
        )
        has_sensitive_variables = False
        soft_time_limit = 1800
        time_limit = 2000

    def _resolve_imap_password(self, imap_password_secret):
        if imap_password_secret is not None:
            return imap_password_secret.get_value()
        env_pw = os.getenv(IMAP_PASSWORD_ENV)
        if not env_pw:
            raise RunJobTaskFailed(
                f"Provide an IMAP password Secret or set the {IMAP_PASSWORD_ENV} environment variable for workers."
            )
        return env_pw

    def _connect_imap(self, *, host, port, use_ssl, username, password):
        if use_ssl:
            client = imaplib.IMAP4_SSL(host, port)
        else:
            client = imaplib.IMAP4(host, port)
        try:
            typ, _ = client.login(username, password)
            if typ != "OK":
                raise RunJobTaskFailed(f"IMAP login failed: {typ}")
        except imaplib.IMAP4.error as exc:
            raise RunJobTaskFailed(f"IMAP login error: {exc}") from exc
        return client

    def _perform_operation(self, data, serializer_class, queryset):
        new_objs = []
        validation_failed = False
        for row, entry in enumerate(data, start=1):
            serializer = serializer_class(data=entry, context={"request": None})
            if serializer.is_valid():
                try:
                    with transaction.atomic():
                        new_obj = serializer.save()
                        if not queryset.filter(pk=new_obj.pk).exists():
                            raise AbortTransaction()
                    self.logger.info('Row %d: Created record "%s"', row, new_obj, extra={"object": new_obj})
                    new_objs.append(new_obj)
                except AbortTransaction:
                    self.logger.error(
                        'Row %d: User "%s" does not have permission to create an object with these attributes',
                        row,
                        self.user,
                    )
                    validation_failed = True
            else:
                validation_failed = True
                for field, errs in serializer.errors.items():
                    for err in errs:
                        self.logger.error("Row %d: `%s`: `%s`", row, field, err)
        return new_objs, validation_failed

    def _perform_atomic_operation(self, data, serializer_class, queryset):
        new_objs = []
        with contextlib.suppress(AbortTransaction):
            with transaction.atomic():
                new_objs, validation_failed = self._perform_operation(data, serializer_class, queryset)
                if validation_failed:
                    raise AbortTransaction()
                return new_objs, validation_failed
        self.logger.warning("Rolling back all %s records from this attachment.", len(new_objs))
        return [], True

    def _import_csv_bytes(self, csv_bytes: bytes, *, roll_back_if_error: bool, dryrun: bool, skip_existing_serial: bool):
        if not self.user.has_perm("dcim.add_device"):
            raise PermissionDenied("User does not have permission to create Device records")

        serializer_class = get_serializer_for_model(Device)
        queryset = Device.objects.restrict(self.user, "add")
        stream = BytesIO(csv_bytes)
        try:
            data = NautobotCSVParser().parse(
                stream=stream,
                parser_context={"request": None, "serializer_class": serializer_class},
            )
        except Exception as exc:
            self.logger.error("CSV parse error: %s", exc)
            raise RunJobTaskFailed(f"CSV parse failed: {exc}") from exc

        if skip_existing_serial:
            filtered = []
            for entry in data:
                serial = None
                if isinstance(entry, dict):
                    serial = (entry.get("serial") or "").strip()
                if serial and Device.objects.restrict(self.user, "view").filter(serial=serial).exists():
                    self.logger.info("Skipping row with existing serial %s", serial)
                    continue
                filtered.append(entry)
            data = filtered

        if dryrun:
            self.logger.info("Dry-run: validated parse produced %d row(s); skipping database writes.", len(data))
            return []

        if roll_back_if_error:
            new_objs, validation_failed = self._perform_atomic_operation(data, serializer_class, queryset)
        else:
            new_objs, validation_failed = self._perform_operation(data, serializer_class, queryset)

        if validation_failed:
            if roll_back_if_error:
                raise RunJobTaskFailed("CSV import not successful; rolled back this attachment")
            raise RunJobTaskFailed("CSV import not fully successful for this attachment; see logs")

        return new_objs

    def run(  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        self,
        *,
        imap_host,
        imap_port,
        imap_use_ssl,
        imap_mailbox,
        imap_search_criteria,
        imap_username,
        imap_password_secret=None,
        sender_allowlist="",
        max_messages=25,
        max_attachment_bytes=5_000_000,
        default_status,
        default_role,
        col_serial="serial",
        col_manufacturer="manufacturer",
        col_device_type_model="device_type",
        col_location_name="location",
        col_location_parent_name="",
        col_status_name="",
        col_role_name="",
        col_name="",
        roll_back_if_error=True,
        skip_existing_serial=True,
        mark_seen_on_success=True,
        move_to_mailbox="",
        dryrun=False,
    ):
        password = self._resolve_imap_password(imap_password_secret)
        client = self._connect_imap(
            host=imap_host,
            port=imap_port,
            use_ssl=imap_use_ssl,
            username=imap_username,
            password=password,
        )

        try:
            typ, _ = client.select(imap_mailbox)
            if typ != "OK":
                raise RunJobTaskFailed(f"Unable to select mailbox {imap_mailbox!r}: {typ}")

            typ, data = client.uid("search", None, imap_search_criteria)
            if typ != "OK":
                raise RunJobTaskFailed(f"IMAP UID SEARCH failed: {typ}")

            raw_ids = data[0] if data else None
            if not raw_ids:
                msg_uids = []
            else:
                msg_uids = raw_ids.split()
            if not msg_uids:
                self.logger.info("No messages matched search %s", imap_search_criteria)
                return

            if len(msg_uids) > max_messages:
                self.logger.warning("Truncating to max_messages=%s (had %s)", max_messages, len(msg_uids))
                msg_uids = msg_uids[:max_messages]

            default_status_name = default_status.name
            default_role_name = default_role.name
            move_mb = str(move_to_mailbox).strip() if move_to_mailbox else ""

            for uid in msg_uids:
                uid_str = _msg_set(uid)
                typ, msg_data = client.uid("fetch", uid_str, "(RFC822)")
                if typ != "OK" or not msg_data or not isinstance(msg_data[0], tuple):
                    self.logger.error("FETCH failed for message UID %s", uid_str)
                    continue
                raw_msg = msg_data[0][1]
                msg = email.message_from_bytes(raw_msg)
                from_header = msg.get("From", "")
                if not _sender_allowed(from_header, sender_allowlist or ""):
                    self.logger.info("Skipping message UID %s: sender not in allowlist", uid_str)
                    continue

                attachments = extract_csv_attachments_from_message(msg, max_bytes=max_attachment_bytes)
                if not attachments:
                    self.logger.info("No CSV attachments on message UID %s", uid_str)
                    continue

                message_import_completed = False
                for filename, raw_csv in attachments:
                    self.logger.info(
                        "Processing CSV attachment %s from message UID %s", filename or "(no filename)", uid_str
                    )
                    text = decode_csv_attachment_bytes(raw_csv)
                    reader = csv.DictReader(io.StringIO(text))
                    norm_rows: list[dict[str, str]] = []
                    for csv_row in reader:
                        try:
                            norm_rows.append(
                                vendor_row_to_nautobot_flat_dict(
                                    {k: (v or "") for k, v in csv_row.items() if k},
                                    default_status_name=default_status_name,
                                    default_role_name=default_role_name,
                                    col_serial=col_serial,
                                    col_manufacturer=col_manufacturer,
                                    col_device_type_model=col_device_type_model,
                                    col_location_name=col_location_name,
                                    col_location_parent_name=col_location_parent_name or None,
                                    col_status_name=col_status_name or None,
                                    col_role_name=col_role_name or None,
                                    col_name=col_name or None,
                                )
                            )
                        except ValueError as exc:
                            self.logger.error("Row skipped: %s (row=%r)", exc, csv_row)
                            raise RunJobTaskFailed(f"Invalid CSV row: {exc}") from exc

                    if not norm_rows:
                        self.logger.info("Attachment %s on UID %s contained no data rows; skipping", filename, uid_str)
                        continue

                    csv_out = normalized_rows_to_csv_bytes(norm_rows)
                    try:
                        self._import_csv_bytes(
                            csv_out,
                            roll_back_if_error=roll_back_if_error,
                            dryrun=dryrun,
                            skip_existing_serial=skip_existing_serial,
                        )
                    except RunJobTaskFailed:
                        self.logger.error("Import failed for attachment %s on message UID %s", filename, uid_str)
                        raise

                    message_import_completed = True

                if message_import_completed and mark_seen_on_success and not dryrun:
                    client.uid("store", uid_str, "+FLAGS", r"\Seen")

                if message_import_completed and move_mb and not dryrun:
                    typ, _ = client.uid("copy", uid_str, move_mb)
                    if typ != "OK":
                        raise RunJobTaskFailed(f"IMAP UID COPY to {move_mb!r} failed: {typ}")
                    client.uid("store", uid_str, "+FLAGS", r"\Deleted")
                    self.logger.info("Moved message UID %s to mailbox %s (copy+delete source)", uid_str, move_mb)

            if move_mb and not dryrun:
                client.expunge()

        finally:
            try:
                client.logout()
            except imaplib.IMAP4.error:
                pass


register_jobs(EmailCsvDeviceImportJob)
