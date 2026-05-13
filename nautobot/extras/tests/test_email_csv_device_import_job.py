"""Tests for JOBS_ROOT email CSV device import job."""

import logging
import os
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from unittest import mock

from django.contrib.auth import get_user_model

from django.test import SimpleTestCase

from nautobot.apps.testing import TransactionTestCase
from nautobot.dcim.factory import DeviceFactory
from nautobot.dcim.models import Device
from nautobot.extras.jobs import RunJobTaskFailed, get_job
from nautobot.extras.test_jobs.email_csv_device_import import (
    EmailCsvDeviceImportJob,
    IMAP_PASSWORD_ENV,
    _sender_allowed,
    decode_csv_attachment_bytes,
    normalized_rows_to_csv_bytes,
    vendor_row_to_nautobot_flat_dict,
)

User = get_user_model()


def _csv_mime_bytes(csv_text: str, *, sender: str = "vendor@example.com") -> bytes:
    root = MIMEMultipart()
    root["From"] = sender
    part = MIMEBase("text", "csv")
    part.set_payload(csv_text.encode("utf-8"))
    part.add_header("Content-Disposition", "attachment", filename="devices.csv")
    root.attach(part)
    return root.as_bytes()


class EmailCsvDeviceImportHelpersTest(SimpleTestCase):
    """Unit tests for pure helpers (no IMAP)."""

    def test_vendor_row_mapping(self):
        row = {
            "serial": "ABC123",
            "manufacturer": "Cisco",
            "device_type": "C9300-48P",
            "location": "Test Site",
        }
        out = vendor_row_to_nautobot_flat_dict(
            row,
            default_status_name="Active",
            default_role_name="Access",
            col_serial="serial",
            col_manufacturer="manufacturer",
            col_device_type_model="device_type",
            col_location_name="location",
            col_location_parent_name=None,
            col_status_name=None,
            col_role_name=None,
            col_name=None,
        )
        self.assertEqual(out["serial"], "ABC123")
        self.assertEqual(out["device_type__manufacturer__name"], "Cisco")
        self.assertEqual(out["device_type__model"], "C9300-48P")
        self.assertEqual(out["location__name"], "Test Site")
        self.assertEqual(out["status__name"], "Active")
        self.assertEqual(out["role__name"], "Access")

    def test_pre_normalized_row_fills_defaults(self):
        row = {
            "serial": "X1",
            "device_type__manufacturer__name": "Aruba",
            "device_type__model": "6300M",
            "location__name": "HQ",
        }
        out = vendor_row_to_nautobot_flat_dict(
            row,
            default_status_name="Planned",
            default_role_name="Core",
            col_serial="serial",
            col_manufacturer="manufacturer",
            col_device_type_model="device_type",
            col_location_name="location",
            col_location_parent_name=None,
            col_status_name=None,
            col_role_name=None,
            col_name=None,
        )
        self.assertEqual(out["status__name"], "Planned")
        self.assertEqual(out["role__name"], "Core")

    def test_normalized_rows_to_csv_bytes_round_trip_header(self):
        rows = [
            {
                "serial": "1",
                "device_type__manufacturer__name": "Cisco",
                "device_type__model": "ISR4331",
                "location__name": "NYC",
                "status__name": "Active",
                "role__name": "Router",
            }
        ]
        raw = normalized_rows_to_csv_bytes(rows)
        text = decode_csv_attachment_bytes(raw)
        self.assertIn("device_type__manufacturer__name", text)
        self.assertIn("ISR4331", text)

    def test_sender_allowlist(self):
        self.assertTrue(_sender_allowed("Vendor <vendor@example.com>", ""))
        self.assertTrue(_sender_allowed("Vendor <vendor@example.com>", "vendor@example.com"))
        self.assertFalse(_sender_allowed("Vendor <other@example.com>", "vendor@example.com"))


class EmailCsvDeviceImportJobRunTest(TransactionTestCase):
    """Job.run() with mocked IMAP."""

    databases = ("default", "job_logs")

    def setUp(self):
        super().setUp()
        self.template = DeviceFactory()
        self.user = User.objects.create_user(
            username="email_csv_job_tester",
            password="password",
            is_superuser=True,
        )

    def _make_job(self):
        job = EmailCsvDeviceImportJob()
        job.user = self.user
        job.logger = logging.getLogger("test_email_csv_import")
        return job

    def _mock_imap_client(self, mime_bytes: bytes):
        client = mock.MagicMock()

        def uid_side_effect(command, *args):
            if command.upper() == "SEARCH":
                return ("OK", [b"42"])
            if command.upper() == "FETCH":
                return ("OK", [(None, mime_bytes)])
            if command.upper() == "STORE":
                return ("OK", [b"42"])
            if command.upper() == "COPY":
                return ("OK", [b""])
            return ("BAD", [b"unknown"])

        client.select.return_value = ("OK", [b"1"])
        client.uid.side_effect = uid_side_effect
        client.expunge.return_value = ("OK", [b""])
        client.logout.return_value = ("BYE", [b""])
        return client

    @mock.patch.dict("os.environ", {"NAUTOBOT_EMAIL_CSV_IMPORT_IMAP_PASSWORD": "secret"}, clear=False)
    @mock.patch.object(EmailCsvDeviceImportJob, "_connect_imap")
    def test_import_creates_device_from_attachment(self, mock_connect):
        serial = f"email-csv-import-{self.template.pk}"
        csv_body = (
            f"serial,manufacturer,device_type,location\n"
            f"{serial},{self.template.device_type.manufacturer.name},"
            f"{self.template.device_type.model},{self.template.location.name}\n"
        )
        mime = _csv_mime_bytes(csv_body)
        mock_connect.return_value = self._mock_imap_client(mime)

        job = self._make_job()
        job.run(
            imap_host="imap.example.com",
            imap_port=993,
            imap_use_ssl=True,
            imap_mailbox="INBOX",
            imap_search_criteria="UNSEEN",
            imap_username="user@example.com",
            imap_password_secret=None,
            sender_allowlist="",
            max_messages=25,
            max_attachment_bytes=5_000_000,
            default_status=self.template.status,
            default_role=self.template.role,
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
        )

        created = Device.objects.get(serial=serial)
        self.assertEqual(created.location_id, self.template.location_id)
        self.assertEqual(created.device_type_id, self.template.device_type_id)
        self.assertEqual(created.status_id, self.template.status_id)
        self.assertEqual(created.role_id, self.template.role_id)

    def test_missing_password_raises(self):
        job = self._make_job()
        with mock.patch.dict(os.environ, {IMAP_PASSWORD_ENV: None}, clear=False):
            with self.assertRaises(RunJobTaskFailed):
                job._resolve_imap_password(None)

    def test_job_is_registered(self):
        from nautobot.extras.jobs import get_jobs

        get_jobs(reload=True)
        cls = get_job("email_csv_device_import.EmailCsvDeviceImportJob")
        self.assertIs(cls, EmailCsvDeviceImportJob)
