from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from urllib.parse import quote, urlparse

import httpx
from tusclient import client as tus_client

from app.config import get_settings


logger = logging.getLogger(__name__)


class SupabaseStorage:
    def __init__(self, bucket: str | None = None):
        self.settings = get_settings()
        self.bucket = bucket or self.settings.raw_bucket
        self.base_url = self.settings.supabase_url.rstrip("/")
        parsed = urlparse(self.base_url)
        hostname = parsed.hostname or ""
        if hostname.endswith(".supabase.co") and ".storage.supabase.co" not in hostname:
            project_ref = hostname.removesuffix(".supabase.co")
            self.resumable_base_url = f"{parsed.scheme}://{project_ref}.storage.supabase.co"
        else:
            self.resumable_base_url = self.base_url
        self.headers = {
            "Authorization": f"Bearer {self.settings.supabase_service_role_key}",
            "apikey": self.settings.supabase_service_role_key,
            "x-upsert": "true",
        }

    def ensure_bucket(self) -> None:
        response = httpx.post(
            f"{self.base_url}/storage/v1/bucket",
            headers={**self.headers, "Content-Type": "application/json"},
            json={"id": self.bucket, "name": self.bucket, "public": False},
            timeout=30,
        )
        if response.status_code in {200, 201, 409}:
            return
        if "already exists" in response.text.lower() or "duplicate" in response.text.lower():
            return
        response.raise_for_status()

    def upload_resumable(self, file_path: Path, object_path: str, content_type: str) -> None:
        self.ensure_bucket()
        endpoint = f"{self.resumable_base_url}/storage/v1/upload/resumable"
        client = tus_client.TusClient(endpoint, headers=self.headers)
        uploader = client.uploader(
            file_path=str(file_path),
            chunk_size=self.settings.storage_upload_chunk_mb * 1024 * 1024,
            retries=8,
            retry_delay=5,
            metadata={
                "bucketName": self.bucket,
                "objectName": object_path,
                "contentType": content_type,
                "cacheControl": "3600",
            },
        )
        uploader.upload()

    def upload_file(self, file_path: Path, object_path: str, content_type: str) -> tuple[int, str]:
        """Upload a raw segment and return size/checksum.

        Small segments use the standard object endpoint. Large segments use TUS.
        Object paths are deterministic and x-upsert makes retries idempotent.
        """
        self.ensure_bucket()
        size = file_path.stat().st_size
        digest = hashlib.sha256()
        with file_path.open("rb") as checksum_handle:
            for chunk in iter(lambda: checksum_handle.read(1024 * 1024), b""):
                digest.update(chunk)
        checksum = digest.hexdigest()
        if size > self.settings.storage_upload_chunk_mb * 1024 * 1024:
            self.upload_resumable(file_path, object_path, content_type)
            return size, checksum
        encoded = quote(object_path, safe="/")
        with file_path.open("rb") as handle:
            response = httpx.post(
                f"{self.base_url}/storage/v1/object/{self.bucket}/{encoded}",
                headers={**self.headers, "Content-Type": content_type},
                content=handle.read(),
                timeout=120,
            )
        if response.status_code not in {200, 201}:
            response.raise_for_status()
        return size, checksum
