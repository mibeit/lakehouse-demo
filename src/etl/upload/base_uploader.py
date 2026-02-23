"""
Shared base class for Azure Blob uploads.

Supports both Bronze (CSV) and Silver/Gold (Parquet) uploads
by parameterizing the file extension.
"""

import logging
import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# Shared log directory (same as transformer logs)
LOG_DIR = Path(__file__).parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)


def _get_upload_logger(name: str, log_file: str) -> logging.Logger:
    """
    Create a dedicated logger for upload scripts.

    Uses the same pattern as get_logger() in base_transformer.py
    but avoids logging.basicConfig() which pollutes the root logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s")

    fh = logging.FileHandler(LOG_DIR / log_file, mode="w")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    return logger


# Reduce noise from Azure SDK loggers
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.storage").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)


class BlobUploader:
    """
    Upload files from a local folder to an Azure Blob Storage container.

    Args:
        container_name: Azure Blob container name
        file_glob:      Glob pattern for scanning (e.g. "*.csv", "*.parquet")
        log_file:       Log filename (e.g. "upload_bronze.log")
    """

    def __init__(self, container_name: str, file_glob: str, log_file: str):
        self.container_name = container_name
        self.file_glob = file_glob
        self.logger = _get_upload_logger(f"upload.{container_name}", log_file)

        connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not connect_str:
            self.logger.critical("AZURE_STORAGE_CONNECTION_STRING is not set in .env")
            raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING environment variable")

        self.client = BlobServiceClient.from_connection_string(connect_str)
        self.container_client = self.client.get_container_client(container_name)

        self.logger.info(f"[INIT] BlobUploader initialized -> Container: {container_name}")

    def scan_folder(self, folder_path: str) -> list[Path]:
        """Recursively scan *folder_path* for files matching *self.file_glob*."""
        folder = Path(folder_path)

        if not folder.exists():
            self.logger.error(f"[ERROR] Folder does not exist: {folder}")
            return []

        files = list(folder.rglob(self.file_glob))
        self.logger.info(f"[SCAN] Found {len(files)} {self.file_glob} file(s) in: {folder}")
        return files

    def upload_file(self, file_path: Path, base_folder: Path) -> bool:
        """Upload a single file to the container.

        Blob name is derived from the relative path to *base_folder*.

        TODO (future):
        - Replace folder name (e.g. 'actual') with current date (YYYY-MM-DD)
          for incremental loads triggered by GitHub Actions.
        """
        blob_name = str(file_path.relative_to(base_folder))

        try:
            with open(file_path, "rb") as f:
                self.container_client.upload_blob(blob_name, f, overwrite=True)
            self.logger.info(f"[OK] Uploaded: {blob_name}")
            return True
        except Exception as e:
            self.logger.error(f"[FAIL] Upload failed: {blob_name} -> {e}")
            return False

    def run(self, folder_path: str) -> None:
        """Scan folder and upload all matching files."""
        base_folder = Path(folder_path)
        files = self.scan_folder(folder_path)

        total = len(files)
        success = 0
        failed = 0

        self.logger.info(f"[RUN] Start upload run -> {total} file(s) from: {base_folder}")

        for file_path in files:
            if self.upload_file(file_path, base_folder):
                success += 1
            else:
                failed += 1

        self.logger.info("[SUMMARY] UPLOAD SUMMARY")
        self.logger.info(f"[SUMMARY] Successful: {success}/{total}")
        self.logger.info(f"[SUMMARY] Failed:     {failed}/{total}")
