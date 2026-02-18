import logging
import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

load_dotenv()

# Determine log directory: <project_root>/src/logs
LOG_DIR = Path(__file__).parents[1] / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Global logging configuration:
# - INFO and above
# - log to file AND console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "upload.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Reduce noise from Azure SDK loggers
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.storage").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)


class BlobUploader:
    """Upload all CSV files from a local folder to a single Azure Blob container."""

    def __init__(self, container_name: str):
        """
        Initialize BlobUploader:
        - Read connection string from .env
        - Create BlobServiceClient and ContainerClient
        """
        self.container_name = container_name

        connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not connect_str:
            # Fail fast if connection string is missing
            logger.critical("AZURE_STORAGE_CONNECTION_STRING is not set in .env")
            raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING environment variable")

        self.client = BlobServiceClient.from_connection_string(connect_str)
        self.container_client = self.client.get_container_client(container_name)

        logger.info(f"[INIT] BlobUploader initialized -> Container: {container_name}")

    def scan_folder(self, folder_path: str) -> list[Path]:
        """
        Recursively scan a folder for all CSV files.
        Returns a list of Path objects.
        """
        folder = Path(folder_path)

        if not folder.exists():
            logger.error(f"[ERROR] Folder does not exist: {folder}")
            return []

        csv_files = list(folder.rglob("*.csv"))
        logger.info(f"[SCAN] Found {len(csv_files)} CSV file(s) in: {folder}")
        return csv_files

    def upload_file(self, file_path: Path, base_folder: Path) -> bool:
        """
        Upload a single CSV file to the container.
        Blob name is derived from the relative path to base_folder.

        TODO (future):
        - Replace folder name (e.g. 'actual') with current date (YYYY-MM-DD)
          for incremental loads triggered by GitHub Actions.
        """
        blob_name = str(file_path.relative_to(base_folder))

        try:
            with open(file_path, "rb") as f:
                # overwrite=True to avoid errors if blob already exists
                self.container_client.upload_blob(blob_name, f, overwrite=True)
            logger.info(f"[OK] Uploaded: {blob_name}")
            return True
        except Exception as e:
            logger.error(f"[FAIL] Upload failed: {blob_name} -> {e}")
            return False

    def run(self, folder_path: str) -> None:
        """
        Orchestrate a full upload run:
        - Scan folder for CSVs
        - Upload each file
        - Log summary (success/fail counts)
        """
        base_folder = Path(folder_path)
        csv_files = self.scan_folder(folder_path)

        total = len(csv_files)
        success = 0
        failed = 0

        logger.info(f"[RUN] Start upload run -> {total} file(s) from: {base_folder}")

        for file_path in csv_files:
            ok = self.upload_file(file_path, base_folder)
            if ok:
                success += 1
            else:
                failed += 1

        logger.info("[SUMMARY] UPLOAD SUMMARY")
        logger.info(f"[SUMMARY] Successful: {success}/{total}")
        logger.info(f"[SUMMARY] Failed:     {failed}/{total}")


if __name__ == "__main__":
    # BASE_DIR = project root (two levels above this file: src/etl/ -> src/ -> <root>)
    BASE_DIR = Path(__file__).parents[2]
    # Current bronze folder for Phase 1 (later: switch to date-based folders)
    bronze_folder = BASE_DIR / "data" / "bronze" / "actual"

    logger.info(f"[START] Starting BlobUploader for folder: {bronze_folder}")

    uploader = BlobUploader(container_name="bronze")
    uploader.run(str(bronze_folder))
    logger.info("[END] BlobUploader run completed")