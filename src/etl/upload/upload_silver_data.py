import logging
import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

LOG_DIR = Path(__file__).parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "upload_silver.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.storage").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)


class BlobUploader:
    """Upload all Parquet files from a local folder to a single Azure Blob container."""

    def __init__(self, container_name: str):
        self.container_name = container_name

        connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not connect_str:
            logger.critical("AZURE_STORAGE_CONNECTION_STRING is not set in .env")
            raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING environment variable")

        self.client = BlobServiceClient.from_connection_string(connect_str)
        self.container_client = self.client.get_container_client(container_name)

        logger.info(f"[INIT] BlobUploader initialized -> Container: {container_name}")

    def scan_folder(self, folder_path: str) -> list[Path]:
        """Recursively scan a folder for all Parquet files."""
        folder = Path(folder_path)

        if not folder.exists():
            logger.error(f"[ERROR] Folder does not exist: {folder}")
            return []

        parquet_files = list(folder.rglob("*.parquet"))
        logger.info(f"[SCAN] Found {len(parquet_files)} Parquet file(s) in: {folder}")
        return parquet_files

    def upload_file(self, file_path: Path, base_folder: Path) -> bool:
        """Upload a single Parquet file to the container."""
        blob_name = str(file_path.relative_to(base_folder))

        try:
            with open(file_path, "rb") as f:
                self.container_client.upload_blob(blob_name, f, overwrite=True)
            logger.info(f"[OK] Uploaded: {blob_name}")
            return True
        except Exception as e:
            logger.error(f"[FAIL] Upload failed: {blob_name} -> {e}")
            return False

    def run(self, folder_path: str) -> None:
        """Scan folder and upload all Parquet files."""
        base_folder = Path(folder_path)
        parquet_files = self.scan_folder(folder_path)

        total = len(parquet_files)
        success = 0
        failed = 0

        logger.info(f"[RUN] Start upload run -> {total} file(s) from: {base_folder}")

        for file_path in parquet_files:
            ok = self.upload_file(file_path, base_folder)
            if ok:
                success += 1
            else:
                failed += 1

        logger.info("[SUMMARY] UPLOAD SUMMARY")
        logger.info(f"[SUMMARY] Successful: {success}/{total}")
        logger.info(f"[SUMMARY] Failed:     {failed}/{total}")


if __name__ == "__main__":
    BASE_DIR = Path(__file__).parents[3]
    silver_folder = BASE_DIR / "data" / "silver"

    logger.info(f"[START] Starting BlobUploader for folder: {silver_folder}")

    uploader = BlobUploader(container_name="silver")
    uploader.run(str(silver_folder))
    logger.info("[END] BlobUploader run completed")
