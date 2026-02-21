"""Upload Bronze CSV files to Azure Blob Storage."""

from pathlib import Path
from src.etl.upload.base_uploader import BlobUploader


if __name__ == "__main__":
    BASE_DIR = Path(__file__).parents[3]
    bronze_folder = BASE_DIR / "data" / "bronze" / "actual"

    uploader = BlobUploader(
        container_name="bronze",
        file_glob="*.csv",
        log_file="upload_bronze.log",
    )
    uploader.logger.info(f"[START] Starting BlobUploader for folder: {bronze_folder}")
    uploader.run(str(bronze_folder))
    uploader.logger.info("[END] BlobUploader run completed")