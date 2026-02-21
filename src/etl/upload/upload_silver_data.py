"""Upload Silver Parquet files to Azure Blob Storage."""

from pathlib import Path
from src.etl.upload.base_uploader import BlobUploader


if __name__ == "__main__":
    BASE_DIR = Path(__file__).parents[3]
    silver_folder = BASE_DIR / "data" / "silver"

    uploader = BlobUploader(
        container_name="silver",
        file_glob="*.parquet",
        log_file="upload_silver.log",
    )
    uploader.logger.info(f"[START] Starting BlobUploader for folder: {silver_folder}")
    uploader.run(str(silver_folder))
    uploader.logger.info("[END] BlobUploader run completed")
