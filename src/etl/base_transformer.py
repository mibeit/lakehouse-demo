import logging
import pandas as pd
from pathlib import Path
from abc import ABC, abstractmethod


# BASE_DIR = project root (two levels above this file: src/etl/ -> src/ -> <root>)
BASE_DIR = Path(__file__).parents[2]

# Log directory
LOG_DIR = BASE_DIR / "src" / "logs"
LOG_DIR.mkdir(exist_ok=True)


def get_logger(log_file: str) -> logging.Logger:
    """
    Create a logger that writes to a specific log file AND console.
    Each Transformer gets its own log file.
    """
    logger = logging.getLogger(log_file)
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers if logger already exists
    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s")

    # File handler
    fh = logging.FileHandler(LOG_DIR / log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # Console handler
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    return logger


class BaseTransformer(ABC):
    """
    Abstract base class for all Bronze -> Silver transformers.

    Subclasses must implement:
    - transform(df): table-specific transformation logic

    Subclasses inherit:
    - load_bronze(): load CSV from Bronze layer
    - _drop_empty_columns(): drop fully empty columns
    - save_silver(): save Parquet to Silver layer
    - run(): orchestrate load -> transform -> save
    """

    def __init__(self, bronze_path: Path, silver_path: Path, log_file: str):
        """
        Args:
            bronze_path: Full path to Bronze CSV file
            silver_path: Directory for Silver Parquet output
            log_file:    Log filename (e.g. "transform_orders.log")
        """
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.logger = get_logger(log_file)

        # Ensure Silver output directory exists
        self.silver_path.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"[INIT] {self.__class__.__name__} initialized")
        self.logger.info(f"[INIT] Bronze source: {self.bronze_path}")
        self.logger.info(f"[INIT] Silver target: {self.silver_path}")


    # ------------------------------------------------------------------ #
    #  Shared methods (used by all transformers)                         #
    # ------------------------------------------------------------------ #

    def load_bronze(self) -> pd.DataFrame:
        """Load raw CSV from Bronze layer into a Pandas DataFrame."""
        if not self.bronze_path.exists():
            self.logger.error(f"[LOAD] Bronze file not found: {self.bronze_path}")
            return pd.DataFrame()

        df = pd.read_csv(self.bronze_path)

        self.logger.info(f"[LOAD] Loaded: {self.bronze_path.name}")
        self.logger.info(f"[LOAD] Shape: {df.shape[0]} rows x {df.shape[1]} columns")

        return df


    def _drop_empty_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop columns where ALL values are NaN."""
        before = df.shape[1]
        df = df.dropna(axis=1, how="all")
        dropped = before - df.shape[1]

        self.logger.info(f"[TRANSFORM] Dropped {dropped} empty column(s) | Remaining: {df.shape[1]}")

        return df


    def save_silver(self, df: pd.DataFrame, filename: str) -> None:
        """
        Save transformed DataFrame as Parquet to Silver layer.

        Args:
            df:       Transformed DataFrame
            filename: Output filename (e.g. "orders.parquet")
        """
        if df.empty:
            self.logger.warning("[SAVE] DataFrame is empty - skipping save")
            return

        output_path = self.silver_path / filename
        df.to_parquet(output_path, index=False)

        self.logger.info(f"[SAVE] Saved: {output_path}")
        self.logger.info(f"[SAVE] Size: {output_path.stat().st_size / 1024:.1f} KB")


    # ------------------------------------------------------------------ #
    #  Abstract method (must be implemented by each subclass)            #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Table-specific transformation logic.
        Must be implemented by every subclass.
        """
        pass


    # ------------------------------------------------------------------ #
    #  Orchestration                                                     #
    # ------------------------------------------------------------------ #

    def run(self) -> None:
        """Orchestrate full Bronze -> Silver pipeline: load -> transform -> save."""
        self.logger.info(f"[RUN] Starting Bronze -> Silver: {self.bronze_path.name}")

        df = self.load_bronze()

        if df.empty:
            self.logger.error("[RUN] Aborting - empty DataFrame after load")
            return

        df = self.transform(df)
        self.save_silver(df, self._output_filename)

        self.logger.info(f"[RUN] Complete: {self._output_filename}")
