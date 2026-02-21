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
    fh = logging.FileHandler(LOG_DIR / log_file, mode="w")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # Console handler
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    return logger


class BaseTransformer(ABC):
    """
    Abstract base class for all transformers (Silver and Gold).

    Provides shared utilities:
    - Logging setup
    - File I/O (load_bronze, load_silver, save_silver, save_gold)
    - Data cleaning helpers (_drop_empty_columns, _to_datetime, _validate_nulls)

    Do not subclass directly — use SilverTransformer or GoldTransformer.
    """

    _output_filename: str  # Must be set by every concrete subclass

    def __init__(
        self,
        log_file: str,
        bronze_path: Path = None,
        silver_path: Path = None,
        gold_path: Path = None,
    ):
        """
        Args:
            log_file:    Log filename (e.g. "transform_orders.log")
            bronze_path: Full path to Bronze CSV file (Silver transformers)
            silver_path: Directory for Silver Parquet output
            gold_path:   Directory for Gold Parquet output (Gold transformers)
        """
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.gold_path = gold_path
        self.logger = get_logger(log_file)

        # Ensure output directories exist
        if self.silver_path:
            self.silver_path.mkdir(parents=True, exist_ok=True)
        if self.gold_path:
            self.gold_path.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"[INIT] {self.__class__.__name__} initialized")
        if self.bronze_path:
            self.logger.info(f"[INIT] Bronze source: {self.bronze_path}")
        if self.silver_path:
            self.logger.info(f"[INIT] Silver target: {self.silver_path}")
        if self.gold_path:
            self.logger.info(f"[INIT] Gold target:   {self.gold_path}")


    # ------------------------------------------------------------------ #
    #  Shared methods (used by all transformers)                         #
    # ------------------------------------------------------------------ #

    def _check_output_filename(self) -> None:
        """Raise early if _output_filename was not set by the subclass."""
        if not getattr(self, "_output_filename", None):
            raise NotImplementedError(
                f"{self.__class__.__name__} must set _output_filename "
                "(as class attribute or in __init__)"
            )

    def load_bronze(self) -> pd.DataFrame:
        """Load raw CSV from Bronze layer into a Pandas DataFrame."""
        if not self.bronze_path or not self.bronze_path.exists():
            self.logger.error(f"[LOAD] Bronze file not found: {self.bronze_path}")
            return pd.DataFrame()

        df = pd.read_csv(self.bronze_path)

        self.logger.info(f"[LOAD] Loaded: {self.bronze_path.name}")
        self.logger.info(f"[LOAD] Shape: {df.shape[0]} rows x {df.shape[1]} columns")

        return df
    
    
    def load_silver(self, silver_path: Path) -> pd.DataFrame:
        """Load cleaned Parquet from Silver layer into a Pandas DataFrame."""
        if not silver_path.exists():
            self.logger.error(f"[LOAD] Silver file not found: {silver_path}")
            return pd.DataFrame()

        df = pd.read_parquet(silver_path)

        self.logger.info(f"[LOAD] Loaded: {silver_path.name}")
        self.logger.info(f"[LOAD] Shape: {df.shape[0]} rows x {df.shape[1]} columns")

        return df


    def _drop_empty_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop columns where ALL values are NaN."""
        before = df.shape[1]
        df = df.dropna(axis=1, how="all")
        dropped = before - df.shape[1]

        self.logger.info(f"[TRANSFORM] Dropped {dropped} empty column(s) | Remaining: {df.shape[1]}")

        return df
    
    def _to_datetime(self, series: pd.Series) -> pd.Series:
        """Cast to datetime64[ns] consistently across all transformers."""
        return pd.to_datetime(series, errors="coerce").astype("datetime64[ns]")

    def _validate_nulls(
        self,
        df: pd.DataFrame,
        expected_nulls: dict = None,
        required_columns: list = None,
    ) -> pd.DataFrame:
        """
        Generic null validation with structured logging.

        Args:
            df:               DataFrame to validate
            expected_nulls:   {column: reason} — NaN values are expected here
            required_columns: columns that must not contain any NaN
        Returns:
            The original DataFrame (unchanged).
        """
        if expected_nulls:
            for col, reason in expected_nulls.items():
                null_count = df[col].isna().sum()
                self.logger.info(
                    f"[NULLS] {col}: {null_count} null(s) -> expected ({reason})"
                )

        if required_columns:
            for col in required_columns:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    self.logger.warning(
                        f"[NULLS] Unexpected nulls in {col}: {null_count}"
                    )
                else:
                    self.logger.info(f"[NULLS] {col}: OK (0 nulls)")

        return df

    # ------------------------------------------------------------------ #
    #  File output                                                       #
    # ------------------------------------------------------------------ #

    def _save_parquet(
        self, df: pd.DataFrame, output_dir: Path, filename: str, layer: str
    ) -> None:
        """Generic save: write DataFrame as Parquet to *output_dir*/*filename*."""
        if df.empty:
            self.logger.warning(f"[SAVE] DataFrame is empty — skipping {layer} save")
            return

        output_path = output_dir / filename
        df.to_parquet(output_path, index=False)

        self.logger.info(f"[SAVE] Saved to {layer}: {output_path}")
        self.logger.info(f"[SAVE] Shape: {df.shape[0]} rows x {df.shape[1]} columns")
        self.logger.info(f"[SAVE] Size: {output_path.stat().st_size / 1024:.1f} KB")

    def save_silver(self, df: pd.DataFrame, filename: str) -> None:
        """Save transformed DataFrame as Parquet to Silver layer."""
        self._save_parquet(df, self.silver_path, filename, "Silver")

    def save_gold(self, df: pd.DataFrame, filename: str) -> None:
        """Save transformed DataFrame as Parquet to Gold layer."""
        self._save_parquet(df, self.gold_path, filename, "Gold")


    # ------------------------------------------------------------------ #
    #  Abstract interface                                                #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def run(self) -> None:
        """Execute the full transformation pipeline."""
        pass


# ====================================================================== #
#  Silver Layer                                                          #
# ====================================================================== #


class SilverTransformer(BaseTransformer):
    """
    Base class for Bronze -> Silver transformers.

    Uses the Template Method pattern.  Subclasses override **hooks**:
      _rename_columns(df)   — column renaming
      _cast_dtypes(df)      — type casting
      _handle_nulls(df)     — null handling / validation

    The default transform() calls the hooks in order:
      drop_empty -> rename -> cast -> handle_nulls

    Override transform() only if you need a fundamentally different pipeline.
    """

    def __init__(self, bronze_path: Path, silver_path: Path, log_file: str):
        super().__init__(
            log_file=log_file,
            bronze_path=bronze_path,
            silver_path=silver_path,
        )

    # ------------------------------------------------------------------ #
    #  Hooks — override in subclasses                                    #
    # ------------------------------------------------------------------ #

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to rename columns. Default: no-op."""
        return df

    def _cast_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to cast data types. Default: no-op."""
        return df

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to handle / validate null values. Default: no-op."""
        return df

    # ------------------------------------------------------------------ #
    #  Template Method                                                   #
    # ------------------------------------------------------------------ #

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Template method: drop_empty -> rename -> cast -> handle_nulls.
        Override individual hooks for table-specific logic.
        """
        source = self.bronze_path.name if self.bronze_path else self.__class__.__name__
        self.logger.info(f"[TRANSFORM] Starting pipeline: {source}")
        df = self._drop_empty_columns(df)
        df = self._rename_columns(df)
        df = self._cast_dtypes(df)
        df = self._handle_nulls(df)
        self.logger.info(f"[TRANSFORM] Complete | Shape: {df.shape[0]} x {df.shape[1]}")
        return df

    # ------------------------------------------------------------------ #
    #  Orchestration                                                     #
    # ------------------------------------------------------------------ #

    def run(self) -> None:
        """Orchestrate full Bronze -> Silver pipeline: load -> transform -> save."""
        self._check_output_filename()

        self.logger.info(f"[RUN] Starting Bronze -> Silver: {self.bronze_path.name}")

        df = self.load_bronze()
        if df.empty:
            self.logger.error("[RUN] Aborting — empty DataFrame after load")
            return

        df = self.transform(df)
        self.save_silver(df, self._output_filename)

        self.logger.info(f"[RUN] Complete: {self._output_filename}")


# ====================================================================== #
#  Gold Layer                                                            #
# ====================================================================== #


class GoldTransformer(BaseTransformer):
    """
    Base class for Silver -> Gold transformers.

    Subclasses must implement transform() which:
      1. Loads required Silver Parquets internally
      2. Joins / enriches / aggregates
      3. Returns the final Gold DataFrame
    """

    def __init__(self, silver_path: Path, gold_path: Path, log_file: str):
        super().__init__(
            log_file=log_file,
            silver_path=silver_path,
            gold_path=gold_path,
        )

    # ------------------------------------------------------------------ #
    #  Abstract                                                          #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def transform(self) -> pd.DataFrame:
        """Silver -> Gold transformation.  Loads own Silver data internally."""
        pass

    # ------------------------------------------------------------------ #
    #  Orchestration                                                     #
    # ------------------------------------------------------------------ #

    def run(self) -> None:
        """Execute full Silver -> Gold transformation and save."""
        self._check_output_filename()

        try:
            self.logger.info(f"[RUN] Starting Silver -> Gold: {self.__class__.__name__}")

            df = self.transform()

            if df.empty:
                self.logger.error("[RUN] Aborting — empty DataFrame after transform")
                return

            self.save_gold(df, self._output_filename)
            self.logger.info(f"[RUN] Complete: {self._output_filename} [OK]")

        except Exception as e:
            self.logger.error(f"[RUN] Error during transformation: {e}", exc_info=True)
            raise
