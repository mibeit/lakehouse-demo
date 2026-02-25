from pathlib import Path
import pandas as pd
import sys

BASE_DIR = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(BASE_DIR / "src"))

from src.etl.base_transformer import GoldTransformer, BASE_DIR


class DimCalendarTransformer(GoldTransformer):
    """
    Generate a Gold layer calendar dimension.

    This transformer:
    1. Scans Silver layer date columns to determine the required date range
    2. Generates one row per calendar day
    3. Enriches with year / quarter / month / week / day attributes
    4. Saves as Parquet to Gold layer
    """

    _output_filename = "dim_calendar.parquet"

    def __init__(self):
        super().__init__(
            silver_path=BASE_DIR / "data" / "silver",
            gold_path=BASE_DIR / "data" / "gold",
            log_file="transform_dim_calendar.log",
        )

    # ------------------------------------------------------------------ #
    #  Date-range detection                                              #
    # ------------------------------------------------------------------ #

    def _detect_date_range(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        """
        Scan Silver layer tables for date columns and return
        (min_date, max_date) rounded to full calendar years.
        """
        self.logger.info("[DETECT] Scanning Silver tables for date range...")

        date_files = {
            "purchase_orders": (
                self.silver_path / "purchasing" / "purchase_orders.parquet",
                ["order_date", "expected_delivery_date"],
            ),
            "purchase_order_lines": (
                self.silver_path / "purchasing" / "purchase_order_lines.parquet",
                ["last_receipt_date"],
            ),
        }

        all_dates: list[pd.Timestamp] = []

        for name, (path, cols) in date_files.items():
            if not path.exists():
                self.logger.warning(f"[DETECT] File not found, skipping: {path}")
                continue

            df = self.load_silver(path)
            for col in cols:
                if col in df.columns:
                    series = pd.to_datetime(df[col], errors="coerce").dropna()
                    if not series.empty:
                        all_dates.append(series.min())
                        all_dates.append(series.max())
                        self.logger.info(
                            f"[DETECT] {name}.{col}: "
                            f"{series.min().date()} → {series.max().date()}"
                        )

        if not all_dates:
            raise ValueError("No valid dates found in Silver layer tables.")

        # Round to full calendar years
        min_date = pd.Timestamp(year=min(all_dates).year, month=1, day=1)
        max_date = pd.Timestamp(year=max(all_dates).year, month=12, day=31)

        self.logger.info(
            f"[DETECT] Calendar range: {min_date.date()} → {max_date.date()}"
        )
        return min_date, max_date

    # ------------------------------------------------------------------ #
    #  Calendar generation                                               #
    # ------------------------------------------------------------------ #

    def _generate_calendar(
        self, min_date: pd.Timestamp, max_date: pd.Timestamp
    ) -> pd.DataFrame:
        """Generate one row per calendar day between min_date and max_date."""
        self.logger.info("[BUILD] Generating calendar dimension...")

        dates = pd.date_range(start=min_date, end=max_date, freq="D")

        df = pd.DataFrame({"date": dates})

        # Integer key  (YYYYMMDD)
        df["date_key"] = df["date"].dt.strftime("%Y%m%d").astype(int)

        # Year / Quarter / Month
        df["year"] = df["date"].dt.year
        df["quarter"] = df["date"].dt.quarter
        df["month"] = df["date"].dt.month
        df["month_name"] = df["date"].dt.month_name()
        df["month_name_short"] = df["date"].dt.strftime("%b")

        # Day
        df["day"] = df["date"].dt.day
        df["day_of_week"] = df["date"].dt.dayofweek  # 0=Mon … 6=Sun
        df["day_name"] = df["date"].dt.day_name()

        # Week
        df["week_of_year"] = df["date"].dt.isocalendar().week.astype(int)

        # Boolean flags
        df["is_weekend"] = df["day_of_week"].isin([5, 6])
        df["is_month_start"] = df["date"].dt.is_month_start
        df["is_month_end"] = df["date"].dt.is_month_end
        df["is_quarter_start"] = df["date"].dt.is_quarter_start
        df["is_quarter_end"] = df["date"].dt.is_quarter_end
        df["is_year_start"] = df["date"].dt.is_year_start
        df["is_year_end"] = df["date"].dt.is_year_end

        self.logger.info(f"[BUILD] Generated {len(df)} calendar rows")
        return df

    # ------------------------------------------------------------------ #
    #  Column selection & ordering                                       #
    # ------------------------------------------------------------------ #

    def _select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select and order columns for dim_calendar."""
        self.logger.info("[SELECT] Selecting dim_calendar columns...")

        columns = [
            "date_key",
            "date",
            "year",
            "quarter",
            "month",
            "month_name",
            "month_name_short",
            "day",
            "day_of_week",
            "day_name",
            "week_of_year",
            "is_weekend",
            "is_month_start",
            "is_month_end",
            "is_quarter_start",
            "is_quarter_end",
            "is_year_start",
            "is_year_end",
        ]

        df = df[columns]
        self.logger.info(f"[SELECT] Selected {len(columns)} columns")
        return df

    # ------------------------------------------------------------------ #
    #  Column type enforcement                                           #
    # ------------------------------------------------------------------ #

    def _enforce_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure correct dtypes for the calendar dimension."""
        self.logger.info("[TYPE] Enforcing column types...")

        bool_cols = [
            "is_weekend",
            "is_month_start",
            "is_month_end",
            "is_quarter_start",
            "is_quarter_end",
            "is_year_start",
            "is_year_end",
        ]
        for col in bool_cols:
            df[col] = df[col].astype(bool)

        df["date"] = pd.to_datetime(df["date"])

        self.logger.info("[TYPE] Column types enforced")
        return df

    # ------------------------------------------------------------------ #
    #  Orchestration                                                     #
    # ------------------------------------------------------------------ #

    def transform(self) -> pd.DataFrame:
        """
        Orchestrate full Silver → Gold transformation pipeline.
        Returns the transformed DataFrame.
        """
        self.logger.info("[TRANSFORM] Starting pipeline: dim_calendar")

        # Detect range
        min_date, max_date = self._detect_date_range()

        # Build
        df = self._generate_calendar(min_date, max_date)
        df = self._select_columns(df)
        df = self._enforce_types(df)

        self.logger.info(f"[TRANSFORM] Complete | Final shape: {df.shape}")
        return df


# ------------------------------------------------------------------ #
#  CLI entry point                                                   #
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    DimCalendarTransformer().run()