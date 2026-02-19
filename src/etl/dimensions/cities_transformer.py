import pandas as pd
from src.etl.base_transformer import BaseTransformer, BASE_DIR


class CitiesTransformer(BaseTransformer):
    """Transform application.cities CSV from Bronze layer to Silver (Parquet)."""

    _output_filename = "cities.parquet"

    def __init__(self):
        super().__init__(
            bronze_path=BASE_DIR / "data" / "bronze" / "actual" / "application.cities.csv",
            silver_path=BASE_DIR / "data" / "silver" / "dimensions",
            log_file="transform_cities.log"
        )

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        column_mapping = {
            "CityID":                    "city_id",
            "CityName":                  "city_name",
            "StateProvinceID":           "state_province_id",
            "Location":                  "location",
            "LatestRecordedPopulation":  "latest_recorded_population",
            "LastEditedBy":              "last_edited_by",
            "ValidFrom":                 "valid_from",
            "ValidTo":                   "valid_to"
        }
        df = df.rename(columns=column_mapping)
        self.logger.info("[TRANSFORM] Columns renamed to snake_case")
        return df

    def _cast_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in ["valid_from", "valid_to"]:
            df[col] = self._to_datetime(df[col])
            self.logger.info(f"[TRANSFORM] Cast to datetime: {col}")

        df["latest_recorded_population"] = df["latest_recorded_population"].astype("Int64")
        self.logger.info("[TRANSFORM] Cast to Int64: latest_recorded_population")

        return df

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        expected_nulls = {
            "latest_recorded_population": "Not all cities have population data",
            "valid_to":                   "Currently active records (SCD pattern)"
        }
        for col, reason in expected_nulls.items():
            null_count = df[col].isna().sum()
            self.logger.info(f"[NULLS] {col}: {null_count} null(s) -> expected ({reason})")

        required_columns = ["city_id", "city_name", "state_province_id"]
        for col in required_columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                self.logger.warning(f"[NULLS] Unexpected nulls in {col}: {null_count}")
            else:
                self.logger.info(f"[NULLS] {col}: OK (0 nulls)")

        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("[TRANSFORM] Starting pipeline: application.cities")
        df = self._drop_empty_columns(df)
        df = self._rename_columns(df)
        df = self._cast_dtypes(df)
        df = self._handle_nulls(df)
        self.logger.info(f"[TRANSFORM] Complete | Shape: {df.shape[0]} x {df.shape[1]}")
        return df


if __name__ == "__main__":
    CitiesTransformer().run()
