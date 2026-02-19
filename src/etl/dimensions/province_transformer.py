import pandas as pd
from src.etl.base_transformer import BaseTransformer, BASE_DIR


class ProvinceTransformer(BaseTransformer):
    """Transform application.province CSV from Bronze layer to Silver (Parquet)."""

    _output_filename = "provinces.parquet"

    def __init__(self):
        super().__init__(
            bronze_path=BASE_DIR / "data" / "bronze" / "actual" / "application.province.csv",
            silver_path=BASE_DIR / "data" / "silver" / "dimensions",
            log_file="transform_provinces.log"
        )

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        column_mapping = {
            "StateProvinceID":           "state_province_id",
            "StateProvinceCode":         "state_province_code",
            "StateProvinceName":         "state_province_name",
            "CountryID":                 "country_id",
            "SalesTerritory":            "sales_territory",
            "Border":                    "border",
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
        return df

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        expected_nulls = {
            "border":   "GeoJSON border not always available",
            "valid_to": "Currently active records (SCD pattern)"
        }
        for col, reason in expected_nulls.items():
            null_count = df[col].isna().sum()
            self.logger.info(f"[NULLS] {col}: {null_count} null(s) -> expected ({reason})")

        required_columns = ["state_province_id", "state_province_name", "country_id"]
        for col in required_columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                self.logger.warning(f"[NULLS] Unexpected nulls in {col}: {null_count}")
            else:
                self.logger.info(f"[NULLS] {col}: OK (0 nulls)")

        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("[TRANSFORM] Starting pipeline: application.province")
        df = self._drop_empty_columns(df)
        df = self._rename_columns(df)
        df = self._cast_dtypes(df)
        df = self._handle_nulls(df)
        self.logger.info(f"[TRANSFORM] Complete | Shape: {df.shape[0]} x {df.shape[1]}")
        return df


if __name__ == "__main__":
    ProvinceTransformer().run()
