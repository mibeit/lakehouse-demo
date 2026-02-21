import pandas as pd
from src.etl.base_transformer import SilverTransformer, BASE_DIR


class ProvinceTransformer(SilverTransformer):
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
        return self._validate_nulls(
            df,
            expected_nulls={
                "border":   "GeoJSON border not always available",
                "valid_to": "Currently active records (SCD pattern)",
            },
            required_columns=["state_province_id", "state_province_name", "country_id"],
        )


if __name__ == "__main__":
    ProvinceTransformer().run()
