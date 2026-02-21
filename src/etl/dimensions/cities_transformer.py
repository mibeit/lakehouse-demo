import pandas as pd
from src.etl.base_transformer import SilverTransformer, BASE_DIR


class CitiesTransformer(SilverTransformer):
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
        return self._validate_nulls(
            df,
            expected_nulls={
                "latest_recorded_population": "Not all cities have population data",
                "valid_to":                   "Currently active records (SCD pattern)",
            },
            required_columns=["city_id", "city_name", "state_province_id"],
        )


if __name__ == "__main__":
    CitiesTransformer().run()
