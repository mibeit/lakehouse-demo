import pandas as pd
from src.etl.base_transformer import BaseTransformer, BASE_DIR


class PeopleTransformer(BaseTransformer):
    """Transform application.people CSV from Bronze layer to Silver (Parquet)."""

    _output_filename = "people.parquet"

    def __init__(self):
        super().__init__(
            bronze_path=BASE_DIR / "data" / "bronze" / "actual" / "application.people.csv",
            silver_path=BASE_DIR / "data" / "silver" / "dimensions",
            log_file="transform_people.log"
        )

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        column_mapping = {
            "PersonID":               "person_id",
            "FullName":               "full_name",
            "PreferredName":          "preferred_name",
            "SearchName":             "search_name",
            "IsPermittedToLogon":     "is_permitted_to_logon",
            "LogonName":              "logon_name",
            "IsExternalLogonProvider":"is_external_logon_provider",
            "IsSystemUser":           "is_system_user",
            "IsEmployee":             "is_employee",
            "IsSalesperson":          "is_salesperson",
            "PhoneNumber":            "phone_number",
            "FaxNumber":              "fax_number",
            "EmailAddress":           "email_address",
            "LastEditedBy":           "last_edited_by",
            "ValidFrom":              "valid_from",
            "ValidTo":                "valid_to",
            "ValidFrom_parsed":       "valid_from_parsed"
        }
        df = df.rename(columns=column_mapping)
        self.logger.info("[TRANSFORM] Columns renamed to snake_case")
        return df

    def _cast_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in ["valid_from", "valid_to"]:
            df[col] = self._to_datetime(df[col])
            self.logger.info(f"[TRANSFORM] Cast to datetime: {col}")

        # Drop redundant parsed column
        df = df.drop(columns=["valid_from_parsed"])
        self.logger.info("[TRANSFORM] Dropped redundant column: valid_from_parsed")

        return df

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        # HashedPassword, UserPreferences, Photo, CustomFields, OtherLanguages
        # werden bereits durch _drop_empty_columns entfernt (alle 906 Nulls)
        expected_nulls = {
            "valid_to": "Currently active records (SCD pattern)"
        }
        for col, reason in expected_nulls.items():
            null_count = df[col].isna().sum()
            self.logger.info(f"[NULLS] {col}: {null_count} null(s) -> expected ({reason})")

        required_columns = ["person_id", "full_name", "is_employee", "is_salesperson"]
        for col in required_columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                self.logger.warning(f"[NULLS] Unexpected nulls in {col}: {null_count}")
            else:
                self.logger.info(f"[NULLS] {col}: OK (0 nulls)")

        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("[TRANSFORM] Starting pipeline: application.people")
        df = self._drop_empty_columns(df)  # entfernt 5 komplett leere Spalten
        df = self._rename_columns(df)
        df = self._cast_dtypes(df)
        df = self._handle_nulls(df)
        self.logger.info(f"[TRANSFORM] Complete | Shape: {df.shape[0]} x {df.shape[1]}")
        return df


if __name__ == "__main__":
    PeopleTransformer().run()
