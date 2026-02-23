import pandas as pd
from src.etl.base_transformer import SilverTransformer, BASE_DIR


class PeopleTransformer(SilverTransformer):
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

        df = df.drop(columns=["valid_from_parsed"], errors="ignore")
        return df

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        # HashedPassword, UserPreferences, Photo, CustomFields, OtherLanguages:
        # already removed by _drop_empty_columns (all 906 nulls)
        return self._validate_nulls(
            df,
            expected_nulls={
                "valid_to": "Currently active records (SCD pattern)",
            },
            required_columns=["person_id", "full_name", "is_employee", "is_salesperson"],
        )


if __name__ == "__main__":
    PeopleTransformer().run()
