import pandas as pd
from src.etl.base_transformer import BaseTransformer, BASE_DIR


class SupplierTransformer(BaseTransformer):
    """Transform purchasing.suppliers CSV from Bronze layer to Silver (Parquet)."""

    _output_filename = "suppliers.parquet"

    def __init__(self):
        super().__init__(
            bronze_path=BASE_DIR / "data" / "bronze" / "actual" / "purchasing.suppliers.csv",
            silver_path=BASE_DIR / "data" / "silver" / "purchasing",
            log_file="transform_suppliers.log"
        )

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        column_mapping = {
            "SupplierID":               "supplier_id",
            "SupplierName":             "supplier_name",
            "SupplierCategoryID":       "supplier_category_id",
            "PrimaryContactPersonID":   "primary_contact_person_id",
            "AlternateContactPersonID": "alternate_contact_person_id",
            "DeliveryMethodID":         "delivery_method_id",
            "DeliveryCityID":           "delivery_city_id",
            "PostalCityID":             "postal_city_id",
            "SupplierReference":        "supplier_reference",
            "BankAccountName":          "bank_account_name",
            "BankAccountBranch":        "bank_account_branch",
            "BankAccountCode":          "bank_account_code",
            "BankAccountNumber":        "bank_account_number",
            "BankInternationalCode":    "bank_international_code",
            "PaymentDays":              "payment_days",
            "InternalComments":         "internal_comments",
            "PhoneNumber":              "phone_number",
            "FaxNumber":                "fax_number",
            "WebsiteURL":               "website_url",
            "DeliveryAddressLine1":     "delivery_address_line1",
            "DeliveryAddressLine2":     "delivery_address_line2",
            "DeliveryPostalCode":       "delivery_postal_code",
            "DeliveryLocation":         "delivery_location",
            "PostalAddressLine1":       "postal_address_line1",
            "PostalAddressLine2":       "postal_address_line2",
            "PostalPostalCode":         "postal_postal_code",
            "LastEditedBy":             "last_edited_by",
            "ValidFrom":                "valid_from",
            "ValidTo":                  "valid_to"
        }
        df = df.rename(columns=column_mapping)
        self.logger.info("[TRANSFORM] Columns renamed to snake_case")
        return df

    def _cast_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in ["valid_from", "valid_to"]:
            df[col] = self._to_datetime(df[col])
            self.logger.info(f"[TRANSFORM] Cast to datetime: {col}")

        df["delivery_method_id"] = df["delivery_method_id"].astype("Int64")
        self.logger.info("[TRANSFORM] Cast to Int64: delivery_method_id")

        return df

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        expected_nulls = {
            "delivery_method_id":  "Delivery method not always assigned",
            "delivery_address_line1": "Some suppliers have no delivery address",
            "internal_comments":   "Comments optional"
        }
        for col, reason in expected_nulls.items():
            null_count = df[col].isna().sum()
            self.logger.info(f"[NULLS] {col}: {null_count} null(s) -> expected ({reason})")

        required_columns = ["supplier_id", "supplier_name", "supplier_category_id"]
        for col in required_columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                self.logger.warning(f"[NULLS] Unexpected nulls in {col}: {null_count}")
            else:
                self.logger.info(f"[NULLS] {col}: OK (0 nulls)")

        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("[TRANSFORM] Starting pipeline: purchasing.suppliers")
        df = self._drop_empty_columns(df)
        df = self._rename_columns(df)
        df = self._cast_dtypes(df)
        df = self._handle_nulls(df)
        self.logger.info(f"[TRANSFORM] Complete | Shape: {df.shape[0]} x {df.shape[1]}")
        return df


if __name__ == "__main__":
    transformer = SupplierTransformer()
    transformer.run()
