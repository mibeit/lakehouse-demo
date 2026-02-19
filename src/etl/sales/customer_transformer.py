import pandas as pd
from pathlib import Path
from src.etl.base_transformer import BaseTransformer, BASE_DIR


class CustomerTransformer(BaseTransformer):
    """Transform sales.customer CSV from Bronze layer to Silver (Parquet)."""

    _output_filename = "customers.parquet"

    def __init__(self):
        super().__init__(
            bronze_path=BASE_DIR / "data" / "bronze" / "actual" / "sales.customer.csv",
            silver_path=BASE_DIR / "data" / "silver" / "sales",
            log_file="transform_customers.log"
        )


    # ------------------------------------------------------------------ #
    #  Private transformation steps                                       #
    # ------------------------------------------------------------------ #

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        column_mapping = {
            "CustomerID":                 "customer_id",
            "CustomerName":               "customer_name",
            "BillToCustomerID":           "bill_to_customer_id",
            "CustomerCategoryID":         "customer_category_id",
            "BuyingGroupID":              "buying_group_id",
            "PrimaryContactPersonID":     "primary_contact_person_id",
            "AlternateContactPersonID":   "alternate_contact_person_id",
            "DeliveryMethodID":           "delivery_method_id",
            "DeliveryCityID":             "delivery_city_id",
            "PostalCityID":               "postal_city_id",
            "CreditLimit":                "credit_limit",
            "AccountOpenedDate":          "account_opened_date",
            "StandardDiscountPercentage": "standard_discount_percentage",
            "IsStatementSent":            "is_statement_sent",
            "IsOnCreditHold":             "is_on_credit_hold",
            "PaymentDays":                "payment_days",
            "PhoneNumber":                "phone_number",
            "FaxNumber":                  "fax_number",
            "WebsiteURL":                 "website_url",
            "DeliveryAddressLine1":       "delivery_address_line1",
            "DeliveryAddressLine2":       "delivery_address_line2",
            "DeliveryPostalCode":         "delivery_postal_code",
            "DeliveryLocation":           "delivery_location",
            "PostalAddressLine1":         "postal_address_line1",
            "PostalAddressLine2":         "postal_address_line2",
            "PostalPostalCode":           "postal_postal_code",
            "LastEditedBy":               "last_edited_by",
            "ValidFrom":                  "valid_from",
            "ValidTo":                    "valid_to",
            "ValidFrom_parsed":           "valid_from_parsed"
        }
        df = df.rename(columns=column_mapping)
        self.logger.info("[TRANSFORM] Columns renamed to snake_case")
        return df


    def _cast_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        # Date strings to datetime
        date_columns = [
            "account_opened_date",
            "valid_from",
            "valid_to"
        ]
        for col in date_columns:
            df[col] = self._to_datetime(df[col])
            self.logger.info(f"[TRANSFORM] Cast to datetime: {col}")

        # Nullable float IDs to Int64
        nullable_int_columns = [
            "buying_group_id",
            "alternate_contact_person_id"
        ]
        for col in nullable_int_columns:
            df[col] = df[col].astype("Int64")
            self.logger.info(f"[TRANSFORM] Cast to Int64: {col}")

        # Drop redundant parsed column
        df = df.drop(columns=["valid_from_parsed"])
        self.logger.info("[TRANSFORM] Dropped redundant column: valid_from_parsed")

        return df


    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        # Expected nulls – dokumentiert warum
        expected_nulls = {
            "buying_group_id":           "Customer not part of a buying group",
            "alternate_contact_person_id": "No alternate contact assigned",
            "credit_limit":              "Customer has no credit limit set",
        }
        for col, reason in expected_nulls.items():
            null_count = df[col].isna().sum()
            self.logger.info(f"[NULLS] {col}: {null_count} null(s) -> expected ({reason})")

        # Required columns
        required_columns = [
            "customer_id",
            "customer_name",
            "customer_category_id",
            "delivery_method_id",
            "account_opened_date"
        ]
        for col in required_columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                self.logger.warning(f"[NULLS] Unexpected nulls in {col}: {null_count}")
            else:
                self.logger.info(f"[NULLS] {col}: OK (0 nulls)")

        return df


    # ------------------------------------------------------------------ #
    #  Orchestration                                                      #
    # ------------------------------------------------------------------ #

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("[TRANSFORM] Starting pipeline: sales.customer")
        df = self._drop_empty_columns(df)   # entfernt DeliveryRun + RunPosition
        df = self._rename_columns(df)
        df = self._cast_dtypes(df)
        df = self._handle_nulls(df)
        self.logger.info(f"[TRANSFORM] Complete | Shape: {df.shape[0]} x {df.shape[1]}")
        return df


if __name__ == "__main__":
    transformer = CustomerTransformer()
    transformer.run()
