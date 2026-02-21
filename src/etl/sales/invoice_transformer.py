import pandas as pd
from src.etl.base_transformer import SilverTransformer, BASE_DIR


class InvoiceTransformer(SilverTransformer):
    """Transform sales.invoices CSV from Bronze layer to Silver (Parquet)."""

    _output_filename = "invoices.parquet"

    def __init__(self):
        super().__init__(
            bronze_path=BASE_DIR / "data" / "bronze" / "actual" / "sales.invoices.csv",
            silver_path=BASE_DIR / "data" / "silver" / "sales",
            log_file="transform_invoices.log"
        )

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        column_mapping = {
            "InvoiceID":                  "invoice_id",
            "CustomerID":                 "customer_id",
            "BillToCustomerID":           "bill_to_customer_id",
            "OrderID":                    "order_id",
            "DeliveryMethodID":           "delivery_method_id",
            "ContactPersonID":            "contact_person_id",
            "AccountsPersonID":           "accounts_person_id",
            "SalespersonPersonID":        "salesperson_id",
            "PackedByPersonID":           "packed_by_id",
            "InvoiceDate":                "invoice_date",
            "CustomerPurchaseOrderNumber":"customer_po_number",
            "IsCreditNote":               "is_credit_note",
            "DeliveryInstructions":       "delivery_instructions",
            "TotalDryItems":              "total_dry_items",
            "TotalChillerItems":          "total_chiller_items",
            "ReturnedDeliveryData":       "returned_delivery_data",
            "ConfirmedDeliveryTime":      "confirmed_delivery_time",
            "ConfirmedReceivedBy":        "confirmed_received_by",
            "LastEditedBy":               "last_edited_by",
            "LastEditedWhen":             "last_edited_when",
            "LastEditedWhen_parsed":      "last_edited_when_parsed"
        }
        df = df.rename(columns=column_mapping)
        self.logger.info("[TRANSFORM] Columns renamed to snake_case")
        return df

    def _cast_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        date_columns = [
            "invoice_date",
            "confirmed_delivery_time",
            "last_edited_when"
        ]
        for col in date_columns:
            df[col] = self._to_datetime(df[col])
            self.logger.info(f"[TRANSFORM] Cast to datetime: {col}")

        df = df.drop(columns=["last_edited_when_parsed"])
        self.logger.info("[TRANSFORM] Dropped redundant column: last_edited_when_parsed")

        return df

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._validate_nulls(
            df,
            required_columns=["invoice_id", "customer_id", "order_id", "invoice_date", "salesperson_id"],
        )


if __name__ == "__main__":
    transformer = InvoiceTransformer()
    transformer.run()
