import pandas as pd
from src.etl.base_transformer import BaseTransformer, BASE_DIR


class SupplierTransactionTransformer(BaseTransformer):
    """Transform purchasing.supplierstransactions CSV from Bronze layer to Silver (Parquet)."""

    _output_filename = "supplier_transactions.parquet"

    def __init__(self):
        super().__init__(
            bronze_path=BASE_DIR / "data" / "bronze" / "actual" / "purchasing.supplierstransactions.csv",
            silver_path=BASE_DIR / "data" / "silver" / "purchasing",
            log_file="transform_supplier_transactions.log"
        )

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        column_mapping = {
            "SupplierTransactionID": "supplier_transaction_id",
            "SupplierID":            "supplier_id",
            "TransactionTypeID":     "transaction_type_id",
            "PurchaseOrderID":       "purchase_order_id",
            "PaymentMethodID":       "payment_method_id",
            "SupplierInvoiceNumber": "supplier_invoice_number",
            "TransactionDate":       "transaction_date",
            "AmountExcludingTax":    "amount_excluding_tax",
            "TaxAmount":             "tax_amount",
            "TransactionAmount":     "transaction_amount",
            "OutstandingBalance":    "outstanding_balance",
            "FinalizationDate":      "finalization_date",
            "IsFinalized":           "is_finalized",
            "LastEditedBy":          "last_edited_by",
            "LastEditedWhen":        "last_edited_when"
        }
        df = df.rename(columns=column_mapping)
        self.logger.info("[TRANSFORM] Columns renamed to snake_case")
        return df

    def _cast_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in ["transaction_date", "finalization_date", "last_edited_when"]:
            df[col] = self._to_datetime(df[col])
            self.logger.info(f"[TRANSFORM] Cast to datetime: {col}")

        nullable_int_columns = ["purchase_order_id", "supplier_invoice_number"]
        for col in nullable_int_columns:
            df[col] = df[col].astype("Int64")
            self.logger.info(f"[TRANSFORM] Cast to Int64: {col}")

        return df

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        expected_nulls = {
            "purchase_order_id":      "Transaction not always linked to a PO",
            "supplier_invoice_number": "Not all transactions have an invoice",
            "finalization_date":       "Transaction not yet finalized"
        }
        for col, reason in expected_nulls.items():
            null_count = df[col].isna().sum()
            self.logger.info(f"[NULLS] {col}: {null_count} null(s) -> expected ({reason})")

        required_columns = [
            "supplier_transaction_id",
            "supplier_id",
            "transaction_date",
            "transaction_amount"
        ]
        for col in required_columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                self.logger.warning(f"[NULLS] Unexpected nulls in {col}: {null_count}")
            else:
                self.logger.info(f"[NULLS] {col}: OK (0 nulls)")

        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("[TRANSFORM] Starting pipeline: purchasing.supplierstransactions")
        df = self._drop_empty_columns(df)
        df = self._rename_columns(df)
        df = self._cast_dtypes(df)
        df = self._handle_nulls(df)
        self.logger.info(f"[TRANSFORM] Complete | Shape: {df.shape[0]} x {df.shape[1]}")
        return df


if __name__ == "__main__":
    transformer = SupplierTransactionTransformer()
    transformer.run()
