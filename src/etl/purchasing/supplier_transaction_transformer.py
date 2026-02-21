import pandas as pd
from src.etl.base_transformer import SilverTransformer, BASE_DIR


class SupplierTransactionTransformer(SilverTransformer):
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
        return self._validate_nulls(
            df,
            expected_nulls={
                "purchase_order_id":       "Transaction not always linked to a PO",
                "supplier_invoice_number": "Not all transactions have an invoice",
                "finalization_date":       "Transaction not yet finalized",
            },
            required_columns=["supplier_transaction_id", "supplier_id", "transaction_date", "transaction_amount"],
        )


if __name__ == "__main__":
    transformer = SupplierTransactionTransformer()
    transformer.run()
