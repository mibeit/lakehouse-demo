import pandas as pd
from src.etl.base_transformer import BaseTransformer, BASE_DIR


class PurchaseOrderTransformer(BaseTransformer):
    """Transform purchase.order CSV from Bronze layer to Silver (Parquet)."""

    _output_filename = "purchase_orders.parquet"

    def __init__(self):
        super().__init__(
            bronze_path=BASE_DIR / "data" / "bronze" / "actual" / "purchase.order.csv",
            silver_path=BASE_DIR / "data" / "silver" / "purchasing",
            log_file="transform_purchase_orders.log"
        )

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        column_mapping = {
            "PurchaseOrderID":       "purchase_order_id",
            "SupplierID":            "supplier_id",
            "OrderDate":             "order_date",
            "DeliveryMethodID":      "delivery_method_id",
            "ContactPersonID":       "contact_person_id",
            "ExpectedDeliveryDate":  "expected_delivery_date",
            "SupplierReference":     "supplier_reference",
            "IsOrderFinalized":      "is_order_finalized",
            "LastEditedBy":          "last_edited_by",
            "LastEditedWhen":        "last_edited_when",
            "LastEditedWhen_parsed": "last_edited_when_parsed"
        }
        df = df.rename(columns=column_mapping)
        self.logger.info("[TRANSFORM] Columns renamed to snake_case")
        return df

    def _cast_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in ["order_date", "expected_delivery_date", "last_edited_when"]:
            df[col] = self._to_datetime(df[col])
            self.logger.info(f"[TRANSFORM] Cast to datetime: {col}")

        df = df.drop(columns=["last_edited_when_parsed"])
        self.logger.info("[TRANSFORM] Dropped redundant column: last_edited_when_parsed")

        return df

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        # Comments + InternalComments bereits durch _drop_empty_columns entfernt
        required_columns = [
            "purchase_order_id",
            "supplier_id",
            "order_date",
            "delivery_method_id"
        ]
        for col in required_columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                self.logger.warning(f"[NULLS] Unexpected nulls in {col}: {null_count}")
            else:
                self.logger.info(f"[NULLS] {col}: OK (0 nulls)")

        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("[TRANSFORM] Starting pipeline: purchase.order")
        df = self._drop_empty_columns(df)  # entfernt Comments + InternalComments
        df = self._rename_columns(df)
        df = self._cast_dtypes(df)
        df = self._handle_nulls(df)
        self.logger.info(f"[TRANSFORM] Complete | Shape: {df.shape[0]} x {df.shape[1]}")
        return df


if __name__ == "__main__":
    transformer = PurchaseOrderTransformer()
    transformer.run()
