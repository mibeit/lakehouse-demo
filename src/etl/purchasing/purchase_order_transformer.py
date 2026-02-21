import pandas as pd
from src.etl.base_transformer import SilverTransformer, BASE_DIR


class PurchaseOrderTransformer(SilverTransformer):
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
        return self._validate_nulls(
            df,
            required_columns=["purchase_order_id", "supplier_id", "order_date", "delivery_method_id"],
        )


if __name__ == "__main__":
    transformer = PurchaseOrderTransformer()
    transformer.run()
