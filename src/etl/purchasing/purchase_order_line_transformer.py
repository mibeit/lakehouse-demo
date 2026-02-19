import pandas as pd
from src.etl.base_transformer import BaseTransformer, BASE_DIR


class PurchaseOrderLineTransformer(BaseTransformer):
    """Transform purchase.orderline CSV from Bronze layer to Silver (Parquet)."""

    _output_filename = "purchase_order_lines.parquet"

    def __init__(self):
        super().__init__(
            bronze_path=BASE_DIR / "data" / "bronze" / "actual" / "purchase.orderline.csv",
            silver_path=BASE_DIR / "data" / "silver" / "purchasing",
            log_file="transform_purchase_order_lines.log"
        )

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        column_mapping = {
            "PurchaseOrderLineID":       "purchase_order_line_id",
            "PurchaseOrderID":           "purchase_order_id",
            "StockItemID":               "stock_item_id",
            "OrderedOuters":             "ordered_outers",
            "Description":               "description",
            "ReceivedOuters":            "received_outers",
            "PackageTypeID":             "package_type_id",
            "ExpectedUnitPricePerOuter": "expected_unit_price_per_outer",
            "LastReceiptDate":           "last_receipt_date",
            "IsOrderLineFinalized":      "is_order_line_finalized",
            "LastEditedBy":              "last_edited_by",
            "LastEditedWhen":            "last_edited_when",
            "LastEditedWhen_parsed":     "last_edited_when_parsed"
        }
        df = df.rename(columns=column_mapping)
        self.logger.info("[TRANSFORM] Columns renamed to snake_case")
        return df

    def _cast_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in ["last_receipt_date", "last_edited_when"]:
            df[col] = self._to_datetime(df[col])
            self.logger.info(f"[TRANSFORM] Cast to datetime: {col}")

        df = df.drop(columns=["last_edited_when_parsed"])
        self.logger.info("[TRANSFORM] Dropped redundant column: last_edited_when_parsed")

        return df

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        required_columns = [
            "purchase_order_line_id",
            "purchase_order_id",
            "stock_item_id",
            "ordered_outers",
            "expected_unit_price_per_outer"
        ]
        for col in required_columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                self.logger.warning(f"[NULLS] Unexpected nulls in {col}: {null_count}")
            else:
                self.logger.info(f"[NULLS] {col}: OK (0 nulls)")

        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("[TRANSFORM] Starting pipeline: purchase.orderline")
        df = self._drop_empty_columns(df)
        df = self._rename_columns(df)
        df = self._cast_dtypes(df)
        df = self._handle_nulls(df)
        self.logger.info(f"[TRANSFORM] Complete | Shape: {df.shape[0]} x {df.shape[1]}")
        return df


if __name__ == "__main__":
    transformer = PurchaseOrderLineTransformer()
    transformer.run()
