import pandas as pd
from pathlib import Path
from src.etl.base_transformer import BaseTransformer, BASE_DIR


class OrderTransformer(BaseTransformer):
    """Transform sales.order CSV from Bronze layer to Silver (Parquet)."""

    _output_filename = "orders.parquet"

    def __init__(self):
        super().__init__(
            bronze_path=BASE_DIR / "data" / "bronze" / "actual" / "sales.order.csv",
            silver_path=BASE_DIR / "data" / "silver" / "sales",
            log_file="transform_orders.log"
        )


    # ------------------------------------------------------------------ #
    #  Private transformation steps                                       #
    # ------------------------------------------------------------------ #

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        column_mapping = {
            "OrderID":                    "order_id",
            "CustomerID":                 "customer_id",
            "SalespersonPersonID":        "salesperson_id",
            "PickedByPersonID":           "picked_by_id",
            "ContactPersonID":            "contact_person_id",
            "BackorderOrderID":           "backorder_order_id",
            "OrderDate":                  "order_date",
            "ExpectedDeliveryDate":       "expected_delivery_date",
            "CustomerPurchaseOrderNumber":"customer_po_number",
            "IsUndersupplyBackordered":   "is_undersupply_backordered",
            "PickingCompletedWhen":       "picking_completed_when",
            "LastEditedBy":               "last_edited_by",
            "LastEditedWhen":             "last_edited_when",
            "LastEditedWhen_parsed":      "last_edited_when_parsed"
        }
        df = df.rename(columns=column_mapping)
        self.logger.info("[TRANSFORM] Columns renamed to snake_case")
        return df


    def _cast_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        date_columns = [
            "order_date",
            "expected_delivery_date",
            "picking_completed_when",
            "last_edited_when"
        ]
        for col in date_columns:
            df[col] = self._to_datetime(df[col])
            self.logger.info(f"[TRANSFORM] Cast to datetime: {col}")

        nullable_int_columns = [
            "picked_by_id",
            "backorder_order_id"
        ]
        for col in nullable_int_columns:
            df[col] = df[col].astype("Int64")
            self.logger.info(f"[TRANSFORM] Cast to Int64: {col}")

        df = df.drop(columns=["last_edited_when_parsed"])
        self.logger.info("[TRANSFORM] Dropped redundant column: last_edited_when_parsed")

        return df


    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        expected_nulls = {
            "picked_by_id":           "Order not yet picked",
            "backorder_order_id":     "No backorder exists",
            "picking_completed_when": "Order not yet completed"
        }
        for col, reason in expected_nulls.items():
            null_count = df[col].isna().sum()
            self.logger.info(f"[NULLS] {col}: {null_count} null(s) -> expected ({reason})")

        required_columns = ["order_id", "customer_id", "order_date", "salesperson_id"]
        for col in required_columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                self.logger.warning(f"[NULLS] Unexpected nulls in {col}: {null_count}")
            else:
                self.logger.info(f"[NULLS] {col}: OK (0 nulls)")

        return df


    # ------------------------------------------------------------------ #
    #  Orchestration (overrides abstract method)                         #
    # ------------------------------------------------------------------ #

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("[TRANSFORM] Starting pipeline: sales.order")
        df = self._drop_empty_columns(df)
        df = self._rename_columns(df)
        df = self._cast_dtypes(df)
        df = self._handle_nulls(df)
        self.logger.info(f"[TRANSFORM] Complete | Shape: {df.shape[0]} x {df.shape[1]}")
        return df


if __name__ == "__main__":
    transformer = OrderTransformer()
    transformer.run()
