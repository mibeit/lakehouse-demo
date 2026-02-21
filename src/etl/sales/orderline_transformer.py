import pandas as pd
from src.etl.base_transformer import SilverTransformer, BASE_DIR


class OrderLineTransformer(SilverTransformer):
    """Transform sales.orderline CSV from Bronze layer to Silver (Parquet)."""

    _output_filename = "order_lines.parquet"

    def __init__(self):
        super().__init__(
            bronze_path=BASE_DIR / "data" / "bronze" / "actual" / "sales.orderline.csv",
            silver_path=BASE_DIR / "data" / "silver" / "sales",
            log_file="transform_order_lines.log"
        )


    # ------------------------------------------------------------------ #
    #  Private transformation steps                                       #
    # ------------------------------------------------------------------ #

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        column_mapping = {
            "OrderLineID":           "order_line_id",
            "OrderID":               "order_id",
            "StockItemID":           "stock_item_id",
            "Description":           "description",
            "PackageTypeID":         "package_type_id",
            "Quantity":              "quantity",
            "UnitPrice":             "unit_price",
            "TaxRate":               "tax_rate",
            "PickedQuantity":        "picked_quantity",
            "PickingCompletedWhen":  "picking_completed_when",
            "LastEditedBy":          "last_edited_by",
            "LastEditedWhen":        "last_edited_when",
            "LastEditedWhen_parsed": "last_edited_when_parsed"
        }
        df = df.rename(columns=column_mapping)
        self.logger.info("[TRANSFORM] Columns renamed to snake_case")
        return df


    def _cast_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        # Cast date strings to datetime
        date_columns = [
            "picking_completed_when",
            "last_edited_when"
        ]
        for col in date_columns:
            df[col] = self._to_datetime(df[col])
            self.logger.info(f"[TRANSFORM] Cast to datetime: {col}")

        # Drop redundant parsed column
        df = df.drop(columns=["last_edited_when_parsed"])
        self.logger.info("[TRANSFORM] Dropped redundant column: last_edited_when_parsed")

        return df


    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._validate_nulls(
            df,
            expected_nulls={
                "picking_completed_when": "Order line not yet picked",
            },
            required_columns=["order_line_id", "order_id", "stock_item_id", "quantity", "unit_price"],
        )

if __name__ == "__main__":
    transformer = OrderLineTransformer()
    transformer.run()
