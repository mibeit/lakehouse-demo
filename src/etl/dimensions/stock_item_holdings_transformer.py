import pandas as pd
from src.etl.base_transformer import BaseTransformer, BASE_DIR


class StockItemHoldingsTransformer(BaseTransformer):
    """Transform warehouse.stockitemholdings CSV from Bronze layer to Silver (Parquet)."""

    _output_filename = "stock_item_holdings.parquet"

    def __init__(self):
        super().__init__(
            bronze_path=BASE_DIR / "data" / "bronze" / "actual" / "warehouse.stockitemholdings.csv",
            silver_path=BASE_DIR / "data" / "silver" / "dimensions",
            log_file="transform_stock_item_holdings.log"
        )

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        column_mapping = {
            "StockItemID":           "stock_item_id",
            "QuantityOnHand":        "quantity_on_hand",
            "BinLocation":           "bin_location",
            "LastStocktakeQuantity": "last_stocktake_quantity",
            "LastCostPrice":         "last_cost_price",
            "ReorderLevel":          "reorder_level",
            "TargetStockLevel":      "target_stock_level",
            "LastEditedBy":          "last_edited_by",
            "LastEditedWhen":        "last_edited_when"
        }
        df = df.rename(columns=column_mapping)
        self.logger.info("[TRANSFORM] Columns renamed to snake_case")
        return df

    def _cast_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        df["last_edited_when"] = self._to_datetime(df["last_edited_when"])
        self.logger.info("[TRANSFORM] Cast to datetime: last_edited_when")
        return df

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        required_columns = [
            "stock_item_id",
            "quantity_on_hand",
            "reorder_level",
            "target_stock_level",
            "last_cost_price"
        ]
        for col in required_columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                self.logger.warning(f"[NULLS] Unexpected nulls in {col}: {null_count}")
            else:
                self.logger.info(f"[NULLS] {col}: OK (0 nulls)")
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("[TRANSFORM] Starting pipeline: warehouse.stockitemholdings")
        df = self._drop_empty_columns(df)
        df = self._rename_columns(df)
        df = self._cast_dtypes(df)
        df = self._handle_nulls(df)
        self.logger.info(f"[TRANSFORM] Complete | Shape: {df.shape[0]} x {df.shape[1]}")
        return df


if __name__ == "__main__":
    StockItemHoldingsTransformer().run()
