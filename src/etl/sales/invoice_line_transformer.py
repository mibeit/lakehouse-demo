import pandas as pd
from src.etl.base_transformer import BaseTransformer, BASE_DIR


class InvoiceLineTransformer(BaseTransformer):
    """Transform sales.incvoiceslines CSV from Bronze layer to Silver (Parquet)."""

    _output_filename = "invoice_lines.parquet"

    def __init__(self):
        super().__init__(
            bronze_path=BASE_DIR / "data" / "bronze" / "actual" / "sales.incvoiceslines.csv",
            silver_path=BASE_DIR / "data" / "silver" / "sales",
            log_file="transform_invoice_lines.log"
        )

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        column_mapping = {
            "InvoiceLineID":       "invoice_line_id",
            "InvoiceID":           "invoice_id",
            "StockItemID":         "stock_item_id",
            "Description":         "description",
            "PackageTypeID":       "package_type_id",
            "Quantity":            "quantity",
            "UnitPrice":           "unit_price",
            "TaxRate":             "tax_rate",
            "TaxAmount":           "tax_amount",
            "LineProfit":          "line_profit",
            "ExtendedPrice":       "extended_price",
            "LastEditedBy":        "last_edited_by",
            "LastEditedWhen":      "last_edited_when",
            "LastEditedWhen_parsed": "last_edited_when_parsed"
        }
        df = df.rename(columns=column_mapping)
        self.logger.info("[TRANSFORM] Columns renamed to snake_case")
        return df

    def _cast_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        df["last_edited_when"] = self._to_datetime(df["last_edited_when"])
        self.logger.info("[TRANSFORM] Cast to datetime: last_edited_when")

        df = df.drop(columns=["last_edited_when_parsed"])
        self.logger.info("[TRANSFORM] Dropped redundant column: last_edited_when_parsed")

        return df

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        required_columns = [
            "invoice_line_id",
            "invoice_id",
            "stock_item_id",
            "quantity",
            "unit_price",
            "extended_price"
        ]
        for col in required_columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                self.logger.warning(f"[NULLS] Unexpected nulls in {col}: {null_count}")
            else:
                self.logger.info(f"[NULLS] {col}: OK (0 nulls)")

        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("[TRANSFORM] Starting pipeline: sales.incvoiceslines")
        df = self._drop_empty_columns(df)
        df = self._rename_columns(df)
        df = self._cast_dtypes(df)
        df = self._handle_nulls(df)
        self.logger.info(f"[TRANSFORM] Complete | Shape: {df.shape[0]} x {df.shape[1]}")
        return df


if __name__ == "__main__":
    transformer = InvoiceLineTransformer()
    transformer.run()
