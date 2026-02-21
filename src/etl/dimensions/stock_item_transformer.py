import pandas as pd
from src.etl.base_transformer import SilverTransformer, BASE_DIR


class StockItemTransformer(SilverTransformer):
    """Transform warehouse.stockitems CSV from Bronze layer to Silver (Parquet)."""

    _output_filename = "stock_items.parquet"

    def __init__(self):
        super().__init__(
            bronze_path=BASE_DIR / "data" / "bronze" / "actual" / "warehouse.stockitems.csv",
            silver_path=BASE_DIR / "data" / "silver" / "dimensions",
            log_file="transform_stock_items.log"
        )

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        column_mapping = {
            "StockItemID":              "stock_item_id",
            "StockItemName":            "stock_item_name",
            "SupplierID":               "supplier_id",
            "ColorID":                  "color_id",
            "UnitPackageID":            "unit_package_id",
            "OuterPackageID":           "outer_package_id",
            "Brand":                    "brand",
            "Size":                     "size",
            "LeadTimeDays":             "lead_time_days",
            "QuantityPerOuter":         "quantity_per_outer",
            "IsChillerStock":           "is_chiller_stock",
            "Barcode":                  "barcode",
            "TaxRate":                  "tax_rate",
            "UnitPrice":                "unit_price",
            "RecommendedRetailPrice":   "recommended_retail_price",
            "TypicalWeightPerUnit":     "typical_weight_per_unit",
            "MarketingComments":        "marketing_comments",
            "CustomFields":             "custom_fields",
            "Tags":                     "tags",
            "SearchDetails":            "search_details",
            "LastEditedBy":             "last_edited_by",
            "ValidFrom":                "valid_from",
            "ValidTo":                  "valid_to"
        }
        df = df.rename(columns=column_mapping)
        self.logger.info("[TRANSFORM] Columns renamed to snake_case")
        return df

    def _cast_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in ["valid_from", "valid_to"]:
            df[col] = self._to_datetime(df[col])
            self.logger.info(f"[TRANSFORM] Cast to datetime: {col}")

        df["color_id"] = df["color_id"].astype("Int64")
        self.logger.info("[TRANSFORM] Cast to Int64: color_id")
        df["barcode"] = df["barcode"].astype(str).replace("nan", None)
        self.logger.info("[TRANSFORM] Cast barcode to str")

        return df

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        # InternalComments + Photo already removed by _drop_empty_columns
        return self._validate_nulls(
            df,
            expected_nulls={
                "color_id":           "Not all items have a color",
                "brand":              "Not all items have a brand",
                "size":               "Not all items have a size",
                "barcode":            "Not all items have a barcode",
                "marketing_comments": "Marketing comments optional",
                "valid_to":           "Currently active records (SCD pattern)",
            },
            required_columns=["stock_item_id", "stock_item_name", "supplier_id", "unit_price", "tax_rate"],
        )


if __name__ == "__main__":
    StockItemTransformer().run()
