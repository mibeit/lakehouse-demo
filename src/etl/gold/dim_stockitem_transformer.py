import pandas as pd
from pathlib import Path
from src.etl.base_transformer import GoldTransformer, BASE_DIR


class DimStockItemTransformer(GoldTransformer):
    """
    Transform Silver layer Parquets into Gold layer Stock Item Dimension.
    
    This transformer:
    1. Loads multiple Silver Parquets (customers, cities, provinces, countries, delivery_methods, people)
    2. Performs joins to create a unified customer dimension
    3. Handles missing values (credit_limit, full_name, email_address)
    4. Renames columns to Gold layer standards
    5. Orders columns logically
    6. Saves as Parquet to Gold layer
    """

    _output_filename = "dim_stock_item.parquet"

    def __init__(self):
        super().__init__(
            silver_path=BASE_DIR / "data" / "silver",
            gold_path=BASE_DIR / "data" / "gold",
            log_file="transform_dim_stock_item.log",
        )

    # ------------------------------------------------------------------ #
    #  Loading                                                           #
    # ------------------------------------------------------------------ #

    def _load_silver_tables(self) -> dict:
        """Load all required Silver Parquets."""
        self.logger.info("[LOAD] Starting to load Silver Parquets...")
        
        tables = {}
        
        tables['stock_items'] = self.load_silver(self.silver_path / "dimensions" / "stock_items.parquet")
        tables['colors'] = self.load_silver(self.silver_path / "dimensions" / "colors.parquet")
        tables['package_types'] = self.load_silver(self.silver_path / "dimensions" / "package_types.parquet")

        return tables

    # ------------------------------------------------------------------ #
    #  Joining & Building Dimension                                     #
    # ------------------------------------------------------------------ #

    def _build_stock_item_dimension(self, tables: dict) -> pd.DataFrame:
        """Build stock item dimension."""
        self.logger.info("[JOIN] Building stock item dimension...")
        
        # Join provinces with countries
        stock_items = tables['stock_items'].merge(
            tables['colors'][["color_id", "color_name"]],
            on="color_id",
            how="left"
        ).rename(columns={"color_name": "color"})

        self.logger.info(f"[JOIN] After colors join: {stock_items.shape}")
        
        # Join with package types unit
        stock_items = stock_items.merge(
            tables['package_types'][["package_type_id", "package_type_name"]],
            right_on="package_type_id",
            left_on="unit_package_id",
            how="left"
        ).rename(columns={"package_type_name": "unit_package"})

        self.logger.info(f"[JOIN] After package types unit join: {stock_items.shape}")

        # Join with package types outer
        stock_items = stock_items.merge(
            tables['package_types'][["package_type_id", "package_type_name"]],
            right_on="package_type_id",
            left_on="outer_package_id",
            how="left"
        ).rename(columns={"package_type_name": "outer_package"})
        
        self.logger.info(f"[JOIN] After package types outer join: {stock_items.shape}")
        
        return stock_items


    def _select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select and order columns for the dimension."""
        self.logger.info("[SELECT] Selecting dimension columns...")
        
        columns = [
            "stock_item_id",
            "stock_item_name",
            "color",
            "unit_package",
            "outer_package",
            "supplier_id",
            "brand",
            "size",
            "typical_weight_per_unit",
            "lead_time_days",
            "quantity_per_outer",
            "is_chiller_stock",
            "tax_rate",
            "unit_price",
            "recommended_retail_price",
            "search_details"
        ]
        
        df = df[columns]
        self.logger.info(f"[SELECT] Selected {len(columns)} columns")
        
        return df


    # ------------------------------------------------------------------ #
    #  Column Renaming & Ordering                                       #
    # ------------------------------------------------------------------ #

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename columns to Gold layer standards."""
        self.logger.info("[RENAME] Applying Gold layer column naming...")
        
        column_mapping = {
            "stock_item_id":              "stock_item_id",
            "stock_item_name":            "product_name",
            "color":                      "color",
            "unit_package":               "unit_package_type",
            "outer_package":              "outer_package_type",
            "supplier_id":                "supplier_id",
            "brand":                      "brand",
            "size":                       "size",
            "typical_weight_per_unit":    "weight_per_unit_kg",
            "lead_time_days":             "lead_time_days",
            "quantity_per_outer":         "quantity_per_outer",
            "is_chiller_stock":           "is_chiller_stock",
            "tax_rate":                   "tax_rate",
            "unit_price":                 "unit_price",
            "recommended_retail_price":   "recommended_retail_price",
            "search_details":             "search_details"
                }
        
        df = df.rename(columns=column_mapping)
        self.logger.info(f"[RENAME] Renamed {len(column_mapping)} columns")
        
        return df

    # ------------------------------------------------------------------ #
    #  Saving                                                            #
    # ------------------------------------------------------------------ #

    # ------------------------------------------------------------------ #
    #  Orchestration                                                     #
    # ------------------------------------------------------------------ #

    def transform(self) -> pd.DataFrame:
        """
        Orchestrate full Silver -> Gold transformation pipeline.
        Returns the transformed DataFrame (does NOT take a df parameter).
        """
        self.logger.info("[TRANSFORM] Starting pipeline: dim_stockitem")
        
        # Load
        tables = self._load_silver_tables()
        
        # Join
        df = self._build_stock_item_dimension(tables)
        df = self._select_columns(df)
        
        self.logger.info(f"[TRANSFORM] After joins: {df.shape}")
         
        # Clean & Format
        df = self._rename_columns(df)
        
        self.logger.info(f"[TRANSFORM] Complete | Final shape: {df.shape}")
        
        return df


if __name__ == "__main__":
    DimStockItemTransformer().run()
