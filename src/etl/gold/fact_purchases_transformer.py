import pandas as pd
from pathlib import Path
from src.etl.base_transformer import GoldTransformer, BASE_DIR


class FactPurchasesTransformer(GoldTransformer):
    """
    Transform Silver layer Parquets into Gold layer Purchases Fact.
    
    This transformer:
    1. Loads multiple Silver Parquets (suppliers, stock_items, package_types, delivery_methods)
    2. Performs joins to create a unified purchases fact
    3. Handles missing values (credit_limit, full_name, email_address)
    4. Renames columns to Gold layer standards
    5. Orders columns logically
    6. Saves as Parquet to Gold layer
    """

    _output_filename = "fact_purchases.parquet"

    def __init__(self):
        super().__init__(
            silver_path=BASE_DIR / "data" / "silver",
            gold_path=BASE_DIR / "data" / "gold",
            log_file="transform_fact_purchases.log",
        )

    # ------------------------------------------------------------------ #
    #  Loading                                                           #
    # ------------------------------------------------------------------ #

    def _load_silver_tables(self) -> dict:
        """Load all required Silver Parquets."""
        self.logger.info("[LOAD] Starting to load Silver Parquets...")
        
        tables = {}
        
        tables['people'] = self.load_silver(self.silver_path / "dimensions" / "people.parquet")
        tables['purchases'] = self.load_silver(self.silver_path / "purchasing" / "purchase_orders.parquet")
        tables['purchase_order_lines'] = self.load_silver(self.silver_path / "purchasing" / "purchase_order_lines.parquet")

        return tables

    # ------------------------------------------------------------------ #
    #  Joining & Building Fact                                          #
    # ------------------------------------------------------------------ #

    def _build_purchases_fact(self, tables: dict) -> pd.DataFrame:
        """Build purchases fact by joining all related tables."""
        self.logger.info("[JOIN] Building purchases fact...")
        
        # Start with customers
        df = tables['purchases'].copy()
        self.logger.info(f"[JOIN] Starting with purchases: {df.shape}")
        
        # Join customers
        df = df.merge(
            tables['people'][["person_id","full_name"]],
            left_on="contact_person_id",
            right_on="person_id",
            how="left"
        ).rename(columns={"full_name": "contact_person"})
        self.logger.info(f"[JOIN] After persons: {df.shape}")
        
        # Join purchase_lines 
        df = df.merge(
            tables['purchase_order_lines'][["purchase_order_id","purchase_order_line_id","stock_item_id","ordered_outers","received_outers","package_type_id","description","package_type_id","expected_unit_price_per_outer","last_receipt_date"]],
            on = ["purchase_order_id"],
            how = "left"
        )

        self.logger.info(f"[JOIN] After purchase_order_lines: {df.shape}")

        duplicate_cols = df.columns[df.columns.duplicated()].tolist()
        if duplicate_cols:
            self.logger.warning(f"[JOIN] Removed duplicate columns: {duplicate_cols}")
            df = df.loc[:, ~df.columns.duplicated(keep="first")]

        return df
    
    def _select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select and order columns for the fact_purchases table."""
        self.logger.info("[SELECT] Selecting fact_purchases columns...")
        
        columns = [
            "purchase_order_id",
            "purchase_order_line_id",

            "supplier_id",
            "contact_person",

            "order_date",
            "expected_delivery_date",
            "last_receipt_date",

            "stock_item_id",
            "description",
            "delivery_method_id",
            "package_type_id",

            "ordered_outers",
            "received_outers",

            "expected_unit_price_per_outer",

            "is_order_finalized"
        ]
        
        df = df[columns]
        self.logger.info(f"[SELECT] Selected {len(columns)} columns")
        
        return df
    

    # ------------------------------------------------------------------ #
    #  Change Column Type is_finalized                                   #
    # ------------------------------------------------------------------ #

    def _change_column_types_is_finalized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Change is_order_finalized to boolean."""
        self.logger.info("[TYPE] Converting is_order_finalized to boolean...")
        
        df["is_order_finalized"] = df["is_order_finalized"].astype(bool)
        self.logger.info("[TYPE] Converted is_order_finalized to boolean")
        
        return df

    # ------------------------------------------------------------------ #
    #  Column Renaming & Ordering                                       #
    # ------------------------------------------------------------------ #

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename columns to Gold layer standards."""
        self.logger.info("[RENAME] Applying Gold layer column naming...")
        
        column_mapping = {
            "ordered_outers": "quantity_ordered",
            "received_outers": "quantity_received",
            "expected_unit_price_per_outer": "unit_price",
            "is_order_finalized": "is_finalized"
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
        self.logger.info("[TRANSFORM] Starting pipeline: fact_purchases")
        
        # Load
        tables = self._load_silver_tables()
        
        # Join
        df = self._build_purchases_fact(tables)
        df = self._select_columns(df)
        df = self._change_column_types_is_finalized(df)
        
        self.logger.info(f"[TRANSFORM] After joins: {df.shape}")
        
        # Clean & Format
        df = self._rename_columns(df)
        
        self.logger.info(f"[TRANSFORM] Complete | Final shape: {df.shape}")
        
        return df


if __name__ == "__main__":
    FactPurchasesTransformer().run()
