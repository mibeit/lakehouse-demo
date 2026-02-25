import pandas as pd
from pathlib import Path
from src.etl.base_transformer import GoldTransformer, BASE_DIR


class FactOrdersTransformer(GoldTransformer):
    """
    Transform Silver layer Parquets into Gold layer Orders Fact.
    
    This transformer:
    1. Loads multiple Silver Parquets (customers, cities, provinces, countries, delivery_methods, people)
    2. Performs joins to create a unified orders fact
    3. Handles missing values (credit_limit, full_name, email_address)
    4. Renames columns to Gold layer standards
    5. Orders columns logically
    6. Saves as Parquet to Gold layer
    """

    _output_filename = "fact_orders.parquet"

    def __init__(self):
        super().__init__(
            silver_path=BASE_DIR / "data" / "silver",
            gold_path=BASE_DIR / "data" / "gold",
            log_file="transform_fact_orders.log",
        )

    # ------------------------------------------------------------------ #
    #  Loading                                                           #
    # ------------------------------------------------------------------ #

    def _load_silver_tables(self) -> dict:
        """Load all required Silver Parquets."""
        self.logger.info("[LOAD] Starting to load Silver Parquets...")
        
        tables = {}
        
        tables['customers'] = self.load_silver(self.silver_path / "sales" / "customers.parquet")
        tables['people'] = self.load_silver(self.silver_path / "dimensions" / "people.parquet")
        tables['orders'] = self.load_silver(self.silver_path / "sales" / "orders.parquet")
        tables['order_lines'] = self.load_silver(self.silver_path / "sales" / "order_lines.parquet")

        return tables

    # ------------------------------------------------------------------ #
    #  Joining & Building Fact                                          #
    # ------------------------------------------------------------------ #

    def _build_orders_fact(self, tables: dict) -> pd.DataFrame:
        """Build orders fact by joining all related tables."""
        self.logger.info("[JOIN] Building orders fact...")
        
        # Start with customers
        df = tables['orders'].copy()
        self.logger.info(f"[JOIN] Starting with orders: {df.shape}")
        
        # Join customers
        df = df.merge(
            tables['customers'][["customer_id","customer_name"]],
            on="customer_id",
            how="left"
        )
        self.logger.info(f"[JOIN] After customers: {df.shape}")
        
        # Join people - sales person
        df = df.merge(
            tables['people'][["person_id", "full_name"]],
            left_on="salesperson_id",
            right_on="person_id",
            how="left"
        ).rename(columns={"full_name": "sales_person"})

        self.logger.info(f"[JOIN] After sales_person: {df.shape}")
        
        # Join people - contact person
        df = df.merge(
            tables['people'][["person_id","full_name"]],
            left_on = "contact_person_id",
            right_on = "person_id",
            how="left"
        ).rename(columns={"full_name": "contact_person"})

        self.logger.info(f"[JOIN] After contact_person: {df.shape}")
        
        # Join order_lines 
        df = df.merge(
            tables['order_lines'][["order_id","order_line_id","stock_item_id","description","package_type_id","quantity","unit_price","tax_rate","picked_quantity","picking_completed_when"]],
            on=["order_id"],
            how="left"
        )

        self.logger.info(f"[JOIN] After order_lines: {df.shape}")
        
        return df
    
        

    def _select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select and order columns for the fact_orders table."""
        self.logger.info("[SELECT] Selecting fact_orders columns...")
        
        columns = [
            "order_id",
            "order_line_id",
            "customer_id",
            "customer_name",
            "customer_po_number",
            "contact_person",
            "sales_person",
            "order_date",
            "expected_delivery_date",
            "stock_item_id",
            "description",
            "package_type_id",
            "picked_by_id",
            "quantity",
            "picked_quantity",
            "unit_price",
            "tax_rate"
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
            "customer_name":"customer",
            "customer_po_number":"customer_phone_number",
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
        self.logger.info("[TRANSFORM] Starting pipeline: fact_orders")
        
        # Load
        tables = self._load_silver_tables()
        
        # Join
        df = self._build_orders_fact(tables)
        df = self._select_columns(df)
        
        self.logger.info(f"[TRANSFORM] After joins: {df.shape}")
        
        # Clean & Format
        df = self._rename_columns(df)
        
        self.logger.info(f"[TRANSFORM] Complete | Final shape: {df.shape}")
        
        return df


if __name__ == "__main__":
    FactOrdersTransformer().run()
