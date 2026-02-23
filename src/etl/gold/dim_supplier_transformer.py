import pandas as pd
from pathlib import Path
from src.etl.base_transformer import GoldTransformer, BASE_DIR


class DimSupplierTransformer(GoldTransformer):
    """
    Transform Silver layer Parquets into Gold layer Supplier Dimension.
    
    This transformer:
    1. Loads multiple Silver Parquets (suppliers, cities, provinces, countries, delivery_methods, people)
    2. Performs joins to create a unified supplier dimension
    3. Handles missing values (credit_limit, full_name, email_address)
    4. Renames columns to Gold layer standards
    5. Orders columns logically
    6. Saves as Parquet to Gold layer
    """

    _output_filename = "dim_supplier.parquet"

    def __init__(self):
        super().__init__(
            silver_path=BASE_DIR / "data" / "silver",
            gold_path=BASE_DIR / "data" / "gold",
            log_file="transform_dim_supplier.log",
        )

    # ------------------------------------------------------------------ #
    #  Loading                                                           #
    # ------------------------------------------------------------------ #

    def _load_silver_tables(self) -> dict:
        """Load all required Silver Parquets."""
        self.logger.info("[LOAD] Starting to load Silver Parquets...")
        
        tables = {}
        
        tables['suppliers'] = self.load_silver(self.silver_path / "purchasing" / "suppliers.parquet")
        tables['cities'] = self.load_silver(self.silver_path / "dimensions" / "cities.parquet")
        tables['delivery_methods'] = self.load_silver(self.silver_path / "dimensions" / "delivery_methods.parquet")
        tables['people'] = self.load_silver(self.silver_path / "dimensions" / "people.parquet")
        
        return tables

    # ------------------------------------------------------------------ #
    #  Joining & Building Dimension                                     #
    # ------------------------------------------------------------------ #

    def _build_supplier_dimension(self, tables: dict) -> pd.DataFrame:
        """Build supplier table from suppliers, cities, delivery_methods, and people."""
        self.logger.info("[JOIN] Building supplier dimension...")
        
        # Join with delivery methods
        suppliers = tables['suppliers'].merge(
            tables['delivery_methods'][["delivery_method_id", "delivery_method_name"]],
            on="delivery_method_id",
            how="left"
        )

        self.logger.info(f"[JOIN] After suppliers+delivery_methods: {suppliers.shape}")
        
        # Join with people
        suppliers = suppliers.merge(
            tables['people'][["person_id","full_name","email_address","phone_number"]],
            left_on="primary_contact_person_id",
            right_on="person_id",
            how="left"
        )

        self.logger.info(f"[JOIN] After people join: {suppliers.shape}")

        # Join with cities delivery
        suppliers = suppliers.merge(
            tables['cities'][["city_id", "city_name"]],
            left_on="delivery_city_id",
            right_on="city_id",
            how="left"
        ).rename(columns={"city_name": "delivery_city"})

        self.logger.info(f"[JOIN] After cities delivery join: {suppliers.shape}")

        # Join with cities postal
        suppliers = suppliers.merge(
            tables['cities'][["city_id", "city_name"]],
            left_on="postal_city_id",
            right_on="city_id",
            how="left"
        ).rename(columns={"city_name": "postal_city"})

        self.logger.info(f"[JOIN] After cities postal join: {suppliers.shape}")

        
        return suppliers


    def _select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select and order columns for the dimension."""
        self.logger.info("[SELECT] Selecting dimension columns...")
        
        columns = [
            "supplier_id",
            "supplier_name",
            "full_name",
            "email_address",
            "phone_number_y",
            "delivery_method_name",
            "delivery_address_line2",
            "delivery_address_line1",
            "delivery_postal_code",
            "delivery_city",
            "postal_address_line2",
            "postal_address_line1",
            "postal_postal_code",
            "postal_city",
            "internal_comments",
            "supplier_reference",
            "bank_account_name",
            "bank_account_branch",
            "bank_account_code"
        ]
        
        df = df[columns]
        self.logger.info(f"[SELECT] Selected {len(columns)} columns")
        
        return df

    # ------------------------------------------------------------------ #
    #  Imputation: Handle Missing Values                                #
    # ------------------------------------------------------------------ #

    def _impute_supplier_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Impute supplier fields:
        1. internal_comments -> "Not available"
        2. delivery_address_line1 -> "No address line"
        3. delivery_method_name -> "Upon request"
        """
        self.logger.info("[IMPUTE] supplier fields...")

        internal_comments_before = df["internal_comments"].isna().sum()
        delivery_address_line1_before = df["delivery_address_line1"].isna().sum()
        delivery_method_name_before = df["delivery_method_name"].isna().sum()

        df["internal_comments"] = df["internal_comments"].fillna("Not available")
        df["delivery_address_line1"] = df["delivery_address_line1"].fillna("No address line")
        df["delivery_method_name"] = df["delivery_method_name"].fillna("Upon request")

        internal_comments_after = df["internal_comments"].isna().sum()
        delivery_address_line1_after = df["delivery_address_line1"].isna().sum()
        delivery_method_name_after = df["delivery_method_name"].isna().sum()

        self.logger.info(
            f"[IMPUTE] internal_comments: {internal_comments_before} -> {internal_comments_after} missing"
        )
        self.logger.info(
            f"[IMPUTE] delivery_address_line1: {delivery_address_line1_before} -> {delivery_address_line1_after} missing"
        )
        self.logger.info(
            f"[IMPUTE] delivery_method_name: {delivery_method_name_before} -> {delivery_method_name_after} missing"
        )
        
        return df

    # ------------------------------------------------------------------ #
    #  Column Renaming & Ordering                                       #
    # ------------------------------------------------------------------ #

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename columns to Gold layer standards."""
        self.logger.info("[RENAME] Applying Gold layer column naming...")
        
        column_mapping = {
            "supplier_id": "supplier_id",
            "supplier_name": "supplier_name",
            'full_name': 'contact_full_name',
            'email_address': 'contact_email',
            'phone_number_y': 'contact_phone',
            "delivery_method_name": "delivery_method",
            "delivery_address_line2": "delivery_street_address",
            "delivery_address_line1": "delivery_address_additional",
            "delivery_postal_code": "delivery_postal_code",
            "delivery_city": "delivery_city",
            "postal_address_line2": "postal_street_address",
            "postal_address_line1": "postal_address_additional",
            "postal_postal_code": "postal_postal_code",
            "postal_city": "postal_city",
            "internal_comments": "internal_comments",
            "supplier_reference": "supplier_reference",
            "bank_account_name": "bank_account_name",
            "bank_account_branch": "bank_account_branch",
            "bank_account_code": "bank_account_code"
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
        self.logger.info("[TRANSFORM] Starting pipeline: dim_supplier")
        
        # Load
        tables = self._load_silver_tables()
        
        # Join
        df = self._build_supplier_dimension(tables)
        df = self._select_columns(df)
        
        self.logger.info(f"[TRANSFORM] After joins: {df.shape}")
        
        # Impute
        df = self._impute_supplier_fields(df)

        
        self.logger.info(f"[TRANSFORM] After imputation: {df.shape}")
        
        # Clean & Format
        df = self._rename_columns(df)

        self.logger.info(f"[TRANSFORM] Complete | Final shape: {df.shape}")
        
        return df


if __name__ == "__main__":
    DimSupplierTransformer().run()
