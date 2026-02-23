import pandas as pd
from pathlib import Path
from src.etl.base_transformer import GoldTransformer, BASE_DIR


class DimCustomerTransformer(GoldTransformer):
    """
    Transform Silver layer Parquets into Gold layer Customer Dimension.
    
    This transformer:
    1. Loads multiple Silver Parquets (customers, cities, provinces, countries, delivery_methods, people)
    2. Performs joins to create a unified customer dimension
    3. Handles missing values (credit_limit, full_name, email_address)
    4. Renames columns to Gold layer standards
    5. Orders columns logically
    6. Saves as Parquet to Gold layer
    """

    _output_filename = "dim_customer.parquet"

    def __init__(self):
        super().__init__(
            silver_path=BASE_DIR / "data" / "silver",
            gold_path=BASE_DIR / "data" / "gold",
            log_file="transform_dim_customer.log",
        )

    # ------------------------------------------------------------------ #
    #  Loading                                                           #
    # ------------------------------------------------------------------ #

    def _load_silver_tables(self) -> dict:
        """Load all required Silver Parquets."""
        self.logger.info("[LOAD] Starting to load Silver Parquets...")
        
        tables = {}
        
        tables['customers'] = self.load_silver(self.silver_path / "sales" / "customers.parquet")
        tables['cities'] = self.load_silver(self.silver_path / "dimensions" / "cities.parquet")
        tables['provinces'] = self.load_silver(self.silver_path / "dimensions" / "provinces.parquet")
        tables['countries'] = self.load_silver(self.silver_path / "dimensions" / "countries.parquet")
        tables['delivery_methods'] = self.load_silver(self.silver_path / "dimensions" / "delivery_methods.parquet")
        tables['people'] = self.load_silver(self.silver_path / "dimensions" / "people.parquet")
        
        return tables

    # ------------------------------------------------------------------ #
    #  Joining & Building Dimension                                     #
    # ------------------------------------------------------------------ #

    def _build_responsible_market(self, tables: dict) -> pd.DataFrame:
        """Build responsible_market table from provinces, countries, and cities."""
        self.logger.info("[JOIN] Building responsible_market dimension...")
        
        # Join provinces with countries
        responsible_market = tables['provinces'].merge(
            tables['countries'][["country_id", "country_name", "iso_alpha3_code", "continent", "region", "subregion"]],
            on="country_id",
            how="left"
        )
        self.logger.info(f"[JOIN] After provinces+countries: {responsible_market.shape}")
        
        # Join with cities
        responsible_market = responsible_market.merge(
            tables['cities'][["state_province_id", "city_id", "city_name"]],
            on="state_province_id",
            how="left"
        )
        self.logger.info(f"[JOIN] After cities join: {responsible_market.shape}")
        
        return responsible_market

    def _build_customer_dimension(self, tables: dict, responsible_market: pd.DataFrame) -> pd.DataFrame:
        """Build customer dimension by joining all related tables."""
        self.logger.info("[JOIN] Building customer dimension...")
        
        # Start with customers
        df = tables['customers'].copy()
        self.logger.info(f"[JOIN] Starting with customers: {df.shape}")
        
        # Join delivery methods
        df = df.merge(
            tables['delivery_methods'][["delivery_method_id", "delivery_method_name"]],
            on="delivery_method_id",
            how="left"
        )
        self.logger.info(f"[JOIN] After delivery_methods: {df.shape}")
        
        # Join responsible_market
        df = df.merge(
            responsible_market[["city_id", "city_name", "country_name", "state_province_code", 
                               "state_province_name", "iso_alpha3_code", "continent", "region", 
                               "subregion", "sales_territory"]],
            left_on="delivery_city_id",
            right_on="city_id",
            how="left"
        )
        self.logger.info(f"[JOIN] After responsible_market: {df.shape}")
        
        # Join people (for contact info)
        df = df.merge(
            tables['people'][["person_id", "full_name", "email_address"]],
            left_on="primary_contact_person_id",
            right_on="person_id",
            how="left"
        )
        self.logger.info(f"[JOIN] After people: {df.shape}")
        
        return df

    def _select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select and order columns for the dimension."""
        self.logger.info("[SELECT] Selecting dimension columns...")
        
        columns = [
            "customer_id", "customer_name", "full_name", "email_address", "phone_number", 
            "fax_number", "website_url", "bill_to_customer_id", "credit_limit", 
            "delivery_method_name", "delivery_address_line1", "delivery_address_line2", 
            "delivery_postal_code", "postal_address_line1", "postal_address_line2", 
            "postal_postal_code", "city_name", "state_province_code", "state_province_name", 
            "sales_territory", "country_name", "iso_alpha3_code", "continent", "region", "subregion"
        ]
        
        df = df[columns]
        self.logger.info(f"[SELECT] Selected {len(columns)} columns")
        
        return df

    # ------------------------------------------------------------------ #
    #  Imputation: Handle Missing Values                                #
    # ------------------------------------------------------------------ #

    def _impute_credit_limit(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Impute credit_limit:
        1. Use parent customer's credit limit
        2. Fall back to average
        """
        self.logger.info("[IMPUTE] credit_limit...")
        before_null = df['credit_limit'].isna().sum()
        self.logger.info(f"[IMPUTE] credit_limit: {before_null} missing ({before_null/len(df)*100:.2f}%)")
        
        # Get parent customer credits
        main_customer_credits = df[['customer_id', 'credit_limit']].rename(
            columns={'customer_id': 'bill_to_customer_id', 'credit_limit': 'parent_credit_limit'}
        )
        df = df.merge(main_customer_credits, on='bill_to_customer_id', how='left')
        
        # Fill strategy: parent credit -> average
        avg_credit = df['credit_limit'].mean()
        df['credit_limit'] = (
            df['credit_limit']
            .fillna(df['parent_credit_limit'])
            .fillna(avg_credit)
        )

        # Remove temporary column immediately
        df = df.drop(columns=['parent_credit_limit'])
        
        after_null = df['credit_limit'].isna().sum()
        self.logger.info(f"[IMPUTE] credit_limit after imputation: {after_null} missing")
        self.logger.info(f"[IMPUTE] Average credit_limit: {avg_credit:.2f}")
        
        return df

    def _impute_contact_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Impute full_name and email_address:
        - full_name: Use 'Head Office' for missing
        - email_address: Generate from customer_name (info@<name>.com)
        """
        self.logger.info("[IMPUTE] Contact info (full_name, email_address)...")
        
        full_name_null = df['full_name'].isna().sum()
        email_null = df['email_address'].isna().sum()
        self.logger.info(f"[IMPUTE] full_name: {full_name_null} missing | email_address: {email_null} missing")
        
        # Impute full_name
        df['full_name'] = df['full_name'].fillna('Head Office')
        
        # Impute email_address
        df['email_address'] = df.apply(
            lambda row: row['email_address'] if pd.notna(row['email_address'])
            else 'info@' + row['customer_name'].replace(' (Head Office)', '').replace(' ', '').lower() + '.com',
            axis=1
        )
        
        self.logger.info(f"[IMPUTE] full_name after imputation: {df['full_name'].isna().sum()} missing")
        self.logger.info(f"[IMPUTE] email_address after imputation: {df['email_address'].isna().sum()} missing")
        
        return df

    # ------------------------------------------------------------------ #
    #  Column Renaming & Ordering                                       #
    # ------------------------------------------------------------------ #

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename columns to Gold layer standards."""
        self.logger.info("[RENAME] Applying Gold layer column naming...")
        
        column_mapping = {
            # Customer Info
            'customer_id': 'customer_id',
            'customer_name': 'customer_name',
            'bill_to_customer_id': 'parent_customer_id',
            'credit_limit': 'credit_limit_amount',
            
            # Contact Info
            'full_name': 'contact_full_name',
            'email_address': 'contact_email',
            'phone_number': 'contact_phone',
            'fax_number': 'contact_fax',
            'website_url': 'customer_website',
            
            # Delivery Address
            'delivery_address_line1': 'delivery_address_additional',
            'delivery_address_line2': 'delivery_street_address',
            'delivery_postal_code': 'delivery_postal_code',
            'delivery_method_name': 'delivery_method',
            
            # Postal Address
            'postal_address_line1': 'postal_address_additional',
            'postal_address_line2': 'postal_street_address',
            'postal_postal_code': 'postal_code',
            
            # Geographic Info
            'city_name': 'city',
            'state_province_code': 'state_code',
            'state_province_name': 'state_name',
            'country_name': 'country',
            'iso_alpha3_code': 'country_code_iso3',
            'continent': 'continent',
            'region': 'region',
            'subregion': 'subregion',
            
            # Sales Info
            'sales_territory': 'sales_territory'
        }
        
        df = df.rename(columns=column_mapping)
        self.logger.info(f"[RENAME] Renamed {len(column_mapping)} columns")
        
        return df

    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reorder columns in Gold layer logical grouping."""
        self.logger.info("[REORDER] Applying logical column order...")
        
        column_order = [
            # Customer Basics
            'customer_id', 'customer_name', 'parent_customer_id',
            # Contact Info
            'contact_full_name', 'contact_email', 'contact_phone', 'contact_fax',
            'customer_website',
            # Credit & Delivery
            'credit_limit_amount', 'delivery_method',
            # Delivery Address
            'delivery_address_additional', 'delivery_street_address', 'delivery_postal_code',
            # Postal Address
            'postal_address_additional', 'postal_street_address', 'postal_code',
            # Geographic
            'city', 'state_code', 'state_name', 'country', 'country_code_iso3',
            'continent', 'region', 'subregion',
            # Sales Territory
            'sales_territory'
        ]
        
        df = df[column_order]
        self.logger.info(f"[REORDER] Reordered to {len(column_order)} columns")
        
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
        self.logger.info("[TRANSFORM] Starting pipeline: dim_customer")
        
        # Load
        tables = self._load_silver_tables()
        
        # Join
        responsible_market = self._build_responsible_market(tables)
        df = self._build_customer_dimension(tables, responsible_market)
        df = self._select_columns(df)
        
        self.logger.info(f"[TRANSFORM] After joins: {df.shape}")
        
        # Impute
        df = self._impute_credit_limit(df)
        df = self._impute_contact_info(df)
        
        self.logger.info(f"[TRANSFORM] After imputation: {df.shape}")
        
        # Clean & Format
        df = self._rename_columns(df)
        df = self._reorder_columns(df)
        
        self.logger.info(f"[TRANSFORM] Complete | Final shape: {df.shape}")
        
        return df


if __name__ == "__main__":
    DimCustomerTransformer().run()
