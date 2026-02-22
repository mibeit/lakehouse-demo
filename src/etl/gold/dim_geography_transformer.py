import pandas as pd
from pathlib import Path
from src.etl.base_transformer import GoldTransformer, BASE_DIR


class DimGeographyTransformer(GoldTransformer):
    """
    Transform Silver layer Parquets into Gold layer Geography Dimension.
    
    This transformer:
    1. Loads multiple Silver Parquets (customers, cities, provinces, countries, delivery_methods, people)
    2. Performs joins to create a unified customer dimension
    3. Handles missing values (credit_limit, full_name, email_address)
    4. Renames columns to Gold layer standards
    5. Orders columns logically
    6. Saves as Parquet to Gold layer
    """

    _output_filename = "dim_geography.parquet"

    def __init__(self):
        super().__init__(
            silver_path=BASE_DIR / "data" / "silver",
            gold_path=BASE_DIR / "data" / "gold",
            log_file="transform_dim_geography.log",
        )

    # ------------------------------------------------------------------ #
    #  Loading                                                           #
    # ------------------------------------------------------------------ #

    def _load_silver_tables(self) -> dict:
        """Load all required Silver Parquets."""
        self.logger.info("[LOAD] Starting to load Silver Parquets...")
        
        tables = {}

        tables['cities'] = self.load_silver(self.silver_path / "dimensions" / "cities.parquet")
        tables['provinces'] = self.load_silver(self.silver_path / "dimensions" / "provinces.parquet")
        tables['countries'] = self.load_silver(self.silver_path / "dimensions" / "countries.parquet")

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



    def _select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select and order columns for the dimension."""
        self.logger.info("[SELECT] Selecting dimension columns...")
        
        columns = [
            "city_id", "city_name",
            "state_province_id", "state_province_code", "state_province_name",
            "country_id", "country_name", "iso_alpha3_code", "continent", "region", "subregion",
            "sales_territory","latest_recorded_population"
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
            #Geographic Basics
            'city_id': 'city_id',
            'state_province_id': 'state_province_id',
            'country_id': 'country_id',            # Geographic Info
            'city_name': 'city',
            'state_province_code': 'state_code',
            'state_province_name': 'state_name',
            'country_name': 'country',
            'iso_alpha3_code': 'country_code_iso3',
            'continent': 'continent',
            'region': 'region',
            'subregion': 'subregion',
            
            # Sales Info
            'sales_territory': 'sales_territory',
            'latest_recorded_population': 'latest_recorded_population'
        }
        
        df = df.rename(columns=column_mapping)
        self.logger.info(f"[RENAME] Renamed {len(column_mapping)} columns")
        
        return df

    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reorder columns in Gold layer logical grouping."""
        self.logger.info("[REORDER] Applying logical column order...")
        
        column_order = [

            # Geographic
            'city_id', 'city','state_province_id', 'state_code', 'state_name','country_id' ,'country', 'country_code_iso3',
            'continent', 'region', 'subregion',
            # Sales Territory
            'sales_territory', 'latest_recorded_population'
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
        self.logger.info("[TRANSFORM] Starting pipeline: dim_geography")
        
        # Load
        tables = self._load_silver_tables()
        
        # Join
        df = self._build_responsible_market(tables)
        df = self._select_columns(df)
        
        self.logger.info(f"[TRANSFORM] After joins: {df.shape}")
        
        # Clean & Format
        df = self._rename_columns(df)
        df = self._reorder_columns(df)
        
        self.logger.info(f"[TRANSFORM] Complete | Final shape: {df.shape}")
        
        return df


if __name__ == "__main__":
    DimGeographyTransformer().run()
