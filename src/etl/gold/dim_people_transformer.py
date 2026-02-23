import pandas as pd
from pathlib import Path
from src.etl.base_transformer import GoldTransformer, BASE_DIR


class DimPeopleTransformer(GoldTransformer):
    """
    Transform Silver layer Parquets into Gold layer People Dimension.
    
    This transformer:
    1. Loads multiple Silver Parquets (customers, cities, provinces, countries, delivery_methods, people)
    2. Performs joins to create a unified people dimension
    3. Handles missing values (credit_limit, full_name, email_address)
    4. Renames columns to Gold layer standards
    5. Orders columns logically
    6. Saves as Parquet to Gold layer
    """

    _output_filename = "dim_people.parquet"

    def __init__(self):
        super().__init__(
            silver_path=BASE_DIR / "data" / "silver",
            gold_path=BASE_DIR / "data" / "gold",
            log_file="transform_dim_people.log",
        )

    # ------------------------------------------------------------------ #
    #  Loading                                                           #
    # ------------------------------------------------------------------ #

    def _load_silver_tables(self) -> dict:
        """Load all required Silver Parquets."""
        self.logger.info("[LOAD] Starting to load Silver Parquets...")
        
        tables = {}

        tables['people'] = self.load_silver(self.silver_path / "dimensions" / "people.parquet")
        
        return tables

    # ------------------------------------------------------------------ #
    #  Joining & Building Dimension                                     #
    # ------------------------------------------------------------------ #

    def _build_people_dimension(self, tables: dict) -> pd.DataFrame:
        """Build responsible_market table from provinces, countries, and cities."""
        self.logger.info("[SELECT] Building people dimension...")
        
        # Join provinces with countries
        people = tables['people'].copy()
        self.logger.info(f"[SELECT] After people: {people.shape}")

        return people


    def _select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select and order columns for the dimension."""
        self.logger.info("[SELECT] Selecting dimension columns...")
        
        columns = [
            "person_id",
            "full_name",
            "email_address",
            "phone_number",
            "fax_number"
        ]
        
        df = df[columns]
        self.logger.info(f"[SELECT] Selected {len(columns)} columns")
        
        return df

    def _filter_person(self, df: pd.DataFrame) -> pd.DataFrame:
        """Exclude system person (person_id == 1) from the dimension."""
        self.logger.info("[FILTER] Excluding person_id == 1...")
        df = df[df["person_id"] != 1]
        self.logger.info(f"[FILTER] After filter: {df.shape}")
        return df


    # ------------------------------------------------------------------ #
    #  Orchestration                                                     #
    # ------------------------------------------------------------------ #

    def transform(self) -> pd.DataFrame:
        """
        Orchestrate full Silver -> Gold transformation pipeline.
        Returns the transformed DataFrame (does NOT take a df parameter).
        """
        self.logger.info("[TRANSFORM] Starting pipeline: dim_people")
        
        # Load
        tables = self._load_silver_tables()
        
        df = self._build_people_dimension(tables)
        df = self._select_columns(df)
        df = self._filter_person(df)
        
        self.logger.info(f"[TRANSFORM] After Build: {df.shape}")
    
        
        return df


if __name__ == "__main__":
    DimPeopleTransformer().run()
