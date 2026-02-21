import pandas as pd
import yaml
from src.etl.base_transformer import SilverTransformer, BASE_DIR


# Load config once at module level
CONFIG_PATH = BASE_DIR / "src" / "config" / "dimensions.yml"

with open(CONFIG_PATH, "r") as f:
    DIMENSIONS_CONFIG = yaml.safe_load(f)["dimensions"]


class DimensionTransformer(SilverTransformer):
    """
    Generic transformer for simple dimension tables.
    Driven entirely by dimensions.yml config – no table-specific code needed.
    """

    def __init__(self, dimension_name: str):
        """
        Args:
            dimension_name: Key in dimensions.yml (e.g. "colors", "countries")
        """
        if dimension_name not in DIMENSIONS_CONFIG:
            raise ValueError(f"Unknown dimension: '{dimension_name}'. Check dimensions.yml")

        self.config = DIMENSIONS_CONFIG[dimension_name]
        self._output_filename = self.config["silver_file"]

        super().__init__(
            bronze_path=BASE_DIR / "data" / "bronze" / "actual" / self.config["bronze_file"],
            silver_path=BASE_DIR / "data" / "silver" / "dimensions",
            log_file=self.config["log_file"]
        )

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns=self.config["rename"])
        self.logger.info("[TRANSFORM] Columns renamed to snake_case")
        return df

    def _cast_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        # Rename first so we use snake_case names
        renamed = {v: v for v in self.config["rename"].values()}
        date_cols_renamed = [
            self.config["rename"].get(col, col)
            for col in self.config["date_columns"]
        ]
        for col in date_cols_renamed:
            if col in df.columns:
                df[col] = self._to_datetime(df[col])
                self.logger.info(f"[TRANSFORM] Cast to datetime: {col}")
        return df

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        expected = {"valid_to": "Currently active records"} if "valid_to" in df.columns else {}
        required = [col for col in df.columns if col != "valid_to"]
        return self._validate_nulls(df, expected, required)


if __name__ == "__main__":
    # Alle generischen Dimensions auf einmal ausführen
    for name in DIMENSIONS_CONFIG:
        transformer = DimensionTransformer(name)
        transformer.run()
