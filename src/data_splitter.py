"""
Data Splitter for WWI Lakehouse Project
Splits data by date columns and years for incremental ingestion
"""

import polars as pl
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataSplitter:
    """
    Splits WWI data by dates and years.
    Supports different date columns per table.
    """
    
    # Table configuration: {Table: DateColumn}
    TABLE_CONFIG = {
        # LastEditedWhen column
        'purchase.order': 'LastEditedWhen',
        'purchase.orderline': 'LastEditedWhen',
        'sales.incvoiceslines': 'LastEditedWhen',
        'sales.invoices': 'LastEditedWhen',
        'sales.order': 'LastEditedWhen',
        'sales.orderline': 'LastEditedWhen',
        
        # ValidFrom column
        'application.people': 'ValidFrom',
        'sales.customer': 'ValidFrom',
    }
    
    def __init__(self, raw_dir: str = 'data/raw', bronze_dir: str = 'data/bronze'):
        """
        Initialize DataSplitter
        
        Args:
            raw_dir: Directory with raw CSVs
            bronze_dir: Target directory for split data
        """
        self.raw_path = Path(raw_dir)
        self.bronze_path = Path(bronze_dir)
        self.raw_path.mkdir(parents=True, exist_ok=True)
        self.bronze_path.mkdir(parents=True, exist_ok=True)
    
    def get_csv_filename(self, table_name: str) -> str:
        """Convert table name to CSV filename"""
        # e.g. 'purchase.order' -> 'purchase.order.csv'
        return f"{table_name}.csv"
    
    def load_table(self, table_name: str) -> Optional[pl.DataFrame]:
        """Load a table from CSV"""
        csv_file = self.raw_path / self.get_csv_filename(table_name)
        
        if not csv_file.exists():
            logger.warning(f"File not found: {csv_file}")
            return None
        
        try:
            df = pl.read_csv(csv_file, truncate_ragged_lines=True, separator=";", infer_schema_length=10000)
            logger.info(f"Loaded {table_name}: {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Error loading {table_name}: {e}")
            return None
    
    def parse_date_column(self, df: pl.DataFrame, date_col: str) -> pl.DataFrame:
        """Parse date column to DateTime format"""
        if date_col not in df.columns:
            logger.warning(f"Date column '{date_col}' not found")
            return df
        
        try:
            # Try different formats
            formats = [
                '%Y-%m-%d %H:%M:%S%.f',  # 2013-04-15 12:34:56.123
                '%Y-%m-%d %H:%M:%S',     # 2013-04-15 12:34:56
                '%Y-%m-%d',               # 2013-04-15
                '%d/%m/%Y',               # 15/04/2013
            ]
            
            for fmt in formats:
                try:
                    df = df.with_columns(
                        pl.col(date_col).str.to_datetime(fmt).alias(f"{date_col}_parsed")
                    )
                    if f"{date_col}_parsed" in df.columns:
                        return df
                except:
                    continue
            
            logger.warning(f"Could not parse '{date_col}' with any format")
            return df
        except Exception as e:
            logger.warning(f"Error parsing date: {e}")
            return df
    
    def split_by_year_range(self, df: pl.DataFrame, date_col: str, 
                           start_year: int, end_year: int) -> pl.DataFrame:
        """Filter data by year range"""
        if f"{date_col}_parsed" not in df.columns:
            df = self.parse_date_column(df, date_col)
        
        if f"{date_col}_parsed" not in df.columns:
            logger.warning(f"No parsed date column available, using original")
            return df
        
        # Extract year and filter
        df_filtered = df.with_columns(
            pl.col(f"{date_col}_parsed").dt.year().alias("year")
        ).filter(
            (pl.col("year") >= start_year) & (pl.col("year") <= end_year)
        ).drop("year")
        
        return df_filtered
    
    def save_table(self, df: pl.DataFrame, table_name: str, folder: str) -> None:
        """Save table as CSV to target folder"""
        # Create target directory: data/bronze/{folder}/
        target_dir = self.bronze_path / folder
        target_dir.mkdir(parents=True, exist_ok=True)
        
        # Save file with table name
        output_file = target_dir / f"{table_name}.csv"
        df.write_csv(output_file)
        logger.info(f"Saved: {output_file} ({len(df)} rows)")
    
    def process_table(self, table_name: str, start_year: int, end_year: int, 
                     year_range_label: str) -> bool:
        """Process a single table"""
        logger.info(f"Processing: {table_name}")
        
        # Load table
        df = self.load_table(table_name)
        if df is None:
            return False
        
        # Get date column from configuration
        date_col = self.TABLE_CONFIG.get(table_name)
        if not date_col:
            logger.warning(f"No date column configured for {table_name}")
            return False
        
        logger.info(f"Date column: {date_col}")
        logger.info(f"Filter: {start_year}-{end_year}")
        
        # Parse and filter by year
        df_parsed = self.parse_date_column(df, date_col)
        df_filtered = self.split_by_year_range(df_parsed, date_col, start_year, end_year)
        
        if len(df_filtered) == 0:
            logger.warning(f"No data found for {start_year}-{end_year}")
            return False
        
        # Save
        self.save_table(df_filtered, table_name, year_range_label)
        return True
    
    def process_all(self, start_year: int = 2013, end_year: int = 2014, 
                   year_range_label: str = "2013-2014") -> None:
        """Process all configured tables"""
        logger.info("=" * 60)
        logger.info(f"DataSplitter - Ingest Phase: {year_range_label}")
        logger.info("=" * 60)
        
        processed = 0
        failed = 0
        
        for table_name in self.TABLE_CONFIG.keys():
            try:
                if self.process_table(table_name, start_year, end_year, year_range_label):
                    processed += 1
                    logger.info(f"  ✅ {table_name}")
                else:
                    failed += 1
                    logger.warning(f"  ❌ {table_name}")
            except Exception as e:
                logger.error(f"  ❌ {table_name} - Error: {e}")
                failed += 1
        
        logger.info("\n" + "=" * 60)
        logger.info(f"Processing completed")
        logger.info(f"Successful: {processed}/{len(self.TABLE_CONFIG)}")
        logger.info(f"Failed: {failed}/{len(self.TABLE_CONFIG)}")
        logger.info(f"Target directory: {self.bronze_path / year_range_label}")
        logger.info("=" * 60 + "\n")
    
    def create_upcoming_placeholder(self) -> None:
        """Save all unfiltered tables to upcoming folder"""
        logger.info("\nProcessing upcoming tables (unfiltered)...")
        
        saved_count = 0
        failed_count = 0
        
        for table_name in self.TABLE_CONFIG.keys():
            df = self.load_table(table_name)
            if df is None:
                failed_count += 1
                logger.warning(f"  ❌ {table_name}")
                continue
            
            try:
                self.save_table(df, table_name, "upcoming")
                saved_count += 1
                logger.info(f"  ✅ {table_name}")
            except Exception as e:
                logger.error(f"  ❌ {table_name} - Error: {e}")
                failed_count += 1
        
        logger.info(f"Upcoming: Saved {saved_count} tables, Failed {failed_count}")


def main():
    """Main function"""
    splitter = DataSplitter()
    
    # Phase 1: 2013-2014 (Initial Ingest)
    splitter.process_all(
        start_year=2013,
        end_year=2014,
        year_range_label="2013-2014"
    )
    
    # Create upcoming folder with all unfiltered data
    splitter.create_upcoming_placeholder()


if __name__ == '__main__':
    main()
