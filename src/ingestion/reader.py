import pandas as pd
import os
from src.utils.logger import setup_logger

class DataReader:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.logger = setup_logger("ingestion", "logs/pipeline.log")
    
    def read_csv(self) -> pd.DataFrame:
        """Read CSV file and return raw dataframe"""
        self.logger.info(f"Reading file: {self.file_path}")
        
        if not os.path.exists(self.file_path):
            self.logger.error(f"File not found: {self.file_path}")
            raise FileNotFoundError(f"File not found: {self.file_path}")
        
        try:
            df = pd.read_csv(self.file_path)
            self.logger.info(f"Successfully read {len(df)} rows from file")
            return df
        except Exception as e:
            self.logger.error(f"Error reading file: {e}")
            raise
    
    def validate(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Validate data and return (valid_df, invalid_df)"""
        self.logger.info("Starting data validation")
        invalid_mask = pd.Series([False] * len(df))
        
        # Check for missing agent_id
        invalid_mask = invalid_mask | df["agent_id"].isna()
        
        # Check for missing retailer_id
        invalid_mask = invalid_mask | df["retailer_id"].isna()
        
        # Check for missing or non-numeric transaction_amount
        invalid_mask = invalid_mask | df["transaction_amount"].isna()
        invalid_mask = invalid_mask | ~pd.to_numeric(df["transaction_amount"], errors="coerce").notna()
        
        # Check for missing date
        invalid_mask = invalid_mask | df["date"].isna()
        
        valid_df = df[~invalid_mask].copy()
        invalid_df = df[invalid_mask].copy()

        # Convert transaction_amount to numeric for valid rows
        valid_df["transaction_amount"] = pd.to_numeric(valid_df["transaction_amount"])
        
        self.logger.info(f"Validation complete: {len(valid_df)} valid, {len(invalid_df)} invalid")
        return valid_df, invalid_df
    
    def log_rejected(self, invalid_df: pd.DataFrame):
        """Log each rejected row"""
        for index, row in invalid_df.iterrows():
            self.logger.warning(f"Rejected row {index}: {row.to_dict()}")
    
    def ingest(self) -> pd.DataFrame:
        """Main method: read, validate, log rejected, return valid data"""
        try:
            df = self.read_csv()
            valid_df, invalid_df = self.validate(df)
            
            if not invalid_df.empty:
                self.log_rejected(invalid_df)
            
            self.logger.info(f"Ingestion complete: {len(valid_df)} valid rows")
            return valid_df
        except Exception as e:
            self.logger.error(f"Ingestion failed: {e}")
            raise