import pytest
import pandas as pd
import os
import tempfile
from src.ingestion.reader import DataReader


class TestDataReader:
    """Tests for DataReader class"""

    def test_read_valid_csv(self, tmp_path):
        """Test reading a valid CSV file"""
        # Create a temp CSV file
        csv_content = """agent_id,retailer_id,transaction_amount,date
A001,R001,1500.00,2024-01-15
A002,R002,2000.00,2024-01-16"""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        reader = DataReader(str(csv_file))
        df = reader.read_csv()

        assert len(df) == 2
        assert list(df.columns) == ["agent_id", "retailer_id", "transaction_amount", "date"]

    def test_file_not_found(self):
        """Test that FileNotFoundError is raised for missing file"""
        reader = DataReader("nonexistent.csv")

        with pytest.raises(FileNotFoundError):
            reader.read_csv()

    def test_validate_all_valid(self, tmp_path):
        """Test validation with all valid rows"""
        csv_content = """agent_id,retailer_id,transaction_amount,date
A001,R001,1500.00,2024-01-15
A002,R002,2000.00,2024-01-16"""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        reader = DataReader(str(csv_file))
        df = reader.read_csv()
        valid_df, invalid_df = reader.validate(df)

        assert len(valid_df) == 2
        assert len(invalid_df) == 0

    def test_validate_missing_agent_id(self, tmp_path):
        """Test validation catches missing agent_id"""
        csv_content = """agent_id,retailer_id,transaction_amount,date
,R001,1500.00,2024-01-15
A002,R002,2000.00,2024-01-16"""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        reader = DataReader(str(csv_file))
        df = reader.read_csv()
        valid_df, invalid_df = reader.validate(df)

        assert len(valid_df) == 1
        assert len(invalid_df) == 1

    def test_validate_invalid_amount(self, tmp_path):
        """Test validation catches non-numeric transaction_amount"""
        csv_content = """agent_id,retailer_id,transaction_amount,date
A001,R001,invalid,2024-01-15
A002,R002,2000.00,2024-01-16"""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        reader = DataReader(str(csv_file))
        df = reader.read_csv()
        valid_df, invalid_df = reader.validate(df)

        assert len(valid_df) == 1
        assert len(invalid_df) == 1

    def test_ingest_returns_valid_data(self, tmp_path):
        """Test full ingest pipeline returns only valid data"""
        csv_content = """agent_id,retailer_id,transaction_amount,date
A001,R001,1500.00,2024-01-15
,R001,500.00,2024-01-16
A002,R002,2000.00,2024-01-17"""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        reader = DataReader(str(csv_file))
        valid_data = reader.ingest()

        assert len(valid_data) == 2
        assert valid_data["transaction_amount"].dtype == float
