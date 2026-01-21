import pytest
import pandas as pd
from src.processing.aggregator import DataAggregator


class TestDataAggregator:
    """Tests for DataAggregator class"""

    @pytest.fixture
    def sample_data(self):
        """Create sample transaction data for testing"""
        return pd.DataFrame({
            "agent_id": ["A001", "A001", "A002"],
            "retailer_id": ["R001", "R002", "R001"],
            "transaction_amount": [1500.0, 2500.0, 3000.0],
            "date": ["2024-01-15", "2024-01-20", "2024-02-10"]
        })

    def test_sales_by_agent(self, sample_data):
        """Test sales aggregation by agent"""
        aggregator = DataAggregator(sample_data)
        result = aggregator.sales_by_agent()

        assert len(result) == 2
        assert "agent_id" in result.columns
        assert "total_sales" in result.columns

        # A001 should have 1500 + 2500 = 4000
        a001_sales = result[result["agent_id"] == "A001"]["total_sales"].values[0]
        assert a001_sales == 4000.0

        # A002 should have 3000
        a002_sales = result[result["agent_id"] == "A002"]["total_sales"].values[0]
        assert a002_sales == 3000.0

    def test_sales_by_retailer(self, sample_data):
        """Test sales aggregation by retailer"""
        aggregator = DataAggregator(sample_data)
        result = aggregator.sales_by_retailer()

        assert len(result) == 2
        assert "retailer_id" in result.columns
        assert "total_sales" in result.columns

        # R001 should have 1500 + 3000 = 4500
        r001_sales = result[result["retailer_id"] == "R001"]["total_sales"].values[0]
        assert r001_sales == 4500.0

    def test_monthly_totals(self, sample_data):
        """Test monthly totals aggregation"""
        aggregator = DataAggregator(sample_data)
        result = aggregator.monthly_totals()

        assert len(result) == 2
        assert "month" in result.columns
        assert "total_sales" in result.columns

    def test_commission_rate_below_threshold(self, sample_data):
        """Test 5% commission rate for sales < 5000"""
        aggregator = DataAggregator(sample_data)
        result = aggregator.calculate_commission()

        # A001 has 4000 in sales (< 5000), should get 5%
        a001 = result[result["agent_id"] == "A001"]
        assert a001["commission_rate"].values[0] == 0.05
        assert a001["commission_amount"].values[0] == 200.0  # 4000 * 0.05

    def test_commission_rate_above_threshold(self):
        """Test 8% commission rate for sales >= 5000"""
        data = pd.DataFrame({
            "agent_id": ["A001", "A001"],
            "retailer_id": ["R001", "R002"],
            "transaction_amount": [3000.0, 3000.0],
            "date": ["2024-01-15", "2024-01-20"]
        })

        aggregator = DataAggregator(data)
        result = aggregator.calculate_commission()

        # A001 has 6000 in sales (>= 5000), should get 8%
        assert result["commission_rate"].values[0] == 0.08
        assert result["commission_amount"].values[0] == 480.0  # 6000 * 0.08
