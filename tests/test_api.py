import pytest
from fastapi.testclient import TestClient
from src.api.routes import app


client = TestClient(app)


class TestAPI:
    """Tests for FastAPI endpoints"""

    def test_root_endpoint(self):
        """Test the root endpoint returns correct message"""
        response = client.get("/")
        assert response.status_code == 200
        assert response.json() == {"message": "Data Pipeline API is running"}

    def test_get_all_agents(self):
        """Test getting all agents"""
        response = client.get("/agents")
        assert response.status_code == 200
        assert "agents" in response.json()

    def test_get_all_retailers(self):
        """Test getting all retailers"""
        response = client.get("/retailers")
        assert response.status_code == 200
        assert "retailers" in response.json()

    def test_get_monthly_report(self):
        """Test getting monthly report"""
        response = client.get("/reports/monthly")
        assert response.status_code == 200
        assert "monthly_sales" in response.json()

    def test_get_agent_commission_not_found(self):
        """Test 404 for non-existent agent"""
        response = client.get("/agents/NONEXISTENT/commission")
        assert response.status_code == 404

    def test_get_retailer_sales_not_found(self):
        """Test 404 for non-existent retailer"""
        response = client.get("/retailers/NONEXISTENT/sales")
        assert response.status_code == 404
