import pandas as pd

class DataAggregator:
    def __init__(self, df: pd.DataFrame):
        self.df = df
    
    def sales_by_agent(self) -> pd.DataFrame:
        """Group by agent_id and sum transaction_amount"""
        result = self.df.groupby("agent_id")["transaction_amount"].sum().reset_index()
        result.columns = ["agent_id", "total_sales"]
        return result
    
    def sales_by_retailer(self) -> pd.DataFrame:
        """Group by retailer_id and sum transaction_amount"""
        result = self.df.groupby("retailer_id")["transaction_amount"].sum().reset_index()
        result.columns = ["retailer_id", "total_sales"]
        return result
    
    def monthly_totals(self) -> pd.DataFrame:
        """Extract month from date and sum transaction_amount by month"""
        df_copy = self.df.copy()
        df_copy["month"] = pd.to_datetime(df_copy["date"]).dt.to_period("M")
        result = df_copy.groupby("month")["transaction_amount"].sum().reset_index()
        result.columns = ["month", "total_sales"]
        return result
    
    def calculate_commission(self) -> pd.DataFrame:
        """Calculate commission: 5% if sales < 5000, 8% if sales >= 5000"""
        agent_sales = self.sales_by_agent()
        
        # Apply commission rate based on sales
        agent_sales["commission_rate"] = agent_sales["total_sales"].apply(
            lambda x: 0.08 if x >= 5000 else 0.05
        )
        
        # Calculate commission amount
        agent_sales["commission_amount"] = agent_sales["total_sales"] * agent_sales["commission_rate"]
        
        return agent_sales