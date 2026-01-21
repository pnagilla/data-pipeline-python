from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from src.storage.models import Agent, Retailer, Transaction, Commission

app = FastAPI(title="Data Pipeline API")

# Database connection
engine = create_engine("sqlite:///data/pipeline.db")
Session = sessionmaker(bind=engine)

@app.get("/")
def root():
    return {"message": "Data Pipeline API is running"}

@app.get("/agents/{agent_id}/commission")
def get_agent_commission(agent_id: str):
    """Get commission details for a specific agent"""
    session = Session()
    try:
        commission = session.query(Commission).filter_by(agent_id=agent_id).first()
        if not commission:
            raise HTTPException(status_code=404, detail=f"Agent {agent_id} not found")
        
        return {
            "agent_id": commission.agent_id,
            "total_sales": commission.total_sales,
            "commission_rate": commission.commission_rate,
            "commission_amount": commission.commission_amount
        }
    finally:
        session.close()

@app.get("/retailers/{retailer_id}/sales")
def get_retailer_sales(retailer_id: str):
    """Get total sales for a specific retailer"""
    session = Session()
    try:
        total_sales = session.query(func.sum(Transaction.transaction_amount)).filter_by(retailer_id=retailer_id).scalar()
        
        if total_sales is None:
            raise HTTPException(status_code=404, detail=f"Retailer {retailer_id} not found")
        
        return {
            "retailer_id": retailer_id,
            "total_sales": total_sales
        }
    finally:
        session.close()

@app.get("/reports/monthly")
def get_monthly_report():
    """Get monthly sales report"""
    session = Session()
    try:
        transactions = session.query(Transaction).all()
        
        # Aggregate by month
        monthly_data = {}
        for t in transactions:
            month_key = t.date.strftime("%Y-%m")
            if month_key not in monthly_data:
                monthly_data[month_key] = 0
            monthly_data[month_key] += t.transaction_amount
        
        return {
            "monthly_sales": [
                {"month": month, "total_sales": sales}
                for month, sales in sorted(monthly_data.items())
            ]
        }
    finally:
        session.close()

@app.get("/agents")
def get_all_agents():
    """Get all agents with their commissions"""
    session = Session()
    try:
        commissions = session.query(Commission).all()
        return {
            "agents": [
                {
                    "agent_id": c.agent_id,
                    "total_sales": c.total_sales,
                    "commission_rate": c.commission_rate,
                    "commission_amount": c.commission_amount
                }
                for c in commissions
            ]
        }
    finally:
        session.close()

@app.get("/retailers")
def get_all_retailers():
    """Get all retailers with their sales"""
    session = Session()
    try:
        retailers = session.query(Retailer).all()
        result = []
        for r in retailers:
            total_sales = session.query(func.sum(Transaction.transaction_amount)).filter_by(retailer_id=r.retailer_id).scalar() or 0
            result.append({
                "retailer_id": r.retailer_id,
                "total_sales": total_sales
            })
        return {"retailers": result}
    finally:
        session.close()