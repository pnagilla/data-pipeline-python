import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.storage.models import Base, Agent, Retailer, Transaction, Commission
from src.utils.logger import setup_logger

class Database:
    def __init__(self, db_url: str = "sqlite:///data/pipeline.db"):
        self.logger = setup_logger("database", "logs/pipeline.log")
        self.logger.info(f"Connecting to database: {db_url}")
        
        try:
            self.engine = create_engine(db_url)
            Base.metadata.create_all(self.engine)
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            self.logger.info("Database connection established")
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            raise
    
    def save_agents(self, df: pd.DataFrame):
        """Save unique agents to database"""
        for agent_id in df["agent_id"].unique():
            existing = self.session.query(Agent).filter_by(agent_id=agent_id).first()
            if not existing:
                agent = Agent(agent_id=agent_id)
                self.session.add(agent)
        self.session.commit()
        self.logger.debug(f"Saved {df['agent_id'].nunique()} agents")
    
    def save_retailers(self, df: pd.DataFrame):
        """Save unique retailers to database"""
        for retailer_id in df["retailer_id"].unique():
            existing = self.session.query(Retailer).filter_by(retailer_id=retailer_id).first()
            if not existing:
                retailer = Retailer(retailer_id=retailer_id)
                self.session.add(retailer)
        self.session.commit()
        self.logger.debug(f"Saved {df['retailer_id'].nunique()} retailers")
    
    def save_transactions(self, df: pd.DataFrame):
        """Save transactions to database"""
        for _, row in df.iterrows():
            transaction = Transaction(
                agent_id=row["agent_id"],
                retailer_id=row["retailer_id"],
                transaction_amount=row["transaction_amount"],
                date=pd.to_datetime(row["date"]).date()
            )
            self.session.add(transaction)
        self.session.commit()
        self.logger.debug(f"Saved {len(df)} transactions")
    
    def save_commissions(self, df: pd.DataFrame):
        """Save commission data to database"""
        # Clear existing commissions (recalculated each run)
        self.session.query(Commission).delete()
        
        for _, row in df.iterrows():
            commission = Commission(
                agent_id=row["agent_id"],
                total_sales=row["total_sales"],
                commission_rate=row["commission_rate"],
                commission_amount=row["commission_amount"]
            )
            self.session.add(commission)
        self.session.commit()
        self.logger.debug(f"Saved {len(df)} commission records")
    
    def save_all(self, transactions_df: pd.DataFrame, commissions_df: pd.DataFrame):
        """Save all data to database"""
        self.logger.info("Starting database save...")
        try:
            self.save_agents(transactions_df)
            self.save_retailers(transactions_df)
            self.save_transactions(transactions_df)
            self.save_commissions(commissions_df)
            self.logger.info("All data saved successfully")
        except Exception as e:
            self.logger.error(f"Database save failed: {e}")
            self.session.rollback()
            raise
    
    def close(self):
        """Close the database session"""
        self.session.close()
        self.logger.info("Database connection closed")