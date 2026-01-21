from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, create_engine
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Agent(Base):
    __tablename__ = "agents"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    agent_id = Column(String, unique=True, nullable=False)
    
    transactions = relationship("Transaction", back_populates="agent")
    commissions = relationship("Commission", back_populates="agent")

class Retailer(Base):
    __tablename__ = "retailers"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    retailer_id = Column(String, unique=True, nullable=False)
    
    transactions = relationship("Transaction", back_populates="retailer")

class Transaction(Base):
    __tablename__ = "transactions"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    agent_id = Column(String, ForeignKey("agents.agent_id"), nullable=False)
    retailer_id = Column(String, ForeignKey("retailers.retailer_id"), nullable=False)
    transaction_amount = Column(Float, nullable=False)
    date = Column(Date, nullable=False)
    
    agent = relationship("Agent", back_populates="transactions")
    retailer = relationship("Retailer", back_populates="transactions")

class Commission(Base):
    __tablename__ = "commissions"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    agent_id = Column(String, ForeignKey("agents.agent_id"), nullable=False)
    total_sales = Column(Float, nullable=False)
    commission_rate = Column(Float, nullable=False)
    commission_amount = Column(Float, nullable=False)
    
    agent = relationship("Agent", back_populates="commissions")