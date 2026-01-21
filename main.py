import sys
from src.utils.logger import setup_logger

def run_pipeline():
    """Run the data pipeline"""
    logger = setup_logger("main", "logs/pipeline.log")
    logger.info("=== Starting Data Pipeline ===")
    
    try:
        # Import here to ensure logger is set up first
        from src.ingestion.reader import DataReader
        from src.processing.aggregator import DataAggregator
        from src.storage.database import Database
        
        # Phase 1: Ingest
        logger.info("Phase 1: Data Ingestion")
        reader = DataReader("data/transactions.csv")
        valid_data = reader.ingest()
        
        # Phase 2: Process
        logger.info("Phase 2: Data Processing")
        aggregator = DataAggregator(valid_data)
        
        print("\n--- Sales by Agent ---")
        print(aggregator.sales_by_agent())
        
        print("\n--- Sales by Retailer ---")
        print(aggregator.sales_by_retailer())
        
        print("\n--- Monthly Totals ---")
        print(aggregator.monthly_totals())
        
        print("\n--- Commissions ---")
        commissions = aggregator.calculate_commission()
        print(commissions)
        
        # Phase 3: Storage
        logger.info("Phase 3: Data Storage")
        print("\n--- Saving to Database ---")
        db = Database()
        db.save_all(valid_data, commissions)
        db.close()
        
        logger.info("=== Pipeline Completed Successfully ===")
        
    except FileNotFoundError as e:
        logger.error(f"File error: {e}")
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        print(f"Error: {e}")
        sys.exit(1)

def run_api():
    """Run the API server"""
    import uvicorn
    from src.api.routes import app
    print("Starting API server at http://localhost:8000")
    print("API docs available at http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "api":
        run_api()
    else:
        run_pipeline()