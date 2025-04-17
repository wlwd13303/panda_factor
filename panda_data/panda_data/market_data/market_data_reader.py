from panda_common.logger_config import logger
from panda_common.handlers.database_handler import DatabaseHandler
import pandas as pd

class MarketDataReader:
    def __init__(self, config):
        self.config = config
        # Initialize DatabaseHandler
        self.db_handler = DatabaseHandler(config)
        self.all_symbols = self.get_all_symbols()
    
    def get_market_data(self, symbols, start_date, end_date, indicator="000985", st=True, fields=None):
        """
        Get market data for given symbols and date range
        
        Args:
            symbols: List of stock symbols or single symbol
            fields: List of fields to retrieve (e.g., ['open', 'close', 'volume'])
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            
        Returns:
            pandas DataFrame with market data
        """
        # If fields is None, return all fields
        if fields is None:
            fields = []
            
        # Convert parameters to list if they're not already
        if isinstance(symbols, str):
            symbols = [symbols]
        if isinstance(fields, str):
            fields = [fields]
        
        if symbols is None:
            symbols = self.all_symbols
            
        # Build query
        query = {
            "symbol": {"$in": symbols},
            "date": {
                "$gte": start_date,
                "$lte": end_date
            }
        }
        if indicator != "000985":
            if indicator == "000300":
                query["index_component"] = "100"
            elif indicator == "000905":
                query["index_component"] = "010"
            elif indicator == "000852":
                query["index_component"] = "001"
        if not st:
            query["name"] = {"$not": {"$regex": "ST"}}
        
        # Get data from MongoDB
        records = self.db_handler.mongo_find(
            self.config["MONGO_DB"],
            "stock_market",
            query
        )
        
        if not records:
            logger.warning(f"No market data found for the specified parameters")
            return None
            
        # Convert to DataFrame
        df = pd.DataFrame(list(records))
        
        # Select only requested fields
        if fields:
            available_fields = [field for field in fields if field in df.columns]
            if not available_fields:
                logger.warning("No requested fields found in the data")
                return None
            df = df[['date', 'symbol'] + available_fields]
            
        return df.drop(columns=['_id'])
    
    def get_all_symbols(self):
        """Get all unique symbols using distinct command"""
        collection = self.db_handler.get_mongo_collection(
            self.config["MONGO_DB"],
            "stock_market"
        )
        return collection.distinct("symbol") 