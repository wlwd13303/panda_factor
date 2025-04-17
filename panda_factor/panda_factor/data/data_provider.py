from abc import ABC, abstractmethod
import pandas as pd
import panda_data
import time
from typing import Optional, List
from panda_common.logger_config import logger

class DataProvider(ABC):
    """Data Provider Interface"""
    
    @abstractmethod
    def get_factor_data(self, factor_name: str, start_date: str, end_date: str, symbols: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """Get factor data"""
        pass

class PandaDataProvider:
    """Use panda_data as the unified data source"""
    
    def __init__(self):
        """Initialize panda_data"""
        panda_data.init()
    
    def get_factor_data(self, factor_name: str, start_date: str, end_date: str, symbols: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """Get factor data from panda_data"""
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                # logger.info(f"Attempting to fetch factor {factor_name} (attempt {attempt + 1})")
                start_time = time.time()
                
                data = panda_data.get_factor(
                    factors=[factor_name],  # Convert to list format
                    start_date=start_date,
                    end_date=end_date,
                    symbols=symbols
                )
                
                if data is None or data.empty:
                    logger.error(f"Failed to fetch factor {factor_name}: empty data")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    return None
                
                # logger.info(f"Successfully fetched factor {factor_name}, took {time.time() - start_time:.2f} seconds")
                return data
                
            except Exception as e:
                logger.error(f"Error while fetching factor {factor_name}: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                return None
                
        logger.error(f"Failed to fetch factor {factor_name}: maximum retry attempts reached")
        return None
            
    def get_available_factors(self) -> List[str]:
        """
        Get list of available factors
        
        Returns:
            list of available factor names
        """
        try:
            # Define base factors that are always available
            base_factors = ['open', 'high', 'low', 'close', 'volume', 'amount','turnover','market_cap']
            
            # For now, just return the base factors
            # TODO: Add support for getting available fields from market data
            return base_factors
        except Exception as e:
            print(f"Error getting available factors: {e}")
            return []

# Additional data provider implementations can be added here, such as Wind, East Money, etc.
class WindProvider(DataProvider):
    def __init__(self, config):
        self.config = config
        # Initialize Wind API
        pass
    
    def get_factor_data(self, factor_name: str, start_date: str, end_date: str, symbols: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        # Wind API implementation
        pass 