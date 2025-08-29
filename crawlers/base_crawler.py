"""
Base class for all crawlers
"""
from abc import ABC, abstractmethod
import time
import logging
from typing import Dict, Any, List
import random

logger = logging.getLogger(__name__)

class BaseCrawler(ABC):
    """Base class for all crawlers"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.retry_count = config.get('max_retries', 3)
        self.delay = config.get('delay', 2)
        self.timeout = config.get('timeout', 30)
        self.user_agent = config.get('user_agent', 
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        
    def retry_with_backoff(self, func, *args, **kwargs):
        """Common retry logic"""
        for attempt in range(self.retry_count):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt < self.retry_count - 1:
                    wait_time = self.delay * (2 ** attempt)
                    jitter = random.uniform(0, 0.1) * wait_time
                    total_wait = wait_time + jitter
                    
                    logger.warning(f"Attempt {attempt + 1} failed: {e}")
                    logger.warning(f"Retrying in {total_wait:.2f} seconds")
                    time.sleep(total_wait)
                else:
                    logger.error(f"All retry attempts failed: {e}")
                    raise
    
    def safe_sleep(self, duration: float = None):
        """Safe wait (with random jitter)"""
        if duration is None:
            duration = self.delay
        
        jitter = random.uniform(0.5, 1.5)
        actual_delay = duration * jitter
        time.sleep(actual_delay)
    
    def validate_response(self, response) -> bool:
        """Validate response"""
        if not response:
            return False
        
        if hasattr(response, 'status_code'):
            return 200 <= response.status_code < 300
        
        return True
    
    def log_crawl_stats(self, source: str, success_count: int, error_count: int, total_time: float):
        """Log crawling statistics"""
        logger.info(f"[{source}] Crawling completed")
        logger.info(f"  Success: {success_count}, Failed: {error_count}")
        logger.info(f"  Duration: {total_time:.2f}s")
        logger.info(f"  Average processing time: {total_time/(success_count+error_count):.2f}s/item")
    
    @abstractmethod
    def crawl_and_save(self, items: List[Any]) -> Dict[str, Any]:
        """Method that each crawler must implement"""
        pass
    
    @abstractmethod
    def get_source_name(self) -> str:
        """Return crawler source name"""
        pass