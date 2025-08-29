"""
모든 크롤러의 베이스 클래스
"""
from abc import ABC, abstractmethod
import time
import logging
from typing import Dict, Any, List
import random

logger = logging.getLogger(__name__)

class BaseCrawler(ABC):
    """모든 크롤러의 베이스 클래스"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.retry_count = config.get('max_retries', 3)
        self.delay = config.get('delay', 2)
        self.timeout = config.get('timeout', 30)
        self.user_agent = config.get('user_agent', 
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        
    def retry_with_backoff(self, func, *args, **kwargs):
        """공통 재시도 로직"""
        for attempt in range(self.retry_count):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt < self.retry_count - 1:
                    wait_time = self.delay * (2 ** attempt)
                    jitter = random.uniform(0, 0.1) * wait_time
                    total_wait = wait_time + jitter
                    
                    logger.warning(f"시도 {attempt + 1} 실패: {e}")
                    logger.warning(f"{total_wait:.2f}초 후 재시도")
                    time.sleep(total_wait)
                else:
                    logger.error(f"모든 재시도 실패: {e}")
                    raise
    
    def safe_sleep(self, duration: float = None):
        """안전한 대기 (랜덤 지터 포함)"""
        if duration is None:
            duration = self.delay
        
        jitter = random.uniform(0.5, 1.5)
        actual_delay = duration * jitter
        time.sleep(actual_delay)
    
    def validate_response(self, response) -> bool:
        """응답 유효성 검증"""
        if not response:
            return False
        
        if hasattr(response, 'status_code'):
            return 200 <= response.status_code < 300
        
        return True
    
    def log_crawl_stats(self, source: str, success_count: int, error_count: int, total_time: float):
        """크롤링 통계 로깅"""
        logger.info(f"[{source}] 크롤링 완료")
        logger.info(f"  성공: {success_count}개, 실패: {error_count}개")
        logger.info(f"  소요시간: {total_time:.2f}초")
        logger.info(f"  평균 처리시간: {total_time/(success_count+error_count):.2f}초/항목")
    
    @abstractmethod
    def crawl_and_save(self, items: List[Any]) -> Dict[str, Any]:
        """각 크롤러가 구현해야 할 메서드"""
        pass
    
    @abstractmethod
    def get_source_name(self) -> str:
        """크롤러의 소스명 반환"""
        pass