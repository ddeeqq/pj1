"""
통합 로깅 시스템 설정
"""
import os
import logging.config
from datetime import datetime

# 로그 디렉토리 생성
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# 로깅 설정
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
        },
        'simple': {
            'format': '%(asctime)s - %(levelname)s - %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'simple',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'detailed',
            'filename': f'{LOG_DIR}/app.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5,
            'encoding': 'utf-8'
        },
        'error_file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'ERROR',
            'formatter': 'detailed',
            'filename': f'{LOG_DIR}/error.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 3,
            'encoding': 'utf-8'
        }
    },
    'loggers': {
        'crawlers': {
            'level': 'INFO',
            'handlers': ['console', 'file'],
            'propagate': False
        },
        'analyzers': {
            'level': 'INFO',
            'handlers': ['console', 'file'],
            'propagate': False
        },
        'database': {
            'level': 'WARNING',
            'handlers': ['console', 'file', 'error_file'],
            'propagate': False
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console', 'file']
    }
}

def setup_logging():
    """로깅 시스템 초기화"""
    logging.config.dictConfig(LOGGING_CONFIG)
    
    # 시작 로그 기록
    logger = logging.getLogger(__name__)
    logger.info(f"🚀 로깅 시스템 초기화 완료 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"📁 로그 파일 위치: {os.path.abspath(LOG_DIR)}")
    return logger

def get_logger(name):
    """특정 모듈용 로거 반환"""
    return logging.getLogger(name)

# 크롤러별 전용 로거
def get_crawler_logger(crawler_name):
    """크롤러 전용 로거 반환"""
    return logging.getLogger(f'crawlers.{crawler_name}')

def get_analyzer_logger(analyzer_name):
    """분석기 전용 로거 반환"""
    return logging.getLogger(f'analyzers.{analyzer_name}')

def get_database_logger():
    """데이터베이스 전용 로거 반환"""
    return logging.getLogger('database')

# 성능 모니터링용 로거
class PerformanceLogger:
    """성능 측정을 위한 컨텍스트 매니저"""
    
    def __init__(self, logger, operation_name):
        self.logger = logger
        self.operation_name = operation_name
        self.start_time = None
    
    def __enter__(self):
        import time
        self.start_time = time.time()
        self.logger.info(f"⏱️ {self.operation_name} 시작")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        import time
        duration = time.time() - self.start_time
        if exc_type is None:
            self.logger.info(f"✅ {self.operation_name} 완료 (소요시간: {duration:.2f}초)")
        else:
            self.logger.error(f"❌ {self.operation_name} 실패 (소요시간: {duration:.2f}초): {exc_val}")

# 설정이 임포트될 때 자동으로 로깅 시스템 초기화
if __name__ != '__main__':
    setup_logging()