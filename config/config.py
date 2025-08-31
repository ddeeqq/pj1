"""
프로젝트 전체 설정 파일 - 환경 변수 지원
"""
import os
from datetime import datetime
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 환경 변수 헬퍼 함수
def get_env_var(key, default=None, var_type=str):
    """환경 변수를 타입과 함께 안전하게 가져오기"""
    value = os.getenv(key)
    if value is None:
        return default
    
    if var_type == bool:
        if isinstance(value, bool):
            return value
        return str(value).lower() in ('true', '1', 'yes', 'on')
    elif var_type == int:
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    elif var_type == float:
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    return str(value)

# 프로젝트 루트 경로
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

# 환경 변수 기반 데이터베이스 설정
DATABASE_CONFIG = {
    'host': get_env_var('DB_HOST', 'localhost'),
    'port': get_env_var('DB_PORT', 3306, int),
    'user': get_env_var('DB_USER', 'root'),
    'password': get_env_var('DB_PASSWORD', 'your_password'),
    'database': get_env_var('DB_NAME', 'car_analysis_db'),
    'charset': 'utf8',
    'auth_plugin': 'mysql_native_password',
    'connection_timeout': 60,
    'autocommit': True,
    'pool_reset_session': False
}



# 데이터 파일 경로
DATA_FILES = {
    'registration': os.path.join(DATA_DIR, 'registration_stats.xlsx'),
    'used_car_prices': os.path.join(DATA_DIR, 'used_car_prices.csv'),
    'new_car_prices': os.path.join(DATA_DIR, 'new_car_prices.csv'),
    'recall_info': os.path.join(DATA_DIR, 'recall_info.csv'),
    'cache': os.path.join(DATA_DIR, 'cache')
}

# 환경 변수 기반 로깅 설정
LOG_CONFIG = {
    'log_dir': os.path.join(PROJECT_ROOT, 'logs'),
    'log_file': f'car_analysis_{datetime.now().strftime("%Y%m%d")}.log',
    'log_level': get_env_var('LOG_LEVEL', 'INFO'),
    'log_to_file': get_env_var('LOG_TO_FILE', True, bool),
    'max_size_mb': get_env_var('LOG_MAX_SIZE_MB', 10, int),
    'backup_count': get_env_var('LOG_BACKUP_COUNT', 5, int)
}

# 환경 변수 기반 Streamlit 설정
STREAMLIT_CONFIG = {
    'page_title': get_env_var('STREAMLIT_PAGE_TITLE', '[AUTO] 데이터 기반 중고차 vs 신차 가성비 분석 시스템'),
    'page_icon': get_env_var('STREAMLIT_PAGE_ICON', '[AUTO]'),
    'layout': get_env_var('STREAMLIT_LAYOUT', 'wide'),
    'initial_sidebar_state': get_env_var('STREAMLIT_SIDEBAR_STATE', 'expanded'),
    'host': get_env_var('STREAMLIT_HOST', 'localhost'),
    'port': get_env_var('STREAMLIT_PORT', 8501, int)
}

# 자동차 제조사 목록
CAR_MANUFACTURERS = [
    '현대', '기아', '제네시스', '쉐보레', '르노코리아', 
    '쌍용', 'BMW', '벤츠', '아우디', '폭스바겐', 
    '토요타', '혼다', '닛산', '마쯔다', '포드'
]

# 인기 모델 리스트 (초기 데이터)
POPULAR_MODELS = {
    '현대': ['그랜저', '쏘나타', '아반떼', '투싼', '싼타페', '팰리세이드'],
    '기아': ['K5', 'K3', 'K7', 'K8', '쏘렌토', '스포티지', '카니발'],
    '제네시스': ['G80', 'G90', 'GV70', 'GV80', 'G70'],
    '쉐보레': ['트레일블레이저', '이쿼녹스', '트래버스'],
    'BMW': ['3시리즈', '5시리즈', '7시리즈', 'X3', 'X5'],
    '벤츠': ['E클래스', 'C클래스', 'S클래스', 'GLC', 'GLE']
}

# 차량 세그먼트 분류
CAR_SEGMENTS = {
    '경차': ['모닝', '레이', '스파크'],
    '소형': ['아반떼', 'K3', '베뉴', '코나'],
    '준중형': ['K5', '쏘나타'],
    '중형': ['그랜저', 'K8', 'G80'],
    '대형': ['G90', 'K9'],
    'SUV소형': ['베뉴', '코나', '티볼리', '트레일블레이저'],
    'SUV중형': ['투싼', '스포티지', '싼타페'],
    'SUV대형': ['팰리세이드', '모하비', 'GV80']
}

# 분석 가중치 설정
ANALYSIS_WEIGHTS = {
    'price_weight': 0.4,      # 가격 가중치
    'reliability_weight': 0.3, # 신뢰도 가중치
    'popularity_weight': 0.2,  # 인기도 가중치
    'age_weight': 0.1         # 연식 가중치
}

# 환경 변수 기반 크롤링 설정
CRAWLING_CONFIG = {
    'kcar': {
        'delay': get_env_var('KCAR_DELAY', 3, int),
        'max_items_per_model': get_env_var('KCAR_MAX_ITEMS', 20, int),
        'max_pages': get_env_var('KCAR_MAX_PAGES', 3, int),
        'search_url': 'https://www.kcar.com/bc/search'
    },
    'encar': {  # 호환성을 위한 기존 설정 유지
        'delay': get_env_var('ENCAR_DELAY', 2, int),
        'max_items_per_model': get_env_var('ENCAR_MAX_ITEMS', 20, int),
        'batch_size': get_env_var('ENCAR_BATCH_SIZE', 5, int),
        'search_url': 'http://www.encar.com/dc/dc_carsearchlist.do'
    },
    'recall': {
        'delay': get_env_var('RECALL_DELAY', 1, int),
        'max_items': get_env_var('RECALL_MAX_ITEMS', 50, int),
        'max_retries': 3,
        'timeout': 30,
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        
        # 크롤링 대상 설정

        # 크롤링 대상 설정
        'target_manufacturers': [
            '현대', '기아', '제네시스', 'BMW', '벤츠', '아우디', 
            '토요타', '렉서스', '닛산', '인피니티', '혼다', '마쓰다',
            '폭스바겐', '벤틀리', '포르쉐', '볼보', '미니', '재규어',
            '랜드로버', '페라리', '람보르기니', '맥라렌', '테슬라'
        ],

        # 심각도별 키워드
        'severity_keywords': {
            '매우심각': ['화재', '폭발', '사망', '중상', '에어백', '브레이크', '조향장치'],
            '심각': ['엔진', '변속기', '연료', '배출가스', '전기계통', '타이어'],
            '보통': ['누수', '소음', '진동', '센서', '램프', '계기판'],
            '경미': ['도색', '내장재', '편의장치', '오디오', '네비게이션']
        },

        # 크롤링 옵션
        'default_options': {
            'max_pages': 10,  # 최대 페이지 수
            'page_size': 20,  # 페이지당 항목 수
            'date_range_days': 30,  # 기본 조회 기간 (일)
            'detail_crawling': True,  # 상세 페이지 크롤링 여부
            'save_statistics': True  # 통계 정보 저장 여부
        },

        # 알림 설정
        'notification': {
            'critical_recall_alert': True,  # 심각한 리콜 발생 시 알림
            'daily_summary': False,  # 일일 요약 리포트
            'weekly_report': True,  # 주간 리포트
            'email_recipients': [],  # 알림 수신자
            'slack_webhook': ''  # 슬랙 웹훅 URL
        }
    },
    'public_data': {
        'file_path': get_env_var('PUBLIC_DATA_FILE_PATH', './data/cache/car_registration_data.xlsx')
    }
}

# 환경 변수 기반 캐시 설정
CACHE_CONFIG = {
    'enable': get_env_var('CACHE_ENABLE', True, bool),
    'ttl': get_env_var('CACHE_TTL', 3600, int),
    'max_size': get_env_var('CACHE_MAX_SIZE', 100, int)
}

# 보안 설정
SECURITY_CONFIG = {
    'secret_key': get_env_var('SECRET_KEY', 'your-secret-key-here-change-this-in-production'),
    'encrypt_logs': get_env_var('ENCRYPT_LOGS', False, bool)
}

# 스케줄러 설정
SCHEDULER_CONFIG = {
    'enable': get_env_var('SCHEDULER_ENABLE', True, bool),
    'interval_hours': get_env_var('SCHEDULER_INTERVAL_HOURS', 24, int)
}

# 환경 변수 검증 함수
def validate_config():
    """필수 환경 변수들이 올바르게 설정되었는지 검증"""
    required_vars = {
        'DB_PASSWORD': DATABASE_CONFIG['password'],
        'SECRET_KEY': SECURITY_CONFIG['secret_key']
    }
    
    warnings = []
    for var_name, value in required_vars.items():
        if value in ('your_password', 'your-secret-key-here-change-this-in-production'):
            warnings.append(f"{var_name}이 기본값으로 설정되어 있습니다. 보안을 위해 변경해주세요.")
    
    return warnings

# 디렉토리 생성
def create_directories():
    """필요한 디렉토리들을 생성합니다."""
    dirs = [DATA_DIR, LOG_CONFIG['log_dir'], DATA_FILES['cache']]
    for dir_path in dirs:
        os.makedirs(dir_path, exist_ok=True)

# 설정 요약 출력
def print_config_summary():
    """현재 설정 요약 정보 출력"""
    print("\n[설정 정보]")
    print(f"  데이터베이스: {DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}")
    print(f"  로그 레벨: {LOG_CONFIG['log_level']}")
    print(f"  Streamlit: {STREAMLIT_CONFIG['host']}:{STREAMLIT_CONFIG['port']}")
    print(f"  스케줄러: {'활성' if SCHEDULER_CONFIG['enable'] else '비활성'}")
    
    warnings = validate_config()
    if warnings:
        print("\n[보안 경고]")
        for warning in warnings:
            print(f"  - {warning}")
        print("\n.env 파일을 생성하여 환경 변수를 설정하세요.")
        print("참고: .env.example 파일을 복사해서 사용하세요.\n")

# 프로젝트 실행 시 디렉토리 생성 및 설정 검증
create_directories()

# 메인 모듈에서 실행될 때만 설정 요약 출력
if __name__ == '__main__':
    print_config_summary()
