"""
프로젝트 전체 설정 파일
"""
import os
from datetime import datetime

# 프로젝트 루트 경로
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

# 데이터베이스 설정
DATABASE_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '1234',
    'database': 'car_analysis_db',
    'charset': 'utf8mb4'
}



# 데이터 파일 경로
DATA_FILES = {
    'registration': os.path.join(DATA_DIR, 'registration_stats.xlsx'),
    'used_car_prices': os.path.join(DATA_DIR, 'used_car_prices.csv'),
    'new_car_prices': os.path.join(DATA_DIR, 'new_car_prices.csv'),
    'recall_info': os.path.join(DATA_DIR, 'recall_info.csv'),
    'cache': os.path.join(DATA_DIR, 'cache')
}

# 로깅 설정
LOG_CONFIG = {
    'log_dir': os.path.join(PROJECT_ROOT, 'logs'),
    'log_file': f'car_analysis_{datetime.now().strftime("%Y%m%d")}.log',
    'log_level': 'INFO'
}

# Streamlit 설정
STREAMLIT_CONFIG = {
    'page_title': '🚗 데이터 기반 중고차 vs 신차 가성비 분석 시스템',
    'page_icon': '🚗',
    'layout': 'wide',
    'initial_sidebar_state': 'expanded'
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

# 크롤링 설정
CRAWLING_CONFIG = {
    'recall': {
        'delay': 2,  # 요청 간격 (초)
        'max_retries': 3,  # 최대 재시도 횟수
        'timeout': 30,  # 타임아웃 (초)
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',

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
    }
}

# 캐시 설정
CACHE_CONFIG = {
    'enable': True,
    'ttl': 3600,  # 캐시 유효 시간 (초)
    'max_size': 100  # 최대 캐시 아이템 수
}

# 디렉토리 생성
def create_directories():
    """필요한 디렉토리들을 생성합니다."""
    dirs = [DATA_DIR, LOG_CONFIG['log_dir'], DATA_FILES['cache']]
    for dir_path in dirs:
        os.makedirs(dir_path, exist_ok=True)

# 프로젝트 실행 시 디렉토리 생성
create_directories()
