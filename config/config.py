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
    'password': 'your_password',  # 실제 비밀번호로 변경 필요
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
