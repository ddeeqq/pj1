# 🚗 데이터 기반 중고차 vs 신차 가성비 분석 시스템

## 📋 프로젝트 개요
자동차 구매를 고려하는 사용자를 위한 데이터 기반 의사결정 지원 시스템입니다.
공공 데이터, 시장 데이터, 신뢰도 데이터를 융합하여 객관적이고 종합적인 차량 정보를 제공합니다.

## 🎯 주요 기능
- **전국 자동차 트렌드 분석**: 지역별, 연료별 등록 현황 시각화
- **모델 상세 분석**: 선택 모델의 가격, 리콜, 인기도 종합 분석
- **중고차 vs 신차 비교**: 예산에 맞는 최적의 선택 추천
- **가성비 점수 산출**: 다각도 분석을 통한 객관적 점수 제공
- **미래 가격 예측**: 감가상각 모델 기반 가격 예측
- **총 소유비용(TCO) 분석**: 5년간 실제 소유 비용 계산

## 🏗️ 프로젝트 구조
```
pj1/
├── config/              # 설정 파일
│   └── config.py       # 프로젝트 전체 설정
├── database/           # 데이터베이스 관련
│   ├── database_schema.py  # DB 스키마 정의
│   └── db_helper.py        # DB 헬퍼 함수
├── crawlers/           # 데이터 수집 모듈
│   ├── encar_crawler.py    # 엔카 크롤러
│   ├── recall_crawler.py   # 리콜 정보 크롤러
│   └── public_data_crawler.py  # 공공데이터 수집
├── analyzers/          # 분석 모듈
│   └── price_analyzer.py   # 가격 분석기
├── ui/                 # 사용자 인터페이스
│   └── streamlit_app.py    # Streamlit 웹 앱
├── data/               # 데이터 저장 폴더
├── logs/               # 로그 파일
└── requirements.txt    # 필요 패키지 목록
```

## 🚀 설치 및 실행 방법

### 1. 사전 요구사항
- Python 3.9 이상
- MySQL 8.0 이상
- Chrome 브라우저 (웹 크롤링용)

### 2. 패키지 설치
```bash
# 가상환경 생성 (권장)
python -m venv venv

# 가상환경 활성화
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# 패키지 설치
pip install -r requirements.txt
```

### 3. 데이터베이스 설정
```bash
# MySQL 설정 수정
# config/config.py 파일에서 DATABASE_CONFIG 수정
DATABASE_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'your_password',  # 본인의 MySQL 비밀번호로 변경
    'database': 'car_analysis_db',
    'charset': 'utf8mb4'
}

# 데이터베이스 초기화
python database/database_schema.py
```

### 4. 애플리케이션 실행
```bash
# Streamlit 앱 실행
streamlit run ui/streamlit_app.py
```

브라우저에서 자동으로 `http://localhost:8501` 페이지가 열립니다.

## 💾 데이터 수집 방법

### 공공데이터 (엑셀 파일)
```python
# public_data_crawler.py 실행
from crawlers.public_data_crawler import PublicDataCrawler

crawler = PublicDataCrawler()
df = crawler.load_registration_data('path/to/excel_file.xlsx')
crawler.save_to_database(df)
```

### 중고차 가격 (웹 크롤링)
```python
# encar_crawler.py 실행
from crawlers.encar_crawler import EncarCrawler

crawler = EncarCrawler()
car_list = [
    {'manufacturer': '현대', 'model_name': '그랜저 IG'},
    {'manufacturer': '기아', 'model_name': 'K5 DL3'},
]
crawler.crawl_and_save(car_list)
```

### 리콜 정보
```python
# recall_crawler.py 실행
from crawlers.recall_crawler import RecallCrawler

crawler = RecallCrawler()
crawler.crawl_and_save(car_list)
```

## 📊 데이터베이스 스키마

### 주요 테이블
1. **CarModel**: 자동차 모델 마스터 정보
2. **RegistrationStats**: 지역별 등록 통계
3. **UsedCarPrice**: 중고차 가격 정보
4. **NewCarPrice**: 신차 가격 정보
5. **RecallInfo**: 리콜 이력 정보
6. **Demographics**: 선호 연령대 정보
7. **FAQ**: 자주 묻는 질문

## 🔧 주요 설정 변경

### 크롤링 딜레이 조정
```python
# config/config.py
CRAWLING_CONFIG = {
    'encar': {
        'delay': 2,  # 요청 간 지연 시간(초)
    }
}
```

### 분석 가중치 조정
```python
# config/config.py
ANALYSIS_WEIGHTS = {
    'price_weight': 0.4,      # 가격 가중치
    'reliability_weight': 0.3, # 신뢰도 가중치
    'popularity_weight': 0.2,  # 인기도 가중치
    'age_weight': 0.1         # 연식 가중치
}
```

## 📝 주의사항
1. **웹 크롤링**: 대상 사이트의 robots.txt를 준수하고, 서버 부하를 고려하여 적절한 딜레이를 설정하세요.
2. **데이터 저장**: 민감한 정보는 암호화하여 저장하고, 개인정보보호법을 준수하세요.
3. **API 키**: 외부 API 사용 시 키는 환경 변수로 관리하세요.

## 🤝 기여 방법
1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 라이선스
This project is licensed under the MIT License.

## 👨‍💻 개발자
- 1인 메인 개발자

## 📞 문의
프로젝트 관련 문의사항이 있으시면 이슈를 등록해주세요.
