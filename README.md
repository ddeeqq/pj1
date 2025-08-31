# ( 9/1 공공데이터 api 및 기타 데이터들 수정 필요 , 안에 쓸데없는 코멘트들 상당량 존재 제거 필요 , 호환되는지 실행 안 해봄 실행 해봐야함)
# 데이터 기반 중고차 vs 신차 가성비 분석 시스템

실제 시장 데이터를 기반으로 중고차와 신차의 가성비를 다각도로 비교 분석하여, 사용자가 최적의 구매 결정을 내릴 수 있도록 지원하는 웹 애플리케이션입니다.

## 주요 기능

- **데이터 자동 수집**: K카, 자동차리콜센터, 공공데이터포털 등의 데이터를 스케줄러를 통해 주기적으로 자동 수집
- **종합 가성비 분석**: 가격, 신뢰도, 인기도를 종합한 가성비 점수 모델링
- **미래 가격 예측**: 간단한 감가상각 모델을 이용한 미래 중고가 예측
- **총 소유비용 (TCO) 분석**: 구매가, 보험료, 유지비 등을 고려한 총 소유비용 계산
- **대안 신차 추천**: 분석 중인 중고차 예산으로 구매 가능한 대안 신차 목록 제시
- **인터랙티브 대시보드**: Streamlit 기반의 시각화 대시보드 및 데이터 관리 기능

## 데이터 소스

### 중고차 가격 정보
- **K카 (kcar.com)**: 실제 중고차 가격 데이터 수집
- 제조사별, 모델별, 연식별 가격 정보 및 통계

### 리콜 정보
- **자동차리콜센터 (car.go.kr)**: 정부 공식 리콜 정보
- 제조사별 리콜 현황 및 심각도 분류
- 차량번호/VIN 기반 리콜 대상 확인

### 공공데이터
- **공공데이터포털 (data.go.kr)**: 자동차 등록 현황
- **한국에너지공단**: 연비 정보 API
- Excel 파일 다운로드 및 API 연동 지원

## 기술 스택

- **Framework**: Streamlit
- **Data Crawling**: Requests, BeautifulSoup4
- **Data Handling**: Pandas, NumPy
- **Database**: MySQL
- **Scheduling**: Schedule
- **Visualization**: Plotly Express
- **Environment**: python-dotenv

## 빠른 시작

### 1. 자동 설치 (권장)

이 프로젝트는 사용자 편의를 위해 대화형 설치 스크립트(`setup.py`)를 제공합니다.

```bash
# 1. 저장소 클론
git clone [repository-url]
cd pj1

# 2. 자동 설치 스크립트 실행
python setup.py
```

> `setup.py`를 실행하면 Python 버전 확인, 필요 패키지 설치, 디렉토리 생성, DB 정보 입력 및 설정, 스키마 생성, 샘플 데이터 생성 여부 확인 등 모든 과정이 자동으로 진행됩니다.

### 2. 수동 설치

**요구사항**
- Python 3.8+
- MySQL 8.0+
- 인터넷 연결 (크롤링용)

**설치 단계**

```bash
# 1. 의존성 설치
pip install -r requirements.txt

# 2. 환경변수 설정
cp .env.example .env
# .env 파일을 편집하여 실제 값으로 수정

# 3. 데이터베이스 스키마 생성
python database/database_schema.py

# 4. 샘플 데이터 생성 (선택 사항)
python init_data.py
```

### 3. 실행

**Windows 사용자**

프로젝트 루트의 `start.bat` 파일을 더블클릭하면 메뉴가 나타나 편리하게 실행할 수 있습니다.

**직접 실행**

```bash
# 웹 애플리케이션 실행
streamlit run ui/streamlit_app.py

# 스케줄러 실행 (백그라운드 권장)
python scheduler_enhanced.py

# 대화형 크롤링 실행
python run.py
```

## 프로젝트 구조

```
pj1/
├── analyzers/          # 데이터 분석 모듈
├── config/             # 설정 파일
│   ├── config.py       # 메인 설정 (환경변수 기반)
│   ├── scheduler_config.json  # 스케줄러 설정
│   └── logging_config.py      # 로깅 설정
├── crawlers/           # 데이터 수집 모듈
│   ├── kcar_crawler.py        # K카 중고차 가격 크롤러
│   ├── recall_crawler.py      # 리콜 정보 크롤러
│   ├── public_data_crawler.py # 공공데이터 크롤러
│   └── base_crawler.py        # 크롤러 기본 클래스
├── database/           # 데이터베이스 관련
│   ├── db_helper.py           # DB 헬퍼 함수
│   └── database_schema.py     # DB 스키마 관리
├── ui/                 # 사용자 인터페이스
│   └── streamlit_app.py       # Streamlit 웹앱
├── logs/              # 로그 파일
├── data/              # 데이터 파일
├── scheduler_enhanced.py  # 자동화 스케줄러
├── init_data.py          # 샘플 데이터 생성기
├── run.py               # 메인 실행 스크립트
├── setup.py             # 자동 설치 스크립트
├── start.bat            # Windows 실행 배치 파일
├── .env.example         # 환경변수 템플릿
└── README.md            # 프로젝트 안내서
```

## 환경변수 설정

`.env.example` 파일을 `.env`로 복사하고 다음 설정을 수정하세요:

```env
# === 데이터베이스 설정 (필수) ===
DB_HOST=localhost
DB_PORT=3306
DB_USER=your_user
DB_PASSWORD=your_password
DB_NAME=car_analysis_db

# === 공공데이터포털 API 설정 (권장) ===
PUBLIC_DATA_API_KEY=your_api_key_here

# === 크롤링 설정 ===
KCAR_DELAY=3
KCAR_MAX_ITEMS=20
KCAR_MAX_PAGES=3

RECALL_DELAY=2
RECALL_MAX_ITEMS=50
RECALL_MAX_PAGES=5

# === 기타 설정 ===
LOG_LEVEL=INFO
STREAMLIT_PORT=8501
```

## 크롤러 설정

모든 스케줄러 및 크롤러의 세부 동작은 `config/scheduler_config.json` 파일에서 제어합니다.

```json
{
  "scheduler": {
    "max_retries": 3,
    "delay_between_tasks": 60
  },
  "crawling": {
    "kcar": {
      "enabled": true,
      "delay": 3,
      "max_items_per_model": 20
    },
    "recall": {
      "enabled": true,
      "delay": 2,
      "max_items": 50
    },
    "public_data": {
      "enabled": true,
      "auto_download": false
    }
  }
}
```

## 주요 크롤러 특징

### KCarCrawler
- K카 사이트에서 중고차 가격 정보 수집
- 제조사, 모델, 연식별 검색 지원
- 평균/최소/최대 가격 통계 계산
- 실제 거래 데이터 기반

### RecallCrawler
- 자동차리콜센터 공식 데이터
- 차량번호/VIN 기반 리콜 대상 확인
- 심각도별 자동 분류 (매우심각/심각/보통/경미)
- 실시간 리콜 현황 모니터링

### PublicDataCrawler
- 공공데이터포털 API 활용
- 자동차 등록 현황 통계
- 연비 정보 수집
- Excel 파일 자동 다운로드/파싱

## 문제 해결

- **MySQL 연결 실패**: `.env` 파일의 DB 설정이 올바른지, MySQL 서버가 실행 중인지 확인하세요.
- **크롤링 실패**: 네트워크 연결 상태를 확인하고, 각 크롤러의 delay 설정을 늘려보세요.
- **API 오류**: 공공데이터포털에서 API 키를 발급받아 `.env` 파일에 설정하세요.
- **Streamlit 실행 오류**: `streamlit run ui/streamlit_app.py --server.port 8502`와 같이 다른 포트를 지정하여 실행해보세요.

## 데이터 수집 주기

- **일일**: K카 중고차 가격 업데이트
- **주간**: 리콜 정보 업데이트
- **월간**: 공공데이터 등록 현황 업데이트

## 기여

이슈 생성이나 Pull Request를 통해 자유롭게 기여할 수 있습니다.

1. 저장소 Fork
2. 기능 브랜치 생성 (`git checkout -b feature/AmazingFeature`)
3. 변경사항 커밋 (`git commit -m 'Add some AmazingFeature'`)
4. 브랜치에 Push (`git push origin feature/AmazingFeature`)
5. Pull Request 열기

## 라이선스

이 프로젝트는 MIT 라이선스를 따릅니다.

## 연락처

- **Email**: jihanki3@naver.com
- **GitHub Issues**: 프로젝트 이슈 페이지에서 버그 리포트나 기능 요청을 해주세요.

## 업데이트 이력

- **2024.08**: K카 크롤러 추가, 공공데이터 API 연동, 리콜 정보 고도화
- **2024.08**: UTF-8 호환성 개선, 이모지 제거, 환경변수 기반 설정 전환
