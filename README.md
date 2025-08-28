# 데이터 기반 중고차 vs 신차 가성비 분석 시스템

실제 시장 데이터를 기반으로 중고차와 신차의 가성비를 다각도로 비교 분석하여, 사용자가 최적의 구매 결정을 내릴 수 있도록 지원하는 웹 애플리케이션입니다.

##  주요 기능

- ** 데이터 자동 수집**: 엔카, 자동차리콜센터 등의 데이터를 스케줄러를 통해 주기적으로 자동 수집
- ** 종합 가성비 분석**: 가격, 신뢰도, 인기도를 종합한 가성비 점수 모델링
- ** 미래 가격 예측**: 간단한 감가상각 모델을 이용한 미래 중고가 예측
- ** 총 소유비용 (TCO) 분석**: 구매가, 보험료, 유지비 등을 고려한 총 소유비용 계산
- ** 대안 신차 추천**: 분석 중인 중고차 예산으로 구매 가능한 대안 신차 목록 제시
- ** 인터랙티브 대시보드**: Streamlit 기반의 시각화 대시보드 및 데이터 관리 기능

##  기술 스택

- **Framework**: Streamlit
- **Data Crawling**: Selenium, BeautifulSoup4, Requests
- **Data Handling**: Pandas, NumPy
- **Database**: MySQL
- **Scheduling**: Schedule
- **Visualization**: Plotly Express

##  빠른 시작

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
- Chrome 브라우저 (크롤링용)

**설치 단계**

```bash
# 1. 의존성 설치
pip install -r requirements.txt

# 2. 데이터베이스 설정
# config/config.py 파일의 DATABASE_CONFIG 변수를 실제 DB 정보에 맞게 수정하세요.
# (주의: setup.py를 실행하면 이 과정이 자동으로 처리됩니다.)

# 3. 데이터베이스 스키마 생성
python database/database_schema.py

# 4. 샘플 데이터 생성 (선택 사항)
# python init_data.py
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
```

##  프로젝트 구조

```
📁 pj1/
├── 📂 analyzers/          # 데이터 분석 모듈
├── 📂 config/             # 설정 파일
├── 📂 crawlers/           # 데이터 수집 모듈
├── 📂 database/           # 데이터베이스 관련
├── 📂 ui/                # 사용자 인터페이스
├── 📂 logs/              # 로그 파일
├── 📂 data/              # 데이터 파일
├── scheduler_enhanced.py  # 자동화 스케줄러
├── init_data.py          # 샘플 데이터 생성기
├── run.py               # 메인 실행 스크립트
├── setup.py             # 자동 설치 스크립트
├── start.bat            # Windows 실행 배치 파일
└── README.md            # 프로젝트 안내서
```

##  주요 설정

### 데이터베이스 설정

`setup.py` 실행 시 자동으로 `config/config.py` 파일이 구성됩니다. 수동 변경이 필요할 경우 해당 파일의 `DATABASE_CONFIG` 변수를 수정하세요.

### 스케줄러 및 크롤러 설정

모든 스케줄러 및 크롤러의 세부 동작은 `config/scheduler_config.json` 파일에서 제어합니다.

```json
{
  "scheduler": {
    "max_retries": 3,
    "delay_between_tasks": 60
  },
  "crawling": {
    "encar": {
      "delay": 2,
      "max_pages_per_model": 5
    }
  },
  "email": {
    "enabled": false,
    "smtp_server": "smtp.gmail.com"
  }
}
```

##  문제 해결

- **MySQL 연결 실패**: `config/config.py`의 DB 설정이 올바른지, MySQL 서버가 실행 중인지, 방화벽에서 3306 포트가 열려 있는지 확인하세요.
- **크롤링 실패**: `pip install --upgrade webdriver-manager`로 Chrome 드라이버를 업데이트하거나, 네트워크 연결 상태를 확인하세요.
- **Streamlit 실행 오류**: `streamlit run ui/streamlit_app.py --server.port 8502`와 같이 다른 포트를 지정하여 실행해보세요.

##  기여

이슈 생성이나 Pull Request를 통해 자유롭게 기여할 수 있습니다.

1. 저장소 Fork
2. 기능 브랜치 생성 (`git checkout -b feature/AmazingFeature`)
3. 변경사항 커밋 (`git commit -m 'Add some AmazingFeature'`)
4. 브랜치에 Push (`git push origin feature/AmazingFeature`)
5. Pull Request 열기

##  라이선스

이 프로젝트는 MIT 라이선스를 따릅니다. 자세한 내용은 `LICENSE` 파일을 참고하세요. (참고: 현재 프로젝트에 LICENSE 파일이 없습니다.)

##  연락처

- **GitHub Issues**: [프로젝트 이슈 페이지 링크]
- **Email**: jihanki3@naver.com