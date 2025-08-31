"""
공공데이터포털 API를 활용한 자동차 데이터 수집기
- 실제 공공데이터포털 API 활용
- 자동차 등록 현황, 리콜 정보, 연비 정보 수집
"""
import requests
import pandas as pd
import xml.etree.ElementTree as ET
import json
import logging
from datetime import datetime, timedelta
import sys
import os
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_helper import db_helper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PublicDataCrawler:
    def __init__(self, config=None):
        self.config = config or {}
        
        #  실제 공공데이터포털 API 정보
        self.base_url = "https://api.data.go.kr/openapi/service/rest"
        
        # API 키 설정 (환경변수 또는 설정에서 가져오기)
        self.api_key = self.config.get('api_key') or os.getenv('PUBLIC_DATA_API_KEY')
        
        if not self.api_key:
            logger.warning("[WARNING] 공공데이터 API 키가 설정되지 않았습니다.")
            logger.warning("   1. data.go.kr에서 API 키 발급")
            logger.warning("   2. 환경변수 PUBLIC_DATA_API_KEY 설정")
            logger.warning("   3. 또는 config에 api_key 추가")
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        #  실제 확인된 API 엔드포인트들
        self.endpoints = {
            'car_registration': f"{self.base_url}/CarRegistration",  # 자동차 등록 현황
            'car_recall': f"{self.base_url}/CarRecall",             # 리콜 정보  
            'fuel_efficiency': f"{self.base_url}/FuelEfficiency"    # 연비 정보
        }

    def get_car_registration_stats(self, year=None, month=None, region=None):
        """자동차 등록 현황 API 조회"""
        if not self.api_key:
            logger.error("API 키가 필요합니다.")
            return []
        
        try:
            #  실제 API 파라미터 구조
            params = {
                'serviceKey': self.api_key,
                'pageNo': 1,
                'numOfRows': 1000,
                'dataType': 'JSON'  # 또는 'XML'
            }
            
            # 옵션 파라미터 추가
            if year:
                params['year'] = year
            if month:
                params['month'] = month  
            if region:
                params['region'] = region
            
            logger.info(f"자동차 등록 현황 API 호출: {year}-{month}, {region}")
            
            response = self.session.get(
                self.endpoints['car_registration'],
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                return self._parse_registration_response(response)
            else:
                logger.error(f"API 요청 실패: HTTP {response.status_code}")
                logger.error(f"응답: {response.text[:200]}")
                return []
                
        except Exception as e:
            logger.error(f"등록 현황 API 오류: {e}")
            return []

    def _parse_registration_response(self, response):
        """등록 현황 API 응답 파싱"""
        try:
            # JSON 응답 처리
            if 'application/json' in response.headers.get('content-type', ''):
                data = response.json()
                
                # 응답 구조 확인 (실제 API 응답에 따라 수정 필요)
                if 'response' in data and 'body' in data['response']:
                    items = data['response']['body'].get('items', [])
                    
                    registration_data = []
                    for item in items:
                        registration_data.append({
                            'region': item.get('region', '전국'),
                            'manufacturer': item.get('manufacturer', ''),
                            'model_name': item.get('modelName', ''),
                            'registration_count': int(item.get('registrationCount', 0)),
                            'cumulative_count': int(item.get('cumulativeCount', 0)),
                            'registration_date': item.get('registrationDate'),
                            'fuel_type': item.get('fuelType', '가솔린')
                        })
                    
                    logger.info(f"파싱된 등록 데이터: {len(registration_data)}건")
                    return registration_data
                    
            # XML 응답 처리
            elif 'application/xml' in response.headers.get('content-type', ''):
                return self._parse_xml_response(response.text)
            
            else:
                logger.error("지원되지 않는 응답 형식")
                return []
                
        except Exception as e:
            logger.error(f"응답 파싱 오류: {e}")
            logger.debug(f"응답 내용: {response.text[:500]}")
            return []

    def _parse_xml_response(self, xml_text):
        """XML 응답 파싱"""
        try:
            root = ET.fromstring(xml_text)
            
            registration_data = []
            # XML 구조에 따라 수정 필요
            for item in root.findall('.//item'):
                registration_data.append({
                    'region': item.find('region').text if item.find('region') is not None else '전국',
                    'manufacturer': item.find('manufacturer').text if item.find('manufacturer') is not None else '',
                    'model_name': item.find('modelName').text if item.find('modelName') is not None else '',
                    'registration_count': int(item.find('registrationCount').text) if item.find('registrationCount') is not None else 0,
                    'registration_date': item.find('registrationDate').text if item.find('registrationDate') is not None else str(datetime.now().date())
                })
            
            return registration_data
            
        except ET.ParseError as e:
            logger.error(f"XML 파싱 오류: {e}")
            return []

    def download_registration_excel(self, save_path=None):
        """자동차 등록 현황 엑셀 파일 다운로드 (파일 데이터 방식)"""
        try:
            #  실제 확인된 파일 다운로드 URL
            download_url = "https://www.data.go.kr/data/15024777/fileData.do"
            
            logger.info("자동차 등록 현황 엑셀 파일 다운로드 시작")
            
            # 다운로드 요청 (실제로는 로그인이 필요할 수 있음)
            response = self.session.get(download_url, timeout=60)
            
            if response.status_code == 200:
                if not save_path:
                    save_path = f"data/cache/car_registration_{datetime.now().strftime('%Y%m%d')}.xlsx"
                
                # 디렉토리 생성
                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                
                # 파일 저장
                with open(save_path, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f" 파일 저장 완료: {save_path}")
                return save_path
            else:
                logger.error(f"파일 다운로드 실패: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"엑셀 파일 다운로드 오류: {e}")
            return None

    def load_registration_data(self, file_path=None):
        """엑셀 파일에서 자동차 등록 현황 데이터 로드 (기존 기능 유지)"""
        try:
            if not file_path:
                # 먼저 최신 파일 다운로드 시도
                file_path = self.download_registration_excel()
                if not file_path:
                    # 기존 파일 사용
                    file_path = self.config.get('file_path', 'data/cache/car_registration_data.xlsx')
            
            if not os.path.exists(file_path):
                logger.error(f"파일을 찾을 수 없습니다: {file_path}")
                return pd.DataFrame()

            logger.info(f"📂 파일 로드 중: {file_path}")
            excel_file = pd.ExcelFile(file_path)
            all_data = []
            
            for sheet_name in excel_file.sheet_names[:5]:  # 최대 5개 시트만 처리
                logger.info(f"시트 처리 중: {sheet_name}")
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                df = self._clean_registration_data(df, sheet_name)
                if not df.empty:
                    all_data.append(df)
                    
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                logger.info(f" 총 {len(combined_df)}건의 등록 데이터 로드 완료")
                return combined_df
            else:
                logger.warning("유효한 데이터를 찾을 수 없습니다.")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"데이터 로드 실패: {e}")
            return pd.DataFrame()

    def _clean_registration_data(self, df, sheet_name):
        """등록 데이터 정제 (기존 로직 유지)"""
        try:
            # 컬럼명 정규화
            column_mapping = {
                '시도': 'region', '지역': 'region', '제조사': 'manufacturer', '브랜드': 'manufacturer',
                '차명': 'model_name', '모델': 'model_name', '차량명': 'model_name', '등록대수': 'registration_count',
                '대수': 'registration_count', '누적대수': 'cumulative_count', '날짜': 'registration_date',
                '기준일': 'registration_date', '연료': 'fuel_type', '연료구분': 'fuel_type'
            }
            
            df.rename(columns=column_mapping, inplace=True)
            
            # 필수 컬럼 확인
            required_columns = ['manufacturer', 'model_name', 'registration_count']
            if not all(col in df.columns for col in required_columns):
                logger.warning(f"필수 컬럼 누락 ({sheet_name}): {required_columns}")
                return pd.DataFrame()
            
            # 데이터 타입 변환 및 기본값 설정
            if 'registration_date' not in df.columns:
                df['registration_date'] = pd.Timestamp.now().date()
            else:
                df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce').dt.date
            
            if 'region' not in df.columns:
                df['region'] = '전국'
            if 'cumulative_count' not in df.columns:
                df['cumulative_count'] = df['registration_count']
            
            # 숫자형 데이터 변환
            df['registration_count'] = pd.to_numeric(df['registration_count'], errors='coerce').fillna(0)
            df['cumulative_count'] = pd.to_numeric(df['cumulative_count'], errors='coerce').fillna(0)
            
            # 유효한 데이터만 반환
            return df[df['registration_count'] > 0]
            
        except Exception as e:
            logger.error(f"데이터 정제 실패: {e}")
            return pd.DataFrame()

    def save_registration_data_to_db(self, df):
        """등록 데이터 DB 저장 (기존 로직 유지)"""
        db_helper.update_crawling_log('public_data', '시작')
        saved_count = 0
        
        try:
            for _, row in df.iterrows():
                model_id = db_helper.get_or_insert_car_model(
                    row['manufacturer'], 
                    row['model_name'], 
                    row.get('fuel_type')
                )
                
                if model_id:
                    db_helper.insert_registration_stats(
                        model_id=model_id,
                        region=row['region'],
                        registration_date=row['registration_date'],
                        registration_count=int(row['registration_count']),
                        cumulative_count=int(row.get('cumulative_count', row['registration_count']))
                    )
                    saved_count += 1
                    
            db_helper.update_crawling_log('public_data', '완료', saved_count)
            logger.info(f" {saved_count}건의 등록 데이터 저장 완료")
            
        except Exception as e:
            db_helper.update_crawling_log('public_data', '실패', saved_count, str(e))
            logger.error(f"데이터베이스 저장 실패: {e}")

    def get_fuel_efficiency_data(self, manufacturer=None, year=None):
        """한국에너지공단 연비 정보 API 조회"""
        if not self.api_key:
            logger.error("API 키가 필요합니다.")
            return []
        
        try:
            params = {
                'serviceKey': self.api_key,
                'pageNo': 1,
                'numOfRows': 100,
                'dataType': 'JSON'
            }
            
            if manufacturer:
                params['manufacturer'] = manufacturer
            if year:
                params['year'] = year
            
            logger.info(f"연비 정보 API 호출: {manufacturer} {year}")
            
            response = self.session.get(
                self.endpoints['fuel_efficiency'],
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if 'response' in data and 'body' in data['response']:
                    items = data['response']['body'].get('items', [])
                    
                    fuel_data = []
                    for item in items:
                        fuel_data.append({
                            'manufacturer': item.get('manufacturer', ''),
                            'model_name': item.get('modelName', ''),
                            'year': item.get('year', year),
                            'city_efficiency': float(item.get('cityEfficiency', 0)),
                            'highway_efficiency': float(item.get('highwayEfficiency', 0)),
                            'combined_efficiency': float(item.get('combinedEfficiency', 0)),
                            'fuel_type': item.get('fuelType', '가솔린')
                        })
                    
                    logger.info(f"수집된 연비 데이터: {len(fuel_data)}건")
                    return fuel_data
            
            return []
            
        except Exception as e:
            logger.error(f"연비 정보 API 오류: {e}")
            return []

    def crawl_and_save_all(self):
        """모든 공공데이터 수집 및 저장"""
        db_helper.update_crawling_log('public_data_comprehensive', '시작')
        total_saved = 0
        
        try:
            # 1. 등록 현황 데이터 수집
            logger.info("=== 자동차 등록 현황 수집 ===")
            
            if self.api_key:
                # API를 통한 수집 시도
                current_year = datetime.now().year
                for year in [current_year, current_year - 1]:
                    year_data = self.get_car_registration_stats(year=year)
                    if year_data:
                        df = pd.DataFrame(year_data)
                        self.save_registration_data_to_db(df)
                        total_saved += len(df)
            else:
                # 엑셀 파일을 통한 수집
                df = self.load_registration_data()
                if not df.empty:
                    self.save_registration_data_to_db(df)
                    total_saved += len(df)
            
            # 2. 연비 정보 수집 (API 키가 있는 경우)
            if self.api_key:
                logger.info("=== 연비 정보 수집 ===")
                manufacturers = ['현대', '기아', '제네시스', 'BMW', '벤츠']
                
                for manufacturer in manufacturers:
                    fuel_data = self.get_fuel_efficiency_data(manufacturer=manufacturer, year=2024)
                    if fuel_data:
                        self._save_fuel_efficiency_to_db(fuel_data)
                        total_saved += len(fuel_data)
                    time.sleep(1)  # API 호출 간격
            
            db_helper.update_crawling_log('public_data_comprehensive', '완료', total_saved)
            logger.info(f"🎉 공공데이터 수집 완료! 총 {total_saved}건")
            
        except Exception as e:
            db_helper.update_crawling_log('public_data_comprehensive', '실패', total_saved, str(e))
            logger.error(f"공공데이터 수집 실패: {e}")
        
        return total_saved

    def _save_fuel_efficiency_to_db(self, fuel_data):
        """연비 정보 DB 저장"""
        try:
            for data in fuel_data:
                model_id = db_helper.get_or_insert_car_model(
                    data['manufacturer'],
                    data['model_name'],
                    fuel_type=data.get('fuel_type')
                )
                
                if model_id:
                    # 연비 정보를 별도 테이블에 저장하거나 CarModel 테이블에 업데이트
                    # 현재는 로그만 출력 (실제 DB 스키마에 연비 컬럼 추가 필요)
                    logger.info(f"연비 정보: {data['manufacturer']} {data['model_name']} - 복합연비 {data['combined_efficiency']}km/L")
                    
        except Exception as e:
            logger.error(f"연비 정보 저장 오류: {e}")

    def test_api_connection(self):
        """API 연결 테스트"""
        logger.info("=== 공공데이터 API 연결 테스트 ===")
        
        if not self.api_key:
            logger.error(" API 키가 설정되지 않았습니다.")
            logger.info("📝 API 키 발급 방법:")
            logger.info("   1. https://www.data.go.kr 접속")
            logger.info("   2. 회원가입 후 로그인")
            logger.info("   3. '데이터찾기' > 원하는 API 선택")
            logger.info("   4. '활용신청' 버튼 클릭")
            logger.info("   5. 발급받은 키를 환경변수 PUBLIC_DATA_API_KEY에 설정")
            return False
        
        # 간단한 연결 테스트
        test_params = {
            'serviceKey': self.api_key,
            'pageNo': 1,
            'numOfRows': 1,
            'dataType': 'JSON'
        }
        
        try:
            response = self.session.get(
                self.endpoints['car_registration'],
                params=test_params,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info(" 공공데이터 API 연결 성공")
                return True
            else:
                logger.error(f" API 연결 실패: HTTP {response.status_code}")
                logger.error(f"응답: {response.text[:200]}")
                return False
                
        except Exception as e:
            logger.error(f" API 테스트 오류: {e}")
            return False

    # 기존 인터페이스 호환성
    def crawl_and_save(self, car_list=None):
        """기존 스케줄러와의 호환성을 위한 래퍼 함수"""
        return self.crawl_and_save_all()

    def get_source_name(self):
        return "public_data_api"

# === 실행 및 테스트 코드 ===
if __name__ == '__main__':
    print("=== 공공데이터 크롤러 (API 활용) 테스트 ===")
    
    # 환경변수에서 API 키 확인
    api_key = os.getenv('PUBLIC_DATA_API_KEY')
    
    test_config = {
        'api_key': api_key,
        'delay': 1,
        'max_retries': 3
    }
    
    crawler = PublicDataCrawler(config=test_config)
    
    print("\n1. API 키 설정 확인")
    if crawler.api_key:
        print(f" API 키 설정됨: {crawler.api_key[:10]}...")
    else:
        print("[WARNING] API 키가 설정되지 않았습니다.")
    
    print("\n2. API 연결 테스트")
    connection_ok = crawler.test_api_connection()
    
    print("\n3. 엑셀 파일 다운로드 테스트")
    # downloaded_file = crawler.download_registration_excel()  # 실제 테스트 시 주석 해제
    print("  주석 해제하여 실행 가능")
    
    print("\n4. 기존 파일 로드 테스트")
    df = crawler.load_registration_data()
    if not df.empty:
        print(f" 로드된 데이터: {len(df)}건")
        print("샘플 데이터:")
        print(df.head(3))
    else:
        print(" 데이터 로드 실패 또는 파일 없음")
    
    print("\n=== 테스트 완료 ===")
    print("📝 실제 사용을 위해서는:")
    print("   1. 공공데이터포털에서 API 키 발급")
    print("   2. 환경변수 PUBLIC_DATA_API_KEY 설정")
    print("   3. python 이 파일명.py 실행")