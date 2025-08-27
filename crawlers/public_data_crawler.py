"""
공공데이터 포털에서 자동차 등록 현황 데이터 수집
"""
import pandas as pd
import requests
import logging
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import DATA_FILES
from database.db_helper import db_helper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PublicDataCrawler:
    def __init__(self):
        self.registration_file = DATA_FILES['registration']
        
    def load_registration_data(self, file_path=None):
        """엑셀 파일에서 자동차 등록 현황 데이터 로드"""
        try:
            # 파일 경로가 없으면 기본 경로 사용
            if not file_path:
                file_path = self.registration_file
                
            # 엑셀 파일 읽기
            logger.info(f"📂 파일 로드 중: {file_path}")
            
            # 여러 시트가 있을 수 있으므로 모든 시트 읽기
            excel_file = pd.ExcelFile(file_path)
            
            all_data = []
            
            for sheet_name in excel_file.sheet_names:
                logger.info(f"시트 처리 중: {sheet_name}")
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                
                # 데이터 정제
                df = self._clean_registration_data(df, sheet_name)
                if not df.empty:
                    all_data.append(df)
                    
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                logger.info(f"✅ 총 {len(combined_df)}건의 등록 데이터 로드 완료")
                return combined_df
            else:
                logger.warning("데이터를 찾을 수 없습니다.")
                return pd.DataFrame()
                
        except FileNotFoundError:
            logger.error(f"파일을 찾을 수 없습니다: {file_path}")
            logger.info("샘플 데이터를 생성합니다...")
            return self._create_sample_registration_data()
            
        except Exception as e:
            logger.error(f"데이터 로드 실패: {e}")
            return pd.DataFrame()
            
    def _clean_registration_data(self, df, sheet_name):
        """등록 데이터 정제"""
        try:
            # 컬럼명 표준화 (실제 데이터에 맞게 수정 필요)
            column_mapping = {
                '시도': 'region',
                '지역': 'region',
                '제조사': 'manufacturer',
                '브랜드': 'manufacturer',
                '차명': 'model_name',
                '모델': 'model_name',
                '차량명': 'model_name',
                '등록대수': 'registration_count',
                '대수': 'registration_count',
                '누적대수': 'cumulative_count',
                '날짜': 'registration_date',
                '기준일': 'registration_date',
                '연료': 'fuel_type',
                '연료구분': 'fuel_type'
            }
            
            # 컬럼명 변경
            df.rename(columns=column_mapping, inplace=True)
            
            # 필수 컬럼 확인
            required_columns = ['manufacturer', 'model_name', 'registration_count']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.warning(f"필수 컬럼 누락: {missing_columns}")
                return pd.DataFrame()
                
            # 날짜 처리
            if 'registration_date' not in df.columns:
                df['registration_date'] = pd.Timestamp.now().date()
            else:
                df['registration_date'] = pd.to_datetime(df['registration_date']).dt.date
                
            # 지역 정보가 없으면 기본값
            if 'region' not in df.columns:
                df['region'] = '전국'
                
            # 누적 대수가 없으면 등록 대수와 동일하게
            if 'cumulative_count' not in df.columns:
                df['cumulative_count'] = df['registration_count']
                
            # 결측치 처리
            df['registration_count'] = pd.to_numeric(df['registration_count'], errors='coerce').fillna(0)
            df['cumulative_count'] = pd.to_numeric(df['cumulative_count'], errors='coerce').fillna(0)
            
            # 유효한 데이터만 필터링
            df = df[df['registration_count'] > 0]
            
            return df
            
        except Exception as e:
            logger.error(f"데이터 정제 실패: {e}")
            return pd.DataFrame()
            
    def _create_sample_registration_data(self):
        """샘플 등록 데이터 생성"""
        sample_data = {
            'manufacturer': ['현대', '현대', '현대', '기아', '기아', '제네시스'] * 5,
            'model_name': ['그랜저 IG', '쏘나타 DN8', '아반떼 CN7', 'K5 DL3', 'K7 프리미어', 'G80'] * 5,
            'region': ['서울', '경기', '부산', '대구', '인천'] * 6,
            'registration_count': [1250, 2340, 890, 1560, 780, 450] * 5,
            'cumulative_count': [12500, 23400, 8900, 15600, 7800, 4500] * 5,
            'registration_date': pd.date_range(start='2024-01-01', periods=30, freq='D').date,
            'fuel_type': ['가솔린', '가솔린', '가솔린', '가솔린', '가솔린', '가솔린'] * 5
        }
        
        df = pd.DataFrame(sample_data)
        logger.info("✅ 샘플 데이터 생성 완료")
        return df
        
    def save_to_database(self, df):
        """데이터프레임을 데이터베이스에 저장"""
        db_helper.update_crawling_log('public_data', '시작')
        saved_count = 0
        
        try:
            for _, row in df.iterrows():
                # 모델 ID 조회 또는 생성
                model_id = db_helper.get_car_model_id(
                    row['manufacturer'], 
                    row['model_name']
                )
                
                if not model_id:
                    # 새 모델 등록
                    db_helper.insert_car_model(
                        manufacturer=row['manufacturer'],
                        model_name=row['model_name'],
                        fuel_type=row.get('fuel_type', None)
                    )
                    model_id = db_helper.get_car_model_id(
                        row['manufacturer'], 
                        row['model_name']
                    )
                    
                if model_id:
                    # 등록 통계 저장
                    db_helper.insert_registration_stats(
                        model_id=model_id,
                        region=row['region'],
                        registration_date=row['registration_date'],
                        registration_count=int(row['registration_count']),
                        cumulative_count=int(row.get('cumulative_count', row['registration_count']))
                    )
                    saved_count += 1
                    
            db_helper.update_crawling_log('public_data', '완료', saved_count)
            logger.info(f"✅ {saved_count}건의 등록 데이터 저장 완료")
            
        except Exception as e:
            db_helper.update_crawling_log('public_data', '실패', saved_count, str(e))
            logger.error(f"데이터베이스 저장 실패: {e}")
            
    def get_popular_models(self, top_n=10):
        """인기 모델 순위 조회"""
        query = """
        SELECT cm.manufacturer, cm.model_name, 
               SUM(rs.registration_count) as total_registrations
        FROM RegistrationStats rs
        JOIN CarModel cm ON rs.model_id = cm.model_id
        WHERE rs.registration_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
        GROUP BY cm.manufacturer, cm.model_name
        ORDER BY total_registrations DESC
        LIMIT %s
        """
        
        df = db_helper.fetch_dataframe(query, [top_n])
        return df
        
    def get_regional_stats(self):
        """지역별 등록 통계"""
        query = """
        SELECT region, 
               SUM(registration_count) as total_registrations,
               COUNT(DISTINCT model_id) as model_variety
        FROM RegistrationStats
        WHERE registration_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
        GROUP BY region
        ORDER BY total_registrations DESC
        """
        
        df = db_helper.fetch_dataframe(query)
        return df

# 테스트 실행
if __name__ == "__main__":
    crawler = PublicDataCrawler()
    
    # 1. 데이터 로드 (엑셀 파일 경로 지정)
    # df = crawler.load_registration_data('path/to/your/excel_file.xlsx')
    
    # 2. 샘플 데이터 로드
    df = crawler.load_registration_data()
    
    if not df.empty:
        print(f"로드된 데이터 수: {len(df)}건")
        print("\n데이터 미리보기:")
        print(df.head())
        
        # 3. 데이터베이스에 저장
        # crawler.save_to_database(df)
        
        print("\n✅ 공공데이터 크롤러 준비 완료!")
    else:
        print("❌ 데이터 로드 실패")
