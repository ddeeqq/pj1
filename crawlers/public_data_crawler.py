"""
공공데이터 포털에서 자동차 등록 현황 데이터 수집
"""
import pandas as pd
import logging
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_helper import db_helper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PublicDataCrawler:
    def __init__(self, config):
        """크롤러 초기화 시 설정(config)을 전달받음"""
        self.config = config
        
    def load_registration_data(self, file_path=None):
        """엑셀 파일에서 자동차 등록 현황 데이터 로드"""
        try:
            # 파일 경로가 없으면 설정의 기본 경로 사용
            if not file_path:
                file_path = self.config.get('file_path')
                if not file_path:
                    logger.error("설정에 file_path가 지정되지 않았습니다.")
                    return pd.DataFrame()

            logger.info(f"📂 파일 로드 중: {file_path}")
            excel_file = pd.ExcelFile(file_path)
            all_data = []
            
            for sheet_name in excel_file.sheet_names:
                logger.info(f"시트 처리 중: {sheet_name}")
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
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
            column_mapping = {
                '시도': 'region', '지역': 'region', '제조사': 'manufacturer', '브랜드': 'manufacturer',
                '차명': 'model_name', '모델': 'model_name', '차량명': 'model_name', '등록대수': 'registration_count',
                '대수': 'registration_count', '누적대수': 'cumulative_count', '날짜': 'registration_date',
                '기준일': 'registration_date', '연료': 'fuel_type', '연료구분': 'fuel_type'
            }
            df.rename(columns=column_mapping, inplace=True)
            
            required_columns = ['manufacturer', 'model_name', 'registration_count']
            if not all(col in df.columns for col in required_columns):
                logger.warning(f"필수 컬럼 누락: {required_columns}")
                return pd.DataFrame()
                
            if 'registration_date' not in df.columns:
                df['registration_date'] = pd.Timestamp.now().date()
            else:
                df['registration_date'] = pd.to_datetime(df['registration_date']).dt.date
            
            for col, default in [('region', '전국'), ('cumulative_count', df.get('registration_count'))]:
                if col not in df.columns:
                    df[col] = default
            
            df['registration_count'] = pd.to_numeric(df['registration_count'], errors='coerce').fillna(0)
            df['cumulative_count'] = pd.to_numeric(df['cumulative_count'], errors='coerce').fillna(0)
            return df[df['registration_count'] > 0]
            
        except Exception as e:
            logger.error(f"데이터 정제 실패: {e}")
            return pd.DataFrame()
            
    def _create_sample_registration_data(self):
        """샘플 등록 데이터 생성"""
        sample_data = {
            'manufacturer': ['현대', '기아', '제네시스'] * 2,
            'model_name': ['그랜저', '쏘나타', '아반떼', 'K5', 'K8', 'G80'],
            'region': ['서울', '경기', '부산', '대구', '인천', '광주'],
            'registration_count': [1250, 2340, 890, 1560, 780, 450],
            'registration_date': [datetime.now().date()] * 6
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
                model_id = db_helper.get_or_insert_car_model(
                    row['manufacturer'], row['model_name'], row.get('fuel_type')
                )
                if model_id:
                    db_helper.insert_registration_stats(
                        model_id=model_id, region=row['region'],
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

if __name__ == '__main__':
    import json
    print("공공데이터 크롤러 단독 테스트 모드")
    
    try:
        with open('config/scheduler_config.json', 'r', encoding='utf-8') as f:
            full_config = json.load(f)
        public_data_config = full_config['crawling']['public_data']
        print("✅ 테스트 설정 로드 완료")

        crawler = PublicDataCrawler(config=public_data_config)
        df = crawler.load_registration_data()
        
        if not df.empty:
            print(f"로드된 데이터 수: {len(df)}건")
            print("\n데이터 미리보기:")
            print(df.head())
            # crawler.save_to_database(df)
        else:
            print("❌ 데이터 로드 실패")

    except FileNotFoundError:
        print("오류: config/scheduler_config.json 파일을 찾을 수 없습니다.")
    except Exception as e:
        print(f"오류 발생: {e}")
