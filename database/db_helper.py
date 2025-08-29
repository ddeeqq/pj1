"""
데이터베이스 연결 및 쿼리 헬퍼 함수
"""
import mysql.connector
from mysql.connector import Error
import pandas as pd
from contextlib import contextmanager
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import DATABASE_CONFIG
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DBHelper:
    def __init__(self):
        self.config = DATABASE_CONFIG
        
    @contextmanager
    def get_db_connection(self):
        """컨텍스트 매니저로 데이터베이스 연결 관리"""
        connection = None
        try:
            connection = mysql.connector.connect(**self.config)
            yield connection
        except Error as e:
            logger.error(f"데이터베이스 연결 오류: {e}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()
                
    def execute_query(self, query, params=None, fetch=True):
        """단일 쿼리 실행"""
        with self.get_db_connection() as connection:
            cursor = connection.cursor(dictionary=True)
            try:
                cursor.execute(query, params or ())
                
                if fetch:
                    result = cursor.fetchall()
                    return result
                else:
                    connection.commit()
                    return cursor.rowcount
                    
            except Error as e:
                logger.error(f"쿼리 실행 오류: {e}")
                connection.rollback()
                raise
            finally:
                cursor.close()
                
    def execute_many(self, query, data_list):
        """다중 쿼리 실행 (bulk insert)"""
        with self.get_db_connection() as connection:
            cursor = connection.cursor()
            try:
                cursor.executemany(query, data_list)
                connection.commit()
                return cursor.rowcount
            except Error as e:
                logger.error(f"다중 쿼리 실행 오류: {e}")
                connection.rollback()
                raise
            finally:
                cursor.close()
                
    def fetch_dataframe(self, query, params=None):
        """쿼리 결과를 pandas DataFrame으로 반환"""
        with self.get_db_connection() as connection:
            try:
                df = pd.read_sql(query, connection, params=params)
                return df
            except Error as e:
                logger.error(f"DataFrame 조회 오류: {e}")
                raise
                
    # === CRUD 함수들 ===
    
    def insert_car_model(self, manufacturer, model_name, segment=None, fuel_type=None, release_year=None):
        """자동차 모델 삽입"""
        query = """
        INSERT INTO CarModel (manufacturer, model_name, segment, fuel_type, release_year)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP
        """
        params = (manufacturer, model_name, segment, fuel_type, release_year)
        return self.execute_query(query, params, fetch=False)
        
    def get_car_model_id(self, manufacturer, model_name, release_year=None):
        """자동차 모델 ID 조회"""
        if release_year:
            query = """
            SELECT model_id FROM CarModel 
            WHERE manufacturer = %s AND model_name = %s AND release_year = %s
            """
            params = (manufacturer, model_name, release_year)
        else:
            query = """
            SELECT model_id FROM CarModel 
            WHERE manufacturer = %s AND model_name = %s
            ORDER BY release_year DESC LIMIT 1
            """
            params = (manufacturer, model_name)
            
        result = self.execute_query(query, params)
        return result[0]['model_id'] if result else None
        
    def insert_used_car_price(self, model_id, year, mileage_range, avg_price, 
                            min_price=None, max_price=None, sample_count=0, 
                            data_source='manual', collected_date=None):
        """중고차 가격 정보 삽입"""
        query = """
        INSERT INTO UsedCarPrice 
        (model_id, year, mileage_range, avg_price, min_price, max_price, 
         sample_count, data_source, collected_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (model_id, year, mileage_range, avg_price, min_price, max_price,
                 sample_count, data_source, collected_date or pd.Timestamp.now().date())
        return self.execute_query(query, params, fetch=False)
        
    def insert_new_car_price(self, model_id, trim_name, base_price, 
                           options=None, total_price=None, promotion_discount=0):
        """신차 가격 정보 삽입"""
        query = """
        INSERT INTO NewCarPrice 
        (model_id, trim_name, base_price, options, total_price, promotion_discount, 
         valid_from, valid_until)
        VALUES (%s, %s, %s, %s, %s, %s, CURDATE(), DATE_ADD(CURDATE(), INTERVAL 1 MONTH))
        """
        total_price = total_price or base_price
        params = (model_id, trim_name, base_price, options, total_price, promotion_discount)
        return self.execute_query(query, params, fetch=False)
        
    def insert_recall_info_legacy(self, model_id, recall_date, recall_title, 
                          recall_reason, affected_units=0, severity_level='보통', 
                          fix_description=None):
        """레거시 리콜 정보 삽입 함수 (사용하지 않음)"""
        # 이 함수는 더 이상 사용되지 않습니다. insert_recall_info(**kwargs)를 사용하세요.
        pass
        
    def insert_registration_stats(self, model_id, region, registration_date, 
                                 registration_count, cumulative_count=0):
        """등록 통계 삽입"""
        query = """
        INSERT INTO RegistrationStats 
        (model_id, region, registration_date, registration_count, cumulative_count)
        VALUES (%s, %s, %s, %s, %s)
        """
        params = (model_id, region, registration_date, registration_count, cumulative_count)
        return self.execute_query(query, params, fetch=False)
        
    def get_used_car_prices(self, model_id=None, year=None):
        """중고차 가격 조회"""
        query = "SELECT * FROM UsedCarPrice WHERE 1=1"
        params = []
        
        if model_id:
            query += " AND model_id = %s"
            params.append(model_id)
        if year:
            query += " AND year = %s"
            params.append(year)
            
        query += " ORDER BY collected_date DESC"
        return self.fetch_dataframe(query, params)
        
    def get_new_car_prices(self, model_id=None):
        """신차 가격 조회"""
        query = """
        SELECT ncp.*, cm.manufacturer, cm.model_name 
        FROM NewCarPrice ncp
        JOIN CarModel cm ON ncp.model_id = cm.model_id
        WHERE valid_from <= CURDATE() AND valid_until >= CURDATE()
        """
        params = []
        
        if model_id:
            query += " AND ncp.model_id = %s"
            params.append(model_id)
            
        query += " ORDER BY cm.manufacturer, cm.model_name, ncp.base_price"
        return self.fetch_dataframe(query, params)
        
    def get_recall_info(self, model_id=None):
        """리콜 정보 조회"""
        query = """
        SELECT ri.*, cm.manufacturer, cm.model_name 
        FROM recall_info ri
        JOIN CarModel cm ON ri.model_id = cm.model_id
        WHERE 1=1
        """
        params = []
        
        if model_id:
            query += " AND ri.model_id = %s"
            params.append(model_id)
            
        query += " ORDER BY ri.recall_date DESC"
        return self.fetch_dataframe(query, params)
        
    def get_registration_stats(self, model_id=None, region=None, start_date=None, end_date=None):
        """등록 통계 조회"""
        query = """
        SELECT rs.*, cm.manufacturer, cm.model_name
        FROM RegistrationStats rs
        JOIN CarModel cm ON rs.model_id = cm.model_id
        WHERE 1=1
        """
        params = []
        
        if model_id:
            query += " AND rs.model_id = %s"
            params.append(model_id)
        if region:
            query += " AND rs.region = %s"
            params.append(region)
        if start_date:
            query += " AND rs.registration_date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND rs.registration_date <= %s"
            params.append(end_date)
            
        query += " ORDER BY rs.registration_date DESC"
        return self.fetch_dataframe(query, params)
        
    def get_car_models(self, manufacturer=None):
        """자동차 모델 목록 조회"""
        query = "SELECT * FROM CarModel WHERE 1=1"
        params = []
        
        if manufacturer:
            query += " AND manufacturer = %s"
            params.append(manufacturer)
            
        query += " ORDER BY manufacturer, model_name, release_year DESC"
        return self.fetch_dataframe(query, params)
        
    def update_crawling_log(self, source, status, records_collected=0, error_message=None):
        """크롤링 로그 업데이트"""
        query = """
        INSERT INTO CrawlingLog (source, status, records_collected, error_message)
        VALUES (%s, %s, %s, %s)
        """
        params = (source, status, records_collected, error_message)
        return self.execute_query(query, params, fetch=False)
        
    def get_latest_prices_comparison(self, model_id):
        """특정 모델의 최신 중고차/신차 가격 비교 데이터"""
        # 중고차 최신 평균가
        used_query = """
        SELECT AVG(avg_price) as used_avg_price, MIN(min_price) as used_min_price, 
               MAX(max_price) as used_max_price
        FROM UsedCarPrice
        WHERE model_id = %s AND collected_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
        """
        
        # 신차 최신가
        new_query = """
        SELECT MIN(base_price) as new_min_price, AVG(base_price) as new_avg_price
        FROM NewCarPrice
        WHERE model_id = %s AND valid_from <= CURDATE() AND valid_until >= CURDATE()
        """
        
        used_prices = self.fetch_dataframe(used_query, [model_id])
        new_prices = self.fetch_dataframe(new_query, [model_id])
        
        return {
            'used_prices': used_prices.to_dict('records')[0] if not used_prices.empty else {},
            'new_prices': new_prices.to_dict('records')[0] if not new_prices.empty else {}
        }

    def insert_recall_info(self, **kwargs):
        """리콜 정보 등록"""
        query = """
        INSERT INTO recall_info (
            model_id, recall_number, recall_date, recall_title, recall_reason,
            defect_content, correction_method, production_period, affected_units,
            target_quantity, corrected_quantity, correction_rate, severity_level,
            recall_type, device_category, recall_status, detail_url, source, collected_date
        ) VALUES (
            %(model_id)s, %(recall_number)s, %(recall_date)s, %(recall_title)s, %(recall_reason)s,
            %(defect_content)s, %(correction_method)s, %(production_period)s, %(affected_units)s,
            %(target_quantity)s, %(corrected_quantity)s, %(correction_rate)s, %(severity_level)s,
            %(recall_type)s, %(device_category)s, %(recall_status)s, %(detail_url)s, %(source)s, %(collected_date)s
        )
        ON DUPLICATE KEY UPDATE
            recall_title=VALUES(recall_title),
            recall_reason=VALUES(recall_reason),
            affected_units=VALUES(affected_units),
            correction_rate=VALUES(correction_rate),
            updated_at=CURRENT_TIMESTAMP
        """

        from datetime import datetime
        data = {
            'model_id': kwargs.get('model_id'),
            'recall_number': kwargs.get('recall_number'),
            'recall_date': kwargs.get('recall_date'),
            'recall_title': kwargs.get('recall_title', ''),
            'recall_reason': kwargs.get('recall_reason', ''),
            'defect_content': kwargs.get('defect_content', ''),
            'correction_method': kwargs.get('correction_method', ''),
            'production_period': kwargs.get('production_period', ''),
            'affected_units': kwargs.get('affected_units', 0),
            'target_quantity': kwargs.get('target_quantity', 0),
            'corrected_quantity': kwargs.get('corrected_quantity', 0),
            'correction_rate': kwargs.get('correction_rate', 0.0),
            'severity_level': kwargs.get('severity_level', '알수없음'),
            'recall_type': kwargs.get('recall_type', '안전'),
            'device_category': kwargs.get('device_category', ''),
            'recall_status': kwargs.get('recall_status', '진행중'),
            'detail_url': kwargs.get('detail_url', ''),
            'source': kwargs.get('source', 'car.go.kr'),
            'collected_date': kwargs.get('collected_date', datetime.now().date())
        }

        return self.execute_insert(query, data)

    def get_recall_statistics(self, manufacturer=None, model_name=None, days=365):
        """리콜 통계 조회"""
        base_query = """
        SELECT 
            cm.manufacturer,
            cm.model_name,
            COUNT(*) as total_recalls,
            SUM(CASE WHEN ri.severity_level = '매우심각' THEN 1 ELSE 0 END) as critical_recalls,
            SUM(CASE WHEN ri.severity_level = '심각' THEN 1 ELSE 0 END) as severe_recalls,
            SUM(CASE WHEN ri.severity_level = '보통' THEN 1 ELSE 0 END) as moderate_recalls,
            SUM(CASE WHEN ri.severity_level = '경미' THEN 1 ELSE 0 END) as minor_recalls,
            SUM(ri.affected_units) as total_affected_units,
            AVG(ri.correction_rate) as avg_correction_rate,
            MAX(ri.recall_date) as last_recall_date
        FROM recall_info ri
        JOIN CarModel cm ON ri.model_id = cm.model_id
        WHERE ri.recall_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
        """

        params = [days]

        if manufacturer:
            base_query += " AND cm.manufacturer = %s"
            params.append(manufacturer)

        if model_name:
            base_query += " AND cm.model_name = %s"
            params.append(model_name)

        base_query += " GROUP BY cm.manufacturer, cm.model_name ORDER BY total_recalls DESC"

        result = self.execute_query(base_query, tuple(params))

        if result:
            columns = ['manufacturer', 'model_name', 'total_recalls', 'critical_recalls',
                      'severe_recalls', 'moderate_recalls', 'minor_recalls', 
                      'total_affected_units', 'avg_correction_rate', 'last_recall_date']
            return pd.DataFrame(result, columns=columns)
        else:
            return pd.DataFrame()

    def insert_car_recall_check(self, car_number, recall_results):
        """차량별 리콜 확인 결과 저장"""
        for recall in recall_results:
            query = """
            INSERT INTO car_recall_history (
                car_number, model_id, recall_status, check_date, notes
            ) VALUES (
                %(car_number)s, %(model_id)s, %(recall_status)s, %(check_date)s, %(notes)s
            )
            ON DUPLICATE KEY UPDATE
                recall_status=VALUES(recall_status),
                check_date=VALUES(check_date)
            """

            # 모델 ID 찾기
            model_id = self.get_car_model_id(recall.get('manufacturer'), recall.get('model_name'))

            from datetime import datetime
            data = {
                'car_number': car_number,
                'model_id': model_id,
                'recall_status': recall.get('recall_status', '확인필요'),
                'check_date': recall.get('collected_date', datetime.now().date()),
                'notes': recall.get('recall_reason', '')
            }

            self.execute_insert(query, data)

    def insert_car_model(self, manufacturer, model_name, **kwargs):
        """자동차 모델 추가"""
        query = """
        INSERT IGNORE INTO CarModel (manufacturer, model_name, release_year, segment, fuel_type)
        VALUES (%s, %s, %s, %s, %s)
        """
        params = (
            manufacturer, 
            model_name, 
            kwargs.get('release_year', 2024),
            kwargs.get('segment', '일반'),
            kwargs.get('fuel_type', '가솔린')
        )
        return self.execute_query(query, params, fetch=False)

    def get_car_model_id(self, manufacturer, model_name):
        """자동차 모델 ID 조회"""
        query = "SELECT model_id FROM CarModel WHERE manufacturer = %s AND model_name = %s LIMIT 1"
        result = self.execute_query(query, (manufacturer, model_name))
        return result[0]['model_id'] if result else None
    
    def get_or_insert_car_model(self, manufacturer, model_name, fuel_type=None, **kwargs):
        """자동차 모델 조회 또는 삽입"""
        # 먼저 모델 ID 조회 시도
        model_id = self.get_car_model_id(manufacturer, model_name)
        
        # 모델이 존재하지 않으면 새로 삽입
        if not model_id:
            self.insert_car_model(
                manufacturer=manufacturer, 
                model_name=model_name, 
                fuel_type=fuel_type,
                **kwargs
            )
            # 다시 조회하여 ID 반환
            model_id = self.get_car_model_id(manufacturer, model_name)
        
        return model_id

    def execute_insert(self, query, data):
        """INSERT 쿼리 실행"""
        with self.get_db_connection() as connection:
            cursor = connection.cursor()
            try:
                if isinstance(data, dict):
                    # 딕셔너리인 경우 named parameter 사용
                    cursor.execute(query, data)
                else:
                    # 튜플/리스트인 경우 positional parameter 사용  
                    cursor.execute(query, data)
                connection.commit()
                return cursor.lastrowid
            except Error as e:
                logger.error(f"INSERT 쿼리 실행 오류: {e}")
                connection.rollback()
                raise

# 싱글톤 패턴으로 인스턴스 생성
db_helper = DBHelper()
