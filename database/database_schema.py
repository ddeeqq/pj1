"""
데이터베이스 스키마 정의 및 초기화
"""
import mysql.connector
from mysql.connector import Error
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import DATABASE_CONFIG

class DatabaseManager:
    def __init__(self):
        self.config = DATABASE_CONFIG.copy()
        self.database_name = self.config.pop('database')
        
    def create_database(self):
        """데이터베이스 생성"""
        try:
            connection = mysql.connector.connect(**self.config)
            cursor = connection.cursor()
            
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            print(f"SUCCESS: 데이터베이스 '{self.database_name}' 생성/확인 완료")
            
            cursor.close()
            connection.close()
            
        except Error as e:
            print(f"ERROR: 데이터베이스 생성 오류: {e}")
            
    def get_connection(self):
        """데이터베이스 연결 반환"""
        try:
            self.config['database'] = self.database_name
            return mysql.connector.connect(**DATABASE_CONFIG)
        except Error as e:
            print(f"ERROR: 연결 오류: {e}")
            return None
            
    def create_tables(self):
        """모든 테이블 생성"""
        connection = self.get_connection()
        if not connection:
            return
            
        cursor = connection.cursor()
        
        # 1. 자동차 모델 마스터 테이블
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS CarModel (
            model_id INT AUTO_INCREMENT PRIMARY KEY,
            manufacturer VARCHAR(50) NOT NULL,
            model_name VARCHAR(100) NOT NULL,
            segment VARCHAR(50),
            fuel_type VARCHAR(30),
            release_year INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_model (manufacturer, model_name, release_year),
            INDEX idx_manufacturer (manufacturer),
            INDEX idx_model_name (model_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 2. 자동차 등록 통계 테이블
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS RegistrationStats (
            stat_id INT AUTO_INCREMENT PRIMARY KEY,
            model_id INT,
            region VARCHAR(50),
            registration_date DATE,
            registration_count INT DEFAULT 0,
            cumulative_count INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (model_id) REFERENCES CarModel(model_id) ON DELETE CASCADE,
            INDEX idx_date (registration_date),
            INDEX idx_region (region)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 3. 중고차 가격 테이블
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS UsedCarPrice (
            price_id INT AUTO_INCREMENT PRIMARY KEY,
            model_id INT,
            year INT,
            mileage_range VARCHAR(50),
            avg_price DECIMAL(12, 2),
            min_price DECIMAL(12, 2),
            max_price DECIMAL(12, 2),
            sample_count INT DEFAULT 0,
            data_source VARCHAR(50),
            collected_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (model_id) REFERENCES CarModel(model_id) ON DELETE CASCADE,
            INDEX idx_year (year),
            INDEX idx_collected_date (collected_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 4. 신차 가격 테이블
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS NewCarPrice (
            price_id INT AUTO_INCREMENT PRIMARY KEY,
            model_id INT,
            trim_name VARCHAR(100),
            base_price DECIMAL(12, 2),
            options TEXT,
            total_price DECIMAL(12, 2),
            promotion_discount DECIMAL(12, 2) DEFAULT 0,
            valid_from DATE,
            valid_until DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (model_id) REFERENCES CarModel(model_id) ON DELETE CASCADE,
            INDEX idx_trim (trim_name),
            INDEX idx_valid_date (valid_from, valid_until)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 5. 리콜 정보 테이블 (통합된 버전)
        # 이 테이블은 나중에 정의된 recall_info 테이블로 대체됩니다.
        
        # 6. 선호 연령대 테이블
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Demographics (
            demo_id INT AUTO_INCREMENT PRIMARY KEY,
            model_id INT,
            age_group VARCHAR(20),
            preference_score DECIMAL(5, 2),
            gender VARCHAR(10),
            analysis_date DATE,
            data_source VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (model_id) REFERENCES CarModel(model_id) ON DELETE CASCADE,
            INDEX idx_age_group (age_group)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 7. FAQ 테이블
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS FAQ (
            faq_id INT AUTO_INCREMENT PRIMARY KEY,
            model_id INT,
            question TEXT NOT NULL,
            answer TEXT,
            category VARCHAR(50),
            view_count INT DEFAULT 0,
            helpful_count INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (model_id) REFERENCES CarModel(model_id) ON DELETE CASCADE,
            INDEX idx_category (category),
            FULLTEXT idx_question_answer (question, answer)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 8. 크롤링 로그 테이블
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS CrawlingLog (
            log_id INT AUTO_INCREMENT PRIMARY KEY,
            source VARCHAR(50),
            status ENUM('시작', '진행중', '완료', '실패') DEFAULT '시작',
            records_collected INT DEFAULT 0,
            error_message TEXT,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP NULL,
            INDEX idx_source (source),
            INDEX idx_status (status),
            INDEX idx_started_at (started_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 9. 사용자 검색 기록 테이블 (향후 개인화 기능용)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS SearchHistory (
            search_id INT AUTO_INCREMENT PRIMARY KEY,
            session_id VARCHAR(100),
            model_id INT,
            search_type VARCHAR(50),
            budget_range VARCHAR(50),
            search_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (model_id) REFERENCES CarModel(model_id) ON DELETE CASCADE,
            INDEX idx_session (session_id),
            INDEX idx_timestamp (search_timestamp)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 10. 가격 변동 이력 테이블
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS PriceHistory (
            history_id INT AUTO_INCREMENT PRIMARY KEY,
            model_id INT,
            price_type ENUM('중고차', '신차'),
            price DECIMAL(12, 2),
            recorded_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (model_id) REFERENCES CarModel(model_id) ON DELETE CASCADE,
            INDEX idx_recorded_date (recorded_date),
            INDEX idx_price_type (price_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # 11. 리콜 정보 테이블
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS RecallInfo (
            id INT PRIMARY KEY AUTO_INCREMENT,
            model_id INT,
            recall_number VARCHAR(50),
            recall_date DATE,
            recall_title VARCHAR(300),
            recall_reason TEXT,
            defect_content TEXT,
            correction_method TEXT,
            production_period VARCHAR(100),
            affected_units INT DEFAULT 0,
            target_quantity INT DEFAULT 0,
            corrected_quantity INT DEFAULT 0,
            correction_rate DECIMAL(5,2) DEFAULT 0.00,
            severity_level VARCHAR(20) DEFAULT '알수없음',
            recall_type VARCHAR(20) DEFAULT '안전',
            device_category VARCHAR(50),
            recall_status VARCHAR(20) DEFAULT '진행중',
            detail_url TEXT,
            source VARCHAR(50) DEFAULT 'car.go.kr',
            collected_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (model_id) REFERENCES CarModel(model_id),
            INDEX idx_recall_date (recall_date),
            INDEX idx_model_severity (model_id, severity_level),
            INDEX idx_collected_date (collected_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 12. 차량별 리콜 확인 이력
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS car_recall_history (
            id INT PRIMARY KEY AUTO_INCREMENT,
            car_number VARCHAR(20),
            vin VARCHAR(50),
            model_id INT,
            recall_id INT,
            check_date DATE,
            recall_status VARCHAR(20),
            correction_completed BOOLEAN DEFAULT FALSE,
            correction_date DATE,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (model_id) REFERENCES CarModel(model_id),
            FOREIGN KEY (recall_id) REFERENCES RecallInfo(id),
            INDEX idx_car_number (car_number),
            INDEX idx_check_date (check_date),
            UNIQUE KEY unique_car_recall (car_number, recall_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # 13. 리콜 통계 테이블
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS recall_statistics (
            id INT PRIMARY KEY AUTO_INCREMENT,
            manufacturer VARCHAR(50),
            model_name VARCHAR(100),
            year INT,
            total_recalls INT DEFAULT 0,
            critical_recalls INT DEFAULT 0,
            severe_recalls INT DEFAULT 0,
            moderate_recalls INT DEFAULT 0,
            minor_recalls INT DEFAULT 0,
            total_affected_units INT DEFAULT 0,
            avg_correction_rate DECIMAL(5,2) DEFAULT 0.00,
            last_recall_date DATE,
            analysis_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_model_year (manufacturer, model_name, year),
            INDEX idx_manufacturer (manufacturer),
            INDEX idx_analysis_date (analysis_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        connection.commit()
        print("SUCCESS: 모든 테이블 (리콜 테이블 포함) 생성 완료!")
        
        cursor.close()
        connection.close()
        
    def initialize_with_sample_data(self):
        """데이터베이스 초기화 및 샘플 데이터 생성"""
        self.create_database()
        self.create_tables()
        
        # init_data.py의 기능 활용
        try:
            from init_data import DataInitializer
            initializer = DataInitializer()
            initializer.initialize_all()
            print("SUCCESS: 데이터베이스 초기화 및 샘플 데이터 생성 완료")
        except ImportError as e:
            print(f"WARNING: init_data 모듈을 찾을 수 없습니다: {e}")
            print("기본 샘플 데이터를 직접 삽입합니다.")
            self._insert_basic_sample_data()
    
    def _insert_basic_sample_data(self):
        """기본 샘플 데이터 삽입 (fallback)"""
        connection = self.get_connection()
        if not connection:
            return
            
        cursor = connection.cursor()
        
        # 기본 자동차 모델 데이터
        sample_models = [
            ('현대', '그랜저 IG', '중형', '가솔린', 2017),
            ('현대', '쏘나타 DN8', '준중형', '가솔린', 2019),
            ('현대', '아반떼 CN7', '소형', '가솔린', 2020),
            ('기아', 'K5 DL3', '준중형', '가솔린', 2019),
            ('기아', 'K7 프리미어', '중형', '가솔린', 2021),
            ('제네시스', 'G80', '중형', '가솔린', 2020),
            ('현대', '투싼 NX4', 'SUV중형', '가솔린', 2020),
            ('기아', '쏘렌토 MQ4', 'SUV중형', '디젤', 2020),
        ]
        
        for model_data in sample_models:
            try:
                cursor.execute("""
                    INSERT INTO CarModel (manufacturer, model_name, segment, fuel_type, release_year)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP
                """, model_data)
            except Error as e:
                print(f"샘플 데이터 삽입 중 오류: {e}")
                
        connection.commit()
        print("SUCCESS: 기본 샘플 데이터 삽입 완료!")
        
        cursor.close()
        connection.close()

    def reset_database(self):
        """데이터베이스 초기화 (주의: 모든 데이터 삭제)"""
        response = input("WARNING: 모든 데이터가 삭제됩니다. 계속하시겠습니까? (yes/no): ")
        if response.lower() != 'yes':
            print("초기화 취소됨")
            return
            
        connection = self.get_connection()
        if not connection:
            return
            
        cursor = connection.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS {self.database_name}")
        cursor.close()
        connection.close()
        
        print("SUCCESS: 데이터베이스 삭제 완료")
        
        # 재생성
        self.initialize_with_sample_data()

# 실행 코드
if __name__ == "__main__":
    db_manager = DatabaseManager()
    
    print("=" * 50)
    print("=== 중고차 vs 신차 분석 시스템 - 데이터베이스 설정 ===")
    print("=" * 50)
    
    # 데이터베이스 생성
    db_manager.create_database()
    
    # 테이블 생성
    db_manager.create_tables()
    
    # 샘플 데이터 삽입
    db_manager.initialize_with_sample_data()
    
    print("\nSUCCESS: 데이터베이스 설정 완룜!")
