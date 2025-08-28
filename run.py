"""
중고차 vs 신차 분석 시스템 - 메인 실행 스크립트
"""
import os
import sys
import argparse
import logging
from datetime import datetime

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/app_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_streamlit():
    """Streamlit 앱 실행"""
    logger.info("🚀 Streamlit 앱을 시작합니다...")
    os.system("streamlit run ui/streamlit_app.py")

def init_database():
    """데이터베이스 초기화"""
    logger.info("🔧 데이터베이스를 초기화합니다...")
    from database.database_schema import DatabaseManager
    
    db_manager = DatabaseManager()
    db_manager.create_database()
    db_manager.create_tables()
    db_manager.insert_sample_data()
    logger.info("✅ 데이터베이스 초기화 완료!")

def crawl_all_data():
    """모든 데이터 크롤링 실행"""
    logger.info("🕷️ 전체 데이터 크롤링을 시작합니다...")
    
    # 공공데이터 수집
    try:
        from crawlers.public_data_crawler import PublicDataCrawler
        crawler = PublicDataCrawler()
        df = crawler.load_registration_data()
        if not df.empty:
            crawler.save_to_database(df)
            logger.info("✅ 공공데이터 수집 완료")
    except Exception as e:
        logger.error(f"❌ 공공데이터 수집 실패: {e}")
    
    # 중고차 가격 수집 (주의: 실제 크롤링 시 시간이 오래 걸림)
    try:
        from crawlers.encar_crawler import EncarCrawler
        from config.config import POPULAR_MODELS
        
        crawler = EncarCrawler()
        car_list = []
        for manufacturer, models in POPULAR_MODELS.items():
            for model in models[:2]:  # 각 제조사별 2개 모델만 테스트
                car_list.append({
                    'manufacturer': manufacturer,
                    'model_name': model
                })
        
        # crawler.crawl_and_save(car_list)  # 실제 크롤링 (주석 해제 시 실행)
        logger.info("✅ 중고차 가격 크롤링 준비 완료")
    except Exception as e:
        logger.error(f"❌ 중고차 가격 수집 실패: {e}")
    
    # 리콜 정보 수집
    try:
        from crawlers.recall_crawler import RecallCrawler
        crawler = RecallCrawler()
        # crawler.crawl_and_save(car_list)  # 실제 크롤링 (주석 해제 시 실행)
        logger.info("✅ 리콜 정보 크롤링 준비 완료")
    except Exception as e:
        logger.error(f"❌ 리콜 정보 수집 실패: {e}")

def test_connection():
    """데이터베이스 연결 테스트"""
    logger.info("🔍 데이터베이스 연결을 테스트합니다...")
    
    try:
        from database.db_helper import db_helper
        
        # 연결 테스트
        with db_helper.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()
            logger.info(f"✅ MySQL 버전: {version}")
            
        # 테이블 확인
        tables = db_helper.execute_query("SHOW TABLES")
        logger.info(f"✅ 테이블 수: {len(tables)}개")
        for table in tables:
            table_name = list(table.values())[0]
            count = db_helper.execute_query(f"SELECT COUNT(*) as cnt FROM {table_name}")
            logger.info(f"   - {table_name}: {count[0]['cnt']}개 레코드")
            
    except Exception as e:
        logger.error(f"❌ 데이터베이스 연결 실패: {e}")
        return False
    
    return True

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='중고차 vs 신차 분석 시스템')
    parser.add_argument(
        'command',
        choices=['run', 'init', 'crawl', 'test'],
        help='실행할 명령 (run: 앱 실행, init: DB 초기화, crawl: 데이터 수집, test: 연결 테스트)'
    )
    
    args = parser.parse_args()
    
    # 로그 디렉토리 생성
    os.makedirs('logs', exist_ok=True)
    
    logger.info("=" * 50)
    logger.info("🚗 중고차 vs 신차 가성비 분석 시스템")
    logger.info("=" * 50)
    
    if args.command == 'run':
        # 연결 테스트 후 앱 실행
        if test_connection():
            run_streamlit()
        else:
            logger.error("데이터베이스 연결 실패. 먼저 'python run.py init'를 실행하세요.")
            
    elif args.command == 'init':
        init_database()
        
    elif args.command == 'crawl':
        crawl_all_data()
        
    elif args.command == 'test':
        if test_connection():
            logger.info("✅ 모든 테스트 통과!")
        else:
            logger.error("❌ 테스트 실패!")

if __name__ == "__main__":
    main()
