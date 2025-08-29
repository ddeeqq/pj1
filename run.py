"""
중고차 vs 신차 분석 시스템 - 메인 실행 스크립트
"""
import os
import sys
import argparse
import logging
import json
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
    """Run Streamlit app"""
    logger.info("Starting Streamlit app...")
    os.system("streamlit run ui/streamlit_app.py")

def init_database():
    """Initialize database"""
    logger.info("Initializing database...")
    from database.database_schema import DatabaseManager
    
    db_manager = DatabaseManager()
    db_manager.initialize_with_sample_data()
    logger.info("Database initialization completed!")

def crawl_all_data():
    """모든 데이터 크롤링 실행 (리팩토링된 버전)"""
    logger.info("Starting full data crawling...")

    # 설정 로드
    try:
        with open('config/scheduler_config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)['crawling']
    except Exception as e:
        logger.error(f"Failed to read config file: {e}")
        return

    # 1. 공공데이터 수집 (비활성화)
    # try:
    #     from crawlers.public_data_crawler import PublicDataCrawler
    #     logger.info("--- 공공데이터 수집 시작 ---")
    #     pd_crawler = PublicDataCrawler(config.get('public_data', {}))
    #     df = pd_crawler.load_registration_data()
    #     if not df.empty:
    #         pd_crawler.save_to_database(df)
    #     logger.info("✅ 공공데이터 수집 완료")
    # except Exception as e:
    #     logger.error(f"❌ 공공데이터 수집 실패: {e}")
    logger.info("WARNING: Public data collection is temporarily disabled.")
    
    # 2. 중고차 가격 수집 (비활성화)
    # logger.warning("엔카 중고차 가격 크롤링은 시간이 오래 걸릴 수 있습니다.")
    # response = input("중고차 가격을 크롤링하시겠습니까? (y/n): ").lower()
    # if response == 'y':
    #     try:
    #         from crawlers.encar_crawler import EncarCrawler
    #         from config.config import POPULAR_MODELS
    #         logger.info("--- 중고차 가격 수집 시작 ---")
    logger.info("WARNING: Used car price crawling is temporarily disabled.")
            
    #         encar_crawler = EncarCrawler(config.get('encar', {}))
    #         car_list = []
    #         # 테스트를 위해 각 제조사별 1개 모델만 크롤링
    #         for manufacturer, models in POPULAR_MODELS.items():
    #             if models:
    #                 car_list.append({'manufacturer': manufacturer, 'model_name': models[0]})
    #         
    #         encar_crawler.crawl_and_save(car_list)
    #         logger.info("✅ 중고차 가격 크롤링 완료")
    #     except Exception as e:
    #         logger.error(f"❌ 중고차 가격 수집 실패: {e}")

    # 3. 리콜 정보 수집 (비활성화)
    # response = input("리콜 정보를 크롤링하시겠습니까? (y/n): ").lower()
    # if response == 'y':
    #     try:
    #         from crawlers.recall_crawler import RecallCrawler
    #         from database.db_helper import db_helper
    #         logger.info("--- 리콜 정보 수집 시작 ---")

    #         recall_crawler = RecallCrawler(config.get('recall', {}))
    #         models_df = db_helper.get_car_models()
    #         car_list = models_df.to_dict('records')

    #         recall_crawler.crawl_and_save(car_list)
    #         logger.info("✅ 리콜 정보 수집 완료")
    #     except Exception as e:
    #         logger.error(f"❌ 리콜 정보 수집 실패: {e}")
    logger.info("WARNING: Recall information crawling is temporarily disabled.")

def test_connection():
    """Test database connection"""
    logger.info("Testing database connection...")
    try:
        from database.db_helper import db_helper
        with db_helper.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()
            logger.info(f"SUCCESS: MySQL version: {version[0]}")
        return True
    except Exception as e:
        logger.error(f"ERROR: Database connection failed: {e}")
        logger.error("INFO: MySQL is not installed or .env file configuration is required.")
        logger.error("INFO: Please create .env file referring to .env.example file.")
        return False

def main():
    parser = argparse.ArgumentParser(description='Used vs New Car Analysis System')
    parser.add_argument(
        'command',
        choices=['run', 'init', 'crawl', 'test'],
        help='Command to execute (run: run app, init: init DB, crawl: collect data, test: connection test)'
    )
    args = parser.parse_args()
    
    os.makedirs('logs', exist_ok=True)
    logger.info(f"\n{'='*20} {args.command.upper()} 시작 {'='*20}")
    
    if args.command == 'run':
        if test_connection():
            run_streamlit()
        else:
            logger.error("Database connection failed. Please run 'python run.py init' first.")
    elif args.command == 'init':
        init_database()
    elif args.command == 'crawl':
        crawl_all_data()
    elif args.command == 'test':
        if test_connection():
            logger.info("SUCCESS: All tests passed!")
        else:
            logger.error("ERROR: Tests failed!")

if __name__ == "__main__":
    main()
