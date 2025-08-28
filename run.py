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
    """모든 데이터 크롤링 실행 (리팩토링된 버전)"""
    logger.info("🕷️ 전체 데이터 크롤링을 시작합니다...")

    # 설정 로드
    try:
        with open('config/scheduler_config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)['crawling']
    except Exception as e:
        logger.error(f"설정 파일을 읽는 데 실패했습니다: {e}")
        return

    # 1. 공공데이터 수집
    try:
        from crawlers.public_data_crawler import PublicDataCrawler
        logger.info("--- 공공데이터 수집 시작 ---")
        pd_crawler = PublicDataCrawler(config.get('public_data', {}))
        df = pd_crawler.load_registration_data()
        if not df.empty:
            pd_crawler.save_to_database(df)
        logger.info("✅ 공공데이터 수집 완료")
    except Exception as e:
        logger.error(f"❌ 공공데이터 수집 실패: {e}")
    
    # 2. 중고차 가격 수집
    logger.warning("엔카 중고차 가격 크롤링은 시간이 오래 걸릴 수 있습니다.")
    response = input("중고차 가격을 크롤링하시겠습니까? (y/n): ").lower()
    if response == 'y':
        try:
            from crawlers.encar_crawler import EncarCrawler
            from config.config import POPULAR_MODELS
            logger.info("--- 중고차 가격 수집 시작 ---")
            
            encar_crawler = EncarCrawler(config.get('encar', {}))
            car_list = []
            # 테스트를 위해 각 제조사별 1개 모델만 크롤링
            for manufacturer, models in POPULAR_MODELS.items():
                if models:
                    car_list.append({'manufacturer': manufacturer, 'model_name': models[0]})
            
            encar_crawler.crawl_and_save(car_list)
            logger.info("✅ 중고차 가격 크롤링 완료")
        except Exception as e:
            logger.error(f"❌ 중고차 가격 수집 실패: {e}")

    # 3. 리콜 정보 수집
    response = input("리콜 정보를 크롤링하시겠습니까? (y/n): ").lower()
    if response == 'y':
        try:
            from crawlers.recall_crawler import RecallCrawler
            from database.db_helper import db_helper
            logger.info("--- 리콜 정보 수집 시작 ---")

            recall_crawler = RecallCrawler(config.get('recall', {}))
            models_df = db_helper.get_car_models()
            car_list = models_df.to_dict('records')

            recall_crawler.crawl_and_save(car_list)
            logger.info("✅ 리콜 정보 수집 완료")
        except Exception as e:
            logger.error(f"❌ 리콜 정보 수집 실패: {e}")

def test_connection():
    """데이터베이스 연결 테스트"""
    logger.info("🔍 데이터베이스 연결을 테스트합니다...")
    try:
        from database.db_helper import db_helper
        with db_helper.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()
            logger.info(f"✅ MySQL 버전: {version[0]}")
        return True
    except Exception as e:
        logger.error(f"❌ 데이터베이스 연결 실패: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='중고차 vs 신차 분석 시스템')
    parser.add_argument(
        'command',
        choices=['run', 'init', 'crawl', 'test'],
        help='실행할 명령 (run: 앱 실행, init: DB 초기화, crawl: 데이터 수집, test: 연결 테스트)'
    )
    args = parser.parse_args()
    
    os.makedirs('logs', exist_ok=True)
    logger.info(f"\n{'='*20} {args.command.upper()} 시작 {'='*20}")
    
    if args.command == 'run':
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
