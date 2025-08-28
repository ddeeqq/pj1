"""
데이터 수집 자동 스케줄러
"""
import schedule
import time
import logging
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from crawlers.encar_crawler import EncarCrawler
from crawlers.recall_crawler import RecallCrawler
from crawlers.public_data_crawler import PublicDataCrawler
from config.config import POPULAR_MODELS

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/scheduler_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataScheduler:
    def __init__(self):
        self.encar_crawler = None
        self.recall_crawler = RecallCrawler()
        self.public_crawler = PublicDataCrawler()
        
    def daily_price_update(self):
        """일일 가격 업데이트 (새벽 3시)"""
        logger.info("🌙 일일 가격 업데이트 시작...")
        
        try:
            # 인기 모델 Top 10만 업데이트
            car_list = []
            for manufacturer, models in POPULAR_MODELS.items():
                for model in models[:1]:  # 각 제조사별 1개씩만
                    car_list.append({
                        'manufacturer': manufacturer,
                        'model_name': model
                    })
            
            # 엔카 크롤러는 필요시에만 초기화 (리소스 절약)
            if not self.encar_crawler:
                self.encar_crawler = EncarCrawler()
                
            self.encar_crawler.crawl_and_save(car_list)
            logger.info("✅ 일일 가격 업데이트 완료")
            
        except Exception as e:
            logger.error(f"❌ 가격 업데이트 실패: {e}")
    
    def weekly_recall_update(self):
        """주간 리콜 정보 업데이트 (매주 월요일)"""
        logger.info("📅 주간 리콜 정보 업데이트 시작...")
        
        try:
            # 모든 등록된 모델의 리콜 정보 확인
            from database.db_helper import db_helper
            
            models_df = db_helper.get_car_models()
            car_list = []
            
            for _, model in models_df.iterrows():
                car_list.append({
                    'manufacturer': model['manufacturer'],
                    'model_name': model['model_name']
                })
            
            self.recall_crawler.crawl_and_save(car_list)
            logger.info("✅ 리콜 정보 업데이트 완료")
            
        except Exception as e:
            logger.error(f"❌ 리콜 업데이트 실패: {e}")
    
    def monthly_registration_update(self):
        """월간 등록 현황 업데이트 (매월 1일)"""
        logger.info("📊 월간 등록 현황 업데이트 시작...")
        
        try:
            # 엑셀 파일이 있는 경우 로드
            df = self.public_crawler.load_registration_data()
            if not df.empty:
                self.public_crawler.save_to_database(df)
                logger.info("✅ 등록 현황 업데이트 완료")
            else:
                logger.warning("등록 현황 데이터 파일을 찾을 수 없습니다.")
                
        except Exception as e:
            logger.error(f"❌ 등록 현황 업데이트 실패: {e}")
    
    def hourly_health_check(self):
        """시간별 시스템 상태 체크"""
        logger.info("🏥 시스템 상태 체크...")
        
        try:
            from database.db_helper import db_helper
            
            # DB 연결 확인
            with db_helper.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                
            # 최근 크롤링 로그 확인
            recent_logs = db_helper.execute_query("""
                SELECT source, status, records_collected, started_at
                FROM CrawlingLog
                WHERE started_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
                ORDER BY started_at DESC
                LIMIT 5
            """)
            
            if recent_logs:
                logger.info(f"최근 24시간 크롤링 작업: {len(recent_logs)}건")
                for log in recent_logs:
                    logger.info(f"  - {log['source']}: {log['status']} ({log['records_collected']}건)")
            else:
                logger.warning("최근 24시간 동안 크롤링 작업이 없습니다.")
                
            logger.info("✅ 시스템 정상 작동 중")
            
        except Exception as e:
            logger.error(f"❌ 시스템 상태 체크 실패: {e}")
    
    def cleanup_old_data(self):
        """오래된 데이터 정리 (매주 일요일)"""
        logger.info("🧹 오래된 데이터 정리 시작...")
        
        try:
            from database.db_helper import db_helper
            
            # 30일 이상된 가격 데이터 삭제
            query = """
            DELETE FROM UsedCarPrice 
            WHERE collected_date < DATE_SUB(CURDATE(), INTERVAL 30 DAY)
            """
            deleted = db_helper.execute_query(query, fetch=False)
            logger.info(f"✅ {deleted}개의 오래된 가격 데이터 삭제")
            
            # 90일 이상된 크롤링 로그 삭제
            query = """
            DELETE FROM CrawlingLog 
            WHERE started_at < DATE_SUB(NOW(), INTERVAL 90 DAY)
            """
            deleted = db_helper.execute_query(query, fetch=False)
            logger.info(f"✅ {deleted}개의 오래된 로그 삭제")
            
        except Exception as e:
            logger.error(f"❌ 데이터 정리 실패: {e}")
    
    def setup_schedule(self):
        """스케줄 설정"""
        # 매일 실행
        schedule.every().day.at("03:00").do(self.daily_price_update)
        
        # 매주 실행
        schedule.every().monday.at("04:00").do(self.weekly_recall_update)
        schedule.every().sunday.at("02:00").do(self.cleanup_old_data)
        
        # 매월 실행 (1일)
        schedule.every().day.at("05:00").do(self._check_monthly_task)
        
        # 매시간 실행
        schedule.every().hour.do(self.hourly_health_check)
        
        logger.info("✅ 스케줄 설정 완료")
        logger.info("📅 스케줄 목록:")
        logger.info("  - 매일 03:00: 가격 정보 업데이트")
        logger.info("  - 매주 월요일 04:00: 리콜 정보 업데이트")
        logger.info("  - 매주 일요일 02:00: 오래된 데이터 정리")
        logger.info("  - 매월 1일 05:00: 등록 현황 업데이트")
        logger.info("  - 매시간: 시스템 상태 체크")
    
    def _check_monthly_task(self):
        """월간 작업 체크 (매월 1일에만 실행)"""
        if datetime.now().day == 1:
            self.monthly_registration_update()
    
    def run(self):
        """스케줄러 실행"""
        logger.info("🚀 데이터 수집 스케줄러 시작...")
        
        # 초기 상태 체크
        self.hourly_health_check()
        
        # 스케줄 설정
        self.setup_schedule()
        
        # 스케줄 실행
        logger.info("⏰ 스케줄러가 실행 중입니다. Ctrl+C로 종료하세요.")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # 1분마다 체크
        except KeyboardInterrupt:
            logger.info("🛑 스케줄러 종료...")
            if self.encar_crawler:
                self.encar_crawler.close_driver()

# 실행
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='데이터 수집 스케줄러')
    parser.add_argument('--test', action='store_true', help='테스트 모드 (즉시 실행)')
    parser.add_argument('--task', choices=['price', 'recall', 'registration', 'health', 'cleanup'],
                       help='특정 작업만 실행')
    
    args = parser.parse_args()
    
    scheduler = DataScheduler()
    
    if args.test:
        logger.info("🧪 테스트 모드")
        scheduler.hourly_health_check()
        
    elif args.task:
        logger.info(f"🎯 단일 작업 실행: {args.task}")
        
        if args.task == 'price':
            scheduler.daily_price_update()
        elif args.task == 'recall':
            scheduler.weekly_recall_update()
        elif args.task == 'registration':
            scheduler.monthly_registration_update()
        elif args.task == 'health':
            scheduler.hourly_health_check()
        elif args.task == 'cleanup':
            scheduler.cleanup_old_data()
            
    else:
        scheduler.run()
