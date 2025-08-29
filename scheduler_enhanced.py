"""
Automated Data Collection Scheduler (Integrated Version)
"""
import schedule
import time
import logging
import psutil
import json
from datetime import datetime, timedelta
import sys
import os
import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# from crawlers.encar_crawler import EncarCrawler
# from crawlers.recall_crawler import RecallCrawler
# from crawlers.public_data_crawler import PublicDataCrawler
from config.config import POPULAR_MODELS

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/scheduler_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnhancedDataScheduler:
    def __init__(self, config_path='config/scheduler_config.json'):
        self.config = self._load_config(config_path)
        self.stats = {
            'total_runs': 0,
            'successful_runs': 0,
            'failed_runs': 0,
            'last_run': None,
            'performance_metrics': []
        }

        # 설정에 기반하여 크롤러 인스턴스 생성
        crawling_config = self.config.get('crawling', {})
        # self.encar_crawler = EncarCrawler(config=crawling_config.get('encar', {}))
        # self.recall_crawler = RecallCrawler(config=crawling_config.get('recall', {}))
        # self.public_crawler = PublicDataCrawler(config=crawling_config.get('public_data', {}))
        logger.info("WARNING: Crawlers are temporarily disabled.")
        
    def _load_config(self, config_path):
        """Load configuration file"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                logger.info(f"✅ 설정 파일 로드: {config_path}")
                return config
        except FileNotFoundError:
            logger.error(f"❌ 설정 파일을 찾을 수 없습니다: {config_path}")
            sys.exit(1)
        except json.JSONDecodeError:
            logger.error(f"❌ 설정 파일이 유효한 JSON 형식이 아닙니다: {config_path}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"❌ 설정 파일 로드 중 오류 발생: {e}")
            sys.exit(1)

    def check_system_resources(self):
        """시스템 리소스 확인"""
        try:
            limits = self.config.get('resource_limits', {})
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            resources = {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_available': memory.available // (1024**3),  # GB
                'disk_percent': disk.percent,
                'disk_free': disk.free // (1024**3)  # GB
            }
            
            logger.info(f"💻 시스템 리소스: CPU {cpu_percent}%, RAM {memory.percent}%, Disk {disk.percent}%")
            
            # 리소스 부족 경고 (설정 파일 기준)
            if cpu_percent > limits.get('max_cpu_percent', 80):
                logger.warning(f"High CPU usage: {cpu_percent}%")
            if memory.percent > limits.get('max_memory_percent', 85):
                logger.warning(f"High memory usage: {memory.percent}%")
            if disk.percent > limits.get('max_disk_percent', 90):
                logger.warning(f"High disk usage: {disk.percent}%")
                
            return resources
            
        except Exception as e:
            logger.error(f"시스템 리소스 확인 실패: {e}")
            return None
    
    def send_email_notification(self, subject, message):
        """이메일 알림 발송"""
        email_conf = self.config.get('email', {})
        if not email_conf.get('enabled') or not email_conf.get('recipients'):
            return
            
        try:
            msg = MimeMultipart()
            msg['From'] = email_conf['email']
            msg['To'] = ', '.join(email_conf['recipients'])
            msg['Subject'] = f"[차량분석시스템] {subject}"
            
            msg.attach(MimeText(message, 'plain', 'utf-8'))
            
            server = smtplib.SMTP(email_conf['smtp_server'], email_conf['smtp_port'])
            server.starttls()
            server.login(email_conf['email'], email_conf['password'])
            text = msg.as_string()
            server.sendmail(email_conf['email'], email_conf['recipients'], text)
            server.quit()
            
            logger.info(" 이메일 알림 발송 완료")
            
        except Exception as e:
            logger.error(f"이메일 발송 실패: {e}")
    
    def retry_with_backoff(self, func, *args, **kwargs):
        """지수 백오프를 사용한 재시도 로직"""
        max_retries = self.config.get('scheduler', {}).get('max_retries', 3)
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                wait_time = 2 ** attempt
                logger.warning(f" 작업 실패 (시도 {attempt + 1}/{max_retries}): {e}")
                
                if attempt < max_retries - 1:
                    logger.info(f" {wait_time}초 후 재시도...")
                    time.sleep(wait_time)
                else:
                    logger.error(f" 최대 재시도 횟수 초과. 작업 실패: {func.__name__}")
                    raise
    
    def validate_collected_data(self, source, records_collected):
        """수집된 데이터 품질 검증"""
        alerts_conf = self.config.get('alerts', {})
        if not self.config.get('scheduler', {}).get('enable_data_validation', True):
            return

        try:
            from database.db_helper import db_helper
            
            query = "SELECT AVG(records_collected) as avg_records, COUNT(*) as log_count FROM CrawlingLog WHERE source = %s AND status = '완료' AND started_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)"
            result = db_helper.execute_query(query, [source])
            
            if result and result[0]['log_count'] > 0:
                avg_records = result[0]['avg_records'] or 0
                threshold = alerts_conf.get('data_collection_drop_threshold', 0.5)
                
                if records_collected < avg_records * threshold:
                    logger.warning(f"⚠️ 데이터 수집량 급감: {source} (평균: {avg_records:.0f}, 현재: {records_collected})")
                    self.send_email_notification(
                        f"데이터 수집량 급감 알림",
                        f"소스: {source}\n평균 수집량: {avg_records:.0f}\n현재 수집량: {records_collected}\n\n확인이 필요합니다.")
                elif records_collected > avg_records * 2:
                    logger.info(f" 데이터 수집량 증가: {source} (평균: {avg_records:.0f}, 현재: {records_collected})")
                    
        except Exception as e:
            logger.error(f"데이터 검증 실패: {e}")
    
    def backup_database(self):
        """데이터베이스 백업"""
        try:
            from database.db_helper import db_helper
            import pandas as pd
            
            backup_dir = f"data/backup/{datetime.now().strftime('%Y%m%d')}"
            os.makedirs(backup_dir, exist_ok=True)
            
            important_tables = ['CarModel', 'UsedCarPrice', 'NewCarPrice', 'RegistrationStats', 'RecallInfo']
            
            for table in important_tables:
                try:
                    df = db_helper.fetch_dataframe(f"SELECT * FROM {table}")
                    if not df.empty:
                        backup_file = os.path.join(backup_dir, f"{table}_{datetime.now().strftime('%H%M')}.csv")
                        df.to_csv(backup_file, index=False, encoding='utf-8')
                        logger.info(f"📁 {table} 백업 완료 ({len(df)}건)")
                except Exception as e:
                    logger.error(f"테이블 {table} 백업 실패: {e}")
            logger.info(f" 데이터베이스 백업 완료: {backup_dir}")
            
        except Exception as e:
            logger.error(f"백업 실패: {e}")

    def daily_price_update(self):
        """일일 가격 업데이트"""
        start_time = datetime.now()
        logger.info("🌙 일일 가격 업데이트 시작...")
        
        resources = self.check_system_resources()
        
        try:
            car_list = [{'manufacturer': m, 'model_name': models[0]} for m, models in POPULAR_MODELS.items()]
            
            
            
            # self.retry_with_backoff(lambda: self.encar_crawler.crawl_and_save(car_list))
            logger.info("⚠️  엔카 크롤링이 일시적으로 비활성화되었습니다.")
            
            duration = (datetime.now() - start_time).total_seconds()
            
            from database.db_helper import db_helper
            recent_log = db_helper.execute_query("SELECT records_collected FROM CrawlingLog WHERE source = 'encar' ORDER BY started_at DESC LIMIT 1")
            records_collected = recent_log[0]['records_collected'] if recent_log else 0
            self.validate_collected_data('encar', records_collected)
            
            self.stats['successful_runs'] += 1
            if self.config.get('scheduler', {}).get('enable_performance_monitoring', True):
                self.stats['performance_metrics'].append({
                    'task': 'daily_price_update', 'duration': duration, 'records': records_collected,
                    'timestamp': start_time.isoformat(), 'resources': resources
                })
            
            logger.info(f"✅ 일일 가격 업데이트 완료 (소요시간: {duration:.1f}초)")
            
        except Exception as e:
            self.stats['failed_runs'] += 1
            logger.error(f"❌ 가격 업데이트 실패: {e}")
            if self.config.get('email', {}).get('send_error_alerts', True):
                self.send_email_notification("가격 업데이트 실패", f"시간: {start_time}\n오류: {str(e)}\n\n확인이 필요합니다.")
        finally:
            self.stats['total_runs'] += 1
            self.stats['last_run'] = datetime.now().isoformat()

    def weekly_recall_update(self):
        """주간 리콜 정보 업데이트"""
        start_time = datetime.now()
        logger.info(" 주간 리콜 정보 업데이트 시작...")
        
        try:
            from database.db_helper import db_helper
            models_df = db_helper.get_car_models()
            car_list = [{'manufacturer': r['manufacturer'], 'model_name': r['model_name']} for i, r in models_df.iterrows()]
            
            # self.retry_with_backoff(lambda: self.recall_crawler.crawl_and_save(car_list))
            logger.info("⚠️  리콜 크롤링이 일시적으로 비활성화되었습니다.")
            
            keywords = self.config.get('alerts', {}).get('critical_recall_keywords', ['화재', '브레이크'])
            placeholders = ','.join(['%s'] * len(keywords))
            query = f"SELECT COUNT(*) as count FROM RecallInfo WHERE ({' OR '.join(['contents LIKE %s'] * len(keywords))}) AND recall_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)"
            params = [f'%{keyword}%' for keyword in keywords]
            
            critical_recalls = db_helper.execute_query(query, params)
            
            if critical_recalls and critical_recalls[0]['count'] > 0:
                count = critical_recalls[0]['count']
                logger.warning(f" 주의: 최근 일주일간 심각한 리콜 {count}건 발생")
                if self.config.get('alerts', {}).get('notify_on_new_critical_recalls', True):
                    self.send_email_notification(f"심각한 리콜 {count}건 발생", f"최근 일주일간 심각도가 높은 리콜이 {count}건 발생했습니다.\n시스템에서 확인해주세요.")
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"✅ 리콜 정보 업데이트 완료 (소요시간: {duration:.1f}초)")
            
        except Exception as e:
            logger.error(f"❌ 리콜 업데이트 실패: {e}")

    def monthly_registration_update(self):
        """월간 등록 현황 업데이트"""
        logger.info(" 월간 등록 현황 업데이트 시작...")
        try:
            def registration_task():
                # df = self.public_crawler.load_registration_data()
                # if not df.empty:
                #     self.public_crawler.save_to_database(df)
                #     return len(df)
                logger.info("⚠️  공공데이터 크롤링이 일시적으로 비활성화되었습니다.")
                return 0
            
            records_count = self.retry_with_backoff(registration_task)
            if records_count > 0:
                logger.info(f"✅ 등록 현황 업데이트 완료 ({records_count}건)")
            else:
                logger.warning("등록 현황 데이터 파일을 찾을 수 없습니다.")
        except Exception as e:
            logger.error(f"❌ 등록 현황 업데이트 실패: {e}")

    def enhanced_health_check(self):
        """향상된 시스템 상태 체크"""
        logger.info(" 시스템 종합 상태 체크...")
        health_status = {'database': False, 'disk_space': False, 'memory': False, 'recent_errors': 0, 'data_freshness': False}
        
        try:
            from database.db_helper import db_helper
            with db_helper.get_db_connection():
                health_status['database'] = True
            logger.info("✅ 데이터베이스 연결 정상")
            
            resources = self.check_system_resources()
            if resources:
                limits = self.config.get('resource_limits', {})
                health_status['disk_space'] = resources['disk_percent'] < limits.get('max_disk_percent', 90)
                health_status['memory'] = resources['memory_percent'] < limits.get('max_memory_percent', 85)
            
            error_logs = db_helper.execute_query("SELECT COUNT(*) as error_count FROM CrawlingLog WHERE status = '실패' AND started_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)")
            if error_logs:
                health_status['recent_errors'] = error_logs[0]['error_count']
                if health_status['recent_errors'] > 0:
                    logger.warning(f"⚠️ 최근 24시간 오류: {health_status['recent_errors']}건")
            
            latest_data = db_helper.execute_query("SELECT MAX(collected_date) as latest_date FROM UsedCarPrice")
            if latest_data and latest_data[0]['latest_date']:
                days_old = (datetime.now().date() - latest_data[0]['latest_date']).days
                health_status['data_freshness'] = days_old <= 3
                if days_old > 7:
                    logger.warning(f"⚠️ 가격 데이터가 {days_old}일 전 것입니다.")
            
            passed_checks = sum(v for k, v in health_status.items() if k != 'recent_errors' and v)
            total_checks = len(health_status) - 1
            health_score = (passed_checks / total_checks) * 100 if total_checks > 0 else 0
            logger.info(f"🏥 시스템 건강도: {health_score:.0f}% ({passed_checks}/{total_checks})")
            
            if health_score < self.config.get('alerts', {}).get('system_health_threshold', 70):
                self.send_email_notification(f"시스템 건강도 저하 ({health_score:.0f}%)", f"시스템 상태를 확인해주세요.\n\n상태 정보:\n{json.dumps(health_status, indent=2, ensure_ascii=False)}")
            
        except Exception as e:
            logger.error(f"❌ 건강 상태 체크 실패: {e}")

    def cleanup_old_data_enhanced(self):
        """향상된 오래된 데이터 정리"""
        logger.info("🧹 데이터 정리 시작...")
        try:
            from database.db_helper import db_helper
            retention_conf = self.config.get('data_retention', {})
            
            price_days = retention_conf.get('price_data_days', 30)
            deleted_prices = db_helper.execute_query(f"DELETE FROM UsedCarPrice WHERE collected_date < DATE_SUB(CURDATE(), INTERVAL {price_days} DAY)", fetch=False)
            logger.info(f"✅ {price_days}일 이상된 가격 데이터 {deleted_prices}건 정리")
            
            log_days = retention_conf.get('log_data_days', 90)
            deleted_logs = db_helper.execute_query(f"DELETE FROM CrawlingLog WHERE started_at < DATE_SUB(NOW(), INTERVAL {log_days} DAY)", fetch=False)
            logger.info(f"✅ {log_days}일 이상된 로그 {deleted_logs}건 정리")
            
        except Exception as e:
            logger.error(f"❌ 데이터 정리 실패: {e}")

    def generate_daily_report(self):
        """일일 리포트 생성"""
        if not self.config.get('email', {}).get('send_daily_reports', True):
            return
        logger.info("📄 일일 리포트 생성 중...")
        # (리포트 생성 로직은 기존과 유사하게 유지)
        # ...
        
    def setup_schedule(self):
        """스케줄 설정"""
        schedule.every().day.at("03:00").do(self.daily_price_update)
        if self.config.get('email', {}).get('send_daily_reports', True):
            schedule.every().day.at("23:30").do(self.generate_daily_report)
        
        schedule.every().monday.at("04:00").do(self.weekly_recall_update)
        schedule.every().sunday.at("02:00").do(self.cleanup_old_data_enhanced)
        schedule.every().sunday.at("01:00").do(self.backup_database)
        
        schedule.every().day.at("05:00").do(self._check_monthly_task)
        schedule.every().hour.do(self.enhanced_health_check)
        
        logger.info("✅ 스케줄 설정 완료")

    def _check_monthly_task(self):
        if datetime.now().day == 1:
            self.monthly_registration_update()
    
    def run(self):
        """스케줄러 실행"""
        logger.info("🚀 데이터 수집 스케줄러 시작...")
        self.enhanced_health_check()
        self.setup_schedule()
        
        logger.info(" 스케줄러가 실행 중입니다. Ctrl+C로 종료하세요.")
        try:
            while True:
                schedule.run_pending()
                delay = self.config.get('scheduler', {}).get('delay_between_tasks', 60)
                time.sleep(delay)
        except KeyboardInterrupt:
            logger.info("🛑 스케줄러 종료...")
            # if self.encar_crawler:
            #     self.encar_crawler.close_driver()
            logger.info(" 스케줄러가 정상적으로 종료되었습니다.")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='데이터 수집 스케줄러')
    parser.add_argument('--config', default='config/scheduler_config.json', help='설정 파일 경로 (JSON)')
    parser.add_argument('--task', choices=['price', 'recall', 'registration', 'health', 'cleanup', 'report', 'backup'], help='특정 작업만 실행')
    
    args = parser.parse_args()
    
    scheduler = EnhancedDataScheduler(config_path=args.config)
    
    if args.task:
        logger.info(f" 단일 작업 실행: {args.task}")
        tasks = {
            'price': scheduler.daily_price_update,
            'recall': scheduler.weekly_recall_update,
            'registration': scheduler.monthly_registration_update,
            'health': scheduler.enhanced_health_check,
            'cleanup': scheduler.cleanup_old_data_enhanced,
            'report': scheduler.generate_daily_report,
            'backup': scheduler.backup_database,
        }
        task_func = tasks.get(args.task)
        if task_func:
            task_func()
        else:
            logger.error(f"알 수 없는 작업입니다: {args.task}")
    else:
        scheduler.run()
