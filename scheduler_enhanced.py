"""
데이터 수집 자동 스케줄러 (Enhanced Version)
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

class EnhancedDataScheduler:
    def __init__(self):
        self.encar_crawler = None
        self.recall_crawler = RecallCrawler()
        self.public_crawler = PublicDataCrawler()
        self.stats = {
            'total_runs': 0,
            'successful_runs': 0,
            'failed_runs': 0,
            'last_run': None,
            'performance_metrics': []
        }
        self.max_retries = 3
        self.email_config = {
            'enabled': False,  # 이메일 알림 사용 여부
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': 587,
            'email': '',  # 발송자 이메일
            'password': '',  # 앱 비밀번호
            'recipients': []  # 수신자 목록
        }
        
    def check_system_resources(self):
        """시스템 리소스 확인"""
        try:
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
            
            # 리소스 부족 경고
            if cpu_percent > 80:
                logger.warning(f"⚠️ CPU 사용률 높음: {cpu_percent}%")
            if memory.percent > 85:
                logger.warning(f"⚠️ 메모리 사용률 높음: {memory.percent}%")
            if disk.percent > 90:
                logger.warning(f"⚠️ 디스크 사용률 높음: {disk.percent}%")
                
            return resources
            
        except Exception as e:
            logger.error(f"시스템 리소스 확인 실패: {e}")
            return None
    
    def send_email_notification(self, subject, message):
        """이메일 알림 발송"""
        if not self.email_config['enabled'] or not self.email_config['recipients']:
            return
            
        try:
            msg = MimeMultipart()
            msg['From'] = self.email_config['email']
            msg['To'] = ', '.join(self.email_config['recipients'])
            msg['Subject'] = f"[차량분석시스템] {subject}"
            
            msg.attach(MimeText(message, 'plain', 'utf-8'))
            
            server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])
            server.starttls()
            server.login(self.email_config['email'], self.email_config['password'])
            text = msg.as_string()
            server.sendmail(self.email_config['email'], self.email_config['recipients'], text)
            server.quit()
            
            logger.info("📧 이메일 알림 발송 완료")
            
        except Exception as e:
            logger.error(f"이메일 발송 실패: {e}")
    
    def retry_with_backoff(self, func, *args, **kwargs):
        """지수 백오프를 사용한 재시도 로직"""
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                wait_time = 2 ** attempt  # 1, 2, 4초
                logger.warning(f"⏰ 작업 실패 (시도 {attempt + 1}/{self.max_retries}): {e}")
                
                if attempt < self.max_retries - 1:
                    logger.info(f"💤 {wait_time}초 후 재시도...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ 최대 재시도 횟수 초과. 작업 실패: {func.__name__}")
                    raise
    
    def validate_collected_data(self, source, records_collected):
        """수집된 데이터 품질 검증"""
        try:
            from database.db_helper import db_helper
            
            # 최근 수집 데이터와 비교
            query = """
            SELECT AVG(records_collected) as avg_records, COUNT(*) as log_count
            FROM CrawlingLog 
            WHERE source = %s AND status = '완료' 
            AND started_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            """
            
            result = db_helper.execute_query(query, [source])
            
            if result and result[0]['log_count'] > 0:
                avg_records = result[0]['avg_records'] or 0
                
                # 평균 대비 50% 이하면 경고
                if records_collected < avg_records * 0.5:
                    logger.warning(f"⚠️ 데이터 수집량 급감: {source} (평균: {avg_records:.0f}, 현재: {records_collected})")
                    self.send_email_notification(
                        f"데이터 수집량 급감 알림",
                        f"소스: {source}\n평균 수집량: {avg_records:.0f}\n현재 수집량: {records_collected}\n\n확인이 필요합니다."
                    )
                elif records_collected > avg_records * 2:
                    logger.info(f"📈 데이터 수집량 증가: {source} (평균: {avg_records:.0f}, 현재: {records_collected})")
                    
        except Exception as e:
            logger.error(f"데이터 검증 실패: {e}")
    
    def backup_database(self):
        """데이터베이스 백업 (주요 테이블만)"""
        try:
            from database.db_helper import db_helper
            import pandas as pd
            
            backup_dir = f"data/backup/{datetime.now().strftime('%Y%m%d')}"
            os.makedirs(backup_dir, exist_ok=True)
            
            # 주요 테이블 백업
            important_tables = [
                'CarModel', 'UsedCarPrice', 'NewCarPrice', 
                'RegistrationStats', 'RecallInfo'
            ]
            
            for table in important_tables:
                try:
                    query = f"SELECT * FROM {table}"
                    df = db_helper.fetch_dataframe(query)
                    
                    if not df.empty:
                        backup_file = os.path.join(backup_dir, f"{table}_{datetime.now().strftime('%H%M')}.csv")
                        df.to_csv(backup_file, index=False, encoding='utf-8')
                        logger.info(f"📁 {table} 백업 완료 ({len(df)}건)")
                        
                except Exception as e:
                    logger.error(f"테이블 {table} 백업 실패: {e}")
                    
            logger.info(f"✅ 데이터베이스 백업 완료: {backup_dir}")
            
        except Exception as e:
            logger.error(f"백업 실패: {e}")
    
    def daily_price_update(self):
        """일일 가격 업데이트 (개선된 버전)"""
        start_time = datetime.now()
        logger.info("🌙 일일 가격 업데이트 시작...")
        
        # 시스템 리소스 확인
        resources = self.check_system_resources()
        
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
            
            # 재시도 로직 적용
            def crawl_task():
                return self.encar_crawler.crawl_and_save(car_list)
            
            self.retry_with_backoff(crawl_task)
            
            # 성능 통계 기록
            duration = (datetime.now() - start_time).total_seconds()
            
            # 수집 데이터 검증
            from database.db_helper import db_helper
            recent_log = db_helper.execute_query("""
                SELECT records_collected FROM CrawlingLog 
                WHERE source = 'encar' 
                ORDER BY started_at DESC LIMIT 1
            """)
            
            records_collected = recent_log[0]['records_collected'] if recent_log else 0
            self.validate_collected_data('encar', records_collected)
            
            self.stats['successful_runs'] += 1
            self.stats['performance_metrics'].append({
                'task': 'daily_price_update',
                'duration': duration,
                'records': records_collected,
                'timestamp': start_time.isoformat(),
                'resources': resources
            })
            
            logger.info(f"✅ 일일 가격 업데이트 완료 (소요시간: {duration:.1f}초)")
            
        except Exception as e:
            self.stats['failed_runs'] += 1
            logger.error(f"❌ 가격 업데이트 실패: {e}")
            
            # 실패 알림
            self.send_email_notification(
                "가격 업데이트 실패",
                f"시간: {start_time}\n오류: {str(e)}\n\n확인이 필요합니다."
            )
            
        finally:
            self.stats['total_runs'] += 1
            self.stats['last_run'] = datetime.now().isoformat()
    
    def weekly_recall_update(self):
        """주간 리콜 정보 업데이트 (개선된 버전)"""
        start_time = datetime.now()
        logger.info("📅 주간 리콜 정보 업데이트 시작...")
        
        try:
            from database.db_helper import db_helper
            
            models_df = db_helper.get_car_models()
            car_list = []
            
            for _, model in models_df.iterrows():
                car_list.append({
                    'manufacturer': model['manufacturer'],
                    'model_name': model['model_name']
                })
            
            def recall_task():
                return self.recall_crawler.crawl_and_save(car_list)
            
            self.retry_with_backoff(recall_task)
            
            # 리콜 통계 확인
            critical_recalls = db_helper.execute_query("""
                SELECT COUNT(*) as count FROM RecallInfo 
                WHERE severity_level IN ('심각', '매우심각') 
                AND recall_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
            """)
            
            if critical_recalls and critical_recalls[0]['count'] > 0:
                count = critical_recalls[0]['count']
                logger.warning(f"🚨 주의: 최근 일주일간 심각한 리콜 {count}건 발생")
                self.send_email_notification(
                    f"심각한 리콜 {count}건 발생",
                    f"최근 일주일간 심각도가 높은 리콜이 {count}건 발생했습니다.\n시스템에서 확인해주세요."
                )
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"✅ 리콜 정보 업데이트 완료 (소요시간: {duration:.1f}초)")
            
        except Exception as e:
            logger.error(f"❌ 리콜 업데이트 실패: {e}")
    
    def monthly_registration_update(self):
        """월간 등록 현황 업데이트 (개선된 버전)"""
        start_time = datetime.now()
        logger.info("📊 월간 등록 현황 업데이트 시작...")
        
        try:
            def registration_task():
                df = self.public_crawler.load_registration_data()
                if not df.empty:
                    self.public_crawler.save_to_database(df)
                    return len(df)
                return 0
            
            records_count = self.retry_with_backoff(registration_task)
            
            if records_count > 0:
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"✅ 등록 현황 업데이트 완료 ({records_count}건, 소요시간: {duration:.1f}초)")
            else:
                logger.warning("등록 현황 데이터 파일을 찾을 수 없습니다.")
                
        except Exception as e:
            logger.error(f"❌ 등록 현황 업데이트 실패: {e}")
    
    def enhanced_health_check(self):
        """향상된 시스템 상태 체크"""
        logger.info("🏥 시스템 종합 상태 체크...")
        
        health_status = {
            'database': False,
            'disk_space': False,
            'memory': False,
            'recent_errors': 0,
            'data_freshness': False
        }
        
        try:
            from database.db_helper import db_helper
            
            # 1. 데이터베이스 연결 확인
            with db_helper.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                health_status['database'] = True
                logger.info("✅ 데이터베이스 연결 정상")
            
            # 2. 시스템 리소스 확인
            resources = self.check_system_resources()
            if resources:
                health_status['disk_space'] = resources['disk_percent'] < 85
                health_status['memory'] = resources['memory_percent'] < 80
            
            # 3. 최근 오류 확인
            error_logs = db_helper.execute_query("""
                SELECT COUNT(*) as error_count
                FROM CrawlingLog
                WHERE status = '실패' AND started_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            """)
            
            if error_logs:
                health_status['recent_errors'] = error_logs[0]['error_count']
                if health_status['recent_errors'] > 0:
                    logger.warning(f"⚠️ 최근 24시간 오류: {health_status['recent_errors']}건")
            
            # 4. 데이터 신선도 확인
            latest_data = db_helper.execute_query("""
                SELECT MAX(collected_date) as latest_date
                FROM UsedCarPrice
            """)
            
            if latest_data and latest_data[0]['latest_date']:
                latest_date = latest_data[0]['latest_date']
                days_old = (datetime.now().date() - latest_date).days
                health_status['data_freshness'] = days_old <= 3
                
                if days_old > 7:
                    logger.warning(f"⚠️ 가격 데이터가 {days_old}일 전 것입니다.")
            
            # 5. 전체 상태 점수 계산
            total_checks = len([k for k in health_status.keys() if k != 'recent_errors'])
            passed_checks = sum([v for k, v in health_status.items() if k != 'recent_errors'])
            health_score = (passed_checks / total_checks) * 100
            
            logger.info(f"🏥 시스템 건강도: {health_score:.0f}% ({passed_checks}/{total_checks})")
            
            # 6. 통계 요약
            logger.info(f"📊 스케줄러 통계:")
            logger.info(f"   총 실행: {self.stats['total_runs']}회")
            logger.info(f"   성공: {self.stats['successful_runs']}회")
            logger.info(f"   실패: {self.stats['failed_runs']}회")
            if self.stats['last_run']:
                logger.info(f"   마지막 실행: {self.stats['last_run']}")
            
            # 7. 건강도가 낮으면 알림
            if health_score < 70:
                self.send_email_notification(
                    f"시스템 건강도 저하 ({health_score:.0f}%)",
                    f"시스템 상태를 확인해주세요.\n\n상태 정보:\n{json.dumps(health_status, indent=2, ensure_ascii=False)}"
                )
            
        except Exception as e:
            logger.error(f"❌ 건강 상태 체크 실패: {e}")
            health_status['database'] = False
    
    def cleanup_old_data_enhanced(self):
        """향상된 오래된 데이터 정리"""
        logger.info("🧹 데이터 정리 시작...")
        
        try:
            from database.db_helper import db_helper
            
            cleanup_stats = {}
            
            # 1. 30일 이상된 가격 데이터 삭제 (최신 1개씩은 보존)
            query = """
            DELETE ucp1 FROM UsedCarPrice ucp1
            WHERE ucp1.collected_date < DATE_SUB(CURDATE(), INTERVAL 30 DAY)
            AND EXISTS (
                SELECT 1 FROM UsedCarPrice ucp2
                WHERE ucp2.model_id = ucp1.model_id 
                AND ucp2.year = ucp1.year 
                AND ucp2.mileage_range = ucp1.mileage_range
                AND ucp2.collected_date > ucp1.collected_date
            )
            """
            deleted = db_helper.execute_query(query, fetch=False)
            cleanup_stats['old_prices'] = deleted
            logger.info(f"✅ 오래된 가격 데이터 {deleted}건 정리")
            
            # 2. 90일 이상된 크롤링 로그 삭제
            query = """
            DELETE FROM CrawlingLog 
            WHERE started_at < DATE_SUB(NOW(), INTERVAL 90 DAY)
            """
            deleted = db_helper.execute_query(query, fetch=False)
            cleanup_stats['old_logs'] = deleted
            logger.info(f"✅ 오래된 로그 {deleted}건 정리")
            
            # 3. 중복된 등록 통계 정리
            query = """
            DELETE rs1 FROM RegistrationStats rs1
            INNER JOIN RegistrationStats rs2
            WHERE rs1.id < rs2.id 
            AND rs1.model_id = rs2.model_id 
            AND rs1.region = rs2.region 
            AND rs1.registration_date = rs2.registration_date
            """
            deleted = db_helper.execute_query(query, fetch=False)
            cleanup_stats['duplicate_stats'] = deleted
            logger.info(f"✅ 중복 등록 통계 {deleted}건 정리")
            
            # 4. 빈 레코드 정리
            empty_tables = [
                "UsedCarPrice WHERE avg_price = 0 OR avg_price IS NULL",
                "RegistrationStats WHERE registration_count = 0 OR registration_count IS NULL"
            ]
            
            for table_condition in empty_tables:
                query = f"DELETE FROM {table_condition}"
                deleted = db_helper.execute_query(query, fetch=False)
                table_name = table_condition.split()[0]
                cleanup_stats[f'empty_{table_name}'] = deleted
                logger.info(f"✅ {table_name} 빈 레코드 {deleted}건 정리")
            
            # 정리 통계 출력
            total_deleted = sum(cleanup_stats.values())
            logger.info(f"🎉 데이터 정리 완료! 총 {total_deleted}건 정리됨")
            
        except Exception as e:
            logger.error(f"❌ 데이터 정리 실패: {e}")
    
    def generate_daily_report(self):
        """일일 리포트 생성"""
        try:
            from database.db_helper import db_helper
            
            logger.info("📄 일일 리포트 생성 중...")
            
            # 오늘의 통계
            today = datetime.now().date()
            
            report = {
                'date': today.isoformat(),
                'crawling_summary': {},
                'data_summary': {},
                'system_performance': {}
            }
            
            # 크롤링 요약
            crawling_logs = db_helper.execute_query("""
                SELECT source, status, COUNT(*) as count, SUM(records_collected) as total_records
                FROM CrawlingLog
                WHERE DATE(started_at) = %s
                GROUP BY source, status
            """, [today])
            
            for log in crawling_logs:
                source = log['source']
                if source not in report['crawling_summary']:
                    report['crawling_summary'][source] = {}
                report['crawling_summary'][source][log['status']] = {
                    'count': log['count'],
                    'records': log['total_records'] or 0
                }
            
            # 데이터 요약
            data_stats = db_helper.execute_query("""
                SELECT 
                    (SELECT COUNT(*) FROM CarModel) as total_models,
                    (SELECT COUNT(*) FROM UsedCarPrice WHERE collected_date = %s) as new_prices,
                    (SELECT COUNT(*) FROM RecallInfo WHERE recall_date = %s) as new_recalls,
                    (SELECT COUNT(*) FROM RegistrationStats WHERE registration_date = %s) as new_registrations
            """, [today, today, today])
            
            if data_stats:
                report['data_summary'] = data_stats[0]
            
            # 시스템 성능
            if self.stats['performance_metrics']:
                recent_metrics = [m for m in self.stats['performance_metrics'] 
                                if datetime.fromisoformat(m['timestamp']).date() == today]
                
                if recent_metrics:
                    avg_duration = sum(m['duration'] for m in recent_metrics) / len(recent_metrics)
                    total_records = sum(m['records'] for m in recent_metrics)
                    
                    report['system_performance'] = {
                        'tasks_completed': len(recent_metrics),
                        'average_duration': round(avg_duration, 2),
                        'total_records_processed': total_records
                    }
            
            # 리포트 저장
            report_file = f"logs/daily_report_{today.strftime('%Y%m%d')}.json"
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"📄 일일 리포트 저장: {report_file}")
            
            # 요약 로그
            logger.info("📊 오늘의 요약:")
            for source, data in report['crawling_summary'].items():
                for status, info in data.items():
                    logger.info(f"   {source} ({status}): {info['count']}회, {info['records']}건")
            
        except Exception as e:
            logger.error(f"일일 리포트 생성 실패: {e}")
    
    def setup_schedule(self):
        """스케줄 설정 (개선된 버전)"""
        # 매일 실행
        schedule.every().day.at("03:00").do(self.daily_price_update)
        schedule.every().day.at("23:30").do(self.generate_daily_report)
        
        # 매주 실행
        schedule.every().monday.at("04:00").do(self.weekly_recall_update)
        schedule.every().sunday.at("02:00").do(self.cleanup_old_data_enhanced)
        schedule.every().sunday.at("01:00").do(self.backup_database)
        
        # 매월 실행 (1일)
        schedule.every().day.at("05:00").do(self._check_monthly_task)
        
        # 시간별 실행
        schedule.every().hour.do(self.enhanced_health_check)
        
        logger.info("✅ 향상된 스케줄 설정 완료")
        logger.info("📅 스케줄 목록:")
        logger.info("  - 매일 03:00: 가격 정보 업데이트")
        logger.info("  - 매일 23:30: 일일 리포트 생성")
        logger.info("  - 매주 월요일 04:00: 리콜 정보 업데이트")
        logger.info("  - 매주 일요일 01:00: 데이터베이스 백업")
        logger.info("  - 매주 일요일 02:00: 오래된 데이터 정리")
        logger.info("  - 매월 1일 05:00: 등록 현황 업데이트")
        logger.info("  - 매시간: 시스템 종합 건강 체크")
    
    def _check_monthly_task(self):
        """월간 작업 체크 (매월 1일에만 실행)"""
        if datetime.now().day == 1:
            self.monthly_registration_update()
    
    def get_status(self):
        """현재 스케줄러 상태 반환"""
        return {
            'stats': self.stats.copy(),
            'next_runs': {
                job.tags[0] if job.tags else 'unknown': job.next_run
                for job in schedule.jobs
            },
            'system_status': 'running'
        }
    
    def run(self):
        """스케줄러 실행 (개선된 버전)"""
        logger.info("🚀 향상된 데이터 수집 스케줄러 시작...")
        
        # 초기 상태 체크
        self.enhanced_health_check()
        
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
            
            # 종료 전 정리
            if self.encar_crawler:
                self.encar_crawler.close_driver()
            
            # 최종 리포트 생성
            logger.info("📊 최종 통계:")
            logger.info(f"   총 실행: {self.stats['total_runs']}회")
            logger.info(f"   성공: {self.stats['successful_runs']}회")
            logger.info(f"   실패: {self.stats['failed_runs']}회")
            
            logger.info("👋 스케줄러가 정상적으로 종료되었습니다.")

# 실행
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='향상된 데이터 수집 스케줄러')
    parser.add_argument('--test', action='store_true', help='테스트 모드 (즉시 실행)')
    parser.add_argument('--task', choices=['price', 'recall', 'registration', 'health', 'cleanup', 'report', 'backup'],
                       help='특정 작업만 실행')
    parser.add_argument('--config', help='설정 파일 경로 (JSON)')
    
    args = parser.parse_args()
    
    scheduler = EnhancedDataScheduler()
    
    # 설정 파일 로드
    if args.config and os.path.exists(args.config):
        try:
            with open(args.config, 'r', encoding='utf-8') as f:
                config = json.load(f)
                if 'email' in config:
                    scheduler.email_config.update(config['email'])
                if 'max_retries' in config:
                    scheduler.max_retries = config['max_retries']
                logger.info(f"✅ 설정 파일 로드: {args.config}")
        except Exception as e:
            logger.error(f"설정 파일 로드 실패: {e}")
    
    if args.test:
        logger.info("🧪 테스트 모드")
        scheduler.enhanced_health_check()
        
    elif args.task:
        logger.info(f"🎯 단일 작업 실행: {args.task}")
        
        if args.task == 'price':
            scheduler.daily_price_update()
        elif args.task == 'recall':
            scheduler.weekly_recall_update()
        elif args.task == 'registration':
            scheduler.monthly_registration_update()
        elif args.task == 'health':
            scheduler.enhanced_health_check()
        elif args.task == 'cleanup':
            scheduler.cleanup_old_data_enhanced()
        elif args.task == 'report':
            scheduler.generate_daily_report()
        elif args.task == 'backup':
            scheduler.backup_database()
            
    else:
        scheduler.run()
