"""
자동차 리콜 센터 정보 크롤러
"""
import requests
from bs4 import BeautifulSoup
import time
import logging
import pandas as pd
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import CRAWLING_CONFIG
from database.db_helper import db_helper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RecallCrawler:
    def __init__(self):
        self.config = CRAWLING_CONFIG['recall']
        self.base_url = self.config['base_url']
        self.search_url = self.config['search_url']
        self.delay = self.config['delay']
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
    def search_recall_info(self, manufacturer, model_name):
        """특정 차량의 리콜 정보 검색"""
        recall_data = []
        
        try:
            # 리콜센터 API 호출 (실제 구조에 맞게 수정 필요)
            params = {
                'manufacturer': manufacturer,
                'model': model_name,
                'pageSize': 100
            }
            
            response = self.session.get(self.search_url, params=params, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 리콜 정보 파싱
            recall_items = soup.select('.recall-item, .list-item, tr')  # 실제 셀렉터로 변경 필요
            
            for item in recall_items:
                try:
                    # 리콜 제목
                    title_elem = item.select_one('.title, .subject, td:nth-child(2)')
                    title = title_elem.text.strip() if title_elem else '제목 없음'
                    
                    # 리콜 날짜
                    date_elem = item.select_one('.date, .regdate, td:nth-child(4)')
                    recall_date = self._parse_date(date_elem.text.strip()) if date_elem else None
                    
                    # 리콜 사유
                    reason_elem = item.select_one('.reason, .content, td:nth-child(3)')
                    reason = reason_elem.text.strip() if reason_elem else '사유 없음'
                    
                    # 영향 대수
                    units_elem = item.select_one('.units, .count, td:nth-child(5)')
                    affected_units = self._extract_number(units_elem.text) if units_elem else 0
                    
                    # 심각도 판단
                    severity = self._determine_severity(title, reason)
                    
                    recall_data.append({
                        'manufacturer': manufacturer,
                        'model_name': model_name,
                        'recall_date': recall_date,
                        'recall_title': title,
                        'recall_reason': reason,
                        'affected_units': affected_units,
                        'severity_level': severity
                    })
                    
                except Exception as e:
                    logger.debug(f"개별 리콜 항목 파싱 오류: {e}")
                    continue
                    
            logger.info(f"✅ {model_name}: {len(recall_data)}건의 리콜 정보 수집")
            
        except Exception as e:
            logger.error(f"리콜 정보 검색 오류: {e}")
            
        return recall_data
        
    def _parse_date(self, date_str):
        """날짜 문자열 파싱"""
        try:
            # 다양한 날짜 형식 처리
            for fmt in ['%Y-%m-%d', '%Y.%m.%d', '%Y/%m/%d', '%Y년 %m월 %d일']:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except:
                    continue
            return None
        except:
            return None
            
    def _extract_number(self, text):
        """텍스트에서 숫자 추출"""
        import re
        numbers = re.findall(r'\d+', text.replace(',', ''))
        return int(numbers[0]) if numbers else 0
        
    def _determine_severity(self, title, reason):
        """리콜 심각도 판단"""
        critical_keywords = ['화재', '엔진', '브레이크', '에어백', '조향']
        severe_keywords = ['변속기', '연료', '배출가스', '전기']
        moderate_keywords = ['누수', '소음', '진동', '센서']
        
        text = (title + ' ' + reason).lower()
        
        if any(keyword in text for keyword in critical_keywords):
            return '매우심각'
        elif any(keyword in text for keyword in severe_keywords):
            return '심각'
        elif any(keyword in text for keyword in moderate_keywords):
            return '보통'
        else:
            return '경미'
            
    def crawl_and_save(self, car_list):
        """차량 목록의 리콜 정보를 크롤링하고 DB에 저장"""
        db_helper.update_crawling_log('recall', '시작')
        total_collected = 0
        
        try:
            for car in car_list:
                manufacturer = car['manufacturer']
                model_name = car['model_name']
                
                # 모델 ID 조회
                model_id = db_helper.get_car_model_id(manufacturer, model_name)
                if not model_id:
                    logger.warning(f"모델을 찾을 수 없음: {manufacturer} {model_name}")
                    continue
                    
                # 리콜 정보 수집
                recall_data = self.search_recall_info(manufacturer, model_name)
                
                # DB에 저장
                for recall in recall_data:
                    db_helper.insert_recall_info(
                        model_id=model_id,
                        recall_date=recall['recall_date'],
                        recall_title=recall['recall_title'],
                        recall_reason=recall['recall_reason'],
                        affected_units=recall['affected_units'],
                        severity_level=recall['severity_level']
                    )
                    
                total_collected += len(recall_data)
                time.sleep(self.delay)  # 서버 부하 방지
                
            db_helper.update_crawling_log('recall', '완료', total_collected)
            logger.info(f"🎉 리콜 정보 크롤링 완료! 총 {total_collected}건 수집")
            
        except Exception as e:
            db_helper.update_crawling_log('recall', '실패', total_collected, str(e))
            logger.error(f"리콜 크롤링 실패: {e}")

# 테스트 실행
if __name__ == "__main__":
    crawler = RecallCrawler()
    
    # 테스트할 차량 목록
    test_cars = [
        {'manufacturer': '현대', 'model_name': '그랜저 IG'},
        {'manufacturer': '현대', 'model_name': '쏘나타 DN8'},
        {'manufacturer': '기아', 'model_name': 'K5 DL3'},
    ]
    
    # 크롤링 실행
    # crawler.crawl_and_save(test_cars)
    
    print("리콜 정보 크롤러 준비 완료!")
