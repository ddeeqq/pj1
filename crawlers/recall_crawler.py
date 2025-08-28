"""
자동차 리콜 센터 정보 크롤러
"""
import re
import time
import logging
import pandas as pd
from datetime import datetime
import sys
import os
import requests
from bs4 import BeautifulSoup

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_helper import db_helper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RecallCrawler:
    def __init__(self, config):
        """크롤러 초기화 시 설정(config)을 전달받음"""
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
    def search_recall_info(self, manufacturer, model_name):
        """특정 차량의 리콜 정보 검색"""
        recall_data = []
        search_url = self.config.get('search_url', 'https://www.car.go.kr/recall/recall_list.car')
        timeout = self.config.get('timeout', 30)

        try:
            params = {'manufacturer': manufacturer, 'model': model_name, 'pageSize': 100}
            response = self.session.get(search_url, params=params, timeout=timeout)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            recall_items = soup.select('.recall-item, .list-item, tr')
            
            for item in recall_items:
                try:
                    title = item.select_one('.title, .subject, td:nth-child(2)').text.strip()
                    recall_date = self._parse_date(item.select_one('.date, .regdate, td:nth-child(4)').text.strip())
                    reason = item.select_one('.reason, .content, td:nth-child(3)').text.strip()
                    affected_units = self._extract_number(item.select_one('.units, .count, td:nth-child(5)').text)
                    
                    recall_data.append({
                        'manufacturer': manufacturer, 'model_name': model_name,
                        'recall_date': recall_date, 'recall_title': title, 'recall_reason': reason,
                        'affected_units': affected_units, 'severity_level': self._determine_severity(title, reason)
                    })
                except Exception as e:
                    logger.debug(f"개별 리콜 항목 파싱 오류: {e}")
                    continue
            logger.info(f"✅ {model_name}: {len(recall_data)}건의 리콜 정보 수집")
        except Exception as e:
            logger.error(f"리콜 정보 검색 오류: {e}")
        return recall_data
        
    def _parse_date(self, date_str):
        for fmt in ['%Y-%m-%d', '%Y.%m.%d', '%Y/%m/%d', '%Y년 %m월 %d일']:
            try: return datetime.strptime(date_str, fmt).date()
            except ValueError: continue
        return None
            
    def _extract_number(self, text):
        numbers = re.findall(r'\d+', text.replace(',', ''))
        return int(numbers[0]) if numbers else 0
        
    def _determine_severity(self, title, reason):
        """리콜 심각도 판단 (설정 파일의 키워드 사용)"""
        # 설정에서 키워드 목록을 가져오고, 없으면 기본값 사용
        critical_keywords = self.config.get('critical_recall_keywords', ['화재', '엔진', '브레이크', '에어백', '조향'])
        severe_keywords = self.config.get('severe_recall_keywords', ['변속기', '연료', '배출가스', '전기'])
        moderate_keywords = self.config.get('moderate_recall_keywords', ['누수', '소음', '진동', '센서'])
        
        text = (title + ' ' + reason).lower()
        
        if any(keyword in text for keyword in critical_keywords): return '매우심각'
        elif any(keyword in text for keyword in severe_keywords): return '심각'
        elif any(keyword in text for keyword in moderate_keywords): return '보통'
        else: return '경미'
            
    def crawl_and_save(self, car_list):
        """차량 목록의 리콜 정보를 크롤링하고 DB에 저장"""
        db_helper.update_crawling_log('recall', '시작')
        total_collected = 0
        delay = self.config.get('delay', 1)
        
        try:
            for car in car_list:
                model_id = db_helper.get_car_model_id(car['manufacturer'], car['model_name'])
                if not model_id:
                    logger.warning(f"모델을 찾을 수 없음: {car['manufacturer']} {car['model_name']}")
                    continue
                    
                recall_data = self.search_recall_info(car['manufacturer'], car['model_name'])
                
                for recall in recall_data:
                    db_helper.insert_recall_info(
                        model_id=model_id, recall_date=recall['recall_date'],
                        recall_title=recall['recall_title'], recall_reason=recall['recall_reason'],
                        affected_units=recall['affected_units'], severity_level=recall['severity_level']
                    )
                total_collected += len(recall_data)
                time.sleep(delay)
                
            db_helper.update_crawling_log('recall', '완료', total_collected)
            logger.info(f"🎉 리콜 정보 크롤링 완료! 총 {total_collected}건 수집")
        except Exception as e:
            db_helper.update_crawling_log('recall', '실패', total_collected, str(e))
            logger.error(f"리콜 크롤링 실패: {e}")

if __name__ == '__main__':
    import json
    print("리콜 크롤러 단독 테스트 모드")
    
    try:
        with open('config/scheduler_config.json', 'r', encoding='utf-8') as f:
            full_config = json.load(f)
        recall_config = full_config['crawling']['recall']
        print("✅ 테스트 설정 로드 완료")

        crawler = RecallCrawler(config=recall_config)
        
        test_cars = [
            {'manufacturer': '현대', 'model_name': '그랜저 IG'},
        ]
        
        # 실제 크롤링을 실행하려면 아래 주석을 해제하세요.
        # print("테스트 크롤링 시작...")
        # crawler.crawl_and_save(test_cars)

        print("리콜 크롤러 준비 완료!")

    except FileNotFoundError:
        print("오류: config/scheduler_config.json 파일을 찾을 수 없습니다.")
    except Exception as e:
        print(f"오류 발생: {e}")
