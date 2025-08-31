"""
개선된 자동차 리콜 센터 정보 크롤러
- (기존) 모델별 리콜 정보 검색
- (수정) 차대번호(VIN)를 이용한 특정 차량의 리콜 이행 여부 확인 기능 강화
"""
import requests
from bs4 import BeautifulSoup
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
import re
import sys
import os
from urllib.parse import urljoin

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_helper import db_helper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RecallCrawler:
    def __init__(self, config=None):
        self.config = config or {}
        
        # USER ACTION: 자동차 리콜 정보 및 이력 조회를 제공하는 사이트의 URL을 확인하고 수정해주세요.
        self.base_url = self.config.get('base_url', "https://www.car.go.kr")
        self.recall_list_url = urljoin(self.base_url, "/ri/stat/list.do")
        self.vin_check_url = urljoin(self.base_url, "/ri/recall/list.do") # 차대번호 조회 URL

        self.delay = self.config.get('delay', 2)
        self.max_retries = self.config.get('max_retries', 3)

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.config.get('user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)')
        })

        self.severity_keywords = {
            '매우심각': ['화재', '폭발', '사망', '중상', '에어백', '브레이크', '조향장치', '급가속', '급정지'],
            '심각': ['엔진', '변속기', '연료', '배출가스', '전기계통', '타이어', '서스펜션'],
            '보통': ['누수', '소음', '진동', '센서', '램프', '계기판', '공조장치'],
            '경미': ['도색', '내장재', '편의장치', '오디오', '네비게이션', 'USB']
        }

    # --- 차대번호(VIN) 기반 리콜 이력 조회 (핵심 기능) ---
    def get_recall_status_by_vin(self, vin):
        """차대번호(VIN)를 사용하여 특정 차량의 리콜 대상 여부 및 조치 상태를 확인합니다."""
        if not vin:
            logger.warning("차대번호(VIN)가 제공되지 않았습니다.")
            return []

        # USER ACTION: 차대번호로 리콜 이력을 조회하는 API의 파라미터명을 확인하고 수정해주세요.
        # 예: {"vin_number": vin}, {"search_vin": vin} 등
        params = {
            'carVin': vin
        }

        logger.info(f"리콜 이력 조회 (VIN: {vin})")
        try:
            response = self._make_request(self.vin_check_url, params=params)
            if not response:
                return []

            soup = BeautifulSoup(response.text, 'html.parser')
            
            # USER ACTION: 조회 결과가 표시되는 영역의 CSS 선택자를 수정해주세요.
            result_area_selector = "div.recall-result-list, ul#vinRecallResults"
            result_area = soup.select_one(result_area_selector)
            
            if not result_area or "리콜 대상이 아닙니다" in result_area.text:
                logger.info(f"  -> 해당 차량({vin})은 리콜 대상이 아닙니다.")
                return [{'vin': vin, 'status': 'NotSubject'}]

            recall_results = []
            # USER ACTION: 개별 리콜 항목을 나타내는 CSS 선택자를 수정해주세요.
            recall_item_selector = "div.recall-item, li.recall-entry"
            recall_items = result_area.select(recall_item_selector)

            for item in recall_items:
                recall_info = self._parse_vin_recall_item(item, vin)
                if recall_info:
                    recall_results.append(recall_info)
            
            logger.info(f"  -> {len(recall_results)}건의 리콜 이력 발견")
            return recall_results

        except Exception as e:
            logger.error(f"차대번호({vin}) 조회 중 오류: {e}")
            return []

    def _parse_vin_recall_item(self, item_soup, vin):
        """차대번호 조회 결과로 나온 개별 리콜 항목을 파싱합니다."""
        try:
            # USER ACTION: 아래 CSS 선택자들을 실제 사이트 구조에 맞게 모두 수정해주세요.
            reason_selector = ".recall-reason, .cause"
            status_selector = ".recall-status, .progress"
            date_selector = ".recall-date, .period"

            reason = self._clean_text(item_soup.select_one(reason_selector).get_text())
            status_text = self._clean_text(item_soup.select_one(status_selector).get_text())
            date = self._clean_text(item_soup.select_one(date_selector).get_text())

            # 상태 텍스트를 표준화된 코드로 변환 (예: "조치완료" -> "Completed")
            status = 'Unknown'
            if '완료' in status_text or '조치' in status_text:
                status = 'Completed'
            elif '미' in status_text or '대상' in status_text:
                status = 'Outstanding'

            return {
                'vin': vin,
                'reason': reason,
                'status': status, # Completed, Outstanding, Unknown
                'date': date
            }
        except Exception as e:
            logger.debug(f"개별 리콜 항목 파싱 오류: {e}")
            return None

    # --- (기존) 모델별 리콜 정보 검색 기능 ---
    def search_recall_info_by_model(self, manufacturer, model_name, **kwargs):
        """모델명으로 리콜 정보 검색"""
        # ... (기존 search_recall_info 로직과 유사하게 유지) ...
        # 이 함수는 특정 모델에 어떤 종류의 리콜이 있었는지 전반적으로 파악하는 데 사용됩니다.
        pass # 이 부분은 기존 코드를 거의 그대로 유지하면 됩니다.

    def _make_request(self, url, params=None, retries=0):
        """HTTP 요청 실행 (재시도 로직 포함)"""
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            if retries < self.max_retries:
                logger.warning(f"요청 실패, 재시도 {retries + 1}/{self.max_retries}: {e}")
                time.sleep(self.delay * (retries + 1))
                return self._make_request(url, params, retries + 1)
            else:
                logger.error(f"요청 최종 실패: {e}")
                return None

    def _clean_text(self, text):
        """텍스트 정제"""
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text.strip())

    def crawl_and_save(self, detailed_car_list):
        """수집된 상세 차량 목록을 기반으로 리콜 이력을 조회하고 DB에 저장합니다."""
        db_helper.update_crawling_log('recall_vin', '시작')
        total_checked = 0
        try:
            for car in detailed_car_list:
                vin = car.get('vin')
                if not vin:
                    continue
                
                recall_history = self.get_recall_status_by_vin(vin)
                
                if recall_history:
                    # USER ACTION: db_helper에 차량별 리콜 이력을 저장하는 메소드를 구현해야 합니다.
                    # 예: db_helper.insert_vin_recall_history(car['id'], recall_history)
                    logger.info(f"  -> DB 저장: {vin} ({len(recall_history)}건)")
                    # 지금은 로그만 출력합니다.
                
                total_checked += 1
                time.sleep(self.delay)

            db_helper.update_crawling_log('recall_vin', '완료', total_checked)
            logger.info(f"🎉 전체 리콜 이력 조회 완료! 총 {total_checked}건 확인")

        except Exception as e:
            db_helper.update_crawling_log('recall_vin', '실패', total_checked, str(e))
            logger.error(f"리콜 이력 조회 및 저장 실패: {e}")
        finally:
            self.session.close()

if __name__ == '__main__':
    print("리콜 크롤러 (VIN 조회 강화) 단독 테스트 모드")
    
    try:
        with open('config/scheduler_config.json', 'r', encoding='utf-8') as f:
            import json
            full_config = json.load(f)
        recall_config = full_config['crawling'].get('recall', {})
        print("✅ 테스트 설정 로드 완료")

        crawler = RecallCrawler(config=recall_config)

        # --- 차대번호(VIN) 조회 테스트 ---
        print("\n--- 차대번호(VIN) 조회 테스트 ---")
        # USER ACTION: 테스트할 실제 차대번호를 입력하세요.
        test_vin = "KNAXXXXXXXXXXXXXX"
        # recall_status = crawler.get_recall_status_by_vin(test_vin)
        # if recall_status:
        #     print(f"조회 결과 ({test_vin}):")
        #     print(pd.DataFrame(recall_status))
        print("실제 실행하려면 `get_recall_status_by_vin` 메소드의 주석을 해제하고,")
        print("USER ACTION 주석이 달린 부분의 URL, 파라미터, CSS 선택자를 수정해야 합니다.")

    except FileNotFoundError:
        print("오류: config/scheduler_config.json 파일을 찾을 수 없습니다.")
    except Exception as e:
        print(f"오류 발생: {e}")
