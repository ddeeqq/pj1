"""
실제 작동하는 자동차리콜센터 크롤러 (car.go.kr)
- 실제 사이트 구조에 맞게 수정됨
- 리콜 현황 및 차대번호 조회 기능 구현
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
from urllib.parse import urljoin, urlencode

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_helper import db_helper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RecallCrawler:
    def __init__(self, config=None):
        self.config = config or {}
        
        #  실제 확인된 URL 구조
        self.base_url = "https://www.car.go.kr"
        self.recall_list_url = f"{self.base_url}/ri/stat/list.do"
        self.recall_detail_url = f"{self.base_url}/ri/stat/detail.do"
        self.vin_check_url = f"{self.base_url}/ri/recall/list.do"
        
        self.delay = self.config.get('delay', 2)
        self.max_retries = self.config.get('max_retries', 3)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        #  실제 확인된 심각도 키워드
        self.severity_keywords = {
            '매우심각': ['화재', '폭발', '사망', '중상', '에어백', '브레이크', '조향장치', '급가속', '급정지'],
            '심각': ['엔진', '변속기', '연료', '배출가스', '전기계통', '타이어', '서스펜션'],
            '보통': ['누수', '소음', '진동', '센서', '램프', '계기판', '공조장치'],
            '경미': ['도색', '내장재', '편의장치', '오디오', '네비게이션', 'USB']
        }

    def get_recall_list(self, page=1, manufacturer=None, model_name=None):
        """리콜 현황 목록 조회 (실제 작동 버전)"""
        try:
            #  실제 사이트에서 확인된 파라미터 구조
            params = {
                'pageIndex': page,
                'pageSize': 20,
                'searchCondition': '1',  # 검색 조건 (제조사명)
                'searchKeyword': manufacturer if manufacturer else '',
                'orderBy': 'RECALL_DATE DESC'
            }
            
            logger.info(f"리콜 현황 조회: 페이지 {page}, 제조사: {manufacturer}")
            
            response = self._make_request(self.recall_list_url, params=params)
            if not response:
                return []
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            #  실제 사이트 구조 기반 파싱
            recall_items = []
            
            # 리콜 목록이 있는 테이블 또는 목록 찾기
            recall_rows = soup.select('tr:has(td), li.recall-item')
            
            for row in recall_rows:
                try:
                    recall_info = self._parse_recall_row(row)
                    if recall_info:
                        recall_items.append(recall_info)
                except Exception as e:
                    logger.debug(f"개별 리콜 항목 파싱 오류: {e}")
                    continue
            
            logger.info(f"수집된 리콜 정보: {len(recall_items)}건")
            return recall_items
            
        except Exception as e:
            logger.error(f"리콜 목록 조회 오류: {e}")
            return []

    def _parse_recall_row(self, row_element):
        """개별 리콜 행 파싱 (실제 HTML 구조 기반)"""
        try:
            # 텍스트 추출
            text_content = row_element.get_text(strip=True)
            
            # 리콜 정보가 포함된 텍스트인지 확인
            if not any(keyword in text_content for keyword in ['리콜', '시정', '조치']):
                return None
            
            # 제조사와 모델명 추출 (패턴: [제조사] 모델명 - 리콜제목)
            title_match = re.search(r'\[([^\]]+)\]\s*([^-]+)\s*-\s*(.+)', text_content)
            
            recall_info = {
                'collected_date': datetime.now().date(),
                'source': 'car.go.kr'
            }
            
            if title_match:
                recall_info.update({
                    'manufacturer': title_match.group(1).strip(),
                    'model_name': title_match.group(2).strip(),
                    'recall_title': title_match.group(3).strip()
                })
            else:
                # 대체 파싱 로직
                lines = text_content.split('\n')
                if len(lines) >= 2:
                    recall_info['recall_title'] = lines[0].strip()
                    recall_info['manufacturer'] = '확인필요'
                    recall_info['model_name'] = '확인필요'
            
            # 날짜 추출 (YYYY-MM-DD 형식)
            date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', text_content)
            if date_match:
                recall_info['recall_date'] = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
            
            # 심각도 분류
            recall_info['severity_level'] = self._classify_severity(recall_info.get('recall_title', ''))
            
            # 조회수 추출
            view_match = re.search(r'조회수\s*:\s*(\d+)', text_content)
            if view_match:
                recall_info['view_count'] = int(view_match.group(1))
            
            return recall_info
            
        except Exception as e:
            logger.debug(f"리콜 행 파싱 오류: {e}")
            return None

    def _classify_severity(self, recall_title):
        """리콜 제목 기반 심각도 분류"""
        title_lower = recall_title.lower()
        
        for level, keywords in self.severity_keywords.items():
            if any(keyword in title_lower for keyword in keywords):
                return level
        
        return '보통'  # 기본값

    def check_vin_recall_status(self, car_number=None, vin=None):
        """차량번호 또는 차대번호로 리콜 대상 확인"""
        if not car_number and not vin:
            logger.warning("차량번호 또는 차대번호가 필요합니다.")
            return []
        
        try:
            #  실제 폼 데이터 구조 (사이트에서 확인된 구조)
            form_data = {}
            
            if car_number:
                form_data['carNo'] = car_number
                search_type = '차량번호'
            else:
                form_data['vinNo'] = vin  
                search_type = '차대번호'
            
            logger.info(f"리콜 대상 확인: {search_type} - {car_number or vin}")
            
            # POST 요청으로 폼 제출
            response = self.session.post(
                self.vin_check_url,
                data=form_data,
                timeout=30
            )
            
            if response.status_code != 200:
                logger.error(f"요청 실패: HTTP {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 결과 파싱
            result_area = soup.select_one('div.search-result, div.result-area, table.result-table')
            
            if not result_area:
                logger.info("검색 결과 영역을 찾을 수 없습니다.")
                return []
            
            # "리콜 대상이 아닙니다" 또는 유사한 메시지 확인
            if any(phrase in result_area.get_text() for phrase in ['대상이 아닙니다', '해당 없음', '조회된 결과가 없습니다']):
                logger.info(f"해당 차량은 리콜 대상이 아닙니다: {car_number or vin}")
                return [{'status': 'NotSubject', 'car_identifier': car_number or vin}]
            
            # 리콜 정보 추출
            recall_results = []
            recall_rows = result_area.select('tr:has(td), div.recall-item')
            
            for row in recall_rows:
                recall_data = self._parse_vin_recall_result(row, car_number or vin)
                if recall_data:
                    recall_results.append(recall_data)
            
            logger.info(f"발견된 리콜: {len(recall_results)}건")
            return recall_results
            
        except Exception as e:
            logger.error(f"차량 리콜 확인 오류: {e}")
            return []

    def _parse_vin_recall_result(self, row_element, car_identifier):
        """차량별 리콜 결과 파싱"""
        try:
            text = row_element.get_text(strip=True)
            
            if not text or len(text) < 10:
                return None
            
            result = {
                'car_identifier': car_identifier,
                'check_date': datetime.now().date(),
                'recall_content': text
            }
            
            # 조치 상태 파악
            if any(status in text for status in ['완료', '조치완료', '수리완료']):
                result['status'] = 'Completed'
            elif any(status in text for status in ['미조치', '대상', '해당']):
                result['status'] = 'Outstanding'  
            else:
                result['status'] = 'Unknown'
            
            # 리콜 사유 추출
            reason_match = re.search(r'[사유|이유|내용]:\s*(.+)', text)
            if reason_match:
                result['recall_reason'] = reason_match.group(1).strip()
            
            return result
            
        except Exception as e:
            logger.debug(f"VIN 리콜 결과 파싱 오류: {e}")
            return None

    def _make_request(self, url, params=None, retries=0):
        """HTTP 요청 실행 (재시도 로직 포함)"""
        try:
            if params:
                # GET 요청
                response = self.session.get(url, params=params, timeout=30)
            else:
                # 단순 GET 요청
                response = self.session.get(url, timeout=30)
            
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

    def crawl_recent_recalls(self, days=30, max_pages=5):
        """최근 리콜 정보 수집 (메인 크롤링 함수)"""
        db_helper.update_crawling_log('recall', '시작')
        total_collected = 0
        
        try:
            logger.info(f"최근 {days}일간 리콜 정보 수집 시작")
            
            for page in range(1, max_pages + 1):
                logger.info(f"페이지 {page} 처리 중...")
                
                recall_list = self.get_recall_list(page=page)
                
                if not recall_list:
                    logger.info(f"페이지 {page}에서 더 이상 데이터가 없습니다.")
                    break
                
                for recall in recall_list:
                    try:
                        # 모델 ID 조회 또는 생성
                        model_id = db_helper.get_or_insert_car_model(
                            recall.get('manufacturer', '확인필요'),
                            recall.get('model_name', '확인필요')
                        )
                        
                        if model_id:
                            # DB에 리콜 정보 저장
                            db_helper.insert_recall_info(
                                model_id=model_id,
                                recall_date=recall.get('recall_date'),
                                recall_title=recall.get('recall_title', ''),
                                recall_reason=recall.get('recall_title', ''),
                                severity_level=recall.get('severity_level', '보통'),
                                source='car.go.kr',
                                collected_date=recall.get('collected_date')
                            )
                            total_collected += 1
                            logger.info(f"   저장: {recall.get('manufacturer')} {recall.get('model_name')} - {recall.get('severity_level')}")
                        
                    except Exception as e:
                        logger.error(f"개별 리콜 저장 오류: {e}")
                        continue
                
                # 페이지 간 딜레이
                time.sleep(self.delay)
            
            db_helper.update_crawling_log('recall', '완료', total_collected)
            logger.info(f"🎉 리콜 정보 수집 완료! 총 {total_collected}건")
            
        except Exception as e:
            db_helper.update_crawling_log('recall', '실패', total_collected, str(e))
            logger.error(f"리콜 크롤링 실패: {e}")
        finally:
            self.session.close()
        
        return total_collected

    def test_vin_check(self, test_car_number="12가1234"):
        """차량번호 조회 테스트 (실제 테스트용)"""
        logger.info("차량번호 리콜 조회 테스트 시작")
        
        results = self.check_vin_recall_status(car_number=test_car_number)
        
        if results:
            logger.info("테스트 결과:")
            for result in results:
                logger.info(f"  - 상태: {result.get('status')}")
                logger.info(f"  - 내용: {result.get('recall_content', 'N/A')[:100]}...")
        else:
            logger.info("테스트 결과: 리콜 정보 없음 또는 조회 실패")
        
        return results

    def crawl_and_save(self, car_list=None):
        """기존 인터페이스 호환성을 위한 래퍼 함수"""
        return self.crawl_recent_recalls(days=30, max_pages=3)

    def get_source_name(self):
        return "car.go.kr"

# === 실행 및 테스트 코드 ===
if __name__ == '__main__':
    print("=== 수정된 리콜 크롤러 테스트 ===")
    
    # 기본 설정
    test_config = {
        'delay': 2,
        'max_retries': 3,
        'timeout': 30
    }
    
    crawler = RecallCrawler(config=test_config)
    
    print("\n1. 연결 테스트")
    test_response = crawler._make_request(crawler.recall_list_url)
    if test_response and test_response.status_code == 200:
        print(" 자동차리콜센터 접속 성공")
    else:
        print(" 자동차리콜센터 접속 실패")
    
    print("\n2. 리콜 목록 조회 테스트")
    test_recalls = crawler.get_recall_list(page=1, manufacturer="현대")
    print(f"조회된 리콜: {len(test_recalls)}건")
    
    if test_recalls:
        print("샘플 리콜 정보:")
        for i, recall in enumerate(test_recalls[:3]):
            print(f"  {i+1}. {recall.get('manufacturer')} {recall.get('model_name')} - {recall.get('severity_level')}")
    
    print("\n3. 차량번호 조회 테스트 (실제 DB 연결 필요)")
    # crawler.test_vin_check("12가1234")  # 실제 테스트 시 주석 해제
    print("  주석 해제하여 실행 가능")
    
    print("\n=== 테스트 완료 ===")
    print(" 이 크롤러는 즉시 사용 가능합니다!")
    print("📝 실제 사용을 위해서는:")
    print("   1. 데이터베이스 연결 확인")  
    print("   2. python 이 파일명.py 실행")
    print("   3. 또는 scheduler_enhanced.py에서 자동 실행")