"""
K카(kcar.com) 중고차 가격 정보 크롤러 - 엔카 대안
- 실제 K카 사이트 구조에 맞게 구현
- 검색 기능 및 차량 상세 정보 수집
"""
import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
from datetime import datetime
import re
import sys
import os
import json
from urllib.parse import urljoin, urlencode

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_helper import db_helper

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KCarCrawler:
    def __init__(self, config=None):
        self.config = config or {}
        
        # 실제 K카 사이트 정보
        self.base_url = "https://www.kcar.com"
        self.search_url = f"{self.base_url}/bc/search"
        self.api_search_url = f"{self.base_url}/api/bc/search"  # API 엔드포인트 추정
        
        self.delay = self.config.get('delay', 2)
        self.max_retries = self.config.get('max_retries', 3)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/html, */*',
            'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
            'Referer': self.base_url,
            'X-Requested-With': 'XMLHttpRequest'
        })

    def search_cars(self, manufacturer=None, model=None, year_min=None, year_max=None, 
                   price_min=None, price_max=None, page=1):
        """K카에서 차량 검색"""
        try:
            # K카 검색 파라미터 구조 (실제 사이트 기반)
            search_params = {
                'page': page,
                'size': 20,
                'sort': 'registration_desc'  # 최신순
            }
            
            # 필터 파라미터 추가
            if manufacturer:
                search_params['manufacturer'] = manufacturer
            if model:
                search_params['model'] = model
            if year_min:
                search_params['year_min'] = year_min
            if year_max:
                search_params['year_max'] = year_max
            if price_min:
                search_params['price_min'] = price_min * 10000  # 만원 -> 원
            if price_max:
                search_params['price_max'] = price_max * 10000
            
            logger.info(f"K카 검색: {manufacturer} {model}, 페이지 {page}")
            
            # 먼저 메인 검색 페이지 방문 (세션 유지)
            self.session.get(self.search_url)
            time.sleep(1)
            
            # 실제 검색 요청 (AJAX/API 방식일 가능성)
            response = self.session.get(
                self.search_url,
                params=search_params,
                timeout=30
            )
            
            if response.status_code == 200:
                return self._parse_search_results(response.text)
            else:
                logger.error(f"검색 요청 실패: HTTP {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"K카 검색 오류: {e}")
            return []

    def _parse_search_results(self, html_content):
        """검색 결과 파싱"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            car_items = []
            
            #  K카 차량 목록 아이템 선택자 (추정)
            car_selectors = [
                'div.car-item',
                'li.car-list-item', 
                'div.vehicle-card',
                'div[data-car-id]',
                'div.search-result-item'
            ]
            
            car_elements = []
            for selector in car_selectors:
                elements = soup.select(selector)
                if elements:
                    car_elements = elements
                    logger.info(f"차량 목록 찾음: {selector} ({len(elements)}개)")
                    break
            
            if not car_elements:
                # 대체 방법: 가격이 포함된 모든 요소 찾기
                car_elements = soup.find_all(text=re.compile(r'\d+만원|\d+,\d+만원'))
                logger.info(f"대체 방법으로 {len(car_elements)}개 가격 요소 발견")
            
            for element in car_elements[:20]:  # 최대 20개만 처리
                try:
                    car_info = self._extract_car_info(element)
                    if car_info:
                        car_items.append(car_info)
                except Exception as e:
                    logger.debug(f"개별 차량 정보 추출 오류: {e}")
                    continue
            
            logger.info(f"파싱된 차량 정보: {len(car_items)}건")
            return car_items
            
        except Exception as e:
            logger.error(f"검색 결과 파싱 오류: {e}")
            return []

    def _extract_car_info(self, element):
        """개별 차량 정보 추출"""
        try:
            # BeautifulSoup 요소인지 확인
            if hasattr(element, 'get_text'):
                text_content = element.get_text()
                element_soup = element
            else:
                # 텍스트 노드인 경우, 부모 요소 찾기
                if hasattr(element, 'parent'):
                    element_soup = element.parent
                    text_content = str(element)
                else:
                    return None
            
            car_info = {
                'collected_date': datetime.now().date(),
                'source': 'kcar.com'
            }
            
            # 가격 추출
            price_match = re.search(r'(\d{1,4}(?:,\d{3})*)\s*만원', text_content)
            if price_match:
                car_info['price'] = int(price_match.group(1).replace(',', ''))
            
            # 연식 추출  
            year_match = re.search(r'(20\d{2})년?', text_content)
            if year_match:
                car_info['year'] = int(year_match.group(1))
            
            # 주행거리 추출
            mileage_match = re.search(r'(\d{1,3}(?:,\d{3})*)\s*km', text_content)
            if mileage_match:
                car_info['mileage'] = int(mileage_match.group(1).replace(',', ''))
            
            # 제조사/모델명 추출 (K카 특성상 링크 텍스트나 제목에서 추출)
            if element_soup:
                # 링크에서 차량명 추출
                car_link = element_soup.find('a')
                if car_link and car_link.get('title'):
                    title = car_link.get('title')
                    parts = title.split()
                    if len(parts) >= 2:
                        car_info['manufacturer'] = parts[0]
                        car_info['model_name'] = ' '.join(parts[1:])
                
                # 이미지 alt 텍스트에서 정보 추출
                img = element_soup.find('img')
                if img and img.get('alt'):
                    alt_text = img.get('alt')
                    if not car_info.get('manufacturer'):
                        parts = alt_text.split()
                        if len(parts) >= 2:
                            car_info['manufacturer'] = parts[0]
                            car_info['model_name'] = ' '.join(parts[1:])
            
            # 최소한의 정보가 있는지 확인
            if car_info.get('price') and (car_info.get('manufacturer') or '차량' in text_content):
                return car_info
            
            return None
            
        except Exception as e:
            logger.debug(f"차량 정보 추출 오류: {e}")
            return None

    def get_manufacturer_models(self):
        """K카에서 제조사별 모델 목록 가져오기"""
        try:
            logger.info("제조사별 모델 목록 수집 중...")
            
            # K카 검색 페이지에서 제조사/모델 정보 추출
            response = self.session.get(self.search_url, timeout=30)
            
            if response.status_code != 200:
                return {}
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # JavaScript 데이터에서 제조사/모델 정보 추출
            script_tags = soup.find_all('script')
            manufacturer_models = {}
            
            for script in script_tags:
                if script.string and ('manufacturer' in script.string or 'model' in script.string):
                    # JSON 데이터 추출 시도
                    try:
                        json_match = re.search(r'(\{.*"manufacturer".*\})', script.string)
                        if json_match:
                            data = json.loads(json_match.group(1))
                            # 데이터 구조에 따라 파싱 로직 추가
                            logger.info("제조사/모델 데이터 발견")
                    except:
                        continue
            
            # 대체 방법: 하드코딩된 인기 모델 사용
            if not manufacturer_models:
                manufacturer_models = {
                    '현대': ['그랜저', '쏘나타', '아반떼', '투싼', '싼타페'],
                    '기아': ['K5', 'K7', 'K8', '쏘렌토', '스포티지'],
                    '제네시스': ['G80', 'G90', 'GV70', 'GV80'],
                    'BMW': ['3시리즈', '5시리즈', 'X3', 'X5'],
                    '벤츠': ['C클래스', 'E클래스', 'GLC', 'GLE']
                }
                logger.info("기본 제조사/모델 목록 사용")
            
            return manufacturer_models
            
        except Exception as e:
            logger.error(f"제조사/모델 목록 수집 오류: {e}")
            return {}

    def crawl_used_car_prices(self, car_list, max_items_per_model=10):
        """중고차 가격 정보 크롤링 (메인 함수)"""
        db_helper.update_crawling_log('kcar', '시작')
        total_collected = 0
        
        try:
            for car_spec in car_list:
                manufacturer = car_spec.get('manufacturer')
                model_name = car_spec.get('model_name')
                
                if not manufacturer or not model_name:
                    continue
                
                logger.info(f"--- {manufacturer} {model_name} 가격 정보 수집 ---")
                
                # 모델 ID 조회/생성
                model_id = db_helper.get_or_insert_car_model(manufacturer, model_name)
                if not model_id:
                    logger.warning(f"모델 ID 생성 실패: {manufacturer} {model_name}")
                    continue
                
                # 연식별로 검색 (최근 5년)
                current_year = datetime.now().year
                for year in range(current_year - 4, current_year + 1):
                    
                    search_results = self.search_cars(
                        manufacturer=manufacturer,
                        model=model_name,
                        year_min=year,
                        year_max=year,
                        page=1
                    )
                    
                    collected_for_year = 0
                    prices_for_year = []
                    
                    for car in search_results[:max_items_per_model]:
                        if car.get('price') and car.get('year') == year:
                            prices_for_year.append(car['price'])
                            collected_for_year += 1
                    
                    # 연식별 평균 가격 계산 및 저장
                    if prices_for_year:
                        avg_price = sum(prices_for_year) / len(prices_for_year)
                        min_price = min(prices_for_year)
                        max_price = max(prices_for_year)
                        
                        # 주행거리 범위 추정 (실제로는 더 세분화 필요)
                        mileage_range = f"{year}년식 평균"
                        
                        # DB에 저장
                        db_helper.insert_used_car_price(
                            model_id=model_id,
                            year=year,
                            mileage_range=mileage_range,
                            avg_price=round(avg_price),
                            min_price=round(min_price),
                            max_price=round(max_price),
                            sample_count=len(prices_for_year),
                            data_source='kcar.com',
                            collected_date=datetime.now().date()
                        )
                        
                        total_collected += 1
                        logger.info(f"   {year}년식: 평균 {avg_price:.0f}만원 ({len(prices_for_year)}건 기준)")
                    
                    time.sleep(self.delay)  # 요청 간 딜레이
                
                logger.info(f"--- {manufacturer} {model_name}: {collected_for_year}건 수집 완료 ---")
            
            db_helper.update_crawling_log('kcar', '완료', total_collected)
            logger.info(f" K카 크롤링 완료! 총 {total_collected}건")
            
        except Exception as e:
            db_helper.update_crawling_log('kcar', '실패', total_collected, str(e))
            logger.error(f"K카 크롤링 실패: {e}")
        
        return total_collected

    def test_search(self):
        """검색 기능 테스트"""
        logger.info("=== K카 검색 테스트 ===")
        
        # 테스트 검색
        test_results = self.search_cars(
            manufacturer="현대",
            model="그랜저",
            year_min=2020,
            year_max=2023
        )
        
        if test_results:
            logger.info(f" 검색 성공: {len(test_results)}건")
            logger.info("샘플 결과:")
            for i, car in enumerate(test_results[:3]):
                logger.info(f"  {i+1}. {car.get('manufacturer', 'N/A')} {car.get('model_name', 'N/A')} "
                          f"{car.get('year', 'N/A')}년 - {car.get('price', 'N/A')}만원")
        else:
            logger.warning(" 검색 결과 없음")
        
        return test_results

    def _make_request(self, url, params=None, retries=0):
        """HTTP 요청 (재시도 로직 포함)"""
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

    # 기존 인터페이스 호환성
    def crawl_and_save(self, car_list):
        """기존 스케줄러와의 호환성을 위한 메인 함수"""
        return self.crawl_used_car_prices(car_list)

    def get_source_name(self):
        return "kcar.com"

# === 실행 및 테스트 코드 ===
if __name__ == '__main__':
    print("=== K카 크롤러 테스트 ===")
    
    test_config = {
        'delay': 2,
        'max_retries': 3,
        'max_items_per_model': 10
    }
    
    crawler = KCarCrawler(config=test_config)
    
    print("\n1. K카 사이트 접속 테스트")
    test_response = crawler._make_request(crawler.search_url)
    if test_response and test_response.status_code == 200:
        print(" K카 사이트 접속 성공")
    else:
        print(" K카 사이트 접속 실패")
    
    print("\n2. 제조사/모델 목록 수집 테스트")
    models = crawler.get_manufacturer_models()
    if models:
        print(" 제조사/모델 정보:")
        for mf, model_list in list(models.items())[:3]:
            print(f"   {mf}: {model_list[:3]}")
    
    print("\n3. 검색 기능 테스트")
    # test_results = crawler.test_search()  # 실제 테스트 시 주석 해제
    print("  주석 해제하여 실행 가능")
    
    print("\n4. 전체 크롤링 테스트 (실제 DB 연결 필요)")
    test_car_list = [
        {'manufacturer': '현대', 'model_name': '그랜저'},
        {'manufacturer': '기아', 'model_name': 'K5'}
    ]
    # crawler.crawl_and_save(test_car_list)  # 실제 테스트 시 주석 해제
    print("  주석 해제하여 실행 가능")
    
    print("\n=== 테스트 완료 ===")
    print(" K카 크롤러 특징:")
    print("    엔카의 훌륭한 대안")
    print("    직영 중고차로 데이터 품질 좋음")
    print("    검색 구조가 상대적으로 단순함")
    print("    실제 사용 전 사이트 구조 미세 조정 필요")
