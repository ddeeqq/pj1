"""
엔카(encar.com) 중고차 가격 정보 크롤러 (고도화 버전)
- 개별 차량 상세 페이지에서 상세 정보 수집
- 옵션, 사고이력, 소유자 변경이력, 차대번호 등 수집
"""
import re
import time
import pandas as pd
from datetime import datetime
import sys
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from urllib.parse import urljoin

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_helper import db_helper
from config.logging_config import get_crawler_logger

logger = get_crawler_logger('encar_detailed')

class EncarCrawler:
    def __init__(self, config):
        """크롤러 초기화 시 설정(config)을 전달받음"""
        self.config = config
        self.driver = None
        self.base_url = "http://www.encar.com" # USER ACTION: 엔카 사이트의 기본 URL을 확인해주세요.

    def setup_driver(self):
        """Chrome 드라이버 설정"""
        if self.driver:
            return
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument(f"--user-agent={self.config.get('user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')}")
        
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            logger.info("✅ Chrome 드라이버 설정 완료")
        except Exception as e:
            logger.error(f"❌ Chrome 드라이버 설정 실패: {e}")
            raise
            
    def close_driver(self):
        """드라이버 종료"""
        if self.driver:
            self.driver.quit()
            self.driver = None
            logger.info("드라이버 종료 완료")
            
    def get_car_detail_urls(self, manufacturer, model_name, year=None):
        """검색 결과 페이지에서 개별 차량의 상세 페이지 URL 목록을 가져옵니다."""
        detail_urls = []
        try:
            if not self.driver:
                self.setup_driver()
                
            search_query = f"{manufacturer} {model_name}"
            if year:
                search_query += f" {year}"
            
            # USER ACTION: 엔카의 실제 검색 URL 구조에 맞게 수정해주세요.
            search_url_template = self.config.get('search_url', 'http://www.encar.com/dc/dc_carsearchlist.do?carType=kor&searchType=model&searchKey=&prcStart=&prcEnd=&mileStart=&mileEnd=&yearMin=&yearMax=&trans=&fuel=&disp=&size=&color=&options=&q={query}')
            url = search_url_template.format(query=search_query)
            
            logger.info(f"🔍 상세 페이지 URL 수집 중: {search_query}")
            self.driver.get(url)
            time.sleep(self.config.get('delay', 2))
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # USER ACTION: 검색 결과 목록에서 각 차량의 상세 페이지 링크를 가리키는 CSS 선택자를 수정해주세요.
            link_selector = "a.car-item-link, div.list-item > a"
            links = soup.select(link_selector)
            
            max_items = self.config.get('max_items_per_model', 10) # 너무 많은 차량을 수집하지 않도록 제한
            
            for link in links[:max_items]:
                href = link.get('href')
                if href:
                    full_url = urljoin(self.base_url, href)
                    if full_url not in detail_urls:
                        detail_urls.append(full_url)
            
            logger.info(f"✅ {len(detail_urls)}개의 상세 페이지 URL 수집 완료")
            return detail_urls
            
        except Exception as e:
            logger.error(f"상세 페이지 URL 수집 중 오류: {e}")
            return []

    def get_car_detail(self, detail_url):
        """차량 상세 페이지를 방문하여 모든 상세 정보를 추출합니다."""
        try:
            self.driver.get(detail_url)
            time.sleep(self.config.get('delay', 1)) # 페이지 로드를 위한 대기
            
            # USER ACTION: "성능·상태 점검기록부" 팝업이나 프레임이 있다면, 해당 요소를 클릭하거나 전환하는 코드가 필요합니다.
            # 예: WebDriverWait(self.driver, 10).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "performance_check_frame")))
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            car_info = {
                'source_url': detail_url,
                'collected_date': datetime.now().date()
            }

            # --- 기본 정보 추출 ---
            car_info['price'] = self._extract_price(soup)
            car_info['year'] = self._extract_year(soup)
            car_info['mileage'] = self._extract_mileage(soup)
            car_info['manufacturer'], car_info['model_name'], car_info['trim_name'] = self._extract_model_and_trim(soup)
            
            # --- 상세 정보 추출 ---
            car_info['vin'] = self._extract_vin(soup) # 차대번호
            car_info['options'] = self._extract_options(soup) # 옵션
            car_info['ownership_history'] = self._extract_ownership_history(soup) # 소유자 변경 이력
            
            # --- 성능 점검 기록부 정보 추출 ---
            performance_data = self._extract_performance_check(soup)
            car_info.update(performance_data)

            return car_info

        except Exception as e:
            logger.error(f"상세 정보 추출 오류 ({detail_url}): {e}")
            return None

    # --- 데이터 추출 헬퍼 함수들 ---

    def _extract_price(self, soup):
        # USER ACTION: 가격 정보가 있는 요소의 CSS 선택자를 확인하고 수정해주세요.
        price_selector = "span.price, strong.cost"
        elem = soup.select_one(price_selector)
        if elem:
            numbers = re.findall(r'[\d,]+', elem.text)
            if numbers:
                return int(numbers[0].replace(',', '')) * 10000 # '만원' 단위 처리
        return 0

    def _extract_year(self, soup):
        # USER ACTION: 연식 정보가 있는 요소의 CSS 선택자를 확인하고 수정해주세요.
        year_selector = "span.year, div.car-info .year"
        elem = soup.select_one(year_selector)
        if elem:
            match = re.search(r'(20\d{2})|(\d{2}년)', elem.text)
            if match:
                return int(match.group(1)) if match.group(1) else 2000 + int(match.group(2)[:2])
        return None

    def _extract_mileage(self, soup):
        # USER ACTION: 주행거리 정보가 있는 요소의 CSS 선택자를 확인하고 수정해주세요.
        mileage_selector = "span.mileage, div.car-info .km"
        elem = soup.select_one(mileage_selector)
        if elem:
            numbers = re.findall(r'[\d,]+', elem.text)
            if numbers:
                mileage = int(numbers[0].replace(',', ''))
                return mileage * 10000 if '만' in elem.text else mileage
        return 0

    def _extract_model_and_trim(self, soup):
        # USER ACTION: 제조사, 모델명, 세부 트림명이 포함된 요소의 CSS 선택자를 수정해주세요.
        title_selector = "h1.car-name, div.car-title"
        elem = soup.select_one(title_selector)
        if elem:
            full_title = elem.text.strip()
            # 예: "현대 그랜저 IG 2.4 프리미엄" -> ["현대", "그랜저 IG", "2.4 프리미엄"]
            # 실제 파싱 로직은 사이트의 제목 구조에 따라 매우 달라질 수 있습니다.
            parts = full_title.split()
            if len(parts) >= 3:
                return parts[0], " ".join(parts[1:-1]), parts[-1]
            elif len(parts) == 2:
                return parts[0], parts[1], None
            elif len(parts) == 1:
                return None, parts[0], None
        return None, None, None

    def _extract_vin(self, soup):
        # USER ACTION: 차대번호가 있는 요소의 CSS 선택자를 수정해주세요. (성능점검기록부 내에 있을 가능성이 높음)
        vin_selector = "td.vin, span#carVin"
        elem = soup.select_one(vin_selector)
        if elem:
            return elem.text.strip()
        return None

    def _extract_options(self, soup):
        # USER ACTION: 옵션 목록을 포함하는 각 옵션 항목의 CSS 선택자를 수정해주세요.
        option_selector = "ul.options-list > li, div.options .item"
        options = [opt.text.strip() for opt in soup.select(option_selector)]
        return options if options else []

    def _extract_ownership_history(self, soup):
        # USER ACTION: 소유자 변경 이력 횟수 정보가 있는 요소의 CSS 선택자를 수정해주세요.
        owner_selector = "td.owner-changes"
        elem = soup.select_one(owner_selector)
        if elem:
            numbers = re.findall(r'\d+', elem.text)
            return int(numbers[0]) if numbers else 0
        return 0

    def _extract_performance_check(self, soup):
        """성능·상태 점검기록부에서 사고이력, 누유, 특이사항 등을 추출합니다."""
        data = {
            'accident_type': '없음', # 없음, 무사고(단순수리), 사고
            'accident_details': [],
            'leakage_points': [],
            'special_notes': ''
        }
        
        # USER ACTION: 성능점검기록부 영역을 가리키는 CSS 선택자를 수정해주세요.
        perf_check_area = soup.select_one("div#performanceCheck, table.performance-table")
        if not perf_check_area:
            return data

        # 사고유무 (프레임 사고)
        # USER ACTION: '주요골격' 또는 '사고' 관련 항목의 CSS 선택자를 수정해주세요.
        frame_damage_selector = "td.frame-damage-check"
        frame_elem = perf_check_area.select_one(frame_damage_selector)
        if frame_elem and ('있음' in frame_elem.text or '사고' in frame_elem.text):
            data['accident_type'] = '사고'

        # 단순수리 (외판)
        # USER ACTION: '단순수리' 또는 '외판' 관련 항목의 CSS 선택자를 수정해주세요.
        panel_repair_selector = "td.panel-repair-check"
        panel_elem = perf_check_area.select_one(panel_repair_selector)
        if panel_elem and ('있음' in panel_elem.text or '교환' in panel_elem.text):
            if data['accident_type'] == '없음':
                data['accident_type'] = '무사고(단순수리)'

        # 사고/수리 부위 상세
        # USER ACTION: 수리된 각 부위를 나타내는 요소의 CSS 선택자를 수정해주세요. (예: 체크된 이미지)
        repaired_parts_selector = "td.repaired"
        repaired_parts = [part.get('data-part-name', part.text.strip()) for part in perf_check_area.select(repaired_parts_selector)]
        data['accident_details'] = repaired_parts

        # 누유 정보
        # USER ACTION: 누유가 체크된 항목의 CSS 선택자를 수정해주세요.
        leakage_selector = "td.leakage-point.checked"
        leaks = [leak.get('data-part-name', leak.text.strip()) for leak in perf_check_area.select(leakage_selector)]
        data['leakage_points'] = leaks
        
        # 특이사항
        # USER ACTION: 특이사항 텍스트가 있는 요소의 CSS 선택자를 수정해주세요.
        notes_selector = "td.special-notes"
        notes_elem = perf_check_area.select_one(notes_selector)
        if notes_elem:
            data['special_notes'] = notes_elem.text.strip()

        return data

    def crawl_and_save(self, car_list):
        """주어진 차종 목록에 대해 상세 정보를 크롤링하고 DB에 저장합니다."""
        db_helper.update_crawling_log('encar_detailed', '시작')
        total_collected = 0
        
        try:
            self.setup_driver()
            for car_spec in car_list:
                manufacturer = car_spec['manufacturer']
                model_name = car_spec['model_name']
                
                model_id = db_helper.get_or_insert_car_model(manufacturer, model_name)
                if not model_id:
                    logger.warning(f"모델 ID를 가져올 수 없습니다: {manufacturer} {model_name}")
                    continue

                logger.info(f"--- {manufacturer} {model_name} 상세 정보 수집 시작 ---")
                detail_urls = self.get_car_detail_urls(manufacturer, model_name)
                
                collected_for_model = 0
                for url in detail_urls:
                    car_detail = self.get_car_detail(url)
                    if car_detail:
                        car_detail['model_id'] = model_id
                        # USER ACTION: db_helper에 상세 차량 정보를 저장하는 메소드를 구현해야 합니다.
                        # 예: db_helper.insert_detailed_used_car(car_detail)
                        logger.info(f"  - 수집 성공: {car_detail.get('trim_name', 'N/A')}, {car_detail.get('price')}만원")
                        # 지금은 단순히 로그만 출력합니다. DB 저장 로직을 추가해야 합니다.
                        collected_for_model += 1
                    time.sleep(self.config.get('delay', 2)) # 각 상세 페이지 접근 사이의 딜레이

                logger.info(f"--- {manufacturer} {model_name}: {collected_for_model}건 수집 완료 ---")
                total_collected += collected_for_model

            db_helper.update_crawling_log('encar_detailed', '완료', total_collected)
            logger.info(f"🎉 전체 상세 크롤링 완료! 총 {total_collected}건 수집")
            
        except Exception as e:
            db_helper.update_crawling_log('encar_detailed', '실패', total_collected, str(e))
            logger.error(f"상세 크롤링 실패: {e}")
        finally:
            self.close_driver()

if __name__ == '__main__':
    import json
    print("상세 정보 크롤러 단독 테스트 모드")
    
    try:
        with open('config/scheduler_config.json', 'r', encoding='utf-8') as f:
            full_config = json.load(f)
        encar_config = full_config['crawling']['encar']
        print("✅ 테스트 설정 로드 완료")
        
        crawler = EncarCrawler(config=encar_config)
        
        test_cars = [
            {'manufacturer': '현대', 'model_name': '그랜저 IG'},
        ]
        
        # 실제 크롤링을 실행하려면 아래 주석을 해제하세요.
        # print("테스트 상세 크롤링:")
        # crawler.crawl_and_save(test_cars)
        print("실제 실행하려면 `crawl_and_save` 메소드의 주석을 해제하고,")
        print("USER ACTION 주석이 달린 부분의 URL과 CSS 선택자를 실제 사이트에 맞게 수정해야 합니다.")
        
    except FileNotFoundError:
        print("오류: config/scheduler_config.json 파일을 찾을 수 없습니다.")
    except Exception as e:
        print(f"오류 발생: {e}")