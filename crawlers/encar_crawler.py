"""
엔카(encar.com) 중고차 가격 정보 크롤러
"""
import re
import time
import logging
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

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_helper import db_helper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EncarCrawler:
    def __init__(self, config):
        """크롤러 초기화 시 설정(config)을 전달받음"""
        self.config = config
        self.driver = None
        
    def setup_driver(self):
        """Chrome 드라이버 설정"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
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
            logger.info("드라이버 종료 완료")
            
    def search_car_prices(self, manufacturer, model_name, year=None):
        """특정 차량의 가격 정보 검색"""
        try:
            if not self.driver:
                self.setup_driver()
                
            search_query = f"{manufacturer} {model_name}"
            if year:
                search_query += f" {year}"
            
            # 설정에서 URL과 딜레이 값 사용
            search_url = self.config.get('search_url', 'http://www.encar.com/dc/dc_carsearchlist.do')
            delay = self.config.get('delay', 2)
            url = f"{search_url}?q={search_query}"
            
            logger.info(f"🔍 검색 중: {search_query}")
            self.driver.get(url)
            time.sleep(delay)
            
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            car_data = self._parse_car_listings(soup, manufacturer, model_name, year)
            return car_data
            
        except Exception as e:
            logger.error(f"검색 중 오류: {e}")
            return []
            
    def _parse_car_listings(self, soup, manufacturer, model_name, year):
        """차량 목록 파싱 (실제 사이트 구조에 맞게 수정 필요)"""
        car_data = []
        try:
            listings = soup.select('.car-item, .lst-wrap, .area')
            max_items = self.config.get('max_items_per_model', 20) # 설정에서 최대 아이템 수 가져오기
            
            for listing in listings[:max_items]:
                try:
                    price_elem = listing.select_one('.price, .pri, .cost')
                    if not price_elem:
                        continue
                    
                    price = self._extract_price(price_elem.text)
                    year_from_page = self._extract_year(listing.select_one('.year, .inf, .detail').text) if listing.select_one('.year, .inf, .detail') else None
                    mileage_elem = listing.select_one('.mileage, .km, .distance')
                    mileage = self._extract_mileage(mileage_elem.text) if mileage_elem else 0
                    
                    car_data.append({
                        'manufacturer': manufacturer,
                        'model_name': model_name,
                        'year': year or year_from_page,
                        'price': price,
                        'mileage_range': self._get_mileage_range(mileage),
                        'source': 'encar',
                        'collected_date': datetime.now().date()
                    })
                except Exception as e:
                    logger.debug(f"개별 항목 파싱 오류: {e}")
                    continue
            logger.info(f"✅ {len(car_data)}개 차량 정보 수집 완료")
        except Exception as e:
            logger.error(f"파싱 중 오류: {e}")
        return car_data
        
    def _extract_price(self, text):
        numbers = re.findall(r'[\d,]+', text)
        return int(numbers[0].replace(',', '')) if numbers else 0
        
    def _extract_year(self, text):
        year_match = re.search(r'(20\d{2})|(\d{2}년)', text)
        if year_match:
            if year_match.group(1):
                return int(year_match.group(1))
            if year_match.group(2):
                return 2000 + int(year_match.group(2)[:2])
        return None
        
    def _extract_mileage(self, text):
        numbers = re.findall(r'[\d,]+', text)
        if numbers:
            mileage = int(numbers[0].replace(',', ''))
            return mileage * 10000 if '만' in text else mileage
        return 0
        
    def _get_mileage_range(self, mileage):
        if mileage == 0: return '알수없음'
        elif mileage < 30000: return '3만km 미만'
        elif mileage < 50000: return '3-5만km'
        elif mileage < 70000: return '5-7만km'
        elif mileage < 100000: return '7-10만km'
        elif mileage < 150000: return '10-15만km'
        else: return '15만km 이상'
            
    def crawl_and_save(self, car_list):
        """차량 목록을 크롤링하고 DB에 저장"""
        db_helper.update_crawling_log('encar', '시작')
        total_collected = 0
        delay = self.config.get('delay', 2)
        
        try:
            for car in car_list:
                model_id = db_helper.get_or_insert_car_model(car['manufacturer'], car['model_name'])
                car_data = self.search_car_prices(car['manufacturer'], car['model_name'], car.get('year'))
                
                if car_data:
                    df = pd.DataFrame(car_data)
                    grouped = df.groupby(['year', 'mileage_range'])['price'].agg(
                        avg_price=('mean'), min_price=('min'),
                        max_price=('max'), sample_count=('count')
                    ).reset_index()
                    
                    for _, row in grouped.iterrows():
                        db_helper.insert_used_car_price(
                            model_id=model_id, year=int(row['year']) if pd.notna(row['year']) else None,
                            mileage_range=row['mileage_range'], avg_price=float(row['avg_price']),
                            min_price=float(row['min_price']), max_price=float(row['max_price']),
                            sample_count=int(row['sample_count']), data_source='encar'
                        )
                    total_collected += len(car_data)
                    logger.info(f"✅ {car['model_name']} 데이터 저장 완료 ({len(car_data)}건)")
                time.sleep(delay)
                
            db_helper.update_crawling_log('encar', '완료', total_collected)
            logger.info(f"🎉 전체 크롤링 완료! 총 {total_collected}건 수집")
        except Exception as e:
            db_helper.update_crawling_log('encar', '실패', total_collected, str(e))
            logger.error(f"크롤링 실패: {e}")
        finally:
            self.close_driver()

if __name__ == '__main__':
    import json
    print("크롤러 단독 테스트 모드")
    
    # 테스트를 위해 설정 파일을 직접 로드
    try:
        with open('config/scheduler_config.json', 'r', encoding='utf-8') as f:
            full_config = json.load(f)
        encar_config = full_config['crawling']['encar']
        print("✅ 테스트 설정 로드 완료")
        
        crawler = EncarCrawler(config=encar_config)
        
        test_cars = [
            {'manufacturer': '현대', 'model_name': '그랜저 IG'},
            {'manufacturer': '기아', 'model_name': 'K5 DL3'},
        ]
        
        # 실제 크롤링을 실행하려면 아래 주석을 해제하세요.
        # print("테스트 크롤링 시작...")
        # crawler.crawl_and_save(test_cars)
        
        print("엔카 크롤러 준비 완료!")
        print("실제 크롤링을 실행하려면 코드의 주석을 해제하세요.")

    except FileNotFoundError:
        print("오류: config/scheduler_config.json 파일을 찾을 수 없습니다.")
    except Exception as e:
        print(f"오류 발생: {e}")
