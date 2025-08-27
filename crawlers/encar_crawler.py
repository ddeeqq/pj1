"""
엔카(encar.com) 중고차 가격 정보 크롤러
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
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

class EncarCrawler:
    def __init__(self):
        self.config = CRAWLING_CONFIG['encar']
        self.base_url = self.config['base_url']
        self.search_url = self.config['search_url']
        self.delay = self.config['delay']
        self.driver = None
        
    def setup_driver(self):
        """Chrome 드라이버 설정"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # 백그라운드 실행
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
        try:
            # ChromeDriver 자동 다운로드 (selenium 4.x)
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            logger.info("✅ Chrome 드라이버 설정 완료")
        except Exception as e:
            logger.error(f"❌ Chrome 드라이버 설정 실패: {e}")
            logger.info("ChromeDriver를 수동으로 설치하거나 webdriver-manager를 설치해주세요.")
            logger.info("pip install webdriver-manager")
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
                
            # 검색 URL 구성 (엔카 검색 파라미터)
            search_params = {
                'manufacturer': manufacturer,
                'model': model_name,
            }
            if year:
                search_params['year'] = year
                
            # 엔카 검색 페이지 접속
            search_query = f"{manufacturer} {model_name}"
            if year:
                search_query += f" {year}"
                
            url = f"{self.search_url}?q={search_query}"
            logger.info(f"🔍 검색 중: {search_query}")
            
            self.driver.get(url)
            time.sleep(self.delay)
            
            # 페이지 소스 가져오기
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # 차량 정보 파싱
            car_data = self._parse_car_listings(soup, manufacturer, model_name, year)
            
            return car_data
            
        except Exception as e:
            logger.error(f"검색 중 오류: {e}")
            return []
            
    def _parse_car_listings(self, soup, manufacturer, model_name, year):
        """차량 목록 파싱"""
        car_data = []
        
        try:
            # 실제 엔카 HTML 구조에 맞게 수정 필요
            # 아래는 예시 셀렉터입니다
            listings = soup.select('.car-item, .lst-wrap, .area')  # 엔카의 실제 클래스명으로 변경 필요
            
            for listing in listings[:20]:  # 상위 20개만 수집
                try:
                    # 가격 정보 추출 (실제 구조에 맞게 수정 필요)
                    price_elem = listing.select_one('.price, .pri, .cost')
                    if price_elem:
                        price_text = price_elem.text.strip()
                        price = self._extract_price(price_text)
                    else:
                        continue
                        
                    # 연식 정보 추출
                    year_elem = listing.select_one('.year, .inf, .detail')
                    if year_elem and not year:
                        year_text = year_elem.text.strip()
                        year = self._extract_year(year_text)
                        
                    # 주행거리 정보 추출
                    mileage_elem = listing.select_one('.mileage, .km, .distance')
                    mileage_range = '알수없음'
                    if mileage_elem:
                        mileage_text = mileage_elem.text.strip()
                        mileage = self._extract_mileage(mileage_text)
                        mileage_range = self._get_mileage_range(mileage)
                        
                    car_data.append({
                        'manufacturer': manufacturer,
                        'model_name': model_name,
                        'year': year,
                        'price': price,
                        'mileage_range': mileage_range,
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
        """가격 텍스트에서 숫자 추출 (만원 단위)"""
        import re
        # '3,500만원', '3500만원' 등의 형태에서 숫자 추출
        numbers = re.findall(r'[\d,]+', text)
        if numbers:
            price_str = numbers[0].replace(',', '')
            try:
                return int(price_str)
            except:
                return 0
        return 0
        
    def _extract_year(self, text):
        """연식 정보 추출"""
        import re
        # '2020년식', '20년' 등에서 연도 추출
        year_match = re.search(r'20\d{2}|19\d{2}|\d{2}년', text)
        if year_match:
            year_str = year_match.group()
            if len(year_str) == 2 or len(year_str) == 3:  # '20년' 형태
                year_num = int(year_str[:2])
                return 2000 + year_num if year_num < 50 else 1900 + year_num
            else:
                return int(year_str[:4])
        return None
        
    def _extract_mileage(self, text):
        """주행거리 추출 (km 단위)"""
        import re
        numbers = re.findall(r'[\d,]+', text)
        if numbers:
            mileage_str = numbers[0].replace(',', '')
            try:
                mileage = int(mileage_str)
                # 만km 단위인 경우
                if '만' in text:
                    mileage = mileage * 10000
                return mileage
            except:
                return 0
        return 0
        
    def _get_mileage_range(self, mileage):
        """주행거리를 범위로 변환"""
        if mileage == 0:
            return '알수없음'
        elif mileage < 30000:
            return '3만km 미만'
        elif mileage < 50000:
            return '3-5만km'
        elif mileage < 70000:
            return '5-7만km'
        elif mileage < 100000:
            return '7-10만km'
        elif mileage < 150000:
            return '10-15만km'
        else:
            return '15만km 이상'
            
    def crawl_and_save(self, car_list):
        """차량 목록을 크롤링하고 DB에 저장"""
        db_helper.update_crawling_log('encar', '시작')
        total_collected = 0
        
        try:
            for car in car_list:
                manufacturer = car['manufacturer']
                model_name = car['model_name']
                year = car.get('year', None)
                
                # 모델 ID 조회 또는 생성
                model_id = db_helper.get_car_model_id(manufacturer, model_name)
                if not model_id:
                    db_helper.insert_car_model(manufacturer, model_name)
                    model_id = db_helper.get_car_model_id(manufacturer, model_name)
                    
                # 가격 정보 수집
                car_data = self.search_car_prices(manufacturer, model_name, year)
                
                # 데이터 집계
                if car_data:
                    df = pd.DataFrame(car_data)
                    
                    # 연식별, 주행거리별 평균 가격 계산
                    grouped = df.groupby(['year', 'mileage_range'])['price'].agg([
                        ('avg_price', 'mean'),
                        ('min_price', 'min'),
                        ('max_price', 'max'),
                        ('sample_count', 'count')
                    ]).reset_index()
                    
                    # DB에 저장
                    for _, row in grouped.iterrows():
                        db_helper.insert_used_car_price(
                            model_id=model_id,
                            year=int(row['year']) if pd.notna(row['year']) else None,
                            mileage_range=row['mileage_range'],
                            avg_price=float(row['avg_price']),
                            min_price=float(row['min_price']),
                            max_price=float(row['max_price']),
                            sample_count=int(row['sample_count']),
                            data_source='encar'
                        )
                    
                    total_collected += len(car_data)
                    logger.info(f"✅ {model_name} 데이터 저장 완료 ({len(car_data)}건)")
                    
                time.sleep(self.delay)  # 서버 부하 방지
                
            db_helper.update_crawling_log('encar', '완료', total_collected)
            logger.info(f"🎉 전체 크롤링 완료! 총 {total_collected}건 수집")
            
        except Exception as e:
            db_helper.update_crawling_log('encar', '실패', total_collected, str(e))
            logger.error(f"크롤링 실패: {e}")
            
        finally:
            self.close_driver()

# 테스트 실행
if __name__ == "__main__":
    crawler = EncarCrawler()
    
    # 테스트할 차량 목록
    test_cars = [
        {'manufacturer': '현대', 'model_name': '그랜저 IG'},
        {'manufacturer': '현대', 'model_name': '쏘나타 DN8'},
        {'manufacturer': '기아', 'model_name': 'K5 DL3'},
    ]
    
    # 크롤링 실행
    # crawler.crawl_and_save(test_cars)
    
    # 단일 검색 테스트
    # results = crawler.search_car_prices('현대', '그랜저 IG', 2020)
    # print(results)
    
    print("엔카 크롤러 준비 완료!")
    print("실제 크롤링을 실행하려면 주석을 해제하세요.")
