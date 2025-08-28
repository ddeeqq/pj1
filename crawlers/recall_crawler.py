"""
개선된 자동차 리콜 센터 정보 크롤러
실제 URL 구조와 파라미터를 반영한 버전
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
from urllib.parse import urljoin, urlparse

# 상위 디렉토리의 모듈 import를 위한 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_helper import db_helper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RecallCrawler:
    def __init__(self, config=None):
        self.config = config or {}
        
        # 실제 자동차리콜센터 URL 구조
        self.base_url = "https://www.car.go.kr"
        self.recall_list_url = "https://www.car.go.kr/ri/stat/list.do"
        self.recall_detail_url = "https://www.car.go.kr/ri/stat/view.do"
        self.car_check_url = "https://www.car.go.kr/ri/recall/list.do"

        self.delay = self.config.get('delay', 2)
        self.max_retries = self.config.get('max_retries', 3)

        # 세션 설정
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.8,en-US;q=0.6,en;q=0.4',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })

        # 심각도 판단 키워드
        self.severity_keywords = {
            '매우심각': ['화재', '폭발', '사망', '중상', '에어백', '브레이크', '조향장치', '급가속', '급정지'],
            '심각': ['엔진', '변속기', '연료', '배출가스', '전기계통', '타이어', '서스펜션'],
            '보통': ['누수', '소음', '진동', '센서', '램프', '계기판', '공조장치'],
            '경미': ['도색', '내장재', '편의장치', '오디오', '네비게이션', 'USB']
        }

    def search_recall_info(self, manufacturer=None, model_name=None, start_date=None, end_date=None, **kwargs):
        """리콜 정보 검색"""
        recall_data = []

        try:
            # 검색 파라미터 구성
            params = self._build_search_params(manufacturer, model_name, start_date, end_date, **kwargs)

            # 첫 페이지 요청
            response = self._make_request(self.recall_list_url, params)
            if not response:
                return recall_data

            soup = BeautifulSoup(response.text, 'html.parser')

            # 전체 페이지 수 확인
            total_pages = self._get_total_pages(soup)
            max_pages = kwargs.get('max_pages', min(total_pages, 10))  # 최대 10페이지

            logger.info(f"🔍 총 {total_pages}페이지 중 {max_pages}페이지까지 크롤링")

            # 각 페이지별 크롤링
            for page in range(1, max_pages + 1):
                logger.info(f"📄 {page}/{max_pages} 페이지 크롤링 중...")

                if page > 1:
                    params['pageIndex'] = page
                    response = self._make_request(self.recall_list_url, params)
                    if not response:
                        continue
                    soup = BeautifulSoup(response.text, 'html.parser')

                # 페이지별 리콜 정보 추출
                page_data = self._parse_recall_list(soup, manufacturer, model_name)
                recall_data.extend(page_data)

                time.sleep(self.delay)

            logger.info(f"✅ 총 {len(recall_data)}건의 리콜 정보 수집 완료")

        except Exception as e:
            logger.error(f"리콜 정보 검색 오류: {e}")

        return recall_data

    def _build_search_params(self, manufacturer, model_name, start_date, end_date, **kwargs):
        """검색 파라미터 구성"""
        params = {
            'pageIndex': 1,
            'pageUnit': kwargs.get('page_size', 20),
            'searchCondition': '',
            'searchKeyword': ''
        }

        # 제조사 검색
        if manufacturer:
            if params['searchKeyword']:
                params['searchKeyword'] += f" {manufacturer}"
            else:
                params['searchKeyword'] = manufacturer

        # 모델명 검색
        if model_name:
            if params['searchKeyword']:
                params['searchKeyword'] += f" {model_name}"
            else:
                params['searchKeyword'] = model_name

        # 날짜 범위 설정
        if start_date:
            params['searchStartDate'] = start_date
        if end_date:
            params['searchEndDate'] = end_date

        # 추가 필터
        if kwargs.get('recall_type'):
            params['recallType'] = kwargs['recall_type']

        return params

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

    def _get_total_pages(self, soup):
        """전체 페이지 수 확인"""
        try:
            # 페이징 정보에서 마지막 페이지 추출
            paging = soup.select_one('.paging, .pagination, .page_wrap')
            if paging:
                page_links = paging.select('a')
                if page_links:
                    # 숫자로 된 페이지 링크 중 가장 큰 값 찾기
                    max_page = 1
                    for link in page_links:
                        text = link.get_text(strip=True)
                        if text.isdigit():
                            max_page = max(max_page, int(text))
                    return max_page

            return 1
        except Exception as e:
            logger.debug(f"페이지 수 확인 오류: {e}")
            return 1

    def _parse_recall_list(self, soup, manufacturer, model_name):
        """리콜 목록 페이지 파싱"""
        recall_data = []

        try:
            # 리콜 목록 테이블 찾기
            table = soup.select_one('.board_list table, .list_table, tbody')
            if not table:
                logger.warning("리콜 목록 테이블을 찾을 수 없음")
                return recall_data

            # 각 행(리콜 항목) 파싱
            rows = table.select('tr')[1:]  # 헤더 제외

            for row in rows:
                try:
                    recall_info = self._extract_recall_info(row, manufacturer, model_name)
                    if recall_info:
                        # 상세 정보 가져오기
                        detail_info = self._get_recall_detail(recall_info.get('detail_url'))
                        if detail_info:
                            recall_info.update(detail_info)

                        recall_data.append(recall_info)

                except Exception as e:
                    logger.debug(f"개별 리콜 항목 파싱 오류: {e}")
                    continue

        except Exception as e:
            logger.error(f"리콜 목록 파싱 오류: {e}")

        return recall_data

    def _extract_recall_info(self, row, manufacturer, model_name):
        """개별 리콜 정보 추출"""
        try:
            cells = row.select('td')
            if len(cells) < 6:
                return None

            # 기본 정보 추출
            recall_info = {
                'manufacturer': manufacturer or self._clean_text(cells[0].get_text()),
                'model_name': model_name or self._clean_text(cells[1].get_text()),
                'recall_date': self._parse_date(cells[2].get_text()),
                'recall_title': self._clean_text(cells[3].get_text()),
                'recall_reason': self._clean_text(cells[4].get_text()),
                'affected_units': self._extract_number(cells[5].get_text()),
                'source': 'car.go.kr',
                'collected_date': datetime.now().date()
            }

            # 상세 페이지 URL 추출
            detail_link = row.select_one('a[href]')
            if detail_link:
                href = detail_link.get('href')
                recall_info['detail_url'] = urljoin(self.base_url, href)

            # 심각도 자동 판단
            recall_info['severity_level'] = self._determine_severity(
                recall_info['recall_title'], 
                recall_info['recall_reason']
            )

            return recall_info

        except Exception as e:
            logger.debug(f"리콜 정보 추출 오류: {e}")
            return None

    def _get_recall_detail(self, detail_url):
        """리콜 상세 정보 가져오기"""
        if not detail_url:
            return {}

        try:
            response = self._make_request(detail_url)
            if not response:
                return {}

            soup = BeautifulSoup(response.text, 'html.parser')

            detail_info = {}

            # 상세 정보 추출
            content_area = soup.select_one('.view_content, .content_area, .detail_content')
            if content_area:
                # 결함 내용
                defect_elem = content_area.select_one('.defect_content, .fault_detail')
                if defect_elem:
                    detail_info['defect_content'] = self._clean_text(defect_elem.get_text())

                # 시정 방법
                correction_elem = content_area.select_one('.correction_method, .repair_method')
                if correction_elem:
                    detail_info['correction_method'] = self._clean_text(correction_elem.get_text())

                # 생산 기간
                production_elem = content_area.select_one('.production_period, .manufacture_date')
                if production_elem:
                    detail_info['production_period'] = self._clean_text(production_elem.get_text())

            time.sleep(0.5)  # 상세 페이지 요청 간격
            return detail_info

        except Exception as e:
            logger.debug(f"상세 정보 가져오기 오류: {e}")
            return {}

    def _clean_text(self, text):
        """텍스트 정제"""
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text.strip())

    def _parse_date(self, date_str):
        """날짜 파싱"""
        try:
            date_str = self._clean_text(date_str)

            # 다양한 날짜 형식 시도
            date_formats = [
                '%Y-%m-%d',
                '%Y.%m.%d', 
                '%Y/%m/%d',
                '%Y년 %m월 %d일',
                '%Y.%m.%d.'
            ]

            for fmt in date_formats:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue

            # 년월만 있는 경우
            year_month_match = re.search(r'(\d{4})[.-]?(\d{1,2})', date_str)
            if year_month_match:
                year, month = year_month_match.groups()
                return datetime(int(year), int(month), 1).date()

            return None

        except Exception as e:
            logger.debug(f"날짜 파싱 오류: {e}")
            return None

    def _extract_number(self, text):
        """텍스트에서 숫자 추출"""
        try:
            if not text:
                return 0

            # 쉼표 제거 후 숫자 추출
            numbers = re.findall(r'[\d,]+', text.replace(',', ''))
            if numbers:
                # 가장 큰 숫자를 선택 (대상 수량으로 추정)
                return max([int(num.replace(',', '')) for num in numbers if num.replace(',', '').isdigit()])

            return 0

        except Exception as e:
            logger.debug(f"숫자 추출 오류: {e}")
            return 0

    def _determine_severity(self, title, reason):
        """심각도 자동 판단"""
        try:
            text = (title + ' ' + reason).lower()

            for severity, keywords in self.severity_keywords.items():
                if any(keyword in text for keyword in keywords):
                    return severity

            return '경미'

        except Exception as e:
            logger.debug(f"심각도 판단 오류: {e}")
            return '알수없음'

    def search_recall_by_car_number(self, car_number):
        """차량번호로 리콜 대상 확인"""
        try:
            params = {
                'carNumber': car_number
            }

            response = self._make_request(self.car_check_url, params)
            if not response:
                return []

            soup = BeautifulSoup(response.text, 'html.parser')

            # 리콜 대상 여부 확인 및 정보 추출
            recall_results = []
            result_area = soup.select_one('.result_area, .recall_result')

            if result_area:
                recall_items = result_area.select('.recall_item, .result_item')
                for item in recall_items:
                    recall_info = self._extract_car_recall_info(item, car_number)
                    if recall_info:
                        recall_results.append(recall_info)

            return recall_results

        except Exception as e:
            logger.error(f"차량번호 조회 오류: {e}")
            return []

    def _extract_car_recall_info(self, item, car_number):
        """차량별 리콜 정보 추출"""
        try:
            return {
                'car_number': car_number,
                'manufacturer': self._clean_text(item.select_one('.manufacturer, .company').get_text() if item.select_one('.manufacturer, .company') else ''),
                'model_name': self._clean_text(item.select_one('.model, .car_name').get_text() if item.select_one('.model, .car_name') else ''),
                'recall_reason': self._clean_text(item.select_one('.reason, .cause').get_text() if item.select_one('.reason, .cause') else ''),
                'recall_status': self._clean_text(item.select_one('.status, .progress').get_text() if item.select_one('.status, .progress') else ''),
                'correction_period': self._clean_text(item.select_one('.period, .date').get_text() if item.select_one('.period, .date') else ''),
                'source': 'car.go.kr',
                'collected_date': datetime.now().date()
            }

        except Exception as e:
            logger.debug(f"차량 리콜 정보 추출 오류: {e}")
            return None

    def crawl_and_save(self, car_list=None, date_range_days=30):
        """리콜 정보 크롤링 및 DB 저장"""
        db_helper.update_crawling_log('recall', '시작')
        total_collected = 0

        try:
            # 날짜 범위 설정 (최근 30일)
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=date_range_days)

            if car_list:
                # 특정 차량 모델들의 리콜 정보 수집
                for car in car_list:
                    manufacturer = car['manufacturer']
                    model_name = car['model_name']

                    logger.info(f"🚗 {manufacturer} {model_name} 리콜 정보 수집 중...")

                    # 모델 ID 조회 또는 생성
                    model_id = db_helper.get_car_model_id(manufacturer, model_name)
                    if not model_id:
                        db_helper.insert_car_model(manufacturer, model_name)
                        model_id = db_helper.get_car_model_id(manufacturer, model_name)

                    # 리콜 정보 검색
                    recall_data = self.search_recall_info(
                        manufacturer=manufacturer,
                        model_name=model_name,
                        start_date=start_date.strftime('%Y-%m-%d'),
                        end_date=end_date.strftime('%Y-%m-%d'),
                        max_pages=5
                    )

                    # DB에 저장
                    for recall in recall_data:
                        db_helper.insert_recall_info(
                            model_id=model_id,
                            **recall
                        )

                    total_collected += len(recall_data)
                    time.sleep(self.delay)

            else:
                # 전체 리콜 정보 수집 (최근 발생한 리콜들)
                logger.info(f"🔍 전체 리콜 정보 수집 중 ({start_date} ~ {end_date})")

                recall_data = self.search_recall_info(
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date.strftime('%Y-%m-%d'),
                    max_pages=10
                )

                # 제조사/모델별로 그룹핑하여 저장
                for recall in recall_data:
                    manufacturer = recall['manufacturer']
                    model_name = recall['model_name']

                    # 모델 ID 조회 또는 생성
                    model_id = db_helper.get_car_model_id(manufacturer, model_name)
                    if not model_id:
                        db_helper.insert_car_model(manufacturer, model_name)
                        model_id = db_helper.get_car_model_id(manufacturer, model_name)

                    # DB에 저장
                    recall['model_id'] = model_id
                    db_helper.insert_recall_info(**recall)

                total_collected = len(recall_data)

            db_helper.update_crawling_log('recall', '완료', total_collected)
            logger.info(f"🎉 리콜 정보 크롤링 완료! 총 {total_collected}건 수집")

        except Exception as e:
            db_helper.update_crawling_log('recall', '실패', total_collected, str(e))
            logger.error(f"리콜 크롤링 실패: {e}")

        finally:
            self.session.close()


if __name__ == '__main__':
    print("RecallCrawler 모듈")
    print("이 모듈은 run.py를 통해 실행하거나 다른 모듈에서 import하여 사용하세요.")
    print("예시: python run.py crawl")