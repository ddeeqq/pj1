"""
샘플 데이터 생성 및 초기화 스크립트
"""
import random
import pandas as pd
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database.db_helper import db_helper
from config.config import POPULAR_MODELS, CAR_SEGMENTS
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataInitializer:
    def __init__(self):
        self.regions = ['서울', '경기', '인천', '부산', '대구', '대전', '광주', '울산', '세종', 
                       '강원', '충북', '충남', '전북', '전남', '경북', '경남', '제주']
        
    def create_sample_cars(self):
        """샘플 차량 데이터 생성"""
        logger.info("🚗 샘플 차량 데이터 생성 중...")
        
        sample_cars = []
        for manufacturer, models in POPULAR_MODELS.items():
            for model in models:
                # 세그먼트 찾기
                segment = None
                for seg, seg_models in CAR_SEGMENTS.items():
                    if any(m in model for m in seg_models):
                        segment = seg
                        break
                
                # 연료 타입 결정
                fuel_types = ['가솔린', '디젤', '하이브리드', 'LPG', '전기']
                fuel_weights = [0.5, 0.3, 0.1, 0.05, 0.05]
                fuel_type = random.choices(fuel_types, fuel_weights)[0]
                
                # 출시 연도
                release_year = random.randint(2018, 2024)
                
                sample_cars.append({
                    'manufacturer': manufacturer,
                    'model_name': model,
                    'segment': segment or '준중형',
                    'fuel_type': fuel_type,
                    'release_year': release_year
                })
                
        # DB에 저장
        for car in sample_cars:
            db_helper.insert_car_model(**car)
            
        logger.info(f"✅ {len(sample_cars)}개 차량 모델 생성 완료")
        return sample_cars
    
    def create_sample_prices(self, num_records=100):
        """샘플 가격 데이터 생성"""
        logger.info("💰 샘플 가격 데이터 생성 중...")
        
        # 모든 차량 모델 조회
        models_df = db_helper.get_car_models()
        
        if models_df.empty:
            logger.warning("차량 모델이 없습니다. 먼저 차량 데이터를 생성하세요.")
            return
        
        for _, model in models_df.iterrows():
            model_id = model['model_id']
            
            # 중고차 가격 생성 (연식별, 주행거리별)
            for year in range(2019, 2024):
                for mileage_range in ['3만km 미만', '3-5만km', '5-7만km', '7-10만km', '10-15만km']:
                    # 기본 가격 설정 (세그먼트별)
                    base_prices = {
                        '경차': 800, '소형': 1500, '준중형': 2500,
                        '중형': 3500, '대형': 5000, 'SUV소형': 2000,
                        'SUV중형': 3000, 'SUV대형': 4500
                    }
                    
                    base_price = base_prices.get(model['segment'], 2500)
                    
                    # 연식에 따른 감가
                    year_diff = 2024 - year
                    depreciation = 0.85 ** year_diff
                    
                    # 주행거리에 따른 감가
                    mileage_depreciation = {
                        '3만km 미만': 1.0,
                        '3-5만km': 0.95,
                        '5-7만km': 0.90,
                        '7-10만km': 0.85,
                        '10-15만km': 0.80
                    }
                    
                    avg_price = base_price * depreciation * mileage_depreciation[mileage_range]
                    min_price = avg_price * 0.9
                    max_price = avg_price * 1.1
                    
                    db_helper.insert_used_car_price(
                        model_id=model_id,
                        year=year,
                        mileage_range=mileage_range,
                        avg_price=round(avg_price),
                        min_price=round(min_price),
                        max_price=round(max_price),
                        sample_count=random.randint(5, 50),
                        data_source='sample'
                    )
            
            # 신차 가격 생성
            trims = ['기본형', '고급형', '최고급형']
            for i, trim in enumerate(trims):
                base_price = base_prices.get(model['segment'], 2500) * (1 + i * 0.2)
                
                db_helper.insert_new_car_price(
                    model_id=model_id,
                    trim_name=trim,
                    base_price=round(base_price),
                    total_price=round(base_price * 1.1),
                    promotion_discount=round(base_price * 0.05)
                )
                
        logger.info("✅ 가격 데이터 생성 완료")
    
    def create_sample_registrations(self, num_records=500):
        """샘플 등록 통계 생성"""
        logger.info("📊 샘플 등록 통계 생성 중...")
        
        models_df = db_helper.get_car_models()
        
        if models_df.empty:
            logger.warning("차량 모델이 없습니다.")
            return
        
        # 각 모델별로 지역별 등록 데이터 생성
        for _, model in models_df.iterrows():
            model_id = model['model_id']
            
            # 인기도 가중치 (제조사별)
            popularity = {
                '현대': 1.5, '기아': 1.3, '제네시스': 0.8,
                '쉐보레': 0.7, 'BMW': 0.6, '벤츠': 0.6
            }
            
            weight = popularity.get(model['manufacturer'], 1.0)
            
            # 최근 30일간의 데이터 생성
            for days_ago in range(30):
                date = datetime.now().date() - timedelta(days=days_ago)
                
                for region in random.sample(self.regions, k=random.randint(3, 8)):
                    count = random.randint(10, 100) * weight
                    cumulative = count * random.randint(50, 200)
                    
                    db_helper.insert_registration_stats(
                        model_id=model_id,
                        region=region,
                        registration_date=date,
                        registration_count=int(count),
                        cumulative_count=int(cumulative)
                    )
        
        logger.info("✅ 등록 통계 생성 완료")
    
    def create_sample_recalls(self):
        """샘플 리콜 정보 생성"""
        logger.info("⚠️ 샘플 리콜 정보 생성 중...")
        
        models_df = db_helper.get_car_models()
        
        recall_titles = [
            "엔진 오일 누유 가능성",
            "브레이크 패드 조기 마모",
            "에어백 오작동 가능성",
            "배출가스 기준 초과",
            "조향장치 이상",
            "연료 펌프 결함",
            "전기 시스템 단락",
            "변속기 오작동",
            "냉각수 누수",
            "타이어 공기압 센서 오류"
        ]
        
        for _, model in models_df.iterrows():
            # 랜덤하게 0~3개의 리콜 생성
            num_recalls = random.randint(0, 3)
            
            for _ in range(num_recalls):
                recall_date = datetime.now().date() - timedelta(days=random.randint(30, 365))
                title = random.choice(recall_titles)
                
                # 심각도 결정
                if '에어백' in title or '브레이크' in title or '조향' in title:
                    severity = '매우심각'
                elif '엔진' in title or '변속기' in title:
                    severity = '심각'
                elif '배출가스' in title or '연료' in title:
                    severity = '보통'
                else:
                    severity = '경미'
                
                db_helper.insert_recall_info(
                    model_id=model['model_id'],
                    recall_date=recall_date,
                    recall_title=title,
                    recall_reason=f"{title}으로 인한 안전 문제 발생 가능",
                    affected_units=random.randint(100, 10000),
                    severity_level=severity,
                    fix_description="지정 서비스센터 방문 후 무상 수리"
                )
        
        logger.info("✅ 리콜 정보 생성 완료")
    
    def create_sample_faq(self):
        """샘플 FAQ 생성"""
        logger.info("❓ 샘플 FAQ 생성 중...")
        
        models_df = db_helper.get_car_models()
        
        faq_templates = [
            {
                'question': '연비는 어느 정도인가요?',
                'answer': '도심 {city}km/L, 고속도로 {highway}km/L, 복합 {combined}km/L입니다.',
                'category': '성능'
            },
            {
                'question': '보증 기간은 어떻게 되나요?',
                'answer': '일반 보증 3년/6만km, 엔진/변속기 5년/10만km입니다.',
                'category': '보증'
            },
            {
                'question': '안전 옵션에는 어떤 것들이 있나요?',
                'answer': '전방 충돌방지 보조, 차로 유지 보조, 후측방 충돌 경고 등이 기본 탑재되어 있습니다.',
                'category': '안전'
            },
            {
                'question': '정비 비용은 얼마나 드나요?',
                'answer': '정기 점검 비용은 회당 10-20만원 수준이며, 소모품 교체 주기는 차량 사용 설명서를 참고하세요.',
                'category': '유지보수'
            },
            {
                'question': '중고차로 구매 시 주의사항은?',
                'answer': '사고 이력, 침수 여부, 주행거리 조작 여부를 반드시 확인하고, 성능점검기록부를 꼼꼼히 검토하세요.',
                'category': '구매팁'
            }
        ]
        
        for _, model in models_df.iterrows():
            # 각 모델당 3-5개 FAQ 생성
            num_faqs = random.randint(3, 5)
            selected_faqs = random.sample(faq_templates, num_faqs)
            
            for faq in selected_faqs:
                # 연비 정보는 랜덤 생성
                if '연비' in faq['question']:
                    answer = faq['answer'].format(
                        city=random.randint(8, 15),
                        highway=random.randint(12, 20),
                        combined=random.randint(10, 17)
                    )
                else:
                    answer = faq['answer']
                
                query = """
                INSERT INTO FAQ (model_id, question, answer, category, view_count, helpful_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                db_helper.execute_query(
                    query,
                    (model['model_id'], faq['question'], answer, faq['category'],
                     random.randint(10, 1000), random.randint(5, 100)),
                    fetch=False
                )
        
        logger.info("✅ FAQ 생성 완료")
    
    def initialize_all(self):
        """전체 샘플 데이터 초기화"""
        logger.info("🚀 전체 샘플 데이터 초기화 시작...")
        
        # 1. 차량 모델 생성
        self.create_sample_cars()
        
        # 2. 가격 정보 생성
        self.create_sample_prices()
        
        # 3. 등록 통계 생성
        self.create_sample_registrations()
        
        # 4. 리콜 정보 생성
        self.create_sample_recalls()
        
        # 5. FAQ 생성
        self.create_sample_faq()
        
        logger.info("🎉 전체 샘플 데이터 초기화 완료!")
        
        # 통계 출력
        stats = {
            'CarModel': 'SELECT COUNT(*) as cnt FROM CarModel',
            'UsedCarPrice': 'SELECT COUNT(*) as cnt FROM UsedCarPrice',
            'NewCarPrice': 'SELECT COUNT(*) as cnt FROM NewCarPrice',
            'RegistrationStats': 'SELECT COUNT(*) as cnt FROM RegistrationStats',
            'RecallInfo': 'SELECT COUNT(*) as cnt FROM RecallInfo',
            'FAQ': 'SELECT COUNT(*) as cnt FROM FAQ'
        }
        
        logger.info("\n📊 생성된 데이터 통계:")
        for table, query in stats.items():
            result = db_helper.execute_query(query)
            count = result[0]['cnt'] if result else 0
            logger.info(f"   {table}: {count}건")

# 실행
if __name__ == "__main__":
    initializer = DataInitializer()
    
    print("=" * 50)
    print("샘플 데이터 생성 도구")
    print("=" * 50)
    print("1. 전체 데이터 초기화")
    print("2. 차량 모델만 생성")
    print("3. 가격 정보만 생성")
    print("4. 등록 통계만 생성")
    print("5. 리콜 정보만 생성")
    print("6. FAQ만 생성")
    print("=" * 50)
    
    choice = input("선택 (1-6): ")
    
    if choice == '1':
        initializer.initialize_all()
    elif choice == '2':
        initializer.create_sample_cars()
    elif choice == '3':
        initializer.create_sample_prices()
    elif choice == '4':
        initializer.create_sample_registrations()
    elif choice == '5':
        initializer.create_sample_recalls()
    elif choice == '6':
        initializer.create_sample_faq()
    else:
        print("잘못된 선택입니다.")
