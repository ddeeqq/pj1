"""
중고차 vs 신차 가격 분석 모듈
"""
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_helper import db_helper
from config.config import ANALYSIS_WEIGHTS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PriceAnalyzer:
    def __init__(self):
        self.weights = ANALYSIS_WEIGHTS
        
    def calculate_value_score(self, model_id):
        """차량의 가성비 점수 계산"""
        try:
            # 가격 정보 조회
            price_comparison = db_helper.get_latest_prices_comparison(model_id)
            
            # 리콜 정보 조회
            recall_df = db_helper.get_recall_info(model_id)
            
            # 등록 통계 조회
            reg_stats = db_helper.get_registration_stats(model_id)
            
            # 점수 계산
            scores = {}
            
            # 1. 가격 점수 (신차 대비 중고차 가격 비율)
            if price_comparison['used_prices'] and price_comparison['new_prices']:
                used_avg = price_comparison['used_prices'].get('used_avg_price', 0)
                new_avg = price_comparison['new_prices'].get('new_avg_price', 0)
                if new_avg > 0:
                    price_ratio = 1 - (used_avg / new_avg)
                    scores['price_score'] = min(price_ratio * 100, 100)
                else:
                    scores['price_score'] = 50
            else:
                scores['price_score'] = 50
                
            # 2. 신뢰도 점수 (리콜 횟수 기반)
            if not recall_df.empty:
                recall_count = len(recall_df)
                severe_recalls = len(recall_df[recall_df['severity_level'].isin(['심각', '매우심각'])])
                scores['reliability_score'] = max(100 - (recall_count * 10) - (severe_recalls * 10), 0)
            else:
                scores['reliability_score'] = 100
                
            # 3. 인기도 점수 (등록 대수 기반)
            if not reg_stats.empty:
                total_registrations = reg_stats['registration_count'].sum()
                # 로그 스케일로 정규화
                popularity = min(np.log10(total_registrations + 1) * 20, 100)
                scores['popularity_score'] = popularity
            else:
                scores['popularity_score'] = 50
                
            # 4. 종합 점수 계산
            total_score = (
                scores['price_score'] * self.weights['price_weight'] +
                scores['reliability_score'] * self.weights['reliability_weight'] +
                scores['popularity_score'] * self.weights['popularity_weight']
            )
            
            scores['total_score'] = round(total_score, 1)
            
            return scores
            
        except Exception as e:
            logger.error(f"점수 계산 오류: {e}")
            return {
                'price_score': 0,
                'reliability_score': 0,
                'popularity_score': 0,
                'total_score': 0
            }
            
    def find_alternative_new_cars(self, used_car_price, additional_budget=0):
        """중고차 가격 + 추가 예산으로 구매 가능한 신차 찾기"""
        try:
            total_budget = used_car_price + additional_budget
            
            query = """
            SELECT cm.manufacturer, cm.model_name, cm.segment,
                   ncp.trim_name, ncp.base_price, ncp.total_price,
                   ncp.promotion_discount
            FROM NewCarPrice ncp
            JOIN CarModel cm ON ncp.model_id = cm.model_id
            WHERE ncp.base_price <= %s
              AND ncp.valid_from <= CURDATE() 
              AND ncp.valid_until >= CURDATE()
            ORDER BY ncp.base_price DESC
            LIMIT 10
            """
            
            df = db_helper.fetch_dataframe(query, [total_budget])
            
            if not df.empty:
                # 각 신차의 점수 계산
                for idx, row in df.iterrows():
                    model_id = db_helper.get_car_model_id(
                        row['manufacturer'], 
                        row['model_name']
                    )
                    if model_id:
                        scores = self.calculate_value_score(model_id)
                        df.at[idx, 'value_score'] = scores['total_score']
                    else:
                        df.at[idx, 'value_score'] = 0
                        
                # 점수 기준으로 정렬
                df = df.sort_values('value_score', ascending=False)
                
            return df
            
        except Exception as e:
            logger.error(f"대안 신차 검색 오류: {e}")
            return pd.DataFrame()
            
    def predict_future_price(self, model_id, years=3):
        """미래 가격 예측 (간단한 감가상각 모델)"""
        try:
            # 과거 가격 데이터 조회
            query = """
            SELECT year, AVG(avg_price) as avg_price
            FROM UsedCarPrice
            WHERE model_id = %s
            GROUP BY year
            ORDER BY year DESC
            """
            
            df = db_helper.fetch_dataframe(query, [model_id])
            
            if len(df) >= 2:
                # 연간 감가율 계산
                depreciation_rates = []
                for i in range(len(df) - 1):
                    rate = (df.iloc[i]['avg_price'] - df.iloc[i+1]['avg_price']) / df.iloc[i+1]['avg_price']
                    depreciation_rates.append(rate)
                    
                avg_depreciation = np.mean(depreciation_rates)
                
                # 현재 가격
                current_price = df.iloc[0]['avg_price']
                
                # 미래 가격 예측
                predictions = []
                for year in range(1, years + 1):
                    future_price = current_price * ((1 - avg_depreciation) ** year)
                    predictions.append({
                        'year': year,
                        'predicted_price': round(future_price, 0)
                    })
                    
                return pd.DataFrame(predictions)
            else:
                # 데이터 부족시 일반적인 감가율 적용 (연 15%)
                current_price = df.iloc[0]['avg_price'] if not df.empty else 0
                predictions = []
                for year in range(1, years + 1):
                    future_price = current_price * (0.85 ** year)
                    predictions.append({
                        'year': year,
                        'predicted_price': round(future_price, 0)
                    })
                return pd.DataFrame(predictions)
                
        except Exception as e:
            logger.error(f"가격 예측 오류: {e}")
            return pd.DataFrame()
            
    def calculate_total_cost_of_ownership(self, model_id, years=5):
        """총 소유 비용 계산 (TCO)"""
        try:
            # 차량 정보 조회
            car_info = db_helper.execute_query(
                "SELECT * FROM CarModel WHERE model_id = %s",
                [model_id]
            )[0]
            
            # 초기 구매 가격
            price_data = db_helper.get_latest_prices_comparison(model_id)
            initial_price = price_data['used_prices'].get('used_avg_price', 0)
            
            # 예상 비용 계산 (연간)
            tco_breakdown = {
                '구매가격': initial_price,
                '보험료': initial_price * 0.05 * years,  # 연 5%
                '유지보수': 200 * years,  # 연 200만원
                '연료비': 150 * years,  # 연 150만원
                '세금': initial_price * 0.02 * years,  # 연 2%
            }
            
            # 예상 잔존가치
            future_price_df = self.predict_future_price(model_id, years)
            if not future_price_df.empty:
                residual_value = future_price_df.iloc[-1]['predicted_price']
                tco_breakdown['잔존가치'] = -residual_value  # 음수로 표시
                
            # 총 비용 계산
            total_cost = sum(tco_breakdown.values())
            tco_breakdown['총소유비용'] = total_cost
            
            return tco_breakdown
            
        except Exception as e:
            logger.error(f"TCO 계산 오류: {e}")
            return {}
            
    def compare_models(self, model_ids):
        """여러 모델 비교 분석"""
        comparison_data = []
        
        for model_id in model_ids:
            # 차량 정보
            car_info = db_helper.execute_query(
                "SELECT * FROM CarModel WHERE model_id = %s",
                [model_id]
            )
            
            if car_info:
                car_info = car_info[0]
                
                # 가격 정보
                price_data = db_helper.get_latest_prices_comparison(model_id)
                
                # 점수 계산
                scores = self.calculate_value_score(model_id)
                
                # TCO 계산
                tco = self.calculate_total_cost_of_ownership(model_id)
                
                comparison_data.append({
                    'model_name': f"{car_info['manufacturer']} {car_info['model_name']}",
                    'segment': car_info.get('segment', '-'),
                    'used_avg_price': price_data['used_prices'].get('used_avg_price', 0),
                    'new_avg_price': price_data['new_prices'].get('new_avg_price', 0),
                    'value_score': scores['total_score'],
                    'total_cost_ownership': tco.get('총소유비용', 0)
                })
                
        return pd.DataFrame(comparison_data)

# 테스트 실행
if __name__ == "__main__":
    analyzer = PriceAnalyzer()
    
    # 테스트용 모델 ID (실제 DB에서 조회 필요)
    test_model_id = 1
    
    # 가성비 점수 계산
    scores = analyzer.calculate_value_score(test_model_id)
    print("가성비 점수:", scores)
    
    # 대안 신차 찾기
    alternatives = analyzer.find_alternative_new_cars(2500, 500)  # 2500만원 + 500만원
    print("\n대안 신차:")
    print(alternatives)
    
    # 미래 가격 예측
    predictions = analyzer.predict_future_price(test_model_id, 3)
    print("\n미래 가격 예측:")
    print(predictions)
    
    # TCO 계산
    tco = analyzer.calculate_total_cost_of_ownership(test_model_id, 5)
    print("\n총 소유 비용 (5년):")
    for key, value in tco.items():
        print(f"{key}: {value:,.0f}만원")
