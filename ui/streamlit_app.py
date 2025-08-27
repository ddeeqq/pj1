"""
중고차 vs 신차 가성비 분석 시스템 - Streamlit 메인 애플리케이션
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import STREAMLIT_CONFIG, CAR_MANUFACTURERS, POPULAR_MODELS, CAR_SEGMENTS
from database.db_helper import db_helper
from analyzers.price_analyzer import PriceAnalyzer
from crawlers.public_data_crawler import PublicDataCrawler

# 페이지 설정
st.set_page_config(
    page_title=STREAMLIT_CONFIG['page_title'],
    page_icon=STREAMLIT_CONFIG['page_icon'],
    layout=STREAMLIT_CONFIG['layout'],
    initial_sidebar_state=STREAMLIT_CONFIG['initial_sidebar_state']
)

# 세션 상태 초기화
if 'selected_model_id' not in st.session_state:
    st.session_state.selected_model_id = None
if 'comparison_models' not in st.session_state:
    st.session_state.comparison_models = []

# 분석기 인스턴스 생성
analyzer = PriceAnalyzer()
public_crawler = PublicDataCrawler()

# 사이드바 설정
def setup_sidebar():
    """사이드바 구성"""
    st.sidebar.header("🔍 검색 필터")
    
    # 제조사 선택
    manufacturer = st.sidebar.selectbox(
        "제조사 선택",
        options=['전체'] + CAR_MANUFACTURERS,
        index=0
    )
    
    # 모델 선택
    model_options = ['전체']
    if manufacturer != '전체' and manufacturer in POPULAR_MODELS:
        model_options.extend(POPULAR_MODELS[manufacturer])
    
    model = st.sidebar.selectbox(
        "모델 선택",
        options=model_options,
        index=0
    )
    
    # 예산 설정
    st.sidebar.header("💰 예산 설정")
    budget_range = st.sidebar.slider(
        "예산 범위 (만원)",
        min_value=500,
        max_value=10000,
        value=(2000, 4000),
        step=100
    )
    
    additional_budget = st.sidebar.slider(
        "추가 예산 (만원)",
        min_value=0,
        max_value=2000,
        value=500,
        step=100
    )
    
    # 분석 옵션
    st.sidebar.header("⚙️ 분석 옵션")
    show_recall = st.sidebar.checkbox("리콜 정보 표시", value=True)
    show_prediction = st.sidebar.checkbox("가격 예측 표시", value=True)
    show_tco = st.sidebar.checkbox("총 소유비용 분석", value=False)
    
    return {
        'manufacturer': manufacturer,
        'model': model,
        'budget_range': budget_range,
        'additional_budget': additional_budget,
        'show_recall': show_recall,
        'show_prediction': show_prediction,
        'show_tco': show_tco
    }

# 메인 콘텐츠
def main():
    # 타이틀
    st.title("🚗 데이터 기반 중고차 vs 신차 가성비 분석 시스템")
    st.markdown("---")
    
    # 사이드바 설정 가져오기
    filters = setup_sidebar()
    
    # 탭 생성
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 전국 자동차 트렌드",
        "🔍 모델 상세 분석",
        "⚖️ 중고차 vs 신차 비교",
        "📈 데이터 관리"
    ])
    
    # 탭 1: 전국 자동차 트렌드
    with tab1:
        show_trends_dashboard()
        
    # 탭 2: 모델 상세 분석
    with tab2:
        show_model_analysis(filters)
        
    # 탭 3: 중고차 vs 신차 비교
    with tab3:
        show_comparison_analysis(filters)
        
    # 탭 4: 데이터 관리
    with tab4:
        show_data_management()

def show_trends_dashboard():
    """전국 자동차 트렌드 대시보드"""
    st.header("📊 전국 자동차 트렌드 대시보드")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # 메트릭 카드
    with col1:
        total_cars = db_helper.execute_query(
            "SELECT SUM(registration_count) as total FROM RegistrationStats"
        )
        st.metric("전체 등록 대수", f"{total_cars[0]['total'] if total_cars else 0:,}대")
        
    with col2:
        total_models = db_helper.execute_query(
            "SELECT COUNT(DISTINCT model_id) as total FROM CarModel"
        )
        st.metric("등록 모델 수", f"{total_models[0]['total'] if total_models else 0}종")
        
    with col3:
        avg_price = db_helper.execute_query(
            "SELECT AVG(avg_price) as avg FROM UsedCarPrice"
        )
        st.metric("중고차 평균가", f"{avg_price[0]['avg'] if avg_price else 0:,.0f}만원")
        
    with col4:
        recent_recalls = db_helper.execute_query(
            "SELECT COUNT(*) as total FROM RecallInfo WHERE recall_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)"
        )
        st.metric("최근 리콜", f"{recent_recalls[0]['total'] if recent_recalls else 0}건")
    
    st.markdown("---")
    
    # 인기 모델 순위
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏆 인기 모델 TOP 10")
        popular_df = public_crawler.get_popular_models(10)
        
        if not popular_df.empty:
            fig = px.bar(
                popular_df,
                x='total_registrations',
                y='model_name',
                orientation='h',
                color='manufacturer',
                title="모델별 등록 대수"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("데이터를 불러오는 중입니다...")
            
    with col2:
        st.subheader("📍 지역별 등록 현황")
        regional_df = public_crawler.get_regional_stats()
        
        if not regional_df.empty:
            fig = px.pie(
                regional_df,
                values='total_registrations',
                names='region',
                title="지역별 차량 등록 비율"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("데이터를 불러오는 중입니다...")
    
    # 연료별 트렌드
    st.subheader("⛽ 연료별 등록 트렌드")
    fuel_query = """
    SELECT cm.fuel_type, COUNT(*) as count, SUM(rs.registration_count) as total
    FROM RegistrationStats rs
    JOIN CarModel cm ON rs.model_id = cm.model_id
    WHERE cm.fuel_type IS NOT NULL
    GROUP BY cm.fuel_type
    """
    fuel_df = db_helper.fetch_dataframe(fuel_query)
    
    if not fuel_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(fuel_df, x='fuel_type', y='total', title="연료별 총 등록 대수")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.pie(fuel_df, values='total', names='fuel_type', title="연료별 비율")
            st.plotly_chart(fig, use_container_width=True)
    
def show_model_analysis(filters):
    """모델 상세 분석"""
    st.header("🔍 모델 상세 분석")
    
    # 모델 선택
    if filters['manufacturer'] != '전체' and filters['model'] != '전체':
        model_id = db_helper.get_car_model_id(filters['manufacturer'], filters['model'])
        
        if model_id:
            st.session_state.selected_model_id = model_id
            
            # 기본 정보 표시
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.info(f"**제조사:** {filters['manufacturer']}")
            with col2:
                st.info(f"**모델:** {filters['model']}")
            with col3:
                # 가성비 점수 계산
                scores = analyzer.calculate_value_score(model_id)
                st.success(f"**가성비 점수:** {scores['total_score']}점")
            
            # 점수 상세
            st.subheader("📊 평가 점수 상세")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("가격 점수", f"{scores['price_score']:.1f}점")
            with col2:
                st.metric("신뢰도 점수", f"{scores['reliability_score']:.1f}점")
            with col3:
                st.metric("인기도 점수", f"{scores['popularity_score']:.1f}점")
            
            # 가격 정보
            st.subheader("💰 가격 정보")
            price_data = db_helper.get_latest_prices_comparison(model_id)
            
            col1, col2 = st.columns(2)
            with col1:
                st.info("**중고차 가격**")
                if price_data['used_prices']:
                    st.metric("평균가", f"{price_data['used_prices'].get('used_avg_price', 0):,.0f}만원")
                    st.caption(f"최저: {price_data['used_prices'].get('used_min_price', 0):,.0f}만원")
                    st.caption(f"최고: {price_data['used_prices'].get('used_max_price', 0):,.0f}만원")
                else:
                    st.warning("가격 정보 없음")
                    
            with col2:
                st.info("**신차 가격**")
                if price_data['new_prices']:
                    st.metric("평균가", f"{price_data['new_prices'].get('new_avg_price', 0):,.0f}만원")
                    st.caption(f"최저: {price_data['new_prices'].get('new_min_price', 0):,.0f}만원")
                else:
                    st.warning("가격 정보 없음")
            
            # 리콜 정보
            if filters['show_recall']:
                st.subheader("⚠️ 리콜 정보")
                recall_df = db_helper.get_recall_info(model_id)
                
                if not recall_df.empty:
                    # 심각도별 색상 매핑
                    severity_colors = {
                        '경미': '🟢',
                        '보통': '🟡',
                        '심각': '🟠',
                        '매우심각': '🔴'
                    }
                    
                    for _, recall in recall_df.iterrows():
                        with st.expander(
                            f"{severity_colors.get(recall['severity_level'], '⚫')} "
                            f"{recall['recall_title']} ({recall['recall_date']})"
                        ):
                            st.write(f"**사유:** {recall['recall_reason']}")
                            st.write(f"**영향 대수:** {recall['affected_units']:,}대")
                            st.write(f"**심각도:** {recall['severity_level']}")
                else:
                    st.success("리콜 이력 없음")
            
            # 가격 예측
            if filters['show_prediction']:
                st.subheader("📈 미래 가격 예측")
                predictions = analyzer.predict_future_price(model_id, 5)
                
                if not predictions.empty:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=predictions['year'],
                        y=predictions['predicted_price'],
                        mode='lines+markers',
                        name='예상 가격',
                        line=dict(color='blue', width=2)
                    ))
                    fig.update_layout(
                        title="향후 5년간 예상 가격 변동",
                        xaxis_title="년 후",
                        yaxis_title="예상 가격 (만원)",
                        height=400
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
            # TCO 분석
            if filters['show_tco']:
                st.subheader("💸 총 소유비용 분석 (5년)")
                tco = analyzer.calculate_total_cost_of_ownership(model_id, 5)
                
                if tco:
                    # TCO 시각화
                    tco_df = pd.DataFrame(list(tco.items()), columns=['항목', '비용'])
                    tco_df = tco_df[tco_df['항목'] != '총소유비용']
                    
                    fig = px.bar(
                        tco_df,
                        x='항목',
                        y='비용',
                        title=f"5년 총 소유비용: {tco['총소유비용']:,.0f}만원",
                        color='비용',
                        color_continuous_scale=['red', 'yellow', 'green']
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
        else:
            st.warning("선택한 모델의 데이터가 없습니다.")
    else:
        st.info("왼쪽 사이드바에서 제조사와 모델을 선택해주세요.")

def show_comparison_analysis(filters):
    """중고차 vs 신차 비교 분석"""
    st.header("⚖️ 중고차 vs 신차 비교 분석")
    
    if filters['manufacturer'] != '전체' and filters['model'] != '전체':
        model_id = db_helper.get_car_model_id(filters['manufacturer'], filters['model'])
        
        if model_id:
            # 현재 중고차 가격 조회
            price_data = db_helper.get_latest_prices_comparison(model_id)
            
            if price_data['used_prices']:
                used_avg_price = price_data['used_prices'].get('used_avg_price', 0)
                
                st.subheader("💰 예산 설정")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("선택한 중고차 평균가", f"{used_avg_price:,.0f}만원")
                with col2:
                    st.metric("추가 예산", f"{filters['additional_budget']:,.0f}만원")
                with col3:
                    total_budget = used_avg_price + filters['additional_budget']
                    st.metric("총 예산", f"{total_budget:,.0f}만원", 
                            delta=f"+{filters['additional_budget']:,.0f}")
                
                st.markdown("---")
                
                # 대안 신차 찾기
                st.subheader("🚙 구매 가능한 신차 옵션")
                alternatives = analyzer.find_alternative_new_cars(used_avg_price, filters['additional_budget'])
                
                if not alternatives.empty:
                    # 비교 테이블
                    for idx, car in alternatives.iterrows():
                        with st.expander(
                            f"{car['manufacturer']} {car['model_name']} - "
                            f"{car['trim_name']} ({car['base_price']:,.0f}만원)"
                        ):
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.write(f"**세그먼트:** {car['segment']}")
                                st.write(f"**기본가:** {car['base_price']:,.0f}만원")
                                if car['promotion_discount'] > 0:
                                    st.write(f"**할인:** -{car['promotion_discount']:,.0f}만원")
                                    
                            with col2:
                                st.write(f"**가성비 점수:** {car.get('value_score', 0):.1f}점")
                                st.write(f"**총 가격:** {car['total_price']:,.0f}만원")
                                
                            # 비교 버튼
                            if st.button(f"상세 비교", key=f"compare_{idx}"):
                                st.session_state.comparison_models.append(car['model_name'])
                    
                    # 최종 비교표
                    st.subheader("📊 최종 비교")
                    
                    comparison_data = {
                        '구분': ['선택한 중고차', '추천 신차 (최상위)'],
                        '모델': [f"{filters['manufacturer']} {filters['model']}", 
                                alternatives.iloc[0]['model_name'] if not alternatives.empty else '-'],
                        '가격': [f"{used_avg_price:,.0f}만원",
                                f"{alternatives.iloc[0]['base_price']:,.0f}만원" if not alternatives.empty else '-'],
                        '보증': ['제한적/없음', '3년/6만km'],
                        '최신기술': ['기본', '최신'],
                        '추천도': ['⭐⭐⭐', '⭐⭐⭐⭐']
                    }
                    
                    comparison_df = pd.DataFrame(comparison_data)
                    st.table(comparison_df)
                    
                    # 추천 의견
                    st.info(
                        f"💡 **분석 의견**: "
                        f"추가 {filters['additional_budget']}만원의 예산이 있다면, "
                        f"신차 구매도 고려해보시는 것을 추천드립니다. "
                        f"신차는 최신 기술과 보증 혜택을 제공합니다."
                    )
                else:
                    st.warning("해당 예산으로 구매 가능한 신차가 없습니다.")
            else:
                st.warning("중고차 가격 정보가 없습니다.")
    else:
        st.info("왼쪽 사이드바에서 제조사와 모델을 선택해주세요.")

def show_data_management():
    """데이터 관리 페이지"""
    st.header("📈 데이터 관리")
    
    st.subheader("🔄 데이터 업데이트")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📊 공공데이터 업데이트", use_container_width=True):
            with st.spinner("데이터 업데이트 중..."):
                # 여기에 실제 크롤링 코드 연결
                st.success("공공데이터 업데이트 완료!")
                
    with col2:
        if st.button("🚗 중고차 가격 업데이트", use_container_width=True):
            with st.spinner("가격 정보 수집 중..."):
                # 여기에 실제 크롤링 코드 연결
                st.success("중고차 가격 업데이트 완료!")
                
    with col3:
        if st.button("⚠️ 리콜 정보 업데이트", use_container_width=True):
            with st.spinner("리콜 정보 수집 중..."):
                # 여기에 실제 크롤링 코드 연결
                st.success("리콜 정보 업데이트 완료!")
    
    st.markdown("---")
    
    # 크롤링 로그
    st.subheader("📝 크롤링 로그")
    
    log_query = """
    SELECT source, status, records_collected, error_message, started_at
    FROM CrawlingLog
    ORDER BY started_at DESC
    LIMIT 10
    """
    
    log_df = db_helper.fetch_dataframe(log_query)
    
    if not log_df.empty:
        st.dataframe(log_df, use_container_width=True)
    else:
        st.info("크롤링 로그가 없습니다.")
    
    st.markdown("---")
    
    # 데이터 통계
    st.subheader("📊 데이터 통계")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        model_count = db_helper.execute_query("SELECT COUNT(*) as cnt FROM CarModel")[0]['cnt']
        st.metric("등록 모델", f"{model_count}개")
        
    with col2:
        price_count = db_helper.execute_query("SELECT COUNT(*) as cnt FROM UsedCarPrice")[0]['cnt']
        st.metric("중고차 가격 데이터", f"{price_count}건")
        
    with col3:
        recall_count = db_helper.execute_query("SELECT COUNT(*) as cnt FROM RecallInfo")[0]['cnt']
        st.metric("리콜 정보", f"{recall_count}건")
        
    with col4:
        reg_count = db_helper.execute_query("SELECT COUNT(*) as cnt FROM RegistrationStats")[0]['cnt']
        st.metric("등록 통계", f"{reg_count}건")

# 메인 실행
if __name__ == "__main__":
    main()
