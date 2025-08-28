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
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import STREAMLIT_CONFIG, CAR_MANUFACTURERS, POPULAR_MODELS
from database.db_helper import db_helper
from analyzers.price_analyzer import PriceAnalyzer
from crawlers.public_data_crawler import PublicDataCrawler
from crawlers.encar_crawler import EncarCrawler
from crawlers.recall_crawler import RecallCrawler

# 페이지 설정
st.set_page_config(
    page_title=STREAMLIT_CONFIG.get('page_title', '차량 분석 시스템'),
    page_icon=STREAMLIT_CONFIG.get('page_icon', '🚗'),
    layout=STREAMLIT_CONFIG.get('layout', 'wide'),
    initial_sidebar_state=STREAMLIT_CONFIG.get('initial_sidebar_state', 'expanded')
)

# --- 유틸리티 함수 ---
@st.cache_resource
def get_analyzer():
    return PriceAnalyzer()

@st.cache_data
def get_popular_models_data(top_n=10):
    # 이 함수는 PublicDataCrawler의 인스턴스를 필요로 합니다.
    # 지금은 db_helper를 직접 사용하도록 수정하거나, PublicDataCrawler를 인스턴스화해야 합니다.
    # 여기서는 db_helper를 직접 사용하는 것으로 가정합니다.
    query = """
    SELECT cm.manufacturer, cm.model_name, SUM(rs.registration_count) as total_registrations
    FROM RegistrationStats rs JOIN CarModel cm ON rs.model_id = cm.model_id
    WHERE rs.registration_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    GROUP BY cm.manufacturer, cm.model_name ORDER BY total_registrations DESC LIMIT %s
    """
    return db_helper.fetch_dataframe(query, [top_n])

# --- UI 컴포넌트 ---
def setup_sidebar():
    st.sidebar.header("🔍 검색 필터")
    manufacturer = st.sidebar.selectbox("제조사 선택", options=['전체'] + CAR_MANUFACTURERS)
    
    model_options = ['전체']
    if manufacturer != '전체' and manufacturer in POPULAR_MODELS:
        model_options.extend(POPULAR_MODELS[manufacturer])
    model = st.sidebar.selectbox("모델 선택", options=model_options)
    
    st.sidebar.header("💰 예산 설정")
    budget = st.sidebar.slider("추가 예산 (만원)", 0, 2000, 500, 100)
    
    st.sidebar.header("⚙️ 분석 옵션")
    show_options = {
        'recall': st.sidebar.checkbox("리콜 정보 표시", True),
        'prediction': st.sidebar.checkbox("가격 예측 표시", True),
        'tco': st.sidebar.checkbox("총 소유비용 분석", False)
    }
    return {'manufacturer': manufacturer, 'model': model, 'additional_budget': budget, 'options': show_options}

def show_data_management():
    st.header("📈 데이터 관리")
    st.info("데이터를 수동으로 업데이트하고 최신 상태를 확인합니다.")

    def run_task(task_function, success_message, error_message):
        try:
            task_function()
            st.success(success_message)
        except Exception as e:
            st.error(f"{error_message}: {e}")

    # 설정 로드
    try:
        with open('config/scheduler_config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)['crawling']
    except Exception as e:
        st.error(f"설정 파일을 읽는 데 실패했습니다: {e}")
        return

    st.subheader("🔄 데이터 업데이트 실행")
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("📊 공공데이터 업데이트", use_container_width=True):
            with st.spinner("엑셀 파일 처리 및 DB 저장 중..."):
                def task():
                    crawler = PublicDataCrawler(config.get('public_data', {}))
                    df = crawler.load_registration_data()
                    if not df.empty:
                        crawler.save_to_database(df)
                run_task(task, "공공데이터 업데이트 완료!", "공공데이터 처리 실패")

    with col2:
        if st.button("🚗 중고차 가격 업데이트", use_container_width=True):
            with st.spinner("인기 모델의 중고차 가격 수집 중..."):
                def task():
                    crawler = EncarCrawler(config.get('encar', {}))
                    car_list = [{'manufacturer': m, 'model_name': models[0]} for m, models in POPULAR_MODELS.items()]
                    crawler.crawl_and_save(car_list)
                run_task(task, "중고차 가격 업데이트 완료!", "중고차 가격 수집 실패")

    with col3:
        if st.button("⚠️ 리콜 정보 업데이트", use_container_width=True):
            with st.spinner("전체 모델 리콜 정보 수집 중..."):
                def task():
                    crawler = RecallCrawler(config.get('recall', {}))
                    models_df = db_helper.get_car_models()
                    car_list = models_df.to_dict('records')
                    crawler.crawl_and_save(car_list)
                run_task(task, "리콜 정보 업데이트 완료!", "리콜 정보 수집 실패")

    st.markdown("---")
    st.subheader("📝 최근 크롤링 로그")
    try:
        log_df = db_helper.fetch_dataframe("SELECT * FROM CrawlingLog ORDER BY started_at DESC LIMIT 10")
        st.dataframe(log_df, use_container_width=True, height=300)
    except Exception as e:
        st.error(f"로그를 불러오는 데 실패했습니다: {e}")

# --- 메인 애플리케이션 ---
def main():
    st.title(STREAMLIT_CONFIG.get('page_title', '차량 분석 시스템'))
    filters = setup_sidebar()
    
    tab1, tab2, tab3 = st.tabs(["📊 전국 자동차 트렌드", "🔍 모델 상세 분석", "📈 데이터 관리"])
    
    with tab1:
        st.header("📊 전국 자동차 트렌드 대시보드")
        popular_df = get_popular_models_data(10)
        if not popular_df.empty:
            fig = px.bar(popular_df, x='total_registrations', y='model_name', orientation='h', color='manufacturer')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("인기 모델 데이터가 없습니다.")

    with tab2:
        st.header("🔍 모델 상세 분석")
        if filters['manufacturer'] != '전체' and filters['model'] != '전체':
            model_id = db_helper.get_car_model_id(filters['manufacturer'], filters['model'])
            if model_id:
                analyzer = get_analyzer()
                scores = analyzer.calculate_value_score(model_id)
                st.metric("종합 가성비 점수", f"{scores.get('total_score', 0):.1f}점")
                # 여기에 더 많은 분석 결과 표시
            else:
                st.warning("선택한 모델의 데이터가 없습니다.")
        else:
            st.info("사이드바에서 제조사와 모델을 선택해주세요.")

    with tab3:
        show_data_management()

if __name__ == "__main__":
    main()
