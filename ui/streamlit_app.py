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
# from crawlers.public_data_crawler import PublicDataCrawler
# from crawlers.encar_crawler import EncarCrawler
# from crawlers.recall_crawler import RecallCrawler

# 페이지 설정
st.set_page_config(
    page_title=STREAMLIT_CONFIG.get('page_title', '차량 분석 시스템'),
    page_icon=STREAMLIT_CONFIG.get('page_icon', '🚗'),
    layout=STREAMLIT_CONFIG.get('layout', 'wide'),
    initial_sidebar_state=STREAMLIT_CONFIG.get('initial_sidebar_state', 'expanded')
)

# --- 캐싱된 유틸리티 함수 ---
@st.cache_resource
def get_analyzer():
    """가격 분석기 인스턴스 캐싱"""
    return PriceAnalyzer()

@st.cache_resource
def get_db_connection():
    """DB 연결 재사용"""
    return db_helper

@st.cache_data(ttl=3600)  # 1시간 캐시
def get_popular_models_data(top_n=10):
    """캐싱된 인기 모델 데이터 조회"""
    query = """
    SELECT cm.manufacturer, cm.model_name, SUM(rs.registration_count) as total_registrations
    FROM RegistrationStats rs JOIN CarModel cm ON rs.model_id = cm.model_id
    WHERE rs.registration_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    GROUP BY cm.manufacturer, cm.model_name ORDER BY total_registrations DESC LIMIT %s
    """
    return get_db_connection().fetch_dataframe(query, [top_n])

@st.cache_data(ttl=1800)  # 30분 캐시
def get_car_model_id_cached(manufacturer, model):
    """캐싱된 차량 모델 ID 조회"""
    return get_db_connection().get_car_model_id(manufacturer, model)

@st.cache_data(ttl=1800)  # 30분 캐시
def get_latest_prices_comparison(model_id):
    """캐싱된 가격 데이터 조회"""
    return get_db_connection().get_latest_prices_comparison(model_id)

@st.cache_data(ttl=900)   # 15분 캐시
def get_crawling_logs(limit=10):
    """캐싱된 크롤링 로그 조회"""
    return get_db_connection().fetch_dataframe("SELECT * FROM CrawlingLog ORDER BY started_at DESC LIMIT %s", [limit])

# --- UI 컴포넌트 ---
def setup_sidebar():
    st.sidebar.header(" 검색 필터")
    manufacturer = st.sidebar.selectbox("제조사 선택", options=['전체'] + CAR_MANUFACTURERS)
    
    model_options = ['전체']
    if manufacturer != '전체' and manufacturer in POPULAR_MODELS:
        model_options.extend(POPULAR_MODELS[manufacturer])
    model = st.sidebar.selectbox("모델 선택", options=model_options)
    
    st.sidebar.header(" 예산 설정")
    budget = st.sidebar.slider("추가 예산 (만원)", 0, 2000, 500, 100)
    
    st.sidebar.header(" 분석 옵션")
    show_options = {
        'recall': st.sidebar.checkbox("리콜 정보 표시", True),
        'prediction': st.sidebar.checkbox("가격 예측 표시", True),
        'tco': st.sidebar.checkbox("총 소유비용 분석", False)
    }
    return {'manufacturer': manufacturer, 'model': model, 'additional_budget': budget, 'options': show_options}

def show_data_management():
    st.header(" 데이터 관리")
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

    st.subheader(" 데이터 업데이트 실행")
    col1, col2, col3 = st.columns(3)
    with col1:
        # if st.button(" 공공데이터 업데이트", use_container_width=True):
        #     with st.spinner("엑셀 파일 처리 및 DB 저장 중..."):
        #         def task():
        #             crawler = PublicDataCrawler(config.get('public_data', {}))
        #             df = crawler.load_registration_data()
        #             if not df.empty:
        #                 crawler.save_to_database(df)
        #         run_task(task, "공공데이터 업데이트 완료!", "공공데이터 처리 실패")
        st.info(" 공공데이터 크롤링 기능 (일시 비활성화)")

    with col2:
        # if st.button(" 중고차 가격 업데이트", use_container_width=True):
        #     with st.spinner("인기 모델의 중고차 가격 수집 중..."):
        #         def task():
        #             crawler = EncarCrawler(config.get('encar', {}))
        #             car_list = [{'manufacturer': m, 'model_name': models[0]} for m, models in POPULAR_MODELS.items()]
        #             crawler.crawl_and_save(car_list)
        #         run_task(task, "중고차 가격 업데이트 완료!", "중고차 가격 수집 실패")
        st.info(" 중고차 가격 크롤링 기능 (일시 비활성화)")

    with col3:
        # if st.button(" 리콜 정보 업데이트", use_container_width=True):
        #     with st.spinner("전체 모델 리콜 정보 수집 중..."):
        #         def task():
        #             crawler = RecallCrawler(config.get('recall', {}))
        #             models_df = db_helper.get_car_models()
        #             car_list = models_df.to_dict('records')
        #             crawler.crawl_and_save(car_list)
        #         run_task(task, "리콜 정보 업데이트 완료!", "리콜 정보 수집 실패")
        st.info(" 리콜 정보 크롤링 기능 (일시 비활성화)")

    st.markdown("---")
    st.subheader(" 최근 크롤링 로그")
    try:
        with st.spinner("로그 데이터 로딩 중..."):
            log_df = get_crawling_logs(10)
        
        if not log_df.empty:
            st.dataframe(log_df, use_container_width=True, height=300)
        else:
            st.info("크롤링 로그가 없습니다.")
    except Exception as e:
        st.error(f"로그를 불러오는 데 실패했습니다: {e}")

    # 캐시 관리 섹션
    st.markdown("---")
    st.subheader(" 캐시 관리")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("캐시 초기화", type="secondary"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.success("모든 캐시가 초기화되었습니다.")
    
    with col2:
        cache_info = {
            "인기 모델 데이터": "1시간 캐시",
            "모델 ID 조회": "30분 캐시", 
            "가격 비교 데이터": "30분 캐시",
            "크롤링 로그": "15분 캐시"
        }
        st.json(cache_info)

# --- 메인 애플리케이션 ---
def main():
    st.title(STREAMLIT_CONFIG.get('page_title', '차량 분석 시스템'))
    filters = setup_sidebar()
    
    tab1, tab2, tab3 = st.tabs([" 전국 자동차 트렌드", "🔍 모델 상세 분석", "📈 데이터 관리"])
    
    with tab1:
        st.header(" 전국 자동차 트렌드 대시보드")
        
        with st.spinner("데이터 로딩 중..."):
            popular_df = get_popular_models_data(10)
        
        if not popular_df.empty:
            col1, col2 = st.columns([2, 1])
            with col1:
                fig = px.bar(
                    popular_df, 
                    x='total_registrations', 
                    y='model_name', 
                    orientation='h', 
                    color='manufacturer',
                    title="최근 30일 인기 모델 등록 현황"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader(" 상위 5개 모델")
                for idx, row in popular_df.head().iterrows():
                    st.metric(
                        f"{row['manufacturer']} {row['model_name']}",
                        f"{row['total_registrations']:,}대"
                    )
        else:
            st.info("인기 모델 데이터가 없습니다.")

    with tab2:
        st.header(" 모델 상세 분석")
        if filters['manufacturer'] != '전체' and filters['model'] != '전체':
            with st.spinner("모델 분석 중..."):
                model_id = get_car_model_id_cached(filters['manufacturer'], filters['model'])
                
            if model_id:
                analyzer = get_analyzer()
                
                # 성능 최적화된 분석
                with st.spinner("가성비 분석 중..."):
                    scores = analyzer.calculate_value_score(model_id)
                    
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("종합 가성비 점수", f"{scores.get('total_score', 0):.1f}점")
                with col2:
                    st.metric("가격 경쟁력", f"{scores.get('price_score', 0):.1f}점")
                with col3:
                    st.metric("시장 인기도", f"{scores.get('popularity_score', 0):.1f}점")
                
                # 가격 비교 데이터 표시
                if filters['options']['prediction']:
                    with st.spinner("가격 데이터 로딩 중..."):
                        price_data = get_latest_prices_comparison(model_id)
                        if price_data:
                            st.subheader(" 가격 비교 분석")
                            st.dataframe(price_data, use_container_width=True)
            else:
                st.warning("선택한 모델의 데이터가 없습니다.")
        else:
            st.info("사이드바에서 제조사와 모델을 선택해주세요.")

    with tab3:
        show_data_management()

if __name__ == "__main__":
    main()
