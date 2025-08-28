"""
리콜 데이터 분석 및 리포트 생성
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawlers.recall_crawler import RecallCrawler
from database.db_helper import db_helper
import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class RecallAnalyzer:
    def __init__(self):
        self.crawler = RecallCrawler()
        self.db = db_helper

    def generate_monthly_report(self, manufacturer=None):
        """월간 리콜 현황 리포트 생성"""

        # 최근 30일 리콜 통계 조회
        stats_df = self.db.get_recall_statistics(manufacturer=manufacturer, days=30)

        if stats_df.empty:
            print("리콜 데이터가 없습니다.")
            return

        print("=== 월간 리콜 현황 리포트 ===")
        print(f"분석 기간: {datetime.now() - timedelta(days=30)} ~ {datetime.now()}")

        if manufacturer:
            print(f"대상 제조사: {manufacturer}")

        print(f"\n📊 전체 통계:")
        print(f"- 총 리콜 건수: {stats_df['total_recalls'].sum()}")
        print(f"- 매우심각: {stats_df['critical_recalls'].sum()}")
        print(f"- 심각: {stats_df['severe_recalls'].sum()}")
        print(f"- 보통: {stats_df['moderate_recalls'].sum()}")
        print(f"- 경미: {stats_df['minor_recalls'].sum()}")
        print(f"- 총 영향 대수: {stats_df['total_affected_units'].sum():,}대")

        print(f"\n🏆 제조사별 리콜 순위:")
        top_manufacturers = stats_df.groupby('manufacturer')['total_recalls'].sum().sort_values(ascending=False).head(5)
        for i, (mfr, count) in enumerate(top_manufacturers.items(), 1):
            print(f"{i}. {mfr}: {count}건")

        print(f"\n🚨 모델별 심각한 리콜:")
        critical_models = stats_df[stats_df['critical_recalls'] > 0].sort_values('critical_recalls', ascending=False)
        for _, row in critical_models.head(5).iterrows():
            print(f"- {row['manufacturer']} {row['model_name']}: {row['critical_recalls']}건")

        return stats_df

    def check_my_car_recalls(self, car_number):
        """내 차량 리콜 대상 확인"""

        print(f"🚗 차량번호 {car_number} 리콜 확인 중...")

        recall_results = self.crawler.search_recall_by_car_number(car_number)

        if not recall_results:
            print("✅ 현재 리콜 대상이 아닙니다.")
            return

        print(f"⚠️  총 {len(recall_results)}건의 리콜이 있습니다:")

        for i, recall in enumerate(recall_results, 1):
            print(f"\n{i}. {recall['manufacturer']} {recall['model_name']}")
            print(f"   리콜 사유: {recall['recall_reason']}")
            print(f"   조치 상태: {recall['recall_status']}")
            print(f"   시정 기간: {recall['correction_period']}")

        # 결과를 DB에 저장
        self.db.insert_car_recall_check(car_number, recall_results)

        return recall_results

    def monitor_critical_recalls(self):
        """심각한 리콜 모니터링"""

        print("🔍 심각한 리콜 모니터링 중...")

        # 최근 7일간 매우심각/심각 등급 리콜 검색
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=7)

        recall_data = self.crawler.search_recall_info(
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            max_pages=5
        )

        critical_recalls = [r for r in recall_data if r['severity_level'] in ['매우심각', '심각']]

        if not critical_recalls:
            print("✅ 최근 7일간 심각한 리콜이 없습니다.")
            return

        print(f"⚠️  최근 7일간 {len(critical_recalls)}건의 심각한 리콜이 발생했습니다:")

        for recall in critical_recalls:
            print(f"\n📢 [{recall['severity_level']}] {recall['manufacturer']} {recall['model_name']}")
            print(f"   리콜 사유: {recall['recall_reason']}")
            print(f"   영향 대수: {recall['affected_units']:,}대")
            print(f"   발생일: {recall['recall_date']}")

        return critical_recalls

    def export_recall_data(self, manufacturer=None, days=90, filename=None):
        """리콜 데이터 엑셀 파일로 내보내기"""

        # 리콜 데이터 조회
        query = """
        SELECT 
            cm.manufacturer, cm.model_name,
            ri.recall_date, ri.recall_title, ri.recall_reason,
            ri.severity_level, ri.affected_units, ri.correction_rate,
            ri.defect_content, ri.correction_method
        FROM recall_info ri
        JOIN car_models cm ON ri.model_id = cm.id
        WHERE ri.recall_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
        """

        params = [days]
        if manufacturer:
            query += " AND cm.manufacturer = %s"
            params.append(manufacturer)

        query += " ORDER BY ri.recall_date DESC"

        result = self.db.execute_query(query, tuple(params))

        if not result:
            print("내보낼 데이터가 없습니다.")
            return

        # DataFrame 생성
        columns = ['제조사', '모델명', '리콜일', '리콜제목', '리콜사유', 
                  '심각도', '영향대수', '시정률', '결함내용', '시정방법']
        df = pd.DataFrame(result, columns=columns)

        # 파일명 생성
        if not filename:
            suffix = f"_{manufacturer}" if manufacturer else ""
            filename = f"recall_data_{datetime.now().strftime('%Y%m%d')}{suffix}.xlsx"

        # 엑셀 파일로 저장
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='리콜목록', index=False)

                # 통계 시트 추가
                stats_df = self.db.get_recall_statistics(manufacturer=manufacturer, days=days)
                if not stats_df.empty:
                    stats_df.to_excel(writer, sheet_name='통계', index=False)

            print(f"📁 리콜 데이터가 '{filename}'으로 저장되었습니다.")
        except Exception as e:
            print(f"❌ 파일 저장 실패: {e}")
            
        return filename

    def get_recall_summary_by_manufacturer(self, days=365):
        """제조사별 리콜 요약 통계"""
        try:
            query = """
            SELECT 
                cm.manufacturer,
                COUNT(*) as total_recalls,
                SUM(CASE WHEN ri.severity_level = '매우심각' THEN 1 ELSE 0 END) as critical_recalls,
                SUM(CASE WHEN ri.severity_level = '심각' THEN 1 ELSE 0 END) as severe_recalls,
                SUM(ri.affected_units) as total_affected_units,
                AVG(ri.correction_rate) as avg_correction_rate
            FROM recall_info ri
            JOIN CarModel cm ON ri.model_id = cm.model_id
            WHERE ri.recall_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            GROUP BY cm.manufacturer
            ORDER BY total_recalls DESC
            """
            
            result = self.db.execute_query(query, (days,))
            
            if result:
                columns = ['manufacturer', 'total_recalls', 'critical_recalls', 
                          'severe_recalls', 'total_affected_units', 'avg_correction_rate']
                return pd.DataFrame(result, columns=columns)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"제조사별 리콜 요약 조회 실패: {e}")
            return pd.DataFrame()

    def get_recent_critical_recalls(self, days=30):
        """최근 심각한 리콜 목록 조회"""
        try:
            query = """
            SELECT 
                cm.manufacturer, cm.model_name,
                ri.recall_date, ri.recall_title, ri.recall_reason,
                ri.severity_level, ri.affected_units
            FROM recall_info ri
            JOIN CarModel cm ON ri.model_id = cm.model_id
            WHERE ri.recall_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
                AND ri.severity_level IN ('매우심각', '심각')
            ORDER BY ri.recall_date DESC, ri.affected_units DESC
            """
            
            result = self.db.execute_query(query, (days,))
            
            if result:
                columns = ['manufacturer', 'model_name', 'recall_date', 'recall_title', 
                          'recall_reason', 'severity_level', 'affected_units']
                return pd.DataFrame(result, columns=columns)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"최근 심각한 리콜 조회 실패: {e}")
            return pd.DataFrame()


if __name__ == "__main__":
    print("RecallAnalyzer 모듈")
    print("이 모듈은 다른 모듈에서 import하여 사용하거나 Streamlit UI를 통해 접근하세요.")
    print("예시: python run.py run (웹 UI 실행)")