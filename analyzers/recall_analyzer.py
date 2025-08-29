"""
리콜 데이터 분석 및 리포트 생성
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from database.db_helper import db_helper
import logging

logger = logging.getLogger(__name__)

class RecallAnalyzer:
    def export_recall_data(self, days=30):
        """리콜 데이터 내보내기"""
        query = """
        SELECT 
            cm.manufacturer, cm.model_name,
            ri.recall_date, ri.recall_title, ri.recall_reason,
            ri.severity_level, ri.affected_units
        FROM RecallInfo ri
        JOIN CarModel cm ON ri.model_id = cm.model_id
        WHERE ri.recall_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
        """
        return db_helper.fetch_dataframe(query, [days])