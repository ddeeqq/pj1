"""
프로젝트 초기 설치 및 설정 스크립트
"""
import os
import sys
import subprocess
import json
import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProjectSetup:
    def __init__(self):
        self.project_root = os.path.dirname(os.path.abspath(__file__))
        
    def check_python_version(self):
        """Python 버전 확인"""
        logger.info("🐍 Python 버전 확인 중...")
        
        version_info = sys.version_info
        if version_info.major != 3 or version_info.minor < 8:
            logger.error(f"❌ Python 3.8 이상이 필요합니다. 현재: {version_info.major}.{version_info.minor}")
            return False
        
        logger.info(f"✅ Python {version_info.major}.{version_info.minor}.{version_info.micro}")
        return True
    
    def install_requirements(self):
        """필요한 패키지 설치"""
        logger.info("📦 필요한 패키지를 설치합니다...")
        
        try:
            requirements_file = os.path.join(self.project_root, 'requirements.txt')
            
            # pip 업그레이드
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'pip'])
            
            # requirements 설치
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', requirements_file])
            
            # 추가 패키지 설치 (psutil for system monitoring)
            additional_packages = ['psutil']
            for package in additional_packages:
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
            
            logger.info("✅ 패키지 설치 완료")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ 패키지 설치 실패: {e}")
            return False
    
    def create_directories(self):
        """필요한 디렉토리 생성"""
        logger.info("📁 디렉토리 구조를 생성합니다...")
        
        directories = [
            'logs',
            'data',
            'data/backup',
            'data/cache',
            'config'
        ]
        
        for directory in directories:
            full_path = os.path.join(self.project_root, directory)
            os.makedirs(full_path, exist_ok=True)
            logger.info(f"✅ {directory}")
        
        return True
    
    def setup_mysql_database(self):
        """MySQL 데이터베이스 설정"""
        logger.info("🗄️ MySQL 데이터베이스 설정을 시작합니다...")
        
        # 사용자로부터 MySQL 접속 정보 받기
        print("\n" + "="*50)
        print("MySQL 데이터베이스 설정")
        print("="*50)
        
        host = input("MySQL 호스트 (기본: localhost): ").strip() or "localhost"
        port = input("MySQL 포트 (기본: 3306): ").strip() or "3306"
        user = input("MySQL 사용자명 (기본: root): ").strip() or "root"
        password = input("MySQL 비밀번호: ").strip()
        database = input("데이터베이스명 (기본: car_analysis_db): ").strip() or "car_analysis_db"
        
        # 연결 테스트
        try:
            logger.info("🔍 MySQL 연결을 테스트합니다...")
            
            # 데이터베이스 없이 연결
            config = {
                'host': host,
                'port': int(port),
                'user': user,
                'password': password,
                'charset': 'utf8mb4'
            }
            
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor()
            
            # 데이터베이스 생성
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            logger.info(f"✅ 데이터베이스 '{database}' 생성/확인 완료")
            
            cursor.close()
            connection.close()
            
            # config.py 파일 업데이트
            config_file = os.path.join(self.project_root, 'config', 'config.py')
            self.update_database_config(config_file, {
                'host': host,
                'user': user,
                'password': password,
                'database': database
            })
            
            logger.info("✅ 데이터베이스 설정 완료")
            return True
            
        except Error as e:
            logger.error(f"❌ MySQL 연결 실패: {e}")
            logger.info("MySQL 서버가 실행 중인지 확인하고, 접속 정보를 다시 확인해주세요.")
            return False
    
    def update_database_config(self, config_file, db_config):
        """config.py 파일의 데이터베이스 설정 업데이트"""
        try:
            # 기존 파일 읽기
            with open(config_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # DATABASE_CONFIG 부분 교체
            new_config = f"""DATABASE_CONFIG = {{
    'host': '{db_config['host']}',
    'user': '{db_config['user']}',
    'password': '{db_config['password']}',
    'database': '{db_config['database']}',
    'charset': 'utf8mb4'
}}"""
            
            # 기존 설정 교체
            import re
            pattern = r"DATABASE_CONFIG\s*=\s*\{[^}]+\}"
            content = re.sub(pattern, new_config, content, flags=re.DOTALL)
            
            # 파일 저장
            with open(config_file, 'w', encoding='utf-8') as f:
                f.write(content)
                
            logger.info("✅ 데이터베이스 설정 파일 업데이트 완료")
            
        except Exception as e:
            logger.error(f"설정 파일 업데이트 실패: {e}")
    
    def initialize_database_schema(self):
        """데이터베이스 스키마 초기화"""
        logger.info("🏗️ 데이터베이스 스키마를 초기화합니다...")
        
        try:
            # database_schema.py 실행
            schema_file = os.path.join(self.project_root, 'database', 'database_schema.py')
            subprocess.check_call([sys.executable, schema_file])
            
            logger.info("✅ 데이터베이스 스키마 초기화 완료")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ 스키마 초기화 실패: {e}")
            return False
    
    def create_sample_data(self):
        """샘플 데이터 생성"""
        response = input("\n📊 샘플 데이터를 생성하시겠습니까? (y/n): ").strip().lower()
        
        if response == 'y':
            logger.info("📊 샘플 데이터를 생성합니다...")
            
            try:
                init_data_file = os.path.join(self.project_root, 'init_data.py')
                
                # DataInitializer를 import하고 실행
                sys.path.append(self.project_root)
                from init_data import DataInitializer
                
                initializer = DataInitializer()
                initializer.initialize_all()
                
                logger.info("✅ 샘플 데이터 생성 완료")
                return True
                
            except Exception as e:
                logger.error(f"❌ 샘플 데이터 생성 실패: {e}")
                return False
        else:
            logger.info("샘플 데이터 생성을 건너뜁니다.")
            return True
    
    def create_desktop_shortcuts(self):
        """바탕화면 바로가기 생성 (Windows)"""
        if os.name != 'nt':
            return True
            
        try:
            import winshell
            from win32com.client import Dispatch
            
            desktop = winshell.desktop()
            
            # Streamlit 앱 바로가기
            shortcut_path = os.path.join(desktop, "중고차 분석 시스템.lnk")
            shell = Dispatch('WScript.Shell')
            shortcut = shell.CreateShortcut(shortcut_path)
            shortcut.Targetpath = os.path.join(self.project_root, "start.bat")
            shortcut.WorkingDirectory = self.project_root
            shortcut.IconLocation = shortcut.Targetpath
            shortcut.save()
            
            logger.info("✅ 바탕화면 바로가기 생성 완료")
            return True
            
        except ImportError:
            logger.info("바탕화면 바로가기 생성을 건너뜁니다 (pywin32 필요)")
            return True
        except Exception as e:
            logger.warning(f"바탕화면 바로가기 생성 실패: {e}")
            return True
    
    def run_final_test(self):
        """최종 테스트 실행"""
        logger.info("🧪 최종 시스템 테스트를 실행합니다...")
        
        try:
            # run.py test 실행
            test_file = os.path.join(self.project_root, 'run.py')
            result = subprocess.run([sys.executable, test_file, 'test'], 
                                  capture_output=True, text=True, encoding='utf-8')
            
            if result.returncode == 0:
                logger.info("✅ 시스템 테스트 통과")
                return True
            else:
                logger.error(f"❌ 시스템 테스트 실패: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 테스트 실행 실패: {e}")
            return False
    
    def run_setup(self):
        """전체 설치 프로세스 실행"""
        print("🚗 중고차 vs 신차 가성비 분석 시스템 설치")
        print("=" * 50)
        
        steps = [
            ("Python 버전 확인", self.check_python_version),
            ("필요한 패키지 설치", self.install_requirements),
            ("디렉토리 생성", self.create_directories),
            ("MySQL 데이터베이스 설정", self.setup_mysql_database),
            ("데이터베이스 스키마 초기화", self.initialize_database_schema),
            ("샘플 데이터 생성", self.create_sample_data),
            ("바탕화면 바로가기 생성", self.create_desktop_shortcuts),
            ("최종 테스트", self.run_final_test),
        ]
        
        success_count = 0
        
        for step_name, step_func in steps:
            print(f"\n{'='*20} {step_name} {'='*20}")
            
            try:
                if step_func():
                    success_count += 1
                else:
                    logger.error(f"❌ {step_name} 실패")
            except Exception as e:
                logger.error(f"❌ {step_name} 오류: {e}")
        
        print("\n" + "="*50)
        print("설치 결과")
        print("="*50)
        print(f"완료된 단계: {success_count}/{len(steps)}")
        
        if success_count == len(steps):
            print("🎉 모든 설치가 완료되었습니다!")
            print("\n다음 명령어로 시스템을 시작할 수 있습니다:")
            print("  - 웹앱 실행: python run.py run")
            print("  - 스케줄러 실행: python scheduler_enhanced.py")
            print("  - 배치파일 실행: start.bat")
        else:
            print("⚠️ 일부 단계가 실패했습니다. 위의 오류 메시지를 확인해주세요.")
        
        input("\n계속하려면 Enter를 누르세요...")

if __name__ == "__main__":
    setup = ProjectSetup()
    setup.run_setup()
