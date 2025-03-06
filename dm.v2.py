"""
인스타그램 자동 DM 발송 스크립트 (자세한 설명)

기술 스택:
- Selenium: 웹 브라우저 자동화 도구, Chrome 웹드라이버 사용
- Google Sheets API: 데이터 관리 및 메시지 템플릿 저장소로 활용
- Python: 전체 자동화 로직 구현 언어
- Chrome 브라우저: 사용자 프로필 기반 자동화 환경

주요 라이브러리:
- selenium: 웹 브라우저 제어 및 자동화
- googleapiclient: Google API와 상호작용
- datetime, logging: 시간 기록 및 로그 관리
- random, time: 대기 시간 랜덤화로 봇 감지 방지
- os, shutil: 파일 시스템 조작 및 캐시 데이터 관리

핵심 기능:
1. 브라우저 캐시 관리:
   - 로그인 상태 유지하며 캐시/임시 파일 정리
   - 브라우저 자동화 탐지 방지 설정

2. 데이터 관리:
   - Google Sheets에서 DM 대상 URL과 이름 가져오기 (시트 ID: 1VhEWeQASyv02knIghpcccYLgWfJCe2ylUnPsQ_-KNAI, 시트명: dm_list)
   - 메시지 템플릿 랜덤 선택 및 개인화 (시트 ID: 1mwZ37jiEGK7rQnLWp87yUQZHyM6LHb4q6mbB0A07fI0, 시트명: 협찬문의)

3. 자동화 프로세스:
   - 대상 프로필 방문 및 DM 버튼 탐색
   - 맞춤형 메시지 입력 및 발송
   - 발송 결과 및 시간 기록

4. 안전 장치:
   - 랜덤 대기 시간으로 자연스러운 사용자 행동 모방
   - 모든 액션에 예외 처리 구현

데이터 흐름:
- 입력: Google Sheets의 URL/이름 목록, 메시지 템플릿
- 처리: 템플릿에 이름 삽입하여 개인화된 메시지 생성 
- 출력: 인스타그램 DM 발송 및 Google Sheets에 결과 기록

발송 속도:
- URL 접속 후 5-10초 랜덤 대기
- DM 버튼 클릭 후 5-10초 랜덤 대기
- 메시지 입력 후 5-10초 랜덤 대기
- URL 간 이동 시 5초 고정 대기
- 평균 처리 시간: 약 25-40초/DM
- 시간당 예상 처리량: 약 90-140개 DM

보안 및 주의사항:
- 사용자 데이터 디렉토리 활용으로 로그인 정보 보존
- 실제 메시지 전송 기능은 현재 비활성화 (테스트 모드)
- 인스타그램 정책을 고려한 적절한 사용 필요
- 과도한 자동화는 계정 제한 조치를 받을 수 있음
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import random
import time
import os
import shutil
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pyperclip
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from auth import get_credentials
from googleapiclient.discovery import build
import logging
from datetime import datetime

def clear_chrome_data(user_data_dir, keep_login=True):
    default_dir = os.path.join(user_data_dir, 'Default')
    if not os.path.exists(default_dir):
        print("Default 디렉토리가 존재하지 않습니다.")
        return

    dirs_to_clear = ['Cache', 'Code Cache', 'GPUCache']
    files_to_clear = ['History', 'Visited Links', 'Web Data']
    
    for dir_name in dirs_to_clear:
        dir_path = os.path.join(default_dir, dir_name)
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
            print(f"{dir_name} 디렉토리를 삭제했습니다.")

    if not keep_login:
        files_to_clear.extend(['Cookies', 'Login Data'])

    for file_name in files_to_clear:
        file_path = os.path.join(default_dir, file_name)
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"{file_name} 파일을 삭제했습니다.")

options = Options()
options.add_argument("--start-maximized")
options.add_experimental_option("detach", True)
options.add_argument("disable-blink-features=AutomationControlled")
options.add_experimental_option("excludeSwitches", ["enable-logging"])

user_data_dir = "C:\\Users\\USER\\Desktop\\github\\insta_vendor\\user_data\\office_goyamedia_feed"
options.add_argument(f"user-data-dir={user_data_dir}")

# 캐시와 임시 파일 정리 (로그인 정보 유지)
clear_chrome_data(user_data_dir)

# 추가 옵션 설정
options.add_argument("--disable-application-cache")
options.add_argument("--disable-cache")

driver = webdriver.Chrome(options=options)

def get_data_from_sheets():
    logging.info("URL과 이름 데이터 가져오기 시작")
    try:
        creds = get_credentials()
        service = build('sheets', 'v4', credentials=creds)

        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId='1VhEWeQASyv02knIghpcccYLgWfJCe2ylUnPsQ_-KNAI',
                                    range='dm_list!A2:B').execute()
        values = result.get('values', [])

        if not values:
            logging.warning('스프레드시트에서 데이터를 찾을 수 없습니다.')
            return []

        return [(row[0], row[1] if len(row) > 1 else "") for row in values if row]  # URL과 이름(있는 경우) 반환
    except Exception as e:
        logging.error(f"스프레드시트에서 데이터를 가져오는 중 오류 발생: {e}")
        return []

def get_message_templates():
    logging.info("메시지 템플릿 가져오기 시작")
    try:
        creds = get_credentials()
        service = build('sheets', 'v4', credentials=creds)

        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId='1mwZ37jiEGK7rQnLWp87yUQZHyM6LHb4q6mbB0A07fI0',
                                    range='협찬문의!A1:A15').execute()
        values = result.get('values', [])

        if not values:
            logging.warning('메시지 템플릿을 찾을 수 없습니다.')
            return ["안녕하세요"]  # 기본 메시지

        return [row[0] for row in values if row]  # 빈 행 제외
    except Exception as e:
        logging.error(f"메시지 템플릿을 가져오는 중 오류 발생: {e}")
        return ["안녕하세요"]  # 오류 발생 시 기본 메시지 반환

def update_sheet_status(service, row, status, timestamp=None):
    sheet_id = '1VhEWeQASyv02knIghpcccYLgWfJCe2ylUnPsQ_-KNAI'
    range_name = f'dm_list!C{row}:D{row}'
    
    values = [[status, timestamp if timestamp else '']]
    body = {'values': values}
    
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=range_name,
        valueInputOption='USER_ENTERED',
        body=body
    ).execute()

def process_url(driver, url, name, message_template, row, service):
    driver.get(url)
    print(driver.title)
    wait_time = random.uniform(5, 10)
    print(f"URL 접속 후 대기: {wait_time:.2f}초")
    time.sleep(wait_time)

    try:
        message_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'x1i10hfl') and contains(text(), '메시지 보내기')]"))
        )
        print(f"버튼 텍스트: {message_button.text}")
        message_button.click()
        wait_time = random.uniform(5, 10)
        print(f"DM 버튼 클릭 후 대기: {wait_time:.2f}초")
        time.sleep(wait_time)

        message = message_template.replace("{이름}", name)
        actions = ActionChains(driver)
        actions.send_keys(message).perform()
        wait_time = random.uniform(5, 10)
        print(f"메시지 입력 후 대기: {wait_time:.2f}초")
        time.sleep(wait_time)

        # actions.send_keys(Keys.ENTER).perform()

        # 성공적으로 메시지를 보냈을 때
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        update_sheet_status(service, row, 'Y', timestamp)

    except TimeoutException:
        print("'메시지 보내기' 버튼을 찾을 수 없습니다.")
        update_sheet_status(service, row, 'failed')
    except NoSuchElementException:
        print("요소를 찾을 수 없습니다.")
        update_sheet_status(service, row, 'failed')

# 메인 실행 부분
message_templates = get_message_templates()
url_name_pairs = get_data_from_sheets()

creds = get_credentials()
service = build('sheets', 'v4', credentials=creds)

for index, (url, name) in enumerate(url_name_pairs, start=2):  # start=2 because row 1 is header
    message_template = random.choice(message_templates)
    process_url(driver, url, name, message_template, index, service)
    time.sleep(5)  # 다음 URL로 이동하기 전 5초 대기

# 브라우저를 닫지 않고 세션 유지
# driver.quit()  # 필요한 경우 주석 해제
