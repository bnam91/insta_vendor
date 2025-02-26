"""

몽고db : https://cloud.mongodb.com/v2/67b53ffbba89e066f00516de#/clusters
github : https://github.com/bnam91/insta_vendor


입력: 1000 × 200 토큰 = 200,000 토큰
→ $0.150 × (200,000/1,000,000) = $0.03

출력: 1000 × 50 토큰 = 50,000 토큰
→ $0.600 × (50,000/1,000,000) = $0.03

1000개 게시글 처리 시 예상 비용 총 : $0.06 ≈ 80원

====

인스타그램 피드 크롤러 (v7_250204)

주요 기능:
1. 인스타그램 피드 자동 스크롤 및 데이터 수집
   - 자연스러운 스크롤 동작 구현 (랜덤 스크롤 거리, 중간 멈춤)
   - 스크롤 횟수 기반 자동 휴식 (15-25회마다 15-30초 휴식)
   - 새로운 컨텐츠 로딩 확인 및 재시도 로직

2. 데이터 저장 (MongoDB)
   - MongoDB Atlas 클라우드 저장소 연동
   - 수집된 데이터를 구조화된 형식으로 저장
   - 역순 입력 지원 (최신 데이터가 상단에 위치)
   - URL 기반 중복 게시물 체크 기능

3. 날짜 기반 크롤링 제한
   - 5일 이상 된 게시물 10개 발견 시 자동 중단
   - 시간대 처리 (KST 기준)

4. 브라우저 세션 관리
   - 캐시 및 임시 파일 자동 정리
   - 로그인 세션 유지
   - 자동화 감지 방지 설정

5. 세션 로깅
   - 크롤링 세션 시작/종료 기록
   - 수집된 게시물 수 추적
   - 오류 발생 시 로그 기록

수집 데이터 항목:
- 작성시간 (cr_at)
- 작성자 (author)
- 게시물 본문 (content)
- 게시물 링크 (post_url)
- 수집일자 (crawl_date)
- 공구 관련 정보 (추후 분석용)
  * 09_feed (공구피드 여부)
  * 09_brand (브랜드)
  * 09_item (상품)
  * 09_item_category (상품 카테고리)
  * open_date (오픈예정일)
  * end_date (공구마감일)
  * processed (처리여부)

예외 처리:
- 네트워크 오류 대응
- 요소 로딩 대기
- 스크롤 실패 시 재시도
- 데이터 저장 실패 대응

사용된 기술:
- Selenium WebDriver
- BeautifulSoup
- MongoDB Atlas
- 시간/날짜 처리 (datetime)
- 파일 시스템 관리 (os, shutil)

데이터 처리 기준:
1. 게시물 중복 처리
   - 게시물 URL을 기준으로 중복 체크
   - MongoDB에서 중복 검사

2. 수집 중단 기준
   - 5일 이상 된 게시물 10개 발견 시 자동 중단
   - 10회 연속 새로운 컨텐츠 미발견 시 중단
   - 스크롤 실패 시 최대 10회 재시도 후 중단

3. 데이터 정렬 및 저장
   - 최신 게시물이 상단에 오도록 역순 저장
   - MongoDB 저장
   - 한글 인코딩 보장

4. 시간 처리
   - UTC 시간을 KST(+9)로 변환하여 저장
   - ISO 형식 날짜/시간 데이터 사용
   - 수집일자는 KST 기준으로 기록

5. 자동화된 세션 관리
   - 10회 크롤링 후 5-10분 휴식
   - 세션별 수집 현황 로깅
   - 오류 발생 시 5분 후 자동 재시도

3. MongoDB TTL 인덱스 설정
   - crawl_date 필드 기준으로 자동 삭제
   - 기본값: 7일 후 삭제 (7 * 24 * 60 * 60 초)
   - TTL 기간 변경 시:
     * expireAfterSeconds 값 수정
     * 3일 = 3 * 24 * 60 * 60
     * 5일 = 5 * 24 * 60 * 60
     * 10일 = 10 * 24 * 60 * 60
"""

# 코드 작업내용(완료)
# 시트저장하기
# 시트 역순으로 입력
# 시트 게시물링크 기준으로 중복체크
#v6_250203
# 날짜 시트에 입력하기
# api를 통해 피드 분석 후 공구 여부 및 제품 체크하기


from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import os
import shutil
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import unquote
from bs4 import BeautifulSoup
import random  # 파일 상단에 추가
from datetime import datetime, timezone, timedelta  # 파일 상단에 추가
import logging
import json  # 파일 상단에 추가
from datetime import date  # 파일 상단에 추가
from pymongo import MongoClient
from pymongo.server_api import ServerApi

#인스타 로그인 함수
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

# 절대경로에서 상대경로로 변경
#-- 성남집
user_data_dir = os.path.join(os.path.dirname(__file__), "user_data", "home_goyamedia_feed")
#-- 오피스
# user_data_dir = os.path.join(os.path.dirname(__file__), "user_data", "office_goyamedia_feed") 
options.add_argument(f"user-data-dir={user_data_dir}")

# 캐시와 임시 파일 정리 (로그인 정보 유지)
clear_chrome_data(user_data_dir)

# 추가 옵션 설정
options.add_argument("--disable-application-cache")
options.add_argument("--disable-cache")

driver = webdriver.Chrome(options=options)

# MongoDB 연결 설정 (수정된 부분)
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    # 연결 확인
    client.admin.command('ping')
    print("MongoDB 연결 성공!")
    
    # 데이터베이스와 컬렉션 선택
    db = client['insta09_database']
    collection = db['01_main_newfeed_crawl_data']
    
    # post_url에 Unique Index 생성
    collection.create_index("post_url", unique=True)
    print("post_url에 Unique Index가 생성되었습니다.")

    # TTL 인덱스 생성 (자동 삭제 설정)
    # TTL 기간 변경 시 아래 값 수정:
    # 3일 = 3 * 24 * 60 * 60
    # 5일 = 5 * 24 * 60 * 60
    # 10일 = 10 * 24 * 60 * 60
    collection.create_index(
        "crawl_date", 
        expireAfterSeconds=30 * 24 * 60 * 60  # 30일
    )
    print("TTL Index가 생성되었습니다.")

except Exception as e:
    print(f"MongoDB 연결 또는 인덱스 생성 실패: {e}")

def update_mongodb_data(values, current_date):
    try:
        # MongoDB 데이터 구성
        post_data = {
            "cr_at": values[0],
            "author": values[1],
            "content": values[2],
            "post_url": values[3],
            "crawl_date": datetime.now(timezone(timedelta(hours=9))),  # KST 기준 현재 시간을 Date 객체로 저장
            "09_feed": "",
            "09_brand": "",
            "09_item": "",
            "09_item_category": "",
            "09_item_category_2": "",
            "open_date": "",
            "end_date": "",
            "processed": False
        }
        
        # MongoDB에 데이터 저장 시도
        try:
            collection.insert_one(post_data)
            print("MongoDB에 데이터가 저장되었습니다.")
        except Exception as e:
            if "duplicate key error" in str(e):
                print("이미 MongoDB에 존재하는 게시물입니다.")
            else:
                print(f"MongoDB 저장 중 오류 발생: {str(e)}")
                return False

        # 50개 단위로 휴식시간 추가
        post_count = collection.count_documents({})  # MongoDB의 총 데이터 수
        if post_count % 50 == 0:
            rest_time = random.uniform(60, 900)  # 1분(60초)에서 15분(900초) 사이의 랜덤한 시간
            print(f"\n50개의 게시물 수집 완료. ")
            print(f"🚩현재 DB에 총 {post_count}개의 데이터가 있습니다.")
            print(f"{rest_time:.1f}초 동안 휴식을 시작합니다...")
            
            # 카운트다운 시작
            start_time = time.time()
            while True:
                elapsed_time = time.time() - start_time
                remaining_time = rest_time - elapsed_time
                
                if remaining_time <= 0:
                    print("\n휴식 완료! 크롤링을 재개합니다...")
                    break
                
                minutes = int(remaining_time // 60)
                seconds = int(remaining_time % 60)
                print(f"\r남은 휴식 시간: {minutes}분 {seconds}초", end='', flush=True)
        
        print(f"\n현재까지 총 {post_count}개의 게시물이 저장되었습니다.")
        return True
    except Exception as e:
        print(f"데이터 저장 중 오류 발생: {str(e)}")
        return False

def load_processed_posts():
    """MongoDB에서 게시물 URL들을 로드"""
    processed_posts = set()
    
    # MongoDB에서 URL 로드
    try:
        mongo_posts = collection.find({}, {"post_url": 1})
        processed_posts.update(post["post_url"] for post in mongo_posts)
        print(f"🚩MongoDB에서 {len(processed_posts)}개의 게시물 URL을 로드했습니다.")
    except Exception as e:
        print(f"MongoDB 데이터 로드 중 오류 발생: {str(e)}")
    
    return processed_posts

def main_crawling():
    try:
        # 프로필 URL로 이동
        profile_url = "https://www.instagram.com/"
        print(f"\n프로필 URL({profile_url})로 이동합니다...")
        driver.get(profile_url)
        time.sleep(3)

        # 크롤링 시작 전에 기존 데이터 로드
        processed_posts = load_processed_posts()

        # 피드 크롤링을 시작합니다...
        print("피드 크롤링을 시작합니다...")

        # 스크롤하면서 피드 크롤링
        SCROLL_PAUSE_TIME = 2
        SCROLL_COUNT = 0  # 스크롤 횟수 추적
        MAX_SCROLLS_BEFORE_BREAK = random.randint(15, 25)  # 휴식 전 최대 스크롤 횟수
        last_height = driver.execute_script("return document.body.scrollHeight")
        processed_posts = set()  # 이미 처리한 게시물 추적
        old_post_count = 0  # 7일 이상 된 게시물 카운트
        old_post_urls = set()  # 5일 이상 된 게시물의 URL을 저장하는 집합 추가

        print("\n피드 크롤링을 시작합니다...")

        while True:
            try:
                # 스크롤 횟수 증가
                SCROLL_COUNT += 1
                
                # 일정 횟수마다 휴식
                if SCROLL_COUNT % MAX_SCROLLS_BEFORE_BREAK == 0:
                    break_time = random.uniform(15, 30)
                    print(f"\n{MAX_SCROLLS_BEFORE_BREAK}회 스크롤 완료. {break_time:.1f}초 휴식합니다...")
                    time.sleep(break_time)
                    MAX_SCROLLS_BEFORE_BREAK = random.randint(15, 25)
                
                # 피드 게시물이 로드될 때까지 대기 (시간 증가)
                WebDriverWait(driver, 15).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "article"))
                )
                
                
                # 현재 화면에 보이는 모든 게시물을 순서대로 가져오기
                posts = driver.find_elements(By.CSS_SELECTOR, "article")
                
                # 각 게시물을 순서대로 처리
                for post in posts:
                    try:
                        # 해당 게시물이 화면에 보이도록 스크롤
                        driver.execute_script("arguments[0].scrollIntoView(true);", post)
                        time.sleep(random.uniform(0.5, 1))  # 스크롤 후 잠시 대기
                        
                        # 시간 정보 찾기 (현재 게시물 내에서 검색)
                        time_element = post.find_element(By.CSS_SELECTOR, "time")
                        post_time = time_element.get_attribute("datetime")
                        
                        # 게시물 작성 시간 체크
                        post_datetime = datetime.fromisoformat(post_time.replace('Z', '+00:00'))
                        current_time = datetime.now(timezone.utc)
                        time_difference = current_time - post_datetime
                        
                        days_threshold = 5  # 날짜 기준을 변수로 설정(날짜변경,수정)
                        if time_difference.days >= days_threshold and post_link not in old_post_urls:
                            old_post_urls.add(post_link)  # 5일 이상 된 게시물 URL 저장
                            old_post_count += 1
                            print(f"\n{days_threshold}일 이상 된 게시물 발견! (현재까지 {old_post_count}개 발견)")
                            
                            # 로그 파일에 기록 추가
                            log_old_post(post_link, post_time, old_post_count)
                            
                            if old_post_count >= 5:  # 10개에서 5개로 변경
                                print(f"\n{days_threshold}일 이상 된 게시물이 5개 이상 발견되어 크롤링을 종료합니다.")
                                log_session_status(f"5일 이상 된 게시물 5개 발견으로 크롤링 종료", total_posts=collection.count_documents({}))
                                raise StopIteration  # 크롤링 종료를 위해 예외 발생
                        
                        # 사용자명 찾기 (현재 게시물 내에서 검색)
                        username = post.find_element(By.CSS_SELECTOR, "span._aacw._aacx._aad7._aade").text
                        
                        # '더보기' 버튼이 있는지 확인하고 있다면 클릭 (현재 게시물 내에서 검색)
                        try:
                            more_button = post.find_element(By.XPATH, ".//span[contains(text(), '더 보기')]")
                            driver.execute_script("arguments[0].click();", more_button)
                            time.sleep(1)  # 내용이 펼쳐질 때까지 잠시 대기
                        except:
                            pass  # '더보기' 버튼이 없다면 패스
                        
                        # 전체 본문 가져오기 (현재 게시물 내에서 검색)
                        post_text = post.find_element(By.CSS_SELECTOR, "span._ap3a._aaco._aacu._aacx._aad7._aade").text
                        
                        # 게시물 링크 찾기 (현재 게시물 내에서 검색)
                        post_link = post.find_element(By.CSS_SELECTOR, "a[href^='/p/']").get_attribute("href")
                        
                        # 이미 처리한 게시물이면 건너뛰기
                        if post_link in processed_posts:
                            continue
                            
                        # 새로운 게시물 처리
                        processed_posts.add(post_link)
                        
                        print("\n새로운 게시물 발견!")
                        print(f"작성자: {username}")
                        print(f"작성시간: {post_time}")

                        # 본문을 100자까지만 출력하고, 그 이상은 생략
                        post_text_display = post_text if len(post_text) <= 100 else post_text[:100] + "..."
                        print(f"본문: {post_text_display}")
                        print(f"게시물 링크: {post_link}")
                        print("-" * 50)
                        
                        try:
                            # 현재 날짜 가져오기 (KST 기준)
                            kst = timezone(timedelta(hours=9))
                            current_date = datetime.now(kst).strftime('%Y-%m-%d')
                            
                            row_values = [
                                post_time,  # 작성시간
                                username,   # 작성자
                                post_text,  # 본문
                                post_link   # 게시물 링크
                            ]
                            
                            # MongoDB에 데이터 저장
                            if update_mongodb_data(row_values, current_date):
                                print("데이터가 MongoDB에 저장되었습니다.")
                            else:
                                print("MongoDB 저장 중 오류가 발생했습니다.")

                        except Exception as e:
                            print(f"데이터 저장 중 오류 발생: {str(e)}")

                    except StopIteration:
                        raise  # 상위 루프로 예외 전파
                    except Exception as e:
                        print(f"게시물 데이터를 가져오는 중 문제가 발생했습니다. 다음 게시물로 넘어갑니다.")
                        continue
                
                # 페이지 스크롤 (더 큰 범위로 수정)
                scroll_multiplier = random.uniform(1.2, 2.0)  # 120% ~ 200% 사이의 랜덤 배수
                viewport_height = driver.execute_script("return window.innerHeight;")
                scroll_height = int(viewport_height * scroll_multiplier)  # 화면 높이의 120-200%만큼 스크롤
                
                current_position = driver.execute_script("return window.pageYOffset;")
                target_position = min(current_position + scroll_height, last_height)
                
                # 중간 지점들을 만들어 자연스러운 스크롤 구현
                steps = random.randint(2, 4)  # 2-4개의 중간 지점
                for step in range(steps):
                    intermediate_position = current_position + (target_position - current_position) * (step + 1) / steps
                    driver.execute_script(f"window.scrollTo({{top: {intermediate_position}, behavior: 'smooth'}});")
                    time.sleep(random.uniform(0.5, 1))  # 각 중간 스크롤마다 짧은 대기
                
                # 최종 위치로 스크롤
                driver.execute_script(f"window.scrollTo({{top: {target_position}, behavior: 'smooth'}});")
                
                # 더 긴 대기 시간 설정 (2-4초)
                wait_time = random.uniform(2, 4)
                time.sleep(wait_time)
                
                # 새로운 높이 계산 전에 추가 대기
                time.sleep(3)
                new_height = driver.execute_script("return document.body.scrollHeight")

                # 추가 스크롤 시도
                retry_count = 0
                while new_height == last_height and retry_count < 10:
                    print(f"\n새로운 컨텐츠를 찾기 위해 {retry_count + 1}번째 추가 스크롤 시도...")
                    
                    # 현재 위치에서 조금 더 아래로 스크롤
                    current_position = driver.execute_script("return window.pageYOffset;")
                    scroll_amount = random.randint(300, 1000)  # 300~1000픽셀 추가 스크롤
                    driver.execute_script(f"window.scrollTo({current_position}, {current_position + scroll_amount});")
                    
                    time.sleep(3)  # 로딩 대기
                    
                    # 새로운 높이 확인
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    retry_count += 1

                # 10번의 추가 스크롤 시도 후에도 변화가 없으면 종료
                if new_height == last_height and retry_count == 10:
                    print("\n10번의 추가 스크롤 시도 후에도 새로운 게시물이 없어 크롤링을 종료합니다.")
                    break
                    
                print(f"\n스크롤 다운 중... (스크롤 횟수: {SCROLL_COUNT})")
                last_height = new_height
                
            except StopIteration:
                return "STOP_ITERATION"
            except Exception as e:
                print(f"스크롤 중 오류 발생: {str(e)}")
                return "ERROR"
        
        return "COMPLETE"

    except Exception as e:
        print(f"크롤링 실행 중 오류 발생: {str(e)}")
        return "ERROR"

def log_session_status(status, posts_count=None, total_posts=None):
    """세션 상태를 로그 파일에 기록"""
    log_file = os.path.join(os.path.dirname(__file__), "newfeed_crawl_sessions.txt")
    kst = timezone(timedelta(hours=9))
    timestamp = datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')
    
    log_message = f"[{timestamp}] {status}"
    if posts_count is not None:
        log_message += f" - 이번 세션 수집: {posts_count}개"
    if total_posts is not None:
        log_message += f" (현재 DB 총 게시물: {total_posts}개)"
    
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(log_message + "\n")

def log_old_post(post_url, post_time, count):
    """5일 이상 된 게시물 발견 시 로그 파일에 기록"""
    log_file = os.path.join(os.path.dirname(__file__), "newfeed_crawl_sessions.txt")
    kst = timezone(timedelta(hours=9))
    timestamp = datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')
    
    # 게시물 작성 시간을 KST로 변환
    post_datetime = datetime.fromisoformat(post_time.replace('Z', '+00:00'))
    post_datetime_kst = post_datetime.astimezone(kst)
    post_time_kst = post_datetime_kst.strftime('%Y-%m-%d %H:%M:%S')
    
    log_message = f"[{timestamp}] 5일 이상 된 게시물 발견 ({count}번째) - URL: {post_url}, 작성시간: {post_time_kst}"
    
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(log_message + "\n")

def run_crawler():
    global driver  # driver를 전역 변수로 선언
    session_count = 1  # 세션 카운터 추가
    while True:
        try:
            # 세션 시작 로그 (현재 총 게시물 수 포함)
            total_posts = collection.count_documents({})
            print(f"\n=== {session_count}번째 크롤링 세션 시작 ===")
            log_session_status(f"{session_count}번째 크롤링 세션 시작", total_posts=total_posts)
            
            # MongoDB에서 초기 게시물 수 확인
            initial_post_count = collection.count_documents({})
            
            for attempt in range(1):  # 3에서 1로 변경
                print(f"\n=== 크롤링 {attempt + 1}차 시도 시작 ===")
                result = main_crawling()
                print(f"\n=== 크롤링 {attempt + 1}차 시도 완료 ===")
                print(f"결과: {result}")
                
                if attempt < 0:  # 2에서 0으로 변경 (1회만 실행하므로 이 조건은 실행되지 않음)
                    print("\n30초 후 다음 크롤링을 시작합니다...")
                    time.sleep(30)
                    driver.refresh()
                    time.sleep(3)
            
            # MongoDB에서 최종 게시물 수 확인
            final_post_count = collection.count_documents({})
            posts_added = final_post_count - initial_post_count
            
            # 세션 종료 로그
            print(f"\n=== {session_count}번째 크롤링 세션 종료 ===")
            print(f"이번 세션에서 {posts_added}개의 게시물이 추가되었습니다.")
            print(f"현재 DB에 총 {final_post_count}개의 게시물이 있습니다.")
            log_session_status(f"{session_count}번째 크롤링 세션 종료", posts_added, final_post_count)
            
            # 드라이버 종료
            driver.quit()
            print("\n1회의 크롤링이 완료되었습니다.")  # 3회에서 1회로 변경
            
            # 세션 카운터 증가
            session_count += 1
            
            rest_time = random.uniform(300, 600)
            print(f"\n다음 크롤링 세션까지 {rest_time/60:.1f}분 대기합니다...")
            time.sleep(rest_time)
            
            # 새 세션을 위한 드라이버 재생성
            driver = webdriver.Chrome(options=options)
            print(f"\n새 드라이버가 생성되었습니다. {session_count}번째 세션을 시작합니다...")
            
        except KeyboardInterrupt:
            try:
                driver.quit()
            except:
                pass  # 드라이버가 이미 종료되었거나 없는 경우 무시
            total_posts = collection.count_documents({})
            log_session_status(f"{session_count}번째 세션 - 사용자에 의한 크롤링 중단", total_posts=total_posts)
            print(f"\n=== {session_count}번째 크롤링 세션이 사용자에 의해 중단되었습니다 ===")
            break
        except Exception as e:
            try:
                driver.quit()
            except:
                pass  # 드라이버가 이미 종료되었거나 없는 경우 무시
            total_posts = collection.count_documents({})
            log_session_status(f"{session_count}번째 세션 - 오류 발생: {str(e)}", total_posts=total_posts)
            print(f"\n=== {session_count}번째 크롤링 세션 중 오류 발생: {str(e)} ===")
            print("5분 후 다시 시도합니다...")
            time.sleep(300)
            
            # 새 드라이버 생성
            driver = webdriver.Chrome(options=options)

# 크롤러 실행
run_crawler()
