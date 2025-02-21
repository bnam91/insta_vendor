"""
인스타그램 팔로잉 추출기 v1

주요 기능:
1. 인스타그램 계정의 팔로잉 목록을 자동으로 크롤링
2. 3회 반복 실행으로 누락 데이터 최소화
3. 브라우저 자동화를 위한 다양한 스크롤 방식 구현
4. 캐시 관리 및 로그인 세션 유지

데이터 저장:
1. MongoDB (03_main_following_extract_data 컬렉션)
   - 기존 데이터 보존 (from 값이 같은 경우)
   - 동일 계정에서 추출 시 중복 제거 후 신규 데이터만 추가
   - 다른 계정에서 추출 시 해당 계정 데이터만 초기화 후 추가
   - 기존 데이터의 from 값이 같을 경우 삭제 후 신규 데이터로 교체

추출 정보:
- username (사용자 아이디)
- name (프로필 이름)
- profile_link (프로필 URL)
- add_date (크롤링 날짜)
- from (데이터 출처 계정)
- num (고유 식별자)

주의사항:
- 인스타그램 정책 준수를 위한 랜덤 대기 시간 적용
- 브라우저 자동화 감지 방지 기능 포함
- 안정적인 데이터 추출을 위한 다중 예외 처리
- `temp_profile_url.txt`: 크롤링할 프로필의 URL을 임시로 저장하는 파일
"""


"""
#v2_예정
- from이 같다면 업데이트 / 다르다면 json파일 및 시트 초기화 후 업데이트
- 3~5번 반복하여 누락없게끔

"""



from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import os
import sys
import shutil
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import unquote
import random
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import pandas as pd
import json
from datetime import datetime
from pymongo import MongoClient
from pymongo.server_api import ServerApi

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

def try_multiple_scroll_methods(driver):
    def find_scroll_container():
        selectors = [
            'div[role="dialog"] div._aano',
            'div[role="dialog"] div[style*="overflow: hidden auto"]',
            'div[role="dialog"] div[class*="x7r02ix"]',
            'div[role="dialog"] div.x7r02ix',
            'div._aano'
        ]
        
        for selector in selectors:
            try:
                container = driver.find_element(By.CSS_SELECTOR, selector)
                return container
            except:
                continue
        return None

    def count_following_items():
        try:
            items = driver.find_elements(By.CSS_SELECTOR, "span._ap3a._aaco._aacw._aacx._aad7._aade")
            return len(items)
        except:
            return 0

    # 모달이 나타날 때까지 대기
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="dialog"]'))
    )
    time.sleep(2)

    scroll_container = find_scroll_container()
    if not scroll_container:
        print("스크롤 컨테이너를 찾을 수 없습니다.")
        return False

    def method4():
        print("\n방법 4 시도: Scroll into view last element")
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, "span._ap3a._aaco._aacw._aacx._aad7._aade")
            if elements:
                # 스크롤 단위를 6~10개 사이에서 랜덤하게 설정
                scroll_step = random.randint(6, 38)
                for i in range(0, len(elements), scroll_step):
                    current_element = elements[i]
                    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", current_element)
                    time.sleep(random.uniform(0.5, 1.0))  # 대기 시간도 약간 줄임
                
                # 마지막 요소까지 확실히 스크롤
                driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", elements[-1])
                time.sleep(random.uniform(1, 2))
                return True
            return False
        except Exception as e:
            print(f"방법 4 실패: {str(e)}")
            return False

    methods = [method4]
    
    for attempt in range(3):
        print(f"\n시도 {attempt + 1}")
        initial_count = count_following_items()
        
        for method in methods:
            try:
                method()
                time.sleep(random.uniform(1, 2))
                
                print(f"현재 아이템 수: {count_following_items()}")
                last_count = count_following_items()
                no_change_count = 0
                scroll_count = 0
                
                while no_change_count < 3:
                    scroll_count += 1
                    
                    # 이전 항목 수 저장
                    previous_count = count_following_items()
                    
                    method()
                    time.sleep(random.uniform(1, 3.5))
                    current_count = count_following_items()
                    
                    # 스크롤 중간 확인 로그 추가
                    print(f"스크롤 {scroll_count}번째: {previous_count} -> {current_count} 항목")
                    
                    if current_count <= last_count:
                        no_change_count += 1
                        # 마지막 확인을 위한 추가 스크롤
                        if no_change_count == 2:  # 3에서 2로 변경 (마지막 시도 전)
                            print("마지막 확인을 위한 추가 스크롤 시도...")
                            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                            time.sleep(3)
                            final_count = count_following_items()
                            if final_count > current_count:
                                no_change_count = 0
                                last_count = final_count
                    else:
                        no_change_count = 0
                        last_count = current_count
                return True
                
            except Exception as e:
                print(f"메서드 실행 중 오류 발생: {str(e)}")
                continue

    return False

def main():
    try:
        options = Options()
        options.add_argument("--start-maximized")
        options.add_experimental_option("detach", True)
        options.add_argument("disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-logging"])

        # 절대경로에서 상대경로로 변경
        # user_data_dir = os.path.join(os.path.dirname(__file__), "user_data", "home_goyamedia_feed")
        user_data_dir = os.path.join(os.path.dirname(__file__), "user_data", "office_goyamedia_feed")
        options.add_argument(f"user-data-dir={user_data_dir}")

        # 캐시와 임시 파일 정리 (로그인 정보 유지)
        clear_chrome_data(user_data_dir)

        # 추가 옵션 설정
        options.add_argument("--disable-application-cache")
        options.add_argument("--disable-cache")

        driver = webdriver.Chrome(options=options)

        # 프로필 URL 읽기
        try:
            with open('temp_profile_url.txt', 'r', encoding='utf-8') as f:
                profile_url = f.read().strip()
            # 임시 파일 삭제하지 않도록 수정
            # os.remove('temp_profile_url.txt')  # 이 줄 제거
        except FileNotFoundError:
            # 기본 URL 사용
            # profile_url = "https://www.instagram.com/c_____woo/"
            profile_url = "https://www.instagram.com/goyanmedia/"
            # profile_url = "https://www.instagram.com/bnam91/"

        from_user = profile_url.split('/')[-2]  # username 추출
        driver.get(profile_url)
        time.sleep(3)

        # 팔로잉 버튼 클릭
        try:
            print("1. 팔로잉 버튼 찾는 중...")
            following_link = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href$="/following/"]'))
            )
            print("2. 팔로잉 버튼 발견됨")
            
            print("3. 팔로잉 버튼이 클릭 가능할 때까지 대기 중...")
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[href$="/following/"]'))
            )
            
            print("4. 팔로잉 버튼 클릭 시도...")
            # 여러 클릭 방법 시도
            try:
                following_link.click()
            except:
                try:
                    driver.execute_script("arguments[0].click();", following_link)
                except:
                    action = ActionChains(driver)
                    action.move_to_element(following_link).click().perform()
            
            print("5. 팔로잉 버튼 클릭 완료")
            
            # 팔로잉 목록을 저장할 리스트 초기화
            following_list = []

            # 새로운 스크롤 방법 시도
            scroll_success = try_multiple_scroll_methods(driver)
            
            if scroll_success:
                # 최종 팔로잉 목록 수집
                following_items = driver.find_elements(By.CSS_SELECTOR, "div.x1dm5mii.x16mil14")
                for item in following_items:
                    try:
                        # 아이디 추출
                        username_element = item.find_element(By.CSS_SELECTOR, "span._ap3a._aaco._aacw._aacx._aad7._aade")
                        username = username_element.text

                        # 이름 추출
                        try:
                            name_element = item.find_element(By.CSS_SELECTOR, "span.x1lliihq.x193iq5w.x6ikm8r.x10wlt62")
                            name = name_element.text
                        except:
                            name = "이름 없음"

                        # 프로필 링크 추출
                        try:
                            profile_link_element = item.find_element(By.CSS_SELECTOR, "a._a6hd")
                            profile_link = profile_link_element.get_attribute("href")
                            # URL이 중복되는 경우를 처리
                            if "instagram.com" in profile_link:
                                profile_link = profile_link.split("instagram.com")[-1]
                            profile_link = f"https://www.instagram.com{profile_link}"
                        except:
                            profile_link = "링크 없음"

                        # 딕셔너리 형태로 저장
                        user_info = {
                            "username": username,
                            "name": name,
                            "profile_link": profile_link,
                            "add_date": time.strftime("%Y-%m-%d"),  # 키 이름 변경
                            "from": from_user,  # from 사용자 추가
                            "num": None  # 키 이름 변경을 위해 초기화
                        }
                        
                        if username not in [info["username"] for info in following_list]:
                            following_list.append(user_info)
                            print(f"사용자 추가: {username} (이름: {name})")

                    except Exception as e:
                        print(f"항목 처리 중 오류 발생: {str(e)}")

            else:
                print("스크롤 시도 실패")

            # 최종 결과 출력
            print(f"\n총 {len(following_list)}개의 팔로잉 계정을 찾았습니다:")
            for i, user_info in enumerate(following_list, 1):
                print(f"{i}. 아이디: {user_info['username']}")
                print(f"   이름: {user_info['name']}")
                print(f"   프로필 링크: {user_info['profile_link']}")
                print("   " + "-"*50)

            # 데이터프레임 생성
            df = pd.DataFrame(following_list)
            
            # MongoDB 연결 설정
            mongo_uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
            client = MongoClient(mongo_uri, server_api=ServerApi('1'))
            db = client['insta09_database']
            collection = db['03_main_following_extract_data']  # 컬렉션 이름 변경

            # MongoDB에서 모든 데이터 읽기
            all_existing_data = list(collection.find())

            # 현재 from_user와 동일한 from 값을 가진 데이터가 있는지 확인
            same_from_exists = any(data.get('from') == from_user for data in all_existing_data)

            if same_from_exists:
                # from 값이 같은 경우, 기존 데이터를 유지하고 신규 데이터만 추가
                existing_usernames = {data.get('username') for data in all_existing_data if data.get('from') == from_user}
                new_data = []
                for user_info in following_list:
                    if user_info['username'] not in existing_usernames:
                        new_data.append(user_info)
                print(f"기존의 {from_user} 데이터를 유지하고 {len(new_data)}개의 신규 데이터만 추가합니다.")
            else:
                # from 값이 다른 경우, 컬렉션의 모든 데이터를 삭제하고 새로운 데이터로 교체
                print("새로운 from 값이 감지되어 컬렉션의 모든 데이터를 삭제합니다...")
                try:
                    before_count = collection.count_documents({})
                    print(f"삭제 전 문서 수: {before_count}")
                    
                    # 삭제 시도
                    result = collection.delete_many({})
                    print(f"삭제 명령 결과 - 삭제된 문서 수: {result.deleted_count}")
                    
                    # 실제 삭제 확인
                    remaining_docs = list(collection.find())
                    print(f"실제 남아있는 문서들:")
                    for doc in remaining_docs:
                        print(f"- from: {doc.get('from')}, username: {doc.get('username')}")
                    
                    if len(remaining_docs) > 0:
                        print("경고: 문서가 완전히 삭제되지 않았습니다!")
                        # 다시 한번 삭제 시도
                        print("다시 삭제를 시도합니다...")
                        result2 = collection.delete_many({})
                        print(f"두 번째 삭제 시도 결과: {result2.deleted_count}개 삭제됨")
                except Exception as e:
                    print(f"데이터 삭제 중 오류 발생: {str(e)}")
                    print(f"오류 타입: {type(e)}")
                new_data = following_list
                print(f"새로운 from 값({from_user})의 {len(new_data)}개 데이터를 추가합니다.")

            # 마지막 ID 찾기
            try:
                with open('last_id.txt', 'r') as f:
                    last_id = int(f.read().strip())
            except FileNotFoundError:
                last_id = 0
                
            # 새로운 데이터에 ID 부여 및 MongoDB에 데이터 삽입
            for user_info in new_data:
                last_id += 1
                user_info['num'] = last_id  # 키 이름 변경
                
                # MongoDB에 데이터 삽입
                try:
                    collection.insert_one(user_info)  # MongoDB에 데이터 삽입
                    print(f"MongoDB에 데이터 삽입 성공: {user_info['username']}")
                except Exception as e:
                    print(f"MongoDB에 데이터 삽입 중 오류 발생: {str(e)}")

            # 업데이트된 마지막 ID 저장
            with open('last_id.txt', 'w') as f:
                f.write(str(last_id))

            # MongoDB에 데이터 삽입 후 결과 출력
            print(f"\nMongoDB에 총 {len(new_data)}개의 신규 데이터가 추가되었습니다.")
            # 최종 데이터 수를 정확히 계산하기 위해 현재 컬렉션의 문서 수를 다시 조회
            final_count = collection.count_documents({})
            print(f"- 전체 데이터 수: {final_count}개")  # 수정된 부분

            # MongoDB 연결 종료
            client.close()  # MongoDB 연결 종료

        except Exception as e:
            print(f"오류 발생: {str(e)}")
            print("오류 발생 위치:", e.__class__.__name__)
            print("상세 스택트레이스:")
            import traceback
            print(traceback.format_exc())

    except Exception as e:
        print(f"오류 발생: {str(e)}")
        print("오류 발생 위치:", e.__class__.__name__)
        print("상세 스택트레이스:")
        import traceback
        print(traceback.format_exc())
    finally:
        try:
            driver.quit()
        except:
            pass

# 메인 코드 3번 반복 실행
for iteration in range(3):
    print(f"\n{'='*50}")
    print(f"반복 {iteration + 1}/3 시작")
    print(f"{'='*50}\n")
    main()
    if iteration < 2:  # 마지막 반복이 아닌 경우에만 대기
        wait_time = random.randint(30, 180)  # 30~180초 사이 랜덤 대기
        print(f"\n다음 반복까지 {wait_time}초 대기 중...")
        time.sleep(wait_time)

# 모든 반복이 끝난 후 임시 파일 삭제
try:
    os.remove('temp_profile_url.txt')
except FileNotFoundError:
    pass

print("\n모든 반복이 완료되었습니다. 프로그램을 종료합니다...")
sys.exit()
