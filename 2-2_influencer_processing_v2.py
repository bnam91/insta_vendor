'''
[프로그램 설명]
인스타그램 프로필 정보를 크롤링하여 JSON 파일에 저장하는 프로그램

[데이터 흐름]
1. 입력 파일: 2-1_following_extract_data.json
   - 기본 프로필 정보 (id, username, name, profile_link 등)
   - 추가날짜, from 정보

2. 출력/업데이트 파일: 2-2_influencer_processing_data.json
   - 크롤링으로 업데이트되는 항목:
     * 게시물 수
     * 팔로워 수
     * 팔로우 수
     * 이름
     * 소개글
     * 외부프로필링크
     * 공구유무 (Claude API 분석)
     * 이름추출 (Claude API 분석)
   
   - 수동 입력/관리 항목 (보존):
     * 팔로우여부
     * 카테고리
     * 키워드
     * 릴스평균조회수
     * 이미지url
     * 브랜드 정보
     * 콘텐츠/팔로워/게시물/릴스 점수
     * 등급 정보

[주요 처리 로직]
1. JSON 데이터 관리
   - 새로운 프로필 데이터 추가
   - 기존 데이터 보존
   - 크롤링 데이터 업데이트

2. 크롤링 자동화
   - 랜덤 간격 처리 (3-7초)
   - 15-25개마다 1-15분 휴식
   - 80-100개마다 1-2시간 휴식
   - 자연스러운 스크롤 동작

3. API 분석
   - 공구유무 분석 (소개글 기반)
   - 이름 추출 (프로필명 정제)

[실행 조건]
- 게시물 데이터가 없는 프로필만 처리
- 기존 공구유무 데이터는 보존
- 처리할 프로필이 없으면 종료
'''

#v8(예정)
# 비전기능
# 크롤링 결과 업데이트 할 수 있게
# 게시물 및 팔로우 수에 따라 기본 등급 부여

#v9(예정)
# mysql연동 이미그레이션 테이블 생성


from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import os
import shutil
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import unquote
from selenium.common.exceptions import NoSuchElementException
import anthropic
import random
import sys
import subprocess
import datetime
import json
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

# load_dotenv() 호출 추가 (파일 시작 부분에)
load_dotenv()

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
user_data_dir = os.path.join(os.path.dirname(__file__), "user_data", "office_goyamedia_feed")
options.add_argument(f"user-data-dir={user_data_dir}")

# 캐시와 임시 파일 정리 (로그인 정보 유지)
clear_chrome_data(user_data_dir)

# 추가 옵션 설정
options.add_argument("--disable-application-cache")
options.add_argument("--disable-cache")

driver = webdriver.Chrome(options=options)

def get_profile_urls():
    """JSON 파일에서 프로필 데이터 읽기"""
    try:
        json_path = os.path.join(os.path.dirname(__file__), '2-2_influencer_processing_data.json')
        with open(json_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        profile_data = []
        for idx, item in enumerate(json_data):
            # 게시물 데이터가 비어있거나 '-'인 항목만 처리
            if not item.get('게시물') or item.get('게시물') == '-':
                profile_url = item.get('profile_link', '')
                if profile_url:  # URL이 있는 경우만 추가
                    profile_data.append((
                        profile_url,
                        item.get('게시물', ''),
                        idx  # JSON 배열의 인덱스
                    ))
        
        if not profile_data:
            print('처리할 프로필 데이터가 없습니다.')
            return []
            
        print(f'총 {len(profile_data)}개의 프로필을 처리할 예정입니다.')
        return profile_data
        
    except Exception as e:
        print(f'프로필 URL 읽기 오류: {str(e)}')
        return []

def analyze_bio_for_group_purchase(bio_text, external_link):
    """Claude API를 사용하여 소개글과 외부 링크를 분석하여 공동구매 여부 판단"""
    try:
        max_retries = 3
        retry_count = 0
        retry_delay = 2
        
        # 외부 링크가 있는 경우 기본값을 '확인필요(낮음)'로 설정
        has_external_link = external_link != "링크 없음"
        
        while retry_count < max_retries:
            try:
                print("\nClaude API에 소개글 분석 요청 중...")
                client = anthropic.Anthropic(
                    api_key=os.getenv('ANTHROPIC_API_KEY')  # API 키를 .env에서 불러옴
                )
                
                prompt = f"""당신은 인스타그램에서 공구/공동구매 계정을 분류하는 전문가입니다. 
                수년간의 경험으로 계정의 소개글만 보고도 일반 판매와 공구/공동구매 계정을 정확하게 구분할 수 있습니다.
                다음 인스타그램 소개글과 외부 링크 정보를 분석하여 공동구매/공구 진행하는 인원인지 판단해주세요.

                분석할 내용:
                - 소개글: {bio_text}
                - 외부 링크: {'있음' if has_external_link else '없음'}

                1. 'Y' 판정 조건 (아래 조건들 중 하나라도 해당되면 무조건 Y):
                  - [최우선] "공구", "공동구매", "공구링크", "공구일정" 등 직접적인 공동구매 관련 단어
                    예: "공구일정확인과 구매링크⬇️", "공구 일정은 링크에서 확인해주세요"
                   
                  - [최우선] 날짜와 함께 상품명이 명시된 모든 경우 (괄호, 슬래시 등 형식 무관)
                    예: 
                    - "켄트로얄 칫솔•치약 2.11 OPEN"
                    - "락토페린 다이어트 유산균 (2/5)"
                    - "엔트로피 브로우 블리치 (2/17)"
                    - "유산균(2/5)"
                    - "2/3 #피퍼세정제"
                    - "🌟AMT3차마켓 (02.03~09)"
                    - "채칼탈수기 2/10"
                   
                   - [최우선] "오픈", "open", "오픈예정", "재오픈" 안내, "구매", "주문" 등의 구매안내 관련 문구
                    예 : 
                    -"에어그릴2차 open‼️"
                    -"올리브오일 2/6 오픈예정"
                    -"밴딩타올 #open"
                    -"진행중인 공구 제품 구매하기"

                   
                2. '확인필요(높음)' 판정 조건 (Y 조건에 해당하지 않는 경우):
                   - 외부 링크가 있고 OPEN 및 판매 관련 내용이 있으나 구체성이 부족한 경우
                   - 판매/공구 의도가 불명확한 경우

                3. '확인필요(낮음)' 판정 조건:
                   - 외부 링크만 있고 판매 관련 내용이 없는 경우
                   - 판매 의도가 불명확한 경우
                   - 제품 리뷰어/체험단 성향이 보이는 경우

                4. 'N' 판정 조건:
                   - Y, 확인필요 조건에 해당하지 않는 모든 경우
                   - 외부 링크도 없고 판매/공구 관련 내용도 전혀 없는 경우
                   - 공구 안한다고 언급된 경우

                *** 매우 중요: 
                1. 공구 관련 단어가 있으면 무조건 Y (공구안한다고 언급한 경우 N)
                2. 날짜가 포함된 상품 명시는 형식에 관계없이 무조건 Y
                3. 여러 상품을 나열하고 구매/문의 안내가 있으면 무조건 Y
                4. 외부 링크에 'link.inpock.co.kr' 포함된 경우 최소 '확인필요(높음)'으로 분류

                Y, N, 확인필요(높음), 확인필요(낮음) 중 하나로만 답변하세요."""

                print("분석 중...")
                message = client.messages.create(
                    model="claude-3-5-haiku-latest",
                    max_tokens=100,
                    temperature=0,
                    system="정확히 Y, N, 확인필요(높음), 확인필요(낮음) 중 하나로만 답변하세요.",
                    messages=[
                        {"role": "user", "content": prompt}
                    ]
                )
                
                response = str(message.content).strip()
                
                # 외부 링크만 있고 명확한 공구 증거가 없으면 '확인필요(낮음)'으로 처리
                if has_external_link and 'N' in response:
                    return '확인필요(낮음)'
                    
                if 'Y' in response:
                    return 'Y'
                elif '확인필요(높음)' in response:
                    return '확인필요(높음)'
                elif '확인필요(낮음)' in response:
                    return '확인필요(낮음)'
                elif 'N' in response:
                    return 'N'
                else:
                    return '확인필요(낮음)'
                
            except Exception as e:
                retry_count += 1
                print(f"\nClaude API 호출 {retry_count}번째 시도 실패: {str(e)}")
                
                if retry_count < max_retries:
                    print(f"{retry_delay}초 후 재시도합니다...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    print("최대 재시도 횟수를 초과했습니다. '확인필요(낮음)'으로 처리합니다.")
                    return '확인필요(낮음)'
                
    except Exception as e:
        print(f"\nClaude API 분석 중 오류 발생: {str(e)}")
        print("'확인필요(낮음)'로 처리합니다.")
        return '확인필요(낮음)'

def extract_clean_name(display_name):
    """Claude API를 사용하여 표시된 이름에서 대표 닉네임/이름 추출"""
    try:
        client = anthropic.Anthropic(
            api_key=os.getenv('ANTHROPIC_API_KEY')  # API 키를 .env에서 불러옴
        )
        
        prompt = f"""다음 인스타그램 프로필 이름에서 가장 적절한 대표 닉네임을 추출해주세요:
        
        프로필 이름: {display_name}
        
        규칙:
        1. 이모지, 특수문자 제거
        2. 실명보다는 계정을 대표하는 닉네임을 우선 선택
        3. 여러 요소가 있다면 계정의 성격을 가장 잘 나타내는 것을 선택
        4. 실명과 닉네임이 함께 있다면 닉네임 우선
        5. 추출한 닉네임만 답변 (다른 설명 없이)
        6. 가능한 한글로 변환하여 표현
        
        예시:
        입력: "소소홈 (뭉야미)"
        출력: 뭉야미
        
        입력: "유네미니 ㅣ🎧 듣는살림 📻 살림라디오"
        출력: 유네미니
        
        입력: "Kim Ji Eun | 우남매 홈"
        출력: 우남매 홈
        
        입력: "12시에맛나요-YummyAt12"
        출력: 12시에맛나요
        
        입력: "the끌림 | 강보람"
        출력: the끌림
        
        입력: "B텔라 | Home Diary	B텔라"
        출력: B텔라

        입력: "Ji Hye Park"
        출력: 박지혜

        입력: "마로마트 | 다이어트먹트 이채은"
        출력: 마로마트"""

        

        


        message = client.messages.create(
            model="claude-3-5-haiku-latest",
            max_tokens=50,
            temperature=0,
            system="대표 닉네임이나 이름만 정확히 추출하여 답변하세요. 다른 설명은 하지 마세요.",
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # message.content에서 순수 텍스트만 추출
        clean_name = message.content
        if hasattr(clean_name, 'text'):  # TextBlock 객체인 경우
            clean_name = clean_name.text
        elif isinstance(clean_name, list):  # 리스트인 경우
            clean_name = clean_name[0].text if clean_name else display_name
        
        # 문자열로 변환하고 앞뒤 공백 제거
        clean_name = str(clean_name).strip()
        
        print(f"추출 전: {display_name}")  # 디버깅용
        print(f"추출 후: {clean_name}")    # 디버깅용
        
        return clean_name
        
    except Exception as e:
        print(f"\n이름 추출 중 오류 발생: {str(e)}")
        return display_name

def update_profile_data(profile_data, json_index):
    """JSON 파일의 데이터 업데이트"""
    try:
        json_path = os.path.join(os.path.dirname(__file__), '2-2_influencer_processing_data.json')
        
        with open(json_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        if json_index < len(json_data):
            existing_data = json_data[json_index]
            
            # 공구유무 분석 후 팔로우 처리
            if profile_data.get('소개'):
                group_purchase_status = analyze_bio_for_group_purchase(
                    profile_data.get('소개', ''),
                    profile_data.get('외부프로필링크', '링크 없음')
                )
                
                # 기존 공구유무 데이터가 있으면 보존
                if '공구유무' in existing_data and existing_data['공구유무']:
                    print("기존 '공구유무' 데이터가 있어 보존합니다:", existing_data['공구유무'])
                else:
                    profile_data['공구유무'] = group_purchase_status
                
                # 팔로우 조건 확인: Y(또는 확인필요(높음))이고 R등급이 아닌 경우에만 팔로우
                followers = int(profile_data.get('팔로워', '0').replace('-', '0'))
                is_r_grade = followers <= 5000
                
                should_follow = (
                    (group_purchase_status == 'Y' or 
                     (include_high_priority and group_purchase_status == '확인필요(높음)')) 
                    and not is_r_grade
                )
                
                if should_follow:
                    try:
                        # 팔로우 버튼 찾기
                        follow_button = driver.find_element(By.XPATH, 
                            "//button[.//div[contains(text(), '팔로우') or contains(text(), '팔로잉')]]")
                        button_text = follow_button.find_element(By.XPATH, ".//div").text
                        
                        if button_text == "팔로우":
                            follow_button.click()
                            print("팔로우 완료")
                            time.sleep(2)
                            profile_data['팔로우여부'] = '팔로우'
                        else:
                            print("이미 팔로우 중입니다")
                            profile_data['팔로우여부'] = '팔로우'
                            
                    except Exception as e:
                        print(f"팔로우 처리 중 오류 발생: {str(e)}")
                        profile_data['팔로우여부'] = '실패'
                else:
                    print("팔로우 조건에 해당하지 않습니다")
                    profile_data['팔로우여부'] = ''
            
            # 기존 데이터 유지하면서 새 데이터로 업데이트
            json_data[json_index].update(profile_data)
            
            # JSON 파일 저장
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=4)
            
            print(f"JSON 인덱스 {json_index} 업데이트 완료")
            
    except Exception as e:
        print(f"데이터 업데이트 중 오류 발생: {str(e)}")
        print(f"상세 오류 정보: {type(e).__name__}")

def convert_to_number(text):
    """인스타그램 숫자 형식을 순수 숫자로 변환"""
    text = text.replace(',', '')  # 쉼표 제거
    
    if '만' in text:
        number = float(text.replace('만', '')) * 10000
        return str(int(number))
    elif '천' in text:
        number = float(text.replace('천', '')) * 1000
        return str(int(number))
    else:
        return text

def restart_program():
    """1-2시간 사이 랜덤한 시간 후에 프로그램을 재시작"""
    # 1시간(3600초)에서 2시간(7200초) 사이의 랜덤한 시간 설정
    wait_time = random.randint(3600, 7200)
    print(f"\n프로그램이 {wait_time//3600}시간 {(wait_time%3600)//60}분 후에 재시작됩니다...")
    
    # 현재 시간과 다음 실행 시간 출력
    now = datetime.datetime.now()
    next_run = now + datetime.timedelta(seconds=wait_time)
    print(f"현재 시간: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"다음 실행 시간: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 카운트다운 시작
    for remaining in range(wait_time, 0, -1):
        minutes = remaining // 60
        seconds = remaining % 60
        print(f"\r다음 실행까지 남은 시간: {minutes//60:02d}시간 {minutes%60:02d}분 {seconds:02d}초", end='', flush=True)
        time.sleep(1)
    
    print("\n프로그램을 재시작합니다...")
    
    # 현재 스크립트의 경로를 가져와서 재실행
    python_executable = sys.executable
    script_path = os.path.abspath(__file__)
    subprocess.Popen([python_executable, script_path])
    sys.exit()

# MongoDB 연결 설정
mongo_uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(mongo_uri, server_api=ServerApi('1'))
db = client['insta09_database']
collection = db['03_main_following_extract_data']

def load_and_update_json_data():
    """MongoDB에서 데이터를 로드하고 JSON 파일 업데이트"""
    try:
        # 기존 출력 JSON 파일 읽기
        existing_json_data = list(collection.find({}))
        print(f"기존 데이터 수: {len(existing_json_data)}")

        # username을 키로 하는 딕셔너리 생성 (기존 데이터)
        existing_data_dict = {item.get('username', ''): item for item in existing_json_data}

        # 새로운 데이터 처리
        new_data_count = 0
        
        # MongoDB에서 데이터 읽기
        for item in existing_data_dict.values():
            username = item.get('username', '')
            
            # 새로운 데이터 구조 생성
            new_row_dict = {
                "num": item.get('num', ''),  # 수정된 키
                "add_date": item.get('add_date', ''),  # 수정된 키
                "팔로우여부": "",
                "from": item.get('from', ''),
                "username": username,
                "name": item.get('name', ''),
                "profile_link": item.get('profile_link', ''),
                "게시물": "",
                "팔로워": "",
                "팔로우": "",
                "이름": "",
                "이름추출": "",
                "소개": "",
                "외부프로필링크": "",
                "공구유무": "",
                "카테고리": "",
                "키워드": "",
                "릴스평균조회수(최근 15개)": "",
                "이미지url": "",
                "브랜드": [
                    {
                        "name": "",  # 브랜드명 -- 닌자
                        "category": "",  # 브랜드 카테고리 -- 주방가전
                        "products": [
                            {
                                "item": "",  # 아이템명 -- 닌자올인원블렌더
                                "type": "",  # 유형 -- (공구오픈,예고) 
                                "mentioned_date": "",  # 언급된날짜 -- 2025-02-09T00:00:10.000Z
                                "expected_date": "",  # 오픈예정일
                                "end_date": "",  # 공구마감일
                                "item_feed_link": "",  # 아이템 피드 링크 -- https://www.instagram.com/p/xxx
                                "preserve": ""  # 보존 여부
                            }
                        ]
                    }
                ],
                "콘텐츠점수(5점)": 1,  # 기본값을 1로 설정
                "팔로워점수": "",
                "게시물점수": "",
                "릴스점수": "",
                "콘텐츠가산점": "",
                "최종점수": "",
                "등급": "",
                "이전등급": [""]
            }
            
            # 새 데이터를 기존 딕셔너리에 추가
            existing_data_dict[username] = new_row_dict
            new_data_count += 1
        
        # 기존 데이터와 새 데이터를 합쳐서 리스트로 변환
        updated_json_data = list(existing_data_dict.values())
        
        # JSON 파일 업데이트
        output_json_path = os.path.join(os.path.dirname(__file__), '2-2_influencer_processing_data.json')
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(updated_json_data, f, ensure_ascii=False, indent=4)
        
        print(f"기존 데이터 수: {len(existing_json_data)}")
        print(f"새로 추가된 데이터 수: {new_data_count}")
        print(f"최종 데이터 수: {len(updated_json_data)}")
        print(f"JSON 파일이 업데이트되었습니다: {output_json_path}")
        
    except Exception as e:
        print(f"데이터 업데이트 중 오류 발생: {str(e)}")
        raise

# 메인 실행 코드 수정
if __name__ == "__main__":
    # 사용자 입력 받기
    while True:
        response = input("\n'확인필요(높음)' 상태의 계정도 함께 팔로우하시겠습니까? (y/n): ").lower()
        if response in ['y', 'n']:
            break
        print("잘못된 입력입니다. 'y' 또는 'n'을 입력해주세요.")
    
    include_high_priority = response == 'y'
    print(f"\n선택된 팔로우 조건: {'Y 및 확인필요(높음)' if include_high_priority else 'Y만'}")
    
    # JSON 데이터 처리 먼저 실행
    load_and_update_json_data()
    
    # 프로필 URL 가져오기
    profile_urls = get_profile_urls()
    
    # 처리할 프로필이 없는 경우 바로 종료
    if not profile_urls:
        print("처리할 프로필이 없어 프로그램을 종료합니다.")
        driver.quit()
        sys.exit()
    
    # 기존 코드 실행
    crawled_count = 0
    max_profiles = random.randint(80, 100)
    next_rest_at = random.randint(15, 25)
    
    print(f"이번 세션에서 처리할 프로필 수: {max_profiles}")
    print(f"다음 휴식까지 처리할 프로필 수: {next_rest_at}")

    for idx, (profile_url, posts_data, row_index) in enumerate(profile_urls, 1):
        # 최대 프로필 수에 도달하면 1시간 휴식 후 재시작
        if crawled_count >= max_profiles:
            print(f"\n목표 프로필 수({max_profiles}개)에 도달했습니다.")
            driver.quit()
            restart_program()  # 1시간 후 재시작
        
        # 게시물 데이터가 이미 있으면 건너뛰기
        if posts_data.strip():
            print(f"\n{idx}/{len(profile_urls)} - 이미 데이터가 있는 프로필을 건너뜁니다: {profile_url}")
            continue
        
        crawled_count += 1  # 실제 크롤링할 때만 카운트 증가
        
        # 랜덤하게 설정된 프로필 수만큼 크롤링했을 때 휴식
        if crawled_count >= next_rest_at:
            rest_time = random.randint(600, 900)  # 10-15분 휴식
            print(f"\n{next_rest_at}개의 프로필을 크롤링 완료했습니다. 잠시 휴식합니다...")
            
            # 휴식 시간 동안 랜덤하게 스크롤
            start_time = time.time()
            scroll_position = 0  # 현재 스크롤 위치 추적
            
            while time.time() - start_time < rest_time:
                remaining = rest_time - int(time.time() - start_time)
                minutes = remaining // 60
                seconds = remaining % 60
                print(f"\r남은 휴식 시간: {minutes}분 {seconds}초", end='', flush=True)
                
                # 랜덤한 간격으로 스크롤 (빈도 증가)
                if random.random() < 0.4:  # 40% 확률로 스크롤
                    # 더 큰 스크롤 범위 설정 (500-2000 픽셀)
                    scroll_amount = random.randint(500, 2000)
                    
                    # 위/아래 방향 랜덤 결정 (70% 확률로 아래로 스크롤)
                    if random.random() > 0.3:
                        scroll_position += scroll_amount
                    else:
                        scroll_amount = -scroll_amount
                        scroll_position = max(0, scroll_position + scroll_amount)
                    
                    # 부드러운 스크롤 효과 구현
                    steps = random.randint(5, 15)  # 스크롤을 여러 단계로 나눔
                    for step in range(steps):
                        partial_scroll = scroll_amount // steps
                        driver.execute_script(f"window.scrollBy(0, {partial_scroll});")
                        time.sleep(random.uniform(0.1, 0.3))
                    
                    # 스크롤 후 잠시 멈춤
                    time.sleep(random.uniform(1.0, 3.0))
                
                time.sleep(0.5)

            print("\n휴식 완료! 크롤링을 재개합니다.")
            crawled_count = 0  # 카운터 리셋
            next_rest_at = random.randint(15, 25)  # 다음 휴식까지의 프로필 수 다시 랜덤 설정 (35-55개로 증가)
            print(f"다음 휴식까지 처리할 프로필 수: {next_rest_at}")

        print(f"\n{idx}/{len(profile_urls)} - 프로필 처리 중: {profile_url}")
        driver.get(profile_url)
        
        # 페이지 로딩을 위한 랜덤 대기
        time.sleep(random.uniform(2, 3.5))
        
        try:
            profile_data = {}  # 데이터를 저장할 딕셔너리 초기화
            
            # 게시물, 팔로워, 팔로우 수 크롤링 - 각각 개별적으로 try-except 처리
            try:
                posts = convert_to_number(driver.find_element(By.XPATH, "//li[contains(., '게시물')]/div/span/span").text)
                profile_data['게시물'] = posts
            except NoSuchElementException:
                profile_data['게시물'] = '-'
                print("게시물 수를 찾을 수 없습니다.")
            
            try:
                followers = convert_to_number(driver.find_element(By.XPATH, "//li[contains(., '팔로워')]/div/a/span/span").text)
                profile_data['팔로워'] = followers
            except NoSuchElementException:
                profile_data['팔로워'] = '-'
                print("팔로워 수를 찾을 수 없습니다.")
            
            try:
                following = convert_to_number(driver.find_element(By.XPATH, "//li[contains(., '팔로우')]/div/a/span/span").text)
                profile_data['팔로우'] = following
            except NoSuchElementException:
                profile_data['팔로우'] = '-'
                print("팔로우 수를 찾을 수 없습니다.")
            
            # 이름과 소개, 외부 링크 크롤
            try:
                name = driver.find_element(
                    By.XPATH,
                    "//div[contains(@class, 'x7a106z')]//span[contains(@class, 'x1lliihq')]"
                ).text
                profile_data['이름'] = name
                print(f"이름: {name}")
            except Exception as e:
                profile_data['이름'] = '-'
                print(f"이름을 찾을 수 없습니다: {str(e)}")

            # # 'name' 필드와 '이름' 필드 비교 부분 수정
            # if profile_data.get('이름') != json_data[row_index].get('name'):
            #     print(f"이름 필드({json_data[row_index].get('name')})와 크롤링한 이름({profile_data.get('이름')})이 다릅니다. 작업을 중단합니다.")
            #     driver.quit()
            #     sys.exit()
            
            try:
                bio = driver.find_element(By.XPATH, "//span[@class='_ap3a _aaco _aacu _aacx _aad7 _aade']").text
                profile_data['소개'] = bio
            except NoSuchElementException:
                profile_data['소개'] = '-'
                print("소개글을 찾을 수 없습니다.")
            
            try:
                external_link = driver.find_element(By.XPATH, "//div[@class='x6ikm8r x10wlt62']/a").get_attribute("href")
                profile_data['외부프로필링크'] = external_link
            except NoSuchElementException:
                profile_data['외부프로필링크'] = "링크 없음"
                print("외부 링크를 찾을 수 없습니다.")
            
            # 공구유무 분석 추가
            group_purchase_status = analyze_bio_for_group_purchase(
                profile_data.get('소개', ''),
                profile_data.get('외부프로필링크', '링크 없음')
            )
            profile_data['공구유무'] = group_purchase_status
            print(f"공구유무 분석 결과: {group_purchase_status}")
            
            # 데이터가 하나라도 있다면 스프레드시트 업데이트
            if any(profile_data.values()):
                update_profile_data(profile_data, row_index)
                print(f"프로필 처리 완료: {profile_url}")
            else:
                print(f"프로필에서 어떤 정보도 찾을 수 없습니다: {profile_url}")
            
        except Exception as e:
            print(f"프로필 정보 크롤링 중 오류 발생: {str(e)}")
            continue
        
        # 다음 프로필 처리 전 랜덤 대기
        wait_time = random.uniform(3.0, 7.0)
        print(f"다음 프로필 처리까지 {wait_time:.1f}초 대기...")
        time.sleep(wait_time)

    # 모든 프로필 처리가 완료되면 종료
    print("\n모든 프로필 처리가 완료되었습니다.")
    print("프로그램을 종료합니다.")
    driver.quit()
    sys.exit()

