'''
1명 검색시, 약 18원~20원 소모
100명 검색시, 약 1800원~2000원 소모
1000명 검색시, 약 18000원~20000원 소모

[프로그램 설명]
인스타그램 피드 분석 및 카테고리 분류 프로그램

[데이터 흐름]
1. MongoDB 컬렉션: insta09_database.02_test_influencer_data
   - 공구유무('09_is')가 'Y'이고 카테고리가 비어있는 계정 추출
   - 프로필 링크(profile_link)
   - 소개글 정보(bio)

2. MongoDB 업데이트
   - 업데이트 항목:
     * 카테고리(category) (Vision API 분석 결과)
     * 이미지URL(image_url) (ImgBB 업로드 링크)
     * 릴스평균조회수(reels_views(15)) (최근 15개)

[주요 처리 로직]
1. 이미지 처리
   - 최근 18개 피드 이미지 수집
   - 이미지 크롭 및 그리드 생성
   - ImgBB 업로드

2. 카테고리 분석 (Vision API)
   - 10개 카테고리 분류:
     * 뷰티, 패션, 홈/리빙, 푸드, 육아
     * 건강, 맛집탐방, 전시/공연, 반려동물, 기타
   - 상위 3개 카테고리 선정
   - 카테고리별 적합도(%) 계산

3. 릴스 분석
   - 최근 15개 릴스 조회수 수집
   - 상위 3개 제외 평균 계산

4. 자동화 관리
   - 15-25개 프로필마다 1-12분 휴식
   - 80-100개 프로필마다 30분-2시간 휴식
   - 자연스러운 스크롤 동작

[실행 조건]
- 공구유무('09_is')가 'Y'인 계정만 처리
- 카테고리가 이미 있는 계정은 제외
- 최소 15개 이상의 피드 이미지 필요
- 모든 에러는 로그로 기록하고 다음 계정으로 진행
'''

#v2
#  피드 캡쳐해둠 (완료)
# 사이즈 및 해상도 확인하기 (완료)
#v3
# 생성이미지 imgbb저장 (완료)
# 카테고리 분석 (완료)
# 키워드 추출 (3개는 플러스)
# 비전점수 (프롬프트)
# 시트연결 연결 (완료)
# 릴스조회수 분석 (완료)

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import os
import shutil
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from PIL import Image
import requests
from io import BytesIO
import numpy as np
from openai import OpenAI
import base64
import io
import matplotlib.pyplot as plt
import json
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.server_api import ServerApi

load_dotenv()  # .env 파일 로드

# Chrome 브라우저의 캐시와 임시 파일을 정리하는 함수
# keep_login이 True이면 로그인 정보는 유지
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

# JSON 파일에서 분석이 필요한 인스타그램 URL을 가져오는 함수
# 공구유무가 'Y'이고 카테고리가 비어있는 계정만 추출
def get_instagram_urls():
    try:
        # MongoDB 연결
        uri = os.getenv('MONGODB_URI', "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
        client = MongoClient(uri, server_api=ServerApi('1'))
        
        # 데이터베이스와 컬렉션 선택
        db = client['insta09_database']
        collection = db['02_main_influencer_data']
        
        # 공구유무가 'Y'이고 카테고리가 비어있는 계정 찾기
        target_rows = []
        total_accounts = collection.count_documents({"09_is": "Y"})
        processed_accounts = collection.count_documents({
            "09_is": "Y",
            "category": {"$exists": True, "$ne": ""}
        })
        
        # 처리가 필요한 계정 찾기
        cursor = collection.find({
            "09_is": "Y",
            "$or": [
                {"category": {"$exists": False}},
                {"category": ""}
            ]
        })
        
        for doc in cursor:
            target_rows.append({
                'url': doc['profile_link'],
                'description': doc.get('bio', '')
            })
        
        if not target_rows:
            print("처리할 새로운 URL이 없습니다. 모든 카테고리가 이미 분석되었습니다.")
        else:
            print(f"\n=== 처리 현황 ===")
            print(f"전체 공구계정: {total_accounts}개")
            print(f"처리 완료: {processed_accounts}개")
            print(f"남은 처리: {len(target_rows)}개\n")
            
        return target_rows
        
    except Exception as e:
        print(f'URL 가져오기 실패: {str(e)}')
        return None

# Vision API를 사용하여 이미지를 분석하고 카테고리를 분류하는 함수
# 10개 카테고리 중 상위 3개를 선정하고 적합도(%) 계산
def analyze_image_with_vision(image_path, api_key, description):
    categories = {
        '뷰티': '뷰티',
        '패션': '패션',
        '홈/리빙': '홈/리빙',
        '푸드': '푸드',
        '육아': '육아',
        '건강': '건강',
        '맛집탐방': '맛집탐방',
        '전시/공연': '전시/공연',
        '반려동물': '반려동물',
        '기타': '기타'
    }
    
    analysis_results = []
    total_tokens = 0
    
    client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
    
    with open(image_path, "rb") as image_file:
        base64_image = base64.b64encode(image_file.read()).decode('utf-8')
    
    for i in range(2):
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": """당신은 인스타그램 피드 분석 전문가입니다. 
                        1. 각 이미지의 주요 시각적 요소와 전반적인 피드의 스타일을 파악합니다.
                        2. 인플루언서의 전문성과 콘텐츠의 일관성을 고려합니다.
                        3. 정확한 수치로 카테고리 적합도를 평가합니다."""
                    },
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": f"""분석 #{i+1}
                                계정 소개글: {description}

                                다음 카테고리 중에서 가장 적합한 상위 3개를 선택해주세요:
                                {', '.join(categories.keys())}

                                특별 주의사항:
                                - '푸드' 카테고리는 음식 자체가 사진의 80% 이상을 차지하는 경우에만 해당
                                - 주방용품/그릇 위주 사진은 '홈/리빙' 카테고리로 분류
                                - 주방 인테리어/식기가 주된 포커스는 '홈/리빙'으로 분류

                                다음 형식으로만 답변:
                                1. [카테고리명] XX%
                                2. [카테고리명] XX%
                                3. [카테고리명] XX%"""
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{base64_image}",
                                    "detail": "low"
                                }
                            }
                        ]
                    }
                ],
                max_tokens=150
            )
            
            content = response.choices[0].message.content
            total_tokens += response.usage.total_tokens
            print(f"\n분석 #{i+1} 결과:")
            print(content)
            analysis_results.append(content)
            
        except Exception as e:
            print(f"Vision API 호출 중 오류 발생: {e}")
            return {
                'categories': ['기타', '기타', '기타'],
                'description': description,
                'all_categories': []
            }
    
    # 두 결과의 평균 계산
    final_categories = {}
    for result in analysis_results:
        lines = result.split('\n')
        for line in lines:
            if line.strip() and '.' in line:
                parts = line.split('.')
                if len(parts) >= 2:
                    category_info = parts[1].strip()
                    if '%' in category_info:
                        category_name = ' '.join(category_info.split()[:-1])
                        percentage = int(category_info.split()[-1].replace('%', ''))
                        if category_name in final_categories:
                            final_categories[category_name] += percentage
                        else:
                            final_categories[category_name] = percentage

    # 평균 계산 및 정렬
    for category in final_categories:
        final_categories[category] = final_categories[category] / 2

    sorted_categories = sorted(final_categories.items(), key=lambda x: x[1], reverse=True)
    
    # 토큰 비용 계산 (GPT-4 Vision 기준: $0.01/1K tokens)
    token_cost_usd = (total_tokens / 1000) * 0.01
    token_cost_krw = token_cost_usd * 1350  # 환율 적용
    
    print("\n최종 분석 결과 (2회 평균):")
    for category, percentage in sorted_categories[:3]:
        print(f"{category}: {percentage:.1f}%")
    print(f"\n총 토큰 사용량: {total_tokens}")
    print(f"예상 비용: {token_cost_krw:.0f}원 (${token_cost_usd:.4f})")
    
    # 상위 3개 카테고리의 한글명 반환
    top_categories = []
    for category, _ in sorted_categories[:3]:
        top_categories.append(category)  # 이미 한글이므로 그대로 사용
    
    # 부족한 경우 '기타'로 채우기
    while len(top_categories) < 3:
        top_categories.append('기타')
    
    return {
        'categories': top_categories,  # 상위 3개 한글 카테고리
        'description': description,
        'all_categories': [(cat, pct) for cat, pct in sorted_categories[:3]]
    }

# 이미지를 ImgBB 서비스에 업로드하고 URL을 반환하는 함수
def upload_image_to_imgbb(img_base64, api_key):
    url = "https://api.imgbb.com/1/upload"
    payload = {
        "key": api_key,
        "image": img_base64
    }
    
    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"요청 중 오류 발생: {e}")
        return None

    if response.status_code == 200:
        try:
            return response.json()["data"]["url"]
        except KeyError:
            print("예상치 못한 응답 형식입니다.")
            print(f"응답 내용: {response.text}")
            return None
    else:
        print(f"이미지 업로드 중 오류 발생: {response.status_code}")
        print(f"응답 내용: {response.text}")
        return None

# 이미지를 다운로드하고 정사각형 형태로 크롭하는 함수
# 검은색 배경을 제거하고 실제 콘텐츠 영역만 추출
def download_and_crop_image(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            img = Image.open(BytesIO(response.content))
            
            # 이미지를 numpy 배열로 변환
            img_array = np.array(img)
            
            # 알파 채널이 있다면 제거
            if img_array.shape[-1] == 4:
                img_array = img_array[:, :, :3]
            
            # 검정색 배경 감지 (RGB 값이 모두 30 이하인 경우)
            is_black = np.all(img_array < 30, axis=2)
            
            # 검정색이 아닌 실제 내용이 있는 영역 찾기
            rows = np.any(~is_black, axis=1)
            cols = np.any(~is_black, axis=0)
            
            # 실제 내용이 있는 영역의 경계 찾기
            try:
                y_min, y_max = np.where(rows)[0][[0, -1]]
                x_min, x_max = np.where(cols)[0][[0, -1]]
            except IndexError:
                # 내용을 찾지 못한 경우 원본 크기 사용
                y_min, y_max = 0, img_array.shape[0]
                x_min, x_max = 0, img_array.shape[1]
            
            # 크롭 실행
            cropped_img = img.crop((x_min, y_min, x_max, y_max))
            
            # 정사각형으로 만들기
            width, height = cropped_img.size
            size = max(width, height)
            
            # 새로운 정사각형 이미지 생성 (흰색 배경)
            square_img = Image.new('RGB', (size, size), (255, 255, 255))
            
            # 중앙에 배치
            paste_x = (size - width) // 2
            paste_y = (size - height) // 2
            square_img.paste(cropped_img, (paste_x, paste_y))
            
            # 최종 크기로 리사이즈
            target_size = 800
            square_img = square_img.resize((target_size, target_size), Image.Resampling.LANCZOS)
            
            return square_img
            
        except (requests.exceptions.RequestException, IOError) as e:
            if attempt == max_retries - 1:
                print(f"이미지 처리 중 오류 발생 (최종): {e}")
                raise
            else:
                print(f"이미지 처리 중 오류 발생 (재시도 {attempt + 1}/{max_retries}): {e}")
                time.sleep(2)

# 여러 이미지를 그리드 형태로 배치하는 함수
# rows x cols 크기의 정사각형 그리드 생성
def create_grid(images, rows=6, cols=3):
    """
    이미지들을 정사각형 그리드 형태로 배치합니다.
    """
    # 모든 이미지를 동일한 정사각형 크기로 리사이즈
    cell_size = 800  # 각 셀의 크기
    resized_images = []
    
    for img in images:
        # 각 이미지를 정사각형으로 리사이즈
        if img.size[0] != cell_size or img.size[1] != cell_size:
            img = img.resize((cell_size, cell_size), Image.Resampling.LANCZOS)
        resized_images.append(img)
    
    # 그리드 이미지 생성 (가로: cell_size * cols, 세로: cell_size * rows)
    grid = Image.new('RGB', (cell_size * cols, cell_size * rows), (255, 255, 255))
    
    # 이미지 배치
    for idx, img in enumerate(resized_images):
        row = idx // cols
        col = idx % cols
        grid.paste(img, (col * cell_size, row * cell_size))
    
    return grid

# 지정된 수의 이미지가 로드될 때까지 페이지를 스크롤하는 함수
def scroll_until_images(driver, target_count=18):
    """
    지정된 수의 이미지가 로드될 때까지 스크롤합니다.
    """
    INITIAL_WAIT = 5  # URL 접속 후 초기 대기 시간
    SCROLL_PAUSE_TIME = 2.5  # 스크롤 간 대기 시간
    
    print("페이지 로딩 대기 중...")
    time.sleep(INITIAL_WAIT)
    
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    while True:
        # 현재 로드된 이미지 수 확인
        images = driver.find_elements(By.CSS_SELECTOR, "._aagv img")
        print(f"현재 로드된 이미지 수: {len(images)}")
        
        if len(images) >= target_count:
            print("목표 이미지 수에 도달했습니다.")
            break
            
        # 페이지를 조금씩 스크롤
        current_height = driver.execute_script("return window.pageYOffset")
        scroll_step = 800  # 한 번에 스크롤할 픽셀 수
        driver.execute_script(f"window.scrollTo(0, {current_height + scroll_step});")
        time.sleep(SCROLL_PAUSE_TIME)
        
        # 새로운 높이와 이전 높이 비교
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            print("더 이상 스크롤할 수 없습니다.")
            break
        last_height = new_height

# JSON 파일의 데이터를 업데이트하는 함수
# 카테고리, 이미지 URL, 릴스 평균 조회수 정보를 갱신
def update_json_file(profile_link, categories, percentages, image_url, reels_avg_views):
    try:
        json_file_path = os.path.join(os.path.dirname(__file__), '2-2_influencer_processing_data.json')
        
        # JSON 파일 읽기
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 해당 profile_link를 가진 항목 찾아 업데이트
        for item in data:
            if item['profile_link'] == profile_link:
                # 카테고리와 퍼센티지를 포함한 문자열 생성
                category_strings = []
                for cat, pct in zip(categories, percentages):
                    category_strings.append(f"{cat}({pct:.0f}%)")
                category_string = ','.join(category_strings)
                
                # 변경된 필드명으로 업데이트
                item['category'] = category_string
                item['image_url'] = image_url
                item['reels_views(15)'] = int(reels_avg_views)
                break
        
        # JSON 파일 덮어쓰기
        with open(json_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
    except Exception as e:
        print(f'JSON 파일 업데이트 실패: {str(e)}')

# 릴스의 조회수를 수집하고 평균을 계산하는 함수
# 상하위 20%를 제외한 평균 또는 전체 평균 계산
def get_reels_views():
    views_list = []
    
    # 동적 대기 추가
    wait = WebDriverWait(driver, 20)
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "._aajy span.html-span")))
    except:
        print("릴스가 없거나 페이지 로딩에 실패했습니다.")
        return {'average_views': 0}

    # 스크롤 2-3회 시도
    for _ in range(3):
        view_elements = driver.find_elements(By.CSS_SELECTOR, "._aajy span.html-span")
        if not view_elements:
            break
            
        # 페이지 스크롤
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
    
    # 릴스 조회수 수집
    view_elements = driver.find_elements(By.CSS_SELECTOR, "._aajy span.html-span")
    print(f"\n발견된 릴스 수: {len(view_elements)}개")
    
    if not view_elements:
        print("릴스를 찾을 수 없습니다.")
        return {'average_views': 0}
    
    for element in view_elements:
        try:
            text = element.text
            if '만' in text:
                number = float(text.replace('만', '')) * 10000
                views_list.append(int(number))
            else:
                number = int(text.replace(',', ''))
                views_list.append(number)
        except Exception as e:
            continue
    
    # 조회수 처리 로직 수정
    if len(views_list) == 0:
        print("릴스가 없습니다.")
        return {'average_views': 0}
    elif len(views_list) <= 5:  # 5개 이하인 경우
        # 전체 평균 계산
        avg_views = sum(views_list) / len(views_list)
        print(f"릴스 {len(views_list)}개의 전체 평균 계산")
    else:
        # 상위 20%와 하위 20%를 제외한 평균 계산
        sorted_views = sorted(views_list)
        cut_size = int(len(sorted_views) * 0.2)  # 상하위 20% 계산
        trimmed_views = sorted_views[cut_size:-cut_size] if cut_size > 0 else sorted_views
        avg_views = sum(trimmed_views) / len(trimmed_views)
        print(f"상하위 20% 제외한 {len(trimmed_views)}개의 평균 계산")
    
    print(f"전체 릴스 조회수: {views_list}")
    print(f"평균 조회수: {int(avg_views):,}회")
    
    return {'average_views': avg_views}

# 자연스러운 스크롤 동작을 수행하는 함수
# 70% 확률로 위로, 30% 확률로 아래로 스크롤
def natural_scroll(driver):
    """자연스러운 스크롤 동작을 수행합니다."""
    try:
        scroll_height = driver.execute_script("return document.body.scrollHeight")
        current_position = driver.execute_script("return window.pageYOffset")
        
        # 위로 스크롤(70%) 또는 아래로 스크롤(30%) 결정
        scroll_up = np.random.choice([True, False], p=[0.7, 0.3])
        
        if scroll_up:
            target_position = max(0, current_position - np.random.randint(300, 800))
        else:
            target_position = min(scroll_height, current_position + np.random.randint(300, 800))
            
        driver.execute_script(f"window.scrollTo(0, {target_position})")
        time.sleep(np.random.uniform(0.8, 2.0))
        
    except Exception as e:
        print(f"스크롤 중 오류 발생: {e}")

# 프로그램 실행 중 휴식을 취하는 함수
# medium: 1-12분, long: 30분-2시간 휴식
def take_break(driver, break_type="medium"):
    """휴식을 취하고 자연스러운 동작을 수행합니다."""
    if break_type == "medium":
        break_duration = np.random.randint(60, 720)  # 1-12분
        print(f"\n=== 중간 휴식 시작 ({break_duration}초) ===")
        
        start_time = time.time()
        while time.time() - start_time < break_duration:
            if np.random.random() < 0.4:  # 40% 확률로 스크롤
                natural_scroll(driver)
            time.sleep(np.random.uniform(3, 8))
            
    else:  # long break
        break_duration = np.random.randint(1800, 7200)  # 30분-2시간
        print(f"\n=== 대규모 휴식 시작 ({break_duration//60}분) ===")
        time.sleep(break_duration)
        
    print("=== 휴식 종료 ===\n")

# 메인 실행 로직
target_rows = get_instagram_urls()
if not target_rows:
    print("처리할 URL이 없습니다.")
    driver.quit()
    exit()

print("\n=== 분석 시작 ===")
for idx, row_data in enumerate(target_rows, 1):
    # 대규모 휴식 (80-100개 프로필마다)
    if idx > 1 and idx % np.random.randint(80, 101) == 0:
        take_break(driver, "long")
        
    # 중간 휴식 (15-25개 프로필마다)
    elif idx > 1 and idx % np.random.randint(15, 26) == 0:
        take_break(driver, "medium")
    
    print(f"\n[{idx}/{len(target_rows)}] {row_data['url']} 처리 중...")
    
    try:
        driver.get(row_data['url'])
        print("1. 페이지 접속 완료, 이미지 로딩 대기 중...")
        time.sleep(5)
        
        scroll_until_images(driver)
        images = driver.find_elements(By.CSS_SELECTOR, "._aagv img")
        print(f"2. 총 {len(images)}개의 이미지를 찾았습니다.")
        image_urls = [img.get_attribute('src') for img in images[:18]]
        print("3. 이미지 URL 추출 완료")
        
        # 이미지 그리드 생성 및 분석
        cropped_images = []
        for idx, url in enumerate(image_urls, 1):
            try:
                print(f"4. 이미지 처리 중: {idx}/18")
                img = download_and_crop_image(url)
                cropped_images.append(img)
            except Exception as e:
                print(f"이미지 {idx} 처리 실패, 다음 이미지로 진행합니다.")
                continue
        
        if len(cropped_images) >= 15:  # 최소 15개 이상의 이미지가 있으면 진행
            print("5. 이미지 그리드 생성 중...")
            grid = create_grid(cropped_images[:18])  # 최대 18개까지만 사용
            plt.figure(figsize=(12, 24))
            plt.imshow(grid)
            plt.axis('off')
            
            print("6. 이미지 저장 중...")
            buf = io.BytesIO()
            plt.savefig(buf, format='png', bbox_inches='tight', pad_inches=0.05, dpi=300)
            buf.seek(0)
            img_base64 = base64.b64encode(buf.getvalue()).decode('ascii')
            plt.savefig('instagram_grid.jpg', format='jpeg', bbox_inches='tight', pad_inches=0.05, dpi=300)
            plt.close()
            
            # ImgBB 업로드
            print("7. ImgBB에 업로드 중...")
            api_key = os.getenv('IMGBB_API_KEY')
            uploaded_url = upload_image_to_imgbb(img_base64, api_key)
            
            if uploaded_url:
                print(f"8. ImgBB 업로드 성공: {uploaded_url}")
                
                print("9. Vision API로 이미지 분석 중...")
                analysis_result = analyze_image_with_vision('instagram_grid.jpg', api_key, row_data['description'])
                
                # 릴스 조회수 분석
                reels_url = row_data['url'].rstrip('/') + '/reels/'
                print(f"10. 릴스 페이지 접속 중... ({reels_url})")
                driver.get(reels_url)
                time.sleep(5)
                
                reels_data = get_reels_views()
                reels_avg_views = reels_data['average_views']
                print(f"릴스 평균 조회수: {int(reels_avg_views):,}회")
                
                # JSON 파일 업데이트
                categories = analysis_result['categories']
                percentages = [pct for _, pct in analysis_result['all_categories']]
                update_json_file(row_data['url'], categories, percentages, uploaded_url, reels_avg_views)
                
                print("처리 완료!\n")
                
                # 상위 3개 카테고리 결과 출력
                print("\n상위 3개 카테고리 결과:")
                for category, percentage in analysis_result['all_categories']:
                    print(f"{category}: {percentage:.1f}%")
        
    except Exception as e:
        print(f"처리 중 오류 발생: {e}")
        continue

driver.quit()