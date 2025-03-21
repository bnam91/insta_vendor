# 스프레드시트? 혹은 몽고DB에 입력되게 할 것
# 중간에 끊겨도 이어서 할 수 있게끔 하기
# 현재 몇개의 브랜드를 했는지 각각 출력되게 하기

import webbrowser
import urllib.parse
import os
import time
import requests
import json
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import re
import pyperclip
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import base64
from selenium.webdriver.common.action_chains import ActionChains
from captchasolver import save_captcha_image, get_captcha_answer, input_captcha_answer, extract_seller_info, solve_captcha_and_get_info
import pandas as pd
import shutil
import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


# .env 파일 로드
load_dotenv()

# OpenAI API 키 설정 (.env 파일에서 가져오기)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# MongoDB 연결 설정
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))

# 데이터베이스 및 컬렉션 선택
db = client['insta09_database']
collection = db['brand_extract']

# 캐시 데이터 정리 함수
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

# 엑셀 파일에서 브랜드명 가져오기 부분을 MongoDB에서 가져오도록 수정
def get_brand_names_from_mongodb():
    try:
        # brand_extract 컬렉션에서 name 필드 가져오기
        brands = collection.find({}, {"name": 1})
        brand_names = [brand.get("name") for brand in brands if brand.get("name")]
        
        if brand_names:
            print(f"MongoDB에서 불러온 브랜드 수: {len(brand_names)}")
            print(f"첫 5개 브랜드: {brand_names[:5] if len(brand_names) >= 5 else brand_names}")
            return brand_names
        else:
            print("MongoDB에서 브랜드명을 찾을 수 없습니다.")
            return ["샤크닌자"]  # 기본값
    except Exception as e:
        print(f"MongoDB 브랜드 정보 조회 오류: {e}")
        return ["샤크닌자"]  # 기본값

# 페이지 끝까지 스크롤 다운하는 함수 - 일반적인 스크롤 방법만 사용
def scroll_to_bottom():
    print("페이지 스크롤 시작...")
    
    try:
        # 처음 페이지 높이 가져오기
        last_height = browser.execute_script("return document.body.scrollHeight")
        
        # 최대 15번 스크롤 시도 (무한 루프 방지)
        scroll_count = 0
        max_scrolls = 15
        
        while scroll_count < max_scrolls:
            # 페이지 끝까지 스크롤
            browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # 페이지 로딩 대기
            time.sleep(2)
            
            # 새로운 페이지 높이 가져오기
            new_height = browser.execute_script("return document.body.scrollHeight")
            
            # 더 이상 스크롤이 안 되면 멈춤
            if new_height == last_height:
                print("스크롤 완료 (높이 변화 없음).")
                break
            
            last_height = new_height
            scroll_count += 1
            print(f"스크롤 {scroll_count}/{max_scrolls}")
    
    except Exception as e:
        print(f"스크롤 방법 오류: {e}")
    
    print("스크롤 완료")
    return True

# 공식 홈페이지 링크 추출 함수
def extract_official_links(soup, brand_name):
    potential_links = []
    keywords = ['공식', '홈페이지', '스토어', '몰', '사이트', 'official', brand_name]
    
    # 제외할 URL 패턴 목록
    exclude_patterns = [
        'https://blog.naver.com/', 
        'https://shoppinglive.naver.com', 
        'namu.wiki',
        '/ns/home',
        'instagram',
        'youtube.com',
        'facebook.com',
        'twitter.com',
        'musinsa'
    ]
    
    # 모든 링크 검사
    for a_tag in soup.find_all('a', href=True):
        link_text = a_tag.text.lower()
        href = a_tag['href']
        
        # 제외 패턴이 URL에 포함되어 있는지 확인
        should_exclude = any(pattern in href for pattern in exclude_patterns)
        
        # 제외 패턴이 없고, 키워드가 텍스트나 href에 포함된 링크 찾기
        if not should_exclude and (any(keyword.lower() in link_text for keyword in keywords) or \
           any(keyword.lower() in href for keyword in keywords)):
            potential_links.append({
                'text': a_tag.text,
                'href': href,
                'score': sum(1 for keyword in keywords if keyword.lower() in link_text.lower() or keyword.lower() in href.lower())
            })
    
    # 점수에 따라 정렬
    potential_links.sort(key=lambda x: x['score'], reverse=True)
    return potential_links

# 링크 분석 함수
def analyze_links_with_gpt4omini(links, brand_name):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }
    
    links_text = "\n".join([f"{i+1}. 텍스트: {link['text']}, URL: {link['href']}" for i, link in enumerate(links)])
    
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {
                "role": "system",
                "content": "한국어로 답변해주세요. 브랜드의 공식 홈페이지 링크를 식별해주세요."
            },
            {
                "role": "user",
                "content": f"다음은 '{brand_name}' 브랜드의 공식 홈페이지로 추정되는 링크 목록입니다. 가장 가능성이 높은 공식 홈페이지 링크를 선택하고 그 이유를 설명해주세요:\n\n{links_text}"
            }
        ],
        "max_tokens": 150
    }
    
    response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
    return response.json()

# 전화번호 분석 함수 수정
def analyze_phone_with_gpt4omini(soup, brand_name, url):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }
    
    # 페이지 텍스트 중 일부만 추출 (너무 길면 토큰이 많이 소모됨)
    # 고객센터 관련 키워드가 포함된 부분 중심으로 추출
    keywords = ['고객센터', '고객지원', '전화번호', '콜센터', '상담전화', '고객상담', '문의', '상담', 'cs', 'contact', '상호명', '사업자', '주소', '이메일']
    relevant_texts = []
    
    for keyword in keywords:
        elements = soup.find_all(string=lambda string: string and keyword in string.lower())
        for element in elements:
            # 해당 요소와 상위 3개 레벨의 부모 요소 텍스트 추출
            current = element
            for _ in range(3):
                if current.parent:
                    relevant_texts.append(current.parent.get_text(" ", strip=True)[:300])  # 너무 길지 않게 300자로 제한
                    current = current.parent
                else:
                    break
    
    # 중복 제거하고 합치기
    all_text = "\n\n".join(list(set(relevant_texts)))
    
    # 페이지 전체 텍스트가 없으면 일부만 가져오기
    if not all_text:
        all_text = soup.get_text(" ", strip=True)[:1000]  # 처음 1000자만
    
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {
                "role": "system",
                "content": "한국어로 답변해주세요. 당신은 웹페이지에서 기업 정보를 추출하는 전문가입니다. 다음 형식의 JSON으로 응답해야 합니다:\n{\n\"company_name\": \"회사명\",\n\"customer_service_number\": \"전화번호\",\n\"business_address\": \"주소\",\n\"email\": \"이메일\"\n}\n모든 필드는 찾을 수 없으면 \"정보 없음\"으로 표시하세요."
            },
            {
                "role": "user",
                "content": f"다음은 '{brand_name}' 브랜드의 공식 홈페이지({url}) 내용 중 일부입니다. 이 내용에서 상호명, 고객센터/상담 전화번호, 주소, 이메일을 찾아 JSON 형식으로 응답해주세요.\n\n{all_text}"
            }
        ],
        "max_tokens": 200
    }
    
    response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
    return response.json()

# 스마트스토어 URL 처리 후, 브랜드명 일치 체크 함수 추가
def check_product_brand_match(browser, brand_name):
    print(f"\n상품명에 브랜드명 '{brand_name}' 포함 여부 확인 중...")
    
    # 페이지 내의 모든 상품 요소 추출
    soup = BeautifulSoup(browser.page_source, 'html.parser')
    product_items = soup.select('ul.wOWfwtMC_3 li')
    
    if not product_items:
        print("상품 목록을 찾을 수 없습니다.")
        return False, []
    
    total_products = len(product_items)
    brand_matched_products = []
    
    for item in product_items:
        try:
            product_title_elem = item.select_one('._26YxgX-Nu5')
            if not product_title_elem:
                continue
                
            product_title = product_title_elem.text.strip()
            
            # 가격 정보 추출
            price_elem = item.select_one('.zOuEHIx8DC ._2DywKu0J_8')
            price = price_elem.text.strip() + '원' if price_elem else "가격 정보 없음"
            
            link_elem = item.select_one('a._2id8yXpK_k')
            if link_elem and 'href' in link_elem.attrs:
                product_link = "https://smartstore.naver.com" + link_elem['href']
            else:
                product_link = "링크 없음"
            
            # 브랜드명이 상품명에 포함되어 있는지 확인
            if brand_name in product_title:
                brand_matched_products.append({
                    'title': product_title,
                    'price': price,
                    'link': product_link
                })
        except Exception as e:
            print(f"상품 정보 추출 중 오류: {str(e)}")
    
    # 브랜드명 포함 비율 계산
    if total_products > 0:
        match_ratio = len(brand_matched_products) / total_products * 100
        is_authentic = match_ratio >= 50  # 50% 이상이면 진짜 브랜드로 판단
        
        print(f"\n분석 결과:")
        print(f"- 총 상품 수: {total_products}개")
        print(f"- '{brand_name}' 포함 상품 수: {len(brand_matched_products)}개")
        print(f"- 브랜드명 포함 비율: {match_ratio:.1f}%")
        print(f"- 공식 브랜드 판정: {'예' if is_authentic else '아니오'} (기준: 50% 이상)")
        
        if is_authentic and brand_matched_products:
            print("\n브랜드명이 포함된 상품 목록:")
            for idx, product in enumerate(brand_matched_products, 1):
                print(f"{idx}. {product['title']}")
                print(f"   가격: {product['price']}")
                print(f"   링크: {product['link']}")
                
        return is_authentic, brand_matched_products
    else:
        print("상품이 없습니다.")
        return False, []
    
# 추출 정보를 MongoDB에 저장하는 함수 수정
def save_contact_info_to_mongodb(brand_name, contact_info):
    try:
        # 영어 키로 데이터 변환
        english_contact_info = {}
        key_mapping = {
            "브랜드명": "brand_name",
            "상호명": "company_name",
            "고객센터 번호": "customer_service_number",
            "사업장소재지": "business_address",
            "이메일주소": "email",
            "공식 홈페이지 URL": "official_website_url",
            "도메인유형": "domain_type",
            "결과": "result"
        }
        
        for k, v in contact_info.items():
            # "정보 없음"을 빈 문자열로 변환
            if v == "정보 없음":
                v = ""
                
            if k in key_mapping:
                english_contact_info[key_mapping[k]] = v
            else:
                english_contact_info[k] = v
        
        # 브랜드명으로 문서 찾아서 contact 필드를 객체로 설정 ($push 대신 $set 사용)
        result = collection.update_one(
            {"name": brand_name},
            {"$set": {"contact": english_contact_info}}
        )
        
        if result.modified_count > 0:
            print(f"'{brand_name}' 브랜드의 연락처 정보가 MongoDB에 성공적으로 저장되었습니다.")
        else:
            print(f"'{brand_name}' 브랜드를 찾을 수 없거나 정보 업데이트에 실패했습니다.")
        
        return result.modified_count > 0
    except Exception as e:
        print(f"MongoDB 저장 오류: {e}")
        return False

def process_brand(brand_name):
    extracted_info = {}  # 추출된 정보를 저장할 딕셔너리
    extracted_info["브랜드명"] = brand_name
    
    # 브라우저 초기화 코드 제거 (외부에서 정의)
    
    # 검색어를 URL 인코딩
    encoded_query = urllib.parse.quote(brand_name)

    # 네이버 검색 URL 생성
    search_url = f"https://search.naver.com/search.naver?where=nexearch&sm=top_hty&fbm=0&ie=utf8&query={encoded_query}"

    # 검색 URL로 이동
    browser.get(search_url)
    time.sleep(3)
    
    # 직접 링크 썸네일 확인 - 네이버 검색 결과에서 공식 홈페이지로 바로가기 요소
    direct_link_elements = browser.find_elements("css selector", ".direct_link_thumb")

    # direct_link_thumb 요소가 있는 경우
    if direct_link_elements and len(direct_link_elements) > 0:
        try:
            print("공식 홈페이지 바로가기 요소 발견, 클릭 시도 중...")
            
            # 요소가 보이도록 스크롤
            browser.execute_script("arguments[0].scrollIntoView(true);", direct_link_elements[0])
            time.sleep(1)  # 스크롤 후 잠시 대기
            
            # JavaScript로 클릭 시도
            browser.execute_script("arguments[0].click();", direct_link_elements[0])
            time.sleep(3)  # 페이지 로딩 대기
            
            # 새 탭으로 전환
            browser.switch_to.window(browser.window_handles[-1])
            
            # 현재 페이지의 HTML 가져오기 (클릭 후 이동한 페이지)
            page_source = browser.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # 여기서 바로 고객센터 전화번호 분석으로 넘어감
            best_url = browser.current_url
            extracted_info["공식 홈페이지 URL"] = best_url
            print(f"직접 이동한 공식 홈페이지 URL: {best_url}")
            
            # 'coupang.com'이 URL에 포함되어 있는지 확인
            if 'coupang.com' in best_url:
                print("쿠팡링크입니다. 확인해주세요")
                extracted_info["결과"] = "쿠팡링크"
                # 코드 완전히 종료
                return extracted_info  # 프로그램 종료
            
            # 'naver.com'이 URL에 포함되어 있는지 확인
            if 'naver.com' in best_url:
                print("이 URL은 네이버 도메인을 포함하고 있습니다.")
                extracted_info["도메인유형"] = "네이버"
            else:
                print("이 URL은 네이버 도메인을 포함하고 있지 않습니다.")
                extracted_info["도메인유형"] = "외부사이트"
        except Exception as e:
            print(f"바로가기 요소 클릭 중 오류 발생: {e}")
            print("대체 방법으로 링크 분석을 진행합니다...")
            # 오류 발생 시 아래의 링크 추출 방식으로 진행
            best_url = None
    else:
        print("공식 홈페이지 바로가기 요소가 없습니다. 스크롤 후 링크 추출을 시작합니다...")
        best_url = None

    if not best_url:
        # 페이지 끝까지 스크롤 다운
        scroll_to_bottom()
        
        # 현재 페이지의 HTML 가져오기
        page_source = browser.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # 공식 홈페이지 링크 추출
        official_links = extract_official_links(soup, brand_name)
        
        if official_links:
            print("\n공식 홈페이지 후보 링크:")
            for i, link in enumerate(official_links[:10]):  # 상위 10개 출력
                print(f"{i+1}. 텍스트: {link['text']}")
                print(f"   URL: {link['href']}")
                print(f"   점수: {link['score']}")
            
            # GPT-4o mini 모델을 사용하여 추출된 링크 분석
            gpt_mini_response = analyze_links_with_gpt4omini(official_links[:10], brand_name)  # 상위 10개 분석
            
            if "choices" in gpt_mini_response:
                print("\nGPT-4o mini 분석 결과:")
                gpt_analysis = gpt_mini_response["choices"][0]["message"]["content"]
                print(gpt_analysis)
                
                # 토큰 사용량 출력
                if "usage" in gpt_mini_response:
                    # 토큰 사용량 계산 코드는 그대로 유지
                    prompt_tokens = gpt_mini_response["usage"]["prompt_tokens"]
                    completion_tokens = gpt_mini_response["usage"]["completion_tokens"]
                    total_tokens = gpt_mini_response["usage"]["total_tokens"]
                    
                    # 토큰당 가격 (달러)
                    prompt_price_per_token = 0.00000015  # $0.15 / 1M tokens
                    completion_price_per_token = 0.0000006  # $0.60 / 1M tokens
                    
                    # 달러 비용 계산
                    prompt_cost_usd = prompt_tokens * prompt_price_per_token
                    completion_cost_usd = completion_tokens * completion_price_per_token
                    total_cost_usd = prompt_cost_usd + completion_cost_usd
                    
                    # 한화로 환산 (환율: 1 USD = 약 1,350 KRW로 가정)
                    exchange_rate = 1350
                    total_cost_krw = total_cost_usd * exchange_rate
                    
                    print("\n토큰 사용량:")
                    print(f"프롬프트 토큰: {prompt_tokens}")
                    print(f"응답 토큰: {completion_tokens}")
                    print(f"총 토큰: {total_tokens}")
                    print(f"예상 비용: ${total_cost_usd:.6f} (약 {total_cost_krw:.2f}원)")
                
                # GPT 분석 결과에서 URL 추출 로직 개선
                best_url = None
                
                # 1. 마크다운 링크 패턴 찾기 [text](url) 형식
                markdown_link_pattern = re.compile(r'\[([^\]]+)\]\(([^)]+)\)')
                markdown_matches = markdown_link_pattern.findall(gpt_analysis)

                if markdown_matches:
                    # 마크다운 링크에서 실제 URL만 추출 (두 번째 그룹)
                    best_url = markdown_matches[0][1]  # (text, url) 튜플에서 url 부분만 가져옴
                    print(f"\nGPT 결과에서 마크다운 링크에서 추출한 URL: {best_url}")
                else:
                    # 2. 일반 URL 패턴 찾기
                    url_pattern = re.compile(r'https?://[^\s)\'"*]+')
                    found_urls = url_pattern.findall(gpt_analysis)
                    
                    if found_urls:
                        best_url = found_urls[0].rstrip('*.')
                        print(f"\nGPT 결과에서 직접 추출한 URL: {best_url}")
                        
                        # URL에 마크다운 문자가 있는지 확인
                        if '**' in best_url:
                            print("경고: URL에 마크다운 기호(**) 포함됨, 제거 필요")
                            best_url = best_url.replace('**', '')
                            print(f"정제된 URL: {best_url}")

                # 3. 위 방법으로 찾지 못한 경우
                if not best_url and official_links:
                    best_url = official_links[0]['href']
                    print(f"\nGPT 결과에서 URL을 찾지 못해 첫 번째 링크 사용: {best_url}")
                
                print(f"\n선택된 공식 홈페이지 URL: {best_url}")
            else:
                print("API 응답 오류:", gpt_mini_response)
                # 에러 발생 시 첫 번째 링크 사용
                best_url = official_links[0]['href'] if official_links else None
        else:
            print("공식 홈페이지 링크를 찾을 수 없습니다.")
            best_url = None

    # 가장 적절한 URL로 이동하여 전화번호 찾기
    if best_url:
        # 쿠팡 링크인지 확인
        if 'coupang.com' in best_url:
            print("쿠팡링크입니다. 확인해주세요")
            # 코드 완전히 종료
            return extracted_info  # 프로그램 종료
        
        # direct_link_thumb을 통해 이미 이동한 경우가 아니라면 URL로 이동
        if browser.current_url != best_url:
            print(f"\n선택된 공식 홈페이지 URL: {best_url}")
            print("브라우저에서 URL로 이동 중...")
            
            # 스마트스토어 URL인 경우 /category/ALL?cp=1 추가
            if 'smartstore.naver.com' in best_url:
                if not best_url.endswith('/'):
                    best_url += '/'
                best_url += 'category/ALL?cp=1'
                print(f"스마트스토어 카테고리 페이지로 수정된 URL: {best_url}")
            
            browser.get(best_url)
            time.sleep(5)  # 페이지 로딩을 위한 대기 시간
        
        # 네이버 브랜드 스토어 처리 추가
        if 'brand.naver.com' in browser.current_url:
            print("\n네이버 브랜드 스토어가 감지되었습니다!")
            print("페이지 스크롤 시작...")
            scroll_to_bottom()
            print("스크롤 완료")
            time.sleep(3)  # 스크롤 후 추가 대기
            
            # 판매자 정보 버튼 찾기 시도
            try:
                print("판매자 상세정보 버튼을 클릭합니다.")
                
                # 스마트스토어와 동일한 셀렉터 먼저 시도
                try:
                    seller_info_button = browser.find_element("css selector", "button._8Z05k1oEsY._nlog_click[data-shp-area-id='sellerinfo']")
                    
                    # 버튼이 보이도록 스크롤
                    browser.execute_script("arguments[0].scrollIntoView(true);", seller_info_button)
                    time.sleep(1)  # 스크롤 후 잠시 대기
                    
                    # 버튼 클릭
                    seller_info_button.click()
                    print("판매자 상세정보 버튼을 클릭했습니다.")
                    
                    # 새 창으로 전환
                    time.sleep(2)  # 새 창이 열릴 때까지 대기
                    
                    # 모든 창 핸들 가져오기
                    window_handles = browser.window_handles
                    
                    # 새 창으로 전환
                    if len(window_handles) > 1:
                        browser.switch_to.window(window_handles[-1])
                        print("새 창으로 전환했습니다.")
                        
                        # 판매자 정보 추출 시도
                        try:
                            time.sleep(2)  # 대기 시간 증가
                            
                            # 캡챠 이미지가 있는지 확인
                            captcha_exists = len(browser.find_elements("css selector", "img#captchaimg")) > 0
                            
                            if captcha_exists:
                                print("캡챠 이미지가 발견되었습니다. 캡챠 처리를 시작합니다.")
                                # 캡챠 처리 함수 호출
                                seller_info = solve_captcha_and_get_info(browser, OPENAI_API_KEY)
                            else:
                                print("캡챠 없이 바로 판매자 정보가 표시되었습니다.")
                                # 캡챠 없이 바로 판매자 정보 추출
                                seller_info = extract_seller_info(browser)
                            
                            if seller_info:
                                print("\n판매자 정보:")
                                for info_type, value in seller_info.items():
                                    if value:  # 값이 있는 경우만 출력
                                        print(f"{info_type}: {value}")
                                        # 추출된 정보를 딕셔너리에 저장
                                        extracted_info[info_type] = value
                                        
                                # 공식 브랜드가 아닌 경우 추가 경고 메시지 (스마트스토어 처리 시에만 해당)
                                if 'is_authentic' in locals() and not is_authentic:
                                    print("\n⚠️ 경고: 판매자 정보가 정확한 브랜드 공식 정보인지 확인이 필요합니다! ⚠️")
                            else:
                                print("판매자 정보를 추출할 수 없습니다.")
                        
                        except Exception as e:
                            print(f"판매자 정보 추출 중 오류 발생: {e}")
                            print("판매자 정보를 추출할 수 없습니다.")
                        
                        # 다시 원래 창으로 돌아가기
                        browser.switch_to.window(window_handles[0])
                
                except Exception as e:
                    print(f"스마트스토어 셀렉터로 판매자 정보 버튼 찾기 실패: {e}")
                    print("판매자 정보 버튼을 찾을 수 없습니다.")
            
            except Exception as e:
                print(f"네이버 브랜드 스토어 처리 중 오류 발생: {e}")
            print("⚠️ 일반적인 방법으로 페이지 분석을 진행합니다. ⚠️")
            
            # 오류 발생 시 기존 방식으로 계속 진행
            # 페이지 끝까지 스크롤
            print("페이지 끝까지 스크롤 중...")
            scroll_to_bottom()
            
            # 현재 페이지의 HTML 가져오기
            print("페이지 HTML 가져오는 중...")
            page_source = browser.page_source
            
            # BeautifulSoup으로 HTML 파싱
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # GPT-4o mini로 고객센터 전화번호 분석
            print("\nGPT-4o mini로 고객센터 전화번호 분석 중...")
            gpt_phone_response = analyze_phone_with_gpt4omini(soup, brand_name, best_url)
            
            if "choices" in gpt_phone_response:
                # 원본 출력 제거 (중복 방지)
                phone_analysis = gpt_phone_response["choices"][0]["message"]["content"]
                
                # 마크다운 코드 블록(```json) 제거
                clean_json_text = re.sub(r'```json\s*|\s*```', '', phone_analysis)
                
                # 결과를 JSON으로 파싱 시도
                try:
                    import json
                    info_json = json.loads(clean_json_text)
                    
                    # URL 정보 추가
                    info_json["공식 홈페이지 URL"] = best_url
                    
                    # 추출된 정보를 딕셔너리에 저장
                    for key, value in info_json.items():
                        extracted_info[key] = value
                    
                    # 예쁘게 포맷팅하여 출력
                    print("\n======= 추출된 정보 =======")
                    for key, value in info_json.items():
                        print(f"{key}: {value}")
                    print("===========================")
                    
                except json.JSONDecodeError as e:
                    print(f"JSON 파싱 실패: {e}")
                    print("원본 텍스트:")
                    print(phone_analysis)
                    print(f"공식 홈페이지 URL: {best_url}")
                
                # 토큰 사용량 출력
                if "usage" in gpt_phone_response:
                    prompt_tokens = gpt_phone_response["usage"]["prompt_tokens"]
                    completion_tokens = gpt_phone_response["usage"]["completion_tokens"]
                    total_tokens = gpt_phone_response["usage"]["total_tokens"]
                    
                    # 토큰당 가격 (달러)
                    prompt_price_per_token = 0.00000015  # $0.15 / 1M tokens
                    completion_price_per_token = 0.0000006  # $0.60 / 1M tokens
                    
                    # 달러 비용 계산
                    prompt_cost_usd = prompt_tokens * prompt_price_per_token
                    completion_cost_usd = completion_tokens * completion_price_per_token
                    total_cost_usd = prompt_cost_usd + completion_cost_usd
                    
                    # 한화로 환산 (환율: 1 USD = 약 1,350 KRW로 가정)
                    exchange_rate = 1350
                    total_cost_krw = total_cost_usd * exchange_rate
                    
                    print("\n토큰 사용량:")
                    print(f"프롬프트 토큰: {prompt_tokens}")
                    print(f"응답 토큰: {completion_tokens}")
                    print(f"총 토큰: {total_tokens}")
                    print(f"예상 비용: ${total_cost_usd:.6f} (약 {total_cost_krw:.2f}원)")
                else:
                    print("API 응답 오류:", gpt_phone_response)

        # 스마트스토어 URL인 경우 브랜드명 확인 추가
        elif 'smartstore.naver.com' in browser.current_url and '/category/' in browser.current_url:
            print("\n스마트스토어 URL이 감지되었습니다!")
            print("페이지 스크롤 시작...")
            scroll_to_bottom()
            print("스크롤 완료")
            time.sleep(3)  # 스크롤 후 추가 대기
            
            print(f"\n브랜드명 '{brand_name}' 포함 상품 분석 시작...")
            # 브랜드명 확인 함수 호출
            is_authentic, matched_products = check_product_brand_match(browser, brand_name)
            
            # 공식 브랜드 여부에 따른 메시지 출력
            if is_authentic:
                print(f"\n이 스마트스토어는 '{brand_name}' 공식 스토어로 확인되었습니다.")
            else:
                print(f"\n⚠️ 주의: 이 스마트스토어는 '{brand_name}' 공식 스토어가 아닐 수 있습니다! 반드시 확인이 필요합니다! ⚠️")
                print(f"브랜드명 포함 비율이 50% 미만이지만, 판매자 정보를 확인합니다.")
            
            # 공식 스토어 여부와 관계없이 판매자 상세정보 버튼 클릭 시도
            print("판매자 상세정보 버튼을 클릭합니다.")
            
            try:
                # 판매자 상세정보 버튼 찾기 - 스마트스토어용 셀렉터
                seller_info_button = browser.find_element("css selector", "button._8Z05k1oEsY._nlog_click[data-shp-area-id='sellerinfo']")
                
                # 버튼이 보이도록 스크롤
                browser.execute_script("arguments[0].scrollIntoView(true);", seller_info_button)
                time.sleep(1)  # 스크롤 후 잠시 대기
                
                # 버튼 클릭
                seller_info_button.click()
                print("판매자 상세정보 버튼을 클릭했습니다.")
                
                # 새 창으로 전환
                time.sleep(2)  # 새 창이 열릴 때까지 대기
                
                # 모든 창 핸들 가져오기
                window_handles = browser.window_handles
                
                # 새 창으로 전환
                if len(window_handles) > 1:
                    browser.switch_to.window(window_handles[-1])
                    print("새 창으로 전환했습니다.")
                    
                    # 판매자 정보 추출 시도
                    try:
                        time.sleep(2)  # 대기 시간 증가
                        
                        # 캡챠 이미지가 있는지 확인
                        captcha_exists = len(browser.find_elements("css selector", "img#captchaimg")) > 0
                        
                        if captcha_exists:
                            print("캡챠 이미지가 발견되었습니다. 캡챠 처리를 시작합니다.")
                            # 캡챠 처리 함수 호출
                            seller_info = solve_captcha_and_get_info(browser, OPENAI_API_KEY)
                        else:
                            print("캡챠 없이 바로 판매자 정보가 표시되었습니다.")
                            # 캡챠 없이 바로 판매자 정보 추출
                            seller_info = extract_seller_info(browser)
                        
                        if seller_info:
                            print("\n판매자 정보:")
                            for info_type, value in seller_info.items():
                                if value:  # 값이 있는 경우만 출력
                                    print(f"{info_type}: {value}")
                                    # 추출된 정보를 딕셔너리에 저장
                                    extracted_info[info_type] = value
                            
                            # 공식 브랜드가 아닌 경우 추가 경고 메시지 (스마트스토어 처리 시에만 해당)
                            if not is_authentic:
                                print("\n⚠️ 경고: 판매자 정보가 정확한 브랜드 공식 정보인지 확인이 필요합니다! ⚠️")
                        else:
                            print("판매자 정보를 추출할 수 없습니다.")
                    
                    except Exception as e:
                        print(f"판매자 정보 추출 중 오류 발생: {e}")
                        print("판매자 정보를 추출할 수 없습니다.")
                    
                    # 다시 원래 창으로 돌아가기
                    browser.switch_to.window(window_handles[0])
                
            except Exception as e:
                print(f"판매자 상세정보 버튼 클릭 중 오류 발생: {e}")
            print("⚠️ 일반적인 방법으로 페이지 분석을 진행합니다. ⚠️")
            
            # 오류 발생 시 기존 방식으로 계속 진행
            # 페이지 끝까지 스크롤
            print("페이지 끝까지 스크롤 중...")
            scroll_to_bottom()
            
            # 현재 페이지의 HTML 가져오기
            print("페이지 HTML 가져오는 중...")
            page_source = browser.page_source
            
            # BeautifulSoup으로 HTML 파싱
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # GPT-4o mini로 고객센터 전화번호 분석
            print("\nGPT-4o mini로 고객센터 전화번호 분석 중...")
            gpt_phone_response = analyze_phone_with_gpt4omini(soup, brand_name, best_url)
            
            if "choices" in gpt_phone_response:
                # 원본 출력 제거 (중복 방지)
                phone_analysis = gpt_phone_response["choices"][0]["message"]["content"]
                
                # 마크다운 코드 블록(```json) 제거
                clean_json_text = re.sub(r'```json\s*|\s*```', '', phone_analysis)
                
                # 결과를 JSON으로 파싱 시도
                try:
                    import json
                    info_json = json.loads(clean_json_text)
                    
                    # URL 정보 추가
                    info_json["공식 홈페이지 URL"] = best_url
                    
                    # 추출된 정보를 딕셔너리에 저장
                    for key, value in info_json.items():
                        extracted_info[key] = value
                    
                    # 예쁘게 포맷팅하여 출력
                    print("\n======= 추출된 정보 =======")
                    for key, value in info_json.items():
                        print(f"{key}: {value}")
                    print("===========================")
                    
                except json.JSONDecodeError as e:
                    print(f"JSON 파싱 실패: {e}")
                    print("원본 텍스트:")
                    print(phone_analysis)
                    print(f"공식 홈페이지 URL: {best_url}")
                
                # 토큰 사용량 출력
                if "usage" in gpt_phone_response:
                    prompt_tokens = gpt_phone_response["usage"]["prompt_tokens"]
                    completion_tokens = gpt_phone_response["usage"]["completion_tokens"]
                    total_tokens = gpt_phone_response["usage"]["total_tokens"]
                    
                    # 토큰당 가격 (달러)
                    prompt_price_per_token = 0.00000015  # $0.15 / 1M tokens
                    completion_price_per_token = 0.0000006  # $0.60 / 1M tokens
                    
                    # 달러 비용 계산
                    prompt_cost_usd = prompt_tokens * prompt_price_per_token
                    completion_cost_usd = completion_tokens * completion_price_per_token
                    total_cost_usd = prompt_cost_usd + completion_cost_usd
                    
                    # 한화로 환산 (환율: 1 USD = 약 1,350 KRW로 가정)
                    exchange_rate = 1350
                    total_cost_krw = total_cost_usd * exchange_rate
                    
                    print("\n토큰 사용량:")
                    print(f"프롬프트 토큰: {prompt_tokens}")
                    print(f"응답 토큰: {completion_tokens}")
                    print(f"총 토큰: {total_tokens}")
                    print(f"예상 비용: ${total_cost_usd:.6f} (약 {total_cost_krw:.2f}원)")
                else:
                    print("API 응답 오류:", gpt_phone_response)

        else:
            # 일반 웹사이트인 경우 기존 코드대로 처리
            # 페이지 끝까지 스크롤
            print("일반 웹사이트가 감지되었습니다. 페이지 끝까지 스크롤 중...")
            scroll_to_bottom()
            
            # 현재 페이지의 HTML 가져오기
            print("페이지 HTML 가져오는 중...")
            page_source = browser.page_source
            
            # BeautifulSoup으로 HTML 파싱
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # GPT-4o mini로 고객센터 전화번호 분석
            print("\nGPT-4o mini로 고객센터 전화번호 분석 중...")
            gpt_phone_response = analyze_phone_with_gpt4omini(soup, brand_name, best_url)
            
            if "choices" in gpt_phone_response:
                # 원본 출력 제거 (중복 방지)
                phone_analysis = gpt_phone_response["choices"][0]["message"]["content"]
                
                # 마크다운 코드 블록(```json) 제거
                clean_json_text = re.sub(r'```json\s*|\s*```', '', phone_analysis)
                
                # 결과를 JSON으로 파싱 시도
                try:
                    import json
                    info_json = json.loads(clean_json_text)
                    
                    # URL 정보 추가
                    info_json["공식 홈페이지 URL"] = best_url
                    
                    # 추출된 정보를 딕셔너리에 저장
                    for key, value in info_json.items():
                        extracted_info[key] = value
                    
                    # 예쁘게 포맷팅하여 출력
                    print("\n======= 추출된 정보 =======")
                    for key, value in info_json.items():
                        print(f"{key}: {value}")
                    print("===========================")
                    
                except json.JSONDecodeError as e:
                    print(f"JSON 파싱 실패: {e}")
                    print("원본 텍스트:")
                    print(phone_analysis)
                    print(f"공식 홈페이지 URL: {best_url}")
                
                # 토큰 사용량 출력
                if "usage" in gpt_phone_response:
                    prompt_tokens = gpt_phone_response["usage"]["prompt_tokens"]
                    completion_tokens = gpt_phone_response["usage"]["completion_tokens"]
                    total_tokens = gpt_phone_response["usage"]["total_tokens"]
                    
                    # 토큰당 가격 (달러)
                    prompt_price_per_token = 0.00000015  # $0.15 / 1M tokens
                    completion_price_per_token = 0.0000006  # $0.60 / 1M tokens
                    
                    # 달러 비용 계산
                    prompt_cost_usd = prompt_tokens * prompt_price_per_token
                    completion_cost_usd = completion_tokens * completion_price_per_token
                    total_cost_usd = prompt_cost_usd + completion_cost_usd
                    
                    # 한화로 환산 (환율: 1 USD = 약 1,350 KRW로 가정)
                    exchange_rate = 1350
                    total_cost_krw = total_cost_usd * exchange_rate
                    
                    print("\n토큰 사용량:")
                    print(f"프롬프트 토큰: {prompt_tokens}")
                    print(f"응답 토큰: {completion_tokens}")
                    print(f"총 토큰: {total_tokens}")
                    print(f"예상 비용: ${total_cost_usd:.6f} (약 {total_cost_krw:.2f}원)")
                else:
                    print("API 응답 오류:", gpt_phone_response)

        # 사용자가 확인할 시간 제공
        print("\n확인 완료되었습니다. 다음 브랜드로 진행합니다.")

    # CAPTCHA 처리 시작 - 조건부 실행 추가
    try:
        # 먼저 CAPTCHA 이미지 요소가 존재하는지 확인
        captcha_exists = len(browser.find_elements(By.ID, 'captchaimg')) > 0
        
        if captcha_exists:
            print("CAPTCHA 이미지를 발견했습니다. CAPTCHA 처리를 시작합니다.")
            
            # 이미지 다운로드
            img_element = browser.find_element(By.ID, 'captchaimg')
            img_src = img_element.get_attribute('src')

            # 이미지가 base64로 인코딩되어 있는 경우
            if img_src.startswith('data:image'):
                # base64 데이터 추출
                img_data = img_src.split(',')[1]
                img_data = base64.b64decode(img_data)

                # 디렉토리 생성
                os.makedirs('agent_cature', exist_ok=True)

                # 이미지 저장
                with open('agent_cature/captcha.png', 'wb') as f:
                    f.write(img_data)

            # 터미널에 텍스트 출력
            question_element = browser.find_element(By.ID, 'captcha_info')
            question_text = question_element.text
            print(question_text)

            # GPT-4o에 이미지 전송하여 답변 얻기
            def get_captcha_answer(image_path, question):
                # 이미지를 base64로 인코딩
                with open(image_path, "rb") as image_file:
                    encoded_image = base64.b64encode(image_file.read()).decode('utf-8')
                
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {OPENAI_API_KEY}"
                }
                
                payload = {
                    "model": "gpt-4o",
                    "messages": [
                        {
                            "role": "system",
                            "content": "당신은 이미지를 정확하게 해석하는 AI 도우미입니다. 이미지에 표시된 내용을 보고 질문에 대한 정확한 답변만 숫자 혹은 단어로 제공해주세요."
                        },
                        {
                            "role": "user",
                            "content": [
                                {"type": "text", "text": f"다음 이미지를 보고 질문에 답해주세요. 질문: {question}. 필요한 내용만 정확히 답변해주세요."},
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": f"data:image/png;base64,{encoded_image}"
                                    }
                                }
                            ]
                        }
                    ],
                    "max_tokens": 100
                }
                
                response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
                result = response.json()
                
                if "choices" in result:
                    answer = result["choices"][0]["message"]["content"].strip()
                    
                    # 토큰 사용량 계산 및 출력
                    if "usage" in result:
                        # 토큰 사용량 계산
                        prompt_tokens = result["usage"]["prompt_tokens"]
                        completion_tokens = result["usage"]["completion_tokens"]
                        total_tokens = result["usage"]["total_tokens"]
                        
                        # 토큰당 가격 (달러) - GPT-4o 가격
                        prompt_price_per_token = 0.00001  # $0.01 / 1K tokens
                        completion_price_per_token = 0.00003  # $0.03 / 1K tokens
                        
                        # 달러 비용 계산
                        prompt_cost_usd = prompt_tokens * prompt_price_per_token
                        completion_cost_usd = completion_tokens * completion_price_per_token
                        total_cost_usd = prompt_cost_usd + completion_cost_usd
                        
                        # 한화로 환산 (환율: 1 USD = 약 1,350 KRW로 가정)
                        exchange_rate = 1350
                        total_cost_krw = total_cost_usd * exchange_rate
                        
                        print("\nGPT-4o 캡챠 토큰 사용량:")
                        print(f"프롬프트 토큰: {prompt_tokens}")
                        print(f"응답 토큰: {completion_tokens}")
                        print(f"총 토큰: {total_tokens}")
                        print(f"예상 비용: ${total_cost_usd:.6f} (약 {total_cost_krw:.2f}원)")
                    
                    # 숫자만 추출
                    numeric_answer = ''.join(filter(str.isdigit, answer))
                    return numeric_answer if numeric_answer else answer
                else:
                    print("API 응답 오류:", result)
                    return "3"  # 오류 시 기본값

            # 캡차 답변 획득
            captcha_answer = get_captcha_answer('agent_cature/captcha.png', question_text)
            print(f"GPT가 제공한 캡차 답변: {captcha_answer}")

            # 정답 입력 및 확인 버튼 클릭
            input_element = browser.find_element(By.ID, 'captcha')
            input_element.send_keys(captcha_answer)  # GPT가 제공한 답변 사용

            submit_button = browser.find_element(By.CSS_SELECTOR, 'button[type="submit"]')
            submit_button.click()
        else:
            print("이 페이지에는 CAPTCHA가 없습니다. CAPTCHA 처리를 건너뜁니다.")
        
    except Exception as e:
        print(f"CAPTCHA 처리 중 오류 발생: {e}")
        print("CAPTCHA 처리를 건너뛰고 계속 진행합니다.")

    # 추출된 정보 반환
    return extracted_info

# 메인 코드 실행
if __name__ == "__main__":
    # MongoDB에서 브랜드명 가져오기
    brand_names = get_brand_names_from_mongodb()
    
    # 시작 인덱스 설정
    start_index = 0
    
    if start_index < len(brand_names):
        # 브라우저 설정 - 이 부분을 함수 밖으로 이동
        user_data_dir = os.path.join(os.getcwd(), "naver_user_data", "bnam91")
        os.makedirs(user_data_dir, exist_ok=True)

        # 캐시 정리 (로그인 정보 유지)
        clear_chrome_data(user_data_dir)

        chrome_options = Options()
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("detach", True)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        chrome_options.add_argument(f"user-data-dir={user_data_dir}")
        chrome_options.add_argument("--disable-application-cache")
        chrome_options.add_argument("--disable-cache")
        chrome_options.headless = False

        browser = webdriver.Chrome(options=chrome_options)
        
        # 결과를 저장할 리스트
        results = []

        # 건너뛸 브랜드명 리스트
        skip_brands = ['Unknown', 'Unkonwn', '확인필요', 'Unspecified Brand']

        # A1부터 모든 브랜드 처리
        for idx, brand_name in enumerate(brand_names[start_index:]):
            # 건너뛸 브랜드명인지 확인
            if brand_name in skip_brands or brand_name.strip() == '':
                print(f"\n===== 브랜드: {brand_name} - 처리 건너뜀 =====\n")
                # 빈 정보를 결과에 추가 (건너뛰었음을 표시)
                results.append({"브랜드명": brand_name, "결과": "처리 건너뜀"})
                continue
                
            print(f"\n===== 브랜드: {brand_name} 처리 시작 =====\n")
            brand_info = process_brand(brand_name)
            results.append(brand_info)  # 결과 저장
            
            # MongoDB에 결과 저장
            save_contact_info_to_mongodb(brand_name, brand_info)
            
            print(f"\n===== 브랜드: {brand_name} 처리 완료 =====\n")
            
            # 매 5개 브랜드마다 중간 저장 (Excel 저장 코드는 제거했으므로 간단한 로그만 출력)
            if (idx + 1) % 5 == 0:
                print(f"\n중간 진행 상황: {idx + 1}개 브랜드 처리됨\n")

        print("모든 브랜드 처리 완료")
        input("종료하려면 Enter 키를 누르세요...")
    else:
        print("처리할 브랜드가 없습니다.")