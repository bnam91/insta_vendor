"""
인스타그램 피드 분석 스크립트

데이터베이스 구조:
1. 01_test_newfeed_crawl_data (컬렉션)
   - 크롤링된 인스타그램 피드 원본 데이터
   - 포함 정보: 게시물 ID, 본문 내용, 작성일, 작성자 정보 등
   - 분석 결과 필드: 공구 여부, 상품명, 브랜드명, 공구 시작일/종료일, 상품 카테고리

2. 02_test_influencer_data (컬렉션)
   - 인플루언서 정보 데이터
   - 포함 정보: 인플루언서 ID, 공구유무(09_is), 브랜드/상품 정보
   - 브랜드별 상품 이력 관리 (20일 이내 중복 체크)
   - 상품 카테고리 정보 포함

3. 08_test_brand_category_data (컬렉션)
   - 브랜드 카테고리 관리 데이터
   - 포함 정보: 브랜드명, 카테고리 분류 정보, 별칭(aliases), 레벨, 상태

로그 파일:
1. unregistered_influencers.log
   - 미등록 인플루언서 정보 기록
   - 포함 정보: 발견 시간, 인플루언서명, 게시물 링크

2. influencer_update_errors.log
   - 데이터 처리 중 발생하는 오류 로그
   - 포함 정보: 시간, 인플루언서명, 오류 내용

3. product_similarity.log
   - 상품 유사도 처리 로그
   - 포함 정보: 유사도 비교 과정, 결과, API 응답 정보

4. brand_normalization.log
   - 브랜드 정규화 처리 로그
   - 포함 정보: 브랜드 유사도 검사 결과, 병합 과정, 새 브랜드 등록 정보

실행 결과:
1. 콘솔 출력
   - 총 분석할 게시글 수 표시
   - 각 게시물 분석 진행 상황 (순차적으로 표시)
   - 중복 상품 발견 시 알림
   - 카테고리 분석 결과 표시
   - 에러 발생 시 해당 게시물 ID와 에러 내용
   - 전체 분석 완료 메시지

2. MongoDB 데이터 업데이트
   예시) 01_test_newfeed_crawl_data:
   {
     "author": "인플루언서명",
     "content": "게시물 내용",
     "cr_at": "2024-03-15T14:30:00",
     "09_feed": "공구오픈",
     "09_item": "상품명",
     "09_brand": "브랜드명",
     "open_date": "2024-03-15",
     "end_date": "2024-03-20",
     "09_item_category": "🍽주방용품&식기",
     "09_item_category_2": "도마,조리도구,식기세트",
     "processed": true
   }

3. 인플루언서 데이터 구조
   예시) 02_test_influencer_data:
   {
     "username": "인플루언서명",
     "09_is": "Y",
     "brand": [
       {
         "name": "브랜드명",
         "category": "카테고리",
         "products": [
           {
             "item": "상품명",
             "type": "공구오픈",
             "category": "🍽주방용품&식기",
             "category2": "도마,조리도구,식기세트",
             "mentioned_date": "2024-03-15T14:30:00",
             "expected_date": "2024-03-15",
             "end_date": "2024-03-20",
             "item_feed_link": "게시물링크",
             "preserve": ""
           }
         ]
       }
     ]
   }

기능:
1. MongoDB에서 인스타그램 피드 데이터를 읽어옴
2. Claude AI를 사용하여 각 게시물 분석:
   - 공구 게시물 여부 판단 (공구예고/공구오픈/공구리마인드/확인필요/N)
   - 상품명, 브랜드명 추출
   - 공구 시작일/종료일 추출
3. Gemini AI를 사용하여:
   - 각 게시물 1회 분석 후 결과 선택
   - 상품 카테고리 및 서브 카테고리 분류
4. OpenAI API를 사용한 상품 유사도 측정:
   - 20일 이내 유사 상품 중복 체크
   - 70% 이상 유사도 시 중복으로 판단
5. 브랜드 정규화 처리:
   - Jaro-Winkler 알고리즘을 사용한 브랜드명 유사도 측정
   - 유사 브랜드 자동 병합 및 별칭 관리
   - 새로운 브랜드 자동 등록
6. 분석 결과를 MongoDB에 자동 업데이트
7. 인플루언서 데이터 자동 업데이트

데이터 처리 규칙:
1. 공구 분류:
   - 공구예고: 향후 공구 예정 게시물
   - 공구오픈: 현재 진행 중인 공구
   - 공구리마인드: 마감 임박 공구
   - 확인필요: 공구 여부 불명확
   - N: 공구 아님

2. 날짜 처리:
   - YYYY-MM-DD 형식으로 저장
   - '당일' 표시는 게시물 작성일로 변환
   - 연도 미지정시 현재 연도 사용

3. 브랜드 처리:
   - 브랜드명 유사도 0.85 이상 시 유사 브랜드로 판단
   - 유사 브랜드 발견 시 자동 병합 및 별칭 통합
   - 대표 브랜드 선정 기준: 별칭 수, 한글 포함, 공백 없음, 이름 길이
   - 미등록 브랜드 자동 등록 (status: 'ready')
   - 복수 브랜드인 경우 '복합상품'으로 처리

4. 상품 중복 체크:
   - 동일 상품 20일 이내 중복 등록 방지
   - OpenAI API를 통한 상품명 유사도 측정
   - 70% 이상 유사도 시 중복으로 판단

5. 카테고리 분류:
   - OpenAI를 사용하여 상품 카테고리 분석
   - 주 카테고리(이모지 포함)와 서브 카테고리 구분
   - 주방용품, 생활용품, 식품, 뷰티, 유아, 의류, 전자제품 등 분류

오류 처리:
- 개별 게시물 처리 실패시 다음 게시물로 진행
- 오류 상세 내용 로그 파일에 기록
- 미등록 인플루언서 별도 로그 관리
- API 호출 오류 시 기본값 반환

주의사항:
- API 호출 제한 방지를 위한 딜레이 포함
- 이미 처리된 데이터는 건너뜀
- 브랜드명 누락시 '확인필요'로 설정
- 유사도 측정 API 타임아웃 30초 설정

데이터베이스:
- MongoDB Atlas 사용
- 데이터베이스명: insta09_database
- 컬렉션:
  * 01_test_newfeed_crawl_data: 크롤링된 피드 데이터
  * 02_test_influencer_data: 인플루언서 정보
  * 08_test_brand_category_data: 브랜드 카테고리 관리

사용 API:
- Claude API: 게시물 내용 분석 (공구 여부, 상품명, 브랜드명, 날짜 추출)
- Gemini API: 상품 카테고리 분석 (주 카테고리, 서브 카테고리 분류)
- OpenAI API: 상품명 유사도 측정 (중복 체크)
"""


import anthropic
import time
import json
from datetime import datetime
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import re
import openai
from jellyfish import jaro_winkler_similarity
import logging

def get_mongodb_connection():
    uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri, server_api=ServerApi('1'))
    
    try:
        # 연결 테스트
        client.admin.command('ping')
        print("MongoDB 연결 성공!")
        
        # 데이터베이스 선택
        db = client['insta09_database']
        
        # 컬렉션 매핑
        collections = {
            'feeds': db['01_main_newfeed_crawl_data'],
            'influencers': db['02_main_influencer_data'],
            'brands': db['08_main_brand_category_data']
        }
        
        return client, collections
    except Exception as e:
        print(f"MongoDB 연결 실패: {str(e)}")
        raise

def update_influencer_data(item, collections):
    try:
        # 브랜드 카테고리 데이터 로드
        brands_collection = collections['brands']
        
        # 인플루언서 데이터 찾기
        influencers_collection = collections['influencers']
        influencer = influencers_collection.find_one({'username': item['author']})
        
        # 로그 디렉토리 설정
        log_dir = setup_log_directory()
        
        # 브랜드명 정규화 및 카테고리 찾기 함수
        def normalize_brand(brand_name):
            brand_info = brands_collection.find_one({
                '$or': [
                    {'name': brand_name},
                    {'aliases': brand_name}
                ]
            })
            
            if brand_info:
                return {
                    'name': brand_info['name'],
                    'category': brand_info.get('category', '')  # get 메서드로 안전하게 가져오기
                }
            
            # 브랜드가 없는 경우 새로 추가
            if brand_name and brand_name != '확인필요':
                new_brand = {
                    'name': brand_name,
                    'category': '',
                    'aliases': [],
                    'level': '',
                    'status': 'ready'
                }
                brands_collection.insert_one(new_brand)
                return {
                    'name': brand_name,
                    'category': ''
                }
            return {
                'name': brand_name,
                'category': ''
            }

        if not influencer:
            # 미등록 인플루언서 로그 기록
            log_message = f"[{item['cr_at']}] 미등록 인플루언서 발견: {item['author']} (게시물링크: {item['post_url']})\n"
            with open(os.path.join(log_dir, 'unregistered_influencers.log'), 'a', encoding='utf-8') as log_file:
                log_file.write(log_message)
            print(f"미등록 인플루언서 로그 기록: {item['author']}")
            return

        # 공구유무 업데이트
        if influencer.get('09_is') != 'Y':
            influencers_collection.update_one(
                {'username': item['author']},
                {'$set': {'09_is': 'Y'}}
            )

        brands = item['09_brand'].split(', ')
        for brand in brands:
            # 외부 normalize_brand 함수 사용, 작성자 정보 전달
            brand_info = normalize_brand(brand)
            normalized_brand_name = brand_info['name']
            
            # 브랜드 정보 업데이트
            brand_data = {
                'name': normalized_brand_name,
                'category': brand_info['category'],
                'products': [{
                    'item': item['09_item'],
                    'type': item['09_feed'],
                    'category': item.get('09_item_category', ''),
                    'category2': item.get('09_item_category_2', ''),  # 서브 카테고리 정보 추가
                    'mentioned_date': item['cr_at'],
                    'expected_date': item['open_date'],
                    'end_date': item['end_date'],
                    'item_feed_link': item['post_url'],
                    'preserve': ''
                }]
            }

            # 중복 체크 및 업데이트
            existing_brand = influencers_collection.find_one({
                'username': item['author'],
                'brand': {
                    '$elemMatch': {
                        'products': {
                            '$elemMatch': {
                                'mentioned_date': item['cr_at'],
                                'item_feed_link': item['post_url']
                            }
                        }
                    }
                }
            })

            if existing_brand:
                print(f"동일 게시글이 이미 존재합니다: {item['post_url']}")
                continue

            # 기존 브랜드 검색
            existing_brand = influencers_collection.find_one({
                'username': item['author'],
                'brand.name': normalized_brand_name
            })

            if existing_brand:
                # 중복 체크 로직 (20일 이내)
                products = None
                for b in existing_brand['brand']:
                    if b['name'] == normalized_brand_name:
                        products = b['products']
                        break
                
                if not products:
                    continue
                    
                product_exists = False
                
                # 유사도 처리 로그 시작
                with open(os.path.join(log_dir, 'product_similarity.log'), 'a', encoding='utf-8') as log_file:
                    log_file.write(f"\n===== 유사도 처리 시작: {item['author']} - {normalized_brand_name} - {item['09_item']} =====\n")
                
                for product in products:
                    existing_date = datetime.strptime(product['mentioned_date'].split('T')[0], '%Y-%m-%d')
                    new_date = datetime.strptime(item['cr_at'].split('T')[0], '%Y-%m-%d')
                    date_diff = abs((new_date - existing_date).days)
                    
                    # 날짜 차이가 20일 이내인 경우에만 유사도 검사
                    if date_diff <= 20:
                        # 유사도 처리 로그
                        with open(os.path.join(log_dir, 'product_similarity.log'), 'a', encoding='utf-8') as log_file:
                            log_file.write(f"비교 대상: {product['item']} (언급일: {product['mentioned_date']})\n")
                            log_file.write(f"날짜 차이: {date_diff}일\n")
                        
                        # 유사도 측정
                        similarity_score, input_tokens, output_tokens = calculate_similarity(product['item'], item['09_item'], log_dir)
                        
                        # 유사도 결과 로그
                        with open(os.path.join(log_dir, 'product_similarity.log'), 'a', encoding='utf-8') as log_file:
                            log_file.write(f"유사도 점수: {similarity_score}%\n")
                        
                        # 유사도가 70% 이상이면 중복으로 판단
                        if similarity_score >= 70:
                            product_exists = True
                            message = f"중복 상품 발견: {item['09_item']} - 기존:{existing_date.date()} 신규:{new_date.date()} (차이: {date_diff}일, 유사도: {similarity_score}%)"
                            print(message)
                            
                            # 중복 발견 로그
                            with open(os.path.join(log_dir, 'product_similarity.log'), 'a', encoding='utf-8') as log_file:
                                log_file.write(f"결과: {message}\n")
                            
                            break
                
                # 유사도 처리 로그 종료
                with open(os.path.join(log_dir, 'product_similarity.log'), 'a', encoding='utf-8') as log_file:
                    if not product_exists:
                        log_file.write("결과: 중복 상품 없음\n")
                    log_file.write(f"===== 유사도 처리 종료 =====\n")

                if not product_exists:
                    influencers_collection.update_one(
                        {'username': item['author'], 'brand.name': normalized_brand_name},
                        {'$push': {'brand.$.products': brand_data['products'][0]}}
                    )
            else:
                # 새 브랜드 추가
                influencers_collection.update_one(
                    {'username': item['author']},
                    {'$push': {'brand': brand_data}}
                )

        print(f"인플루언서 데이터 업데이트 완료: {item['author']}")

    except Exception as e:
        log_dir = setup_log_directory()
        error_message = f"[{item['cr_at']}] 오류 발생 - 인플루언서: {item['author']}, 오류: {str(e)}\n"
        with open(os.path.join(log_dir, 'influencer_update_errors.log'), 'a', encoding='utf-8') as error_file:
            error_file.write(error_message)
        print(f"인플루언서 데이터 업데이트 중 오류 발생: {str(e)}")

def calculate_similarity(item1, item2, log_dir=None):
    """두 상품의 유사도 계산"""
    try:
        # 로그 디렉토리가 전달되지 않은 경우 설정
        if log_dir is None:
            log_dir = setup_log_directory()
            
        # 유사도 처리 로그
        with open(os.path.join(log_dir, 'product_similarity.log'), 'a', encoding='utf-8') as log_file:
            log_file.write(f"API 요청 시작: {item1} vs {item2}\n")
        
        # OpenAI 클라이언트 초기화
        client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        
        # 유사도 측정 API 호출 (타임아웃 30초 설정)
        similarity_response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": """
                두 상품명의 유사도를 0에서 100 사이의 숫자로 평가해주세요.
                평가 기준:
                - 100: 완전히 동일한 상품
                - 90-99: 같은 상품이지만 표기법만 다른 경우
                - 80-89: 같은 상품이지만 브랜드명이나 부가 설명이 있는 경우
                - 70-79: 같은 상품이지만 에디션이나 옵션이 다른 경우
                - 0: 전혀 다른 상품
                숫자만 응답해주세요.
                """},
                {"role": "user", "content": f"첫 번째 상품명: {item1}\n두 번째 상품명: {item2}"}
            ],
            timeout=30  # 30초 타임아웃 설정
        )
        
        # 토큰 사용량 계산
        input_tokens = similarity_response.usage.prompt_tokens
        output_tokens = similarity_response.usage.completion_tokens
        
        # 응답에서 유사도 점수 추출
        similarity_score = int(similarity_response.choices[0].message.content.strip())
        
        # 유사도 처리 로그
        with open(os.path.join(log_dir, 'product_similarity.log'), 'a', encoding='utf-8') as log_file:
            log_file.write(f"API 응답 수신: 유사도 {similarity_score}%, 입력 토큰: {input_tokens}, 출력 토큰: {output_tokens}\n")
        
        return similarity_score, input_tokens, output_tokens
        
    except Exception as e:
        error_msg = f"유사도 측정 중 오류 발생: {str(e)}"
        print(error_msg)
        
        # 로그 디렉토리가 전달되지 않은 경우 설정
        if log_dir is None:
            log_dir = setup_log_directory()
            
        # 오류 로그
        with open(os.path.join(log_dir, 'product_similarity.log'), 'a', encoding='utf-8') as log_file:
            log_file.write(f"오류: {error_msg}\n")
        
        return 0, 0, 0  # 오류 발생 시 0 반환

def analyze_product_category(product_name):
    """
    OpenAI GPT 모델을 사용하여 상품명을 분석하고 적합한 카테고리를 반환합니다.
    
    Args:
        product_name (str): 분석할 상품명
        
    Returns:
        tuple: (주 카테고리, 서브 카테고리)
    """
    retry_count = 0
    retry_delay = 10  # 초기 대기 시간

    while True:  # 무한 루프 유지
        try:
            # OpenAI 클라이언트 초기화
            client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
            
            # 카테고리 정의
            categories = """
            🍽주방용품&식기
            도마,조리도구,식기세트 - 도마, 칼, 조리도구, 그릇, 접시, 컵, 수저, 식기세트
            냄비&프라이팬 - 냄비, 프라이팬, 웍, 찜기, 압력솥, 뚝배기
            주방가전 - 에어프라이어, 오븐, 토스터, 블렌더, 믹서기, 전기포트, 커피머신
            밀폐/보관 용기 - 밀폐용기, 보관용기, 유리용기, 스테인리스 용기, 진공용기
            기타 - 분류가 안된 것

            🛋생활용품&가전
            청소기&세척기 - 청소기, 로봇청소기, 핸디청소기, 스팀청소기, 세척기
            세탁/욕실용품 - 세제, 섬유유연제, 세탁세제, 주방세제, 고무장갑, 욕실용품, 수건
            조명&가구 - 조명, 스탠드, 책상, 의자, 침대, 소파, 테이블, 수납장
            침구&커튼 - 이불, 베개, 침대패드, 매트리스, 커튼, 블라인드, 러그, 카페트
            구강케어 - 치약, 칫솔, 구강세정제, 치실, 구강스프레이
            기타 - 분류가 안된 것
            
            🥦식품&건강식품
            건강음료&차 - 차, 건강음료, 콤부차, 식혜, 수제청, 과일청, 식초
            간편식&조미료 - 간편식, 즉석식품, 소스, 조미료, 양념, 곰탕, 국, 찌개
            스낵&간식 - 과자, 쿠키, 초콜릿, 젤리, 견과류, 그래놀라, 시리얼
            축산&수산물 - 육류, 해산물, 생선, 계란, 우유, 치즈, 요거트
            과일&신선식품 - 과일, 채소, 샐러드, 나물, 버섯
            건강보조제 - 비타민, 영양제, 프로바이오틱스, 콜라겐, 단백질 보충제
            기타 - 분류가 안된 것

            🧴뷰티&헬스
            헤어&바디 - 샴푸, 트리트먼트, 바디워시, 바디로션, 핸드크림, 바디스크럽
            스킨케어&화장품 - 스킨, 로션, 에센스, 크림, 마스크팩, 선크림, 메이크업
            헬스&피트니스 - 운동기구, 요가매트, 덤벨, 보조제, 프로틴, 다이어트식품
            기타 - 분류가 안된 것

            👶유아&교육
            유아가구 & 침구 - 유아침대, 유아책상, 유아의자, 유아이불, 유아베개
            교육&완구 - 책, 교구, 장난감, 퍼즐, 블록, 보드게임, 인형
            기타 - 분류가 안된 것

            👗의류&잡화
            의류&신발 - 옷, 의류, 패딩, 코트, 자켓, 티셔츠, 바지, 신발, 슬리퍼
            가방&액세서리 - 가방, 백팩, 지갑, 벨트, 모자, 스카프, 목걸이, 귀걸이
            기타 - 분류가 안된 것

            🚗기타
            전자기기 - TV, 스피커, 이어폰, 헤드폰, 충전기, 노트북, 태블릿
            기타 - 분류가 안된 것
            """
            
            # 프롬프트 작성
            prompt = f"""
            당신은 상품 카테고리 분류 전문가입니다. 정확하고 일관된 카테고리 분류가 당신의 주요 업무입니다.
            
            다음 상품명을 분석하여 가장 적합한 카테고리와 서브카테고리를 선택해주세요:
            
            상품명: {product_name}
            
            다음 카테고리 목록에서만 선택하세요:
            {categories}
            
            분류 규칙:
            1. 상품의 주요 기능과 용도를 기준으로 분류하세요.
            2. 여러 카테고리에 걸쳐있는 상품은 주된 용도를 기준으로 분류하세요.
            3. 브랜드명은 무시하고 상품 자체의 특성으로만 판단하세요.
            4. 상품명만으로 분류가 어렵거나 카테고리 목록에 명확히 포함되지 않는 경우 '기타' 카테고리를 사용하세요.
            5. 식품 관련 상품은 최대한 구체적인 식품 서브카테고리로 분류하세요.
            6. 분류가 안된다면 기타로 분류하세요.
            
            주 카테고리는 이모지가 포함된 대분류(예: 🍽주방용품&식기)를 선택하고,
            서브 카테고리는 해당 주 카테고리 아래의 소분류(예: 도마,조리도구,식기세트)를 선택하세요.
            
            예시:
            1. "트리쳐도마+진공밧드세트" → 🍽주방용품&식기 / 도마,조리도구,식기세트
            2. "프리미엄 니트릴 고무장갑" → 🛋생활용품&가전 / 세탁/욕실용품
            3. "포빙이불 (고밀도 모달/면 차렵이불, 침대패드, 베개커버)" → 🛋생활용품&가전 / 침구&커튼
            4. "세탁세제, 섬유유연제, 주방세제" → 🛋생활용품&가전 / 세탁/욕실용품
            5. "화장품 3종세트" → 🧴뷰티&헬스 / 스킨케어&화장품
            6. "클린톡 과일야채 주스" → 🥦식품&건강식품 / 건강음료&차
            7. "유기농 견과류 선물세트" → 🥦식품&건강식품 / 스낵&간식
            8. "한우 선물세트" → 🥦식품&건강식품 / 축산&수산물
            9. "멀티비타민" → 🥦식품&건강식품 / 건강보조제
            10. "요가매트와 덤벨 세트" → 🧴뷰티&헬스 / 헬스&피트니스
            11. "다용도 선물세트(여러 카테고리 상품 혼합)" → 🚗기타&전자제품 / 기타
            12. "유기농 엑스트라 버진 올리브 오일" → 🥦식품&건강식품 / 간편식&조미료
            
            응답 형식:
            {{
            "main_category": "이모지와 함께 주 카테고리명",
            "sub_category": "서브 카테고리명"
            }}
            
            JSON 형식으로만 응답해주세요. 예시 응답:
            {{
            "main_category": "🍽주방용품&식기",
            "sub_category": "도마,조리도구,식기세트"
            }}
            """
            
            # OpenAI API 호출
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "당신은 상품 카테고리 분류 전문가입니다. 정확하고 일관된 카테고리 분류가 당신의 주요 업무입니다."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.2
            )
            
            # 응답 파싱 - response.text 대신 올바른 속성 사용
            try:
                result = json.loads(response.choices[0].message.content)
                return result["main_category"], result["sub_category"]
            except json.JSONDecodeError:
                # JSON 파싱 실패 시 텍스트에서 정보 추출 시도
                text = response.choices[0].message.content
                main_match = re.search(r'"main_category":\s*"([^"]+)"', text)
                sub_match = re.search(r'"sub_category":\s*"([^"]+)"', text)
                
                if main_match and sub_match:
                    return main_match.group(1), sub_match.group(1)
                else:
                    print("응답에서 카테고리 정보를 추출할 수 없습니다.")
                    return "", ""
    
        except Exception as e:
            # API 할당량 초과 시 재시도 로직 추가
            if "429" in str(e):  # Resource exhausted 에러 코드
                retry_count += 1
                print(f"OpenAI API 할당량 초과. {retry_delay}초 후 재시도... (시도 {retry_count}번째)")
                time.sleep(retry_delay)  # retry_delay 변수 사용
                continue  # 루프 처음으로 돌아가 재시도
            else:
                # 다른 종류의 오류는 기존처럼 처리
                print(f"OpenAI API 호출 중 오류 발생: {str(e)}")
                return "", ""  # 오류 발생 시 빈 문자열 반환

def analyze_instagram_feed():
    try:
        # 환경 변수에서 API 키 로드
        load_dotenv()
        
        # OpenAI API 설정 (Gemini 대신 GPT-4o-mini 사용)
        openai.api_key = os.getenv('OPENAI_API_KEY')
        client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        
        # MongoDB 연결
        mongo_client, collections = get_mongodb_connection()
        feeds_collection = collections['feeds']
        
        # 처리되지 않은 피드 데이터 조회 (페이지네이션 방식으로 변경)
        batch_size = 50  # 한 번에 처리할 문서 수
        total_feeds = feeds_collection.count_documents({
            '$or': [
                {'processed': False},
                {'processed': {'$exists': False}}
            ]
        })
        
        print(f"총 {total_feeds}개의 게시글을 분석합니다...")
        
        processed_count = 0
        last_id = None
        
        while processed_count < total_feeds:
            # ID 기반 페이지네이션 쿼리
            query = {
                '$or': [
                    {'processed': False},
                    {'processed': {'$exists': False}}
                ]
            }
            
            if last_id:
                query['_id'] = {'$gt': last_id}
                
            # 배치 크기만큼만 조회
            batch = list(feeds_collection.find(query).sort('_id', 1).limit(batch_size))
            
            if not batch:
                break  # 더 이상 처리할 문서가 없음
                
            for i, item in enumerate(batch, start=processed_count+1):
                print(f"\n[{i}/{total_feeds}] {i}번 게시글 분석 중...")
                last_id = item['_id']  # 마지막 ID 저장
                
                try:
                    # API 할당량 초과 시 재시도 로직
                    retry_count = 0
                    wait_time = 10  # 초기 대기 시간 10초
                    
                    while True:  # 무한 루프로 변경
                        try:
                            # 이미 처리된 데이터 체크 로직
                            if item.get('09_feed'):
                                print(f"{i}번 게시글: 이미 처리된 데이터입니다. 건너뜁니다.")
                                feeds_collection.update_one(
                                    {'_id': item['_id']},
                                    {'$set': {'processed': True}}
                                )
                                break  # 처리 완료 시 루프 종료

                            # 본문 내용 확인
                            content = item.get('content')
                            author = item.get('author')

                            if not content or not content.strip():
                                if not author:  # author도 비어있는 경우
                                    print(f"본문 내용과 작성자가 비어있어 삭제합니다. (게시물 링크: {item['post_url']})")
                                    feeds_collection.delete_one({'_id': item['_id']})  # 도큐먼트 삭제
                                else:
                                    print(f"본문 내용이 비어있습니다. (작성자: {author}, 게시물 링크: {item['post_url']})")
                                
                                # 게시글 처리 상태 업데이트
                                feeds_collection.update_one(
                                    {'_id': item['_id']},
                                    {'$set': {'processed': True}}  # 처리 완료로 설정
                                )
                                break  # 다음 게시물로 넘어감
                            
                            print(f"{i}번 게시글: GPT-4o-mini 분석 요청 중...")
                            
                            # 프롬프트 작성
                            prompt = f"""
                            You are analyzing Instagram feed content. Extract information and respond in the following JSON format only:
                            {{
                                "is_group_buy": "공구예고"/"공구오픈"/"공구리마인드"/"확인필요"/"N",
                                "product_name": "Include all main product categories in the title",
                                "brand_name": "brand name here",
                                "start_date": "MM-DD format only if year is not specified",
                                "end_date": "MM-DD format only if year is not specified"
                            }}
                            
                            For group buy classification:
                            - "공구예고": Post announces future group buy with specific future date
                            - "공구오픈": Post announces group buy opening today or is currently open
                            - "공구리마인드": Post reminds of ongoing group buy or last call
                            - "확인필요": Unclear whether it's a group buy or needs verification
                            - "N": Not a group buy post
                            
                            Important indicators:
                            - 공구예고: "곧 오픈", "오픈 예정", "Coming soon", "준비중"
                            - 공구오픈: "OPEN", "오픈", "𝑶𝑷𝑬𝑵", "open", "시작", "오픈했어요"
                            - 공구리마인드: "마감임박", "오늘마감", "마지막", "재고 얼마없어요"
                            
                            For dates:
                            - If year is not specified in the content, only extract MM-DD
                            - If post mentions "OPEN", "오픈", "𝑶𝑷𝑬𝑵", "open" without specific date, mark as "당일"
                            - For shipping dates (e.g., "2/7일부터 순차 발송"), use post date as start_date
                            - Do not assume or add any year information
                            - Return empty string if no date is found
                            
                            Important indicators for group buy (공구):
                            - Keywords like "OPEN", "오픈", "𝑶𝑷𝑬𝑵", "open"
                            - "프로필 링크에서 구매"
                            - Detailed product information with pricing
                            - Limited time offer implications
                            - Comment inducement for purchase intention
                            - Specific purchase instructions or links
                            
                            Return "확인필요" when:
                            - Post contains some group buy indicators but lacks crucial information
                            - Unclear whether it's a product review or group buy
                            - Contains purchase-related keywords but no clear group buy format

                            For dates:
                            - If year is not specified in the content, only extract MM-DD
                            - If post mentions "OPEN" without specific date, mark as "당일"
                            - Do not assume or add any year information
                            - Return empty string if no date is found
                         
                            
                            Do not include any other text in your response.

                            Analyze the following Instagram post content:
                            {content}
                            """
                            
                            # GPT-4o-mini API 호출
                            response = client.chat.completions.create(
                                model="gpt-4o-mini",
                                messages=[
                                    {"role": "system", "content": "You are a content analyzer specializing in Instagram posts."},
                                    {"role": "user", "content": prompt}
                                ],
                                response_format={"type": "json_object"},
                                temperature=0.2
                            )
                            
                            try:
                                result = json.loads(response.choices[0].message.content)
                            except json.JSONDecodeError:
                                text = response.choices[0].message.content
                                json_match = re.search(r'({.*})', text.replace('\n', ''), re.DOTALL)
                                if json_match:
                                    try:
                                        result = json.loads(json_match.group(1))
                                    except:
                                        result = {
                                            "is_group_buy": "확인필요",
                                            "product_name": "",
                                            "brand_name": "",
                                            "start_date": "",
                                            "end_date": ""
                                        }
                                else:
                                    result = {
                                        "is_group_buy": "확인필요",
                                        "product_name": "",
                                        "brand_name": "",
                                        "start_date": "",
                                        "end_date": ""
                                    }

                            print(f"{i}번 게시글: 분석 결과 사용")
                            
                            # '당일' 처리
                            if result['start_date'] == '당일':
                                date_parts = item['cr_at'].split('T')[0].split('-')
                                result['start_date'] = f"{date_parts[1]}-{date_parts[2]}"
                                
                            if result['end_date'] == '당일':
                                date_parts = item['cr_at'].split('T')[0].split('-')
                                result['end_date'] = f"{date_parts[1]}-{date_parts[2]}"

                            # 브랜드명이 비어있고 상품명이 있는 경우 '확인필요'로 설정
                            if not result['brand_name'] and result['product_name']:
                                result['brand_name'] = '확인필요'

                            # GPT-4o-mini 응답 출력
                            print(f"{i}번 게시글: GPT-4o-mini 응답 내용: {json.dumps(result, ensure_ascii=False, indent=4)} (작성자: {item['author']}, 게시물 링크: {item['post_url']})")
                            
                            # 날짜 형식 검증 및 정리 함수
                            def validate_date(date_str, created_date=None):
                                if not date_str or date_str.strip() == '':
                                    return ''
                                try:
                                    if date_str == '당일' and created_date:
                                        # created_date에서 월-일만 추출 (MM-DD 형식)
                                        date_parts = created_date.split('T')[0].split('-')
                                        return f"{date_parts[1]}-{date_parts[2]}"
                                    
                                    # MM-DD 형식을 처리
                                    date_str = date_str.replace('/', '-').replace('.', '-')
                                    parts = date_str.split('-')
                                    
                                    if len(parts) == 2:
                                        # created_date에서 연도 추출
                                        year = created_date.split('T')[0].split('-')[0] if created_date else "2024"
                                        month = parts[0].zfill(2)
                                        day = parts[1].zfill(2)
                                        return f"{year}-{month}-{day}"
                                    elif len(parts) == 3:
                                        year = parts[0]
                                        if len(year) == 2:
                                            year = f"20{year}"
                                        month = parts[1].zfill(2)
                                        day = parts[2].zfill(2)
                                        return f"{year}-{month}-{day}"
                                    return ''
                                except:
                                    return ''

                            # 날짜 검증 및 처리
                            created_date = item.get('cr_at')
                            
                            update_data = {}
                            
                            if result['is_group_buy'] in ['공구예고', '공구오픈', '공구리마인드']:
                                # 시작일 처리 - 이제 '당일' 처리는 위에서 완료됨
                                start_date = validate_date(str(result['start_date']), created_date)
                                end_date = validate_date(str(result['end_date']), created_date)
                                
                                # 브랜드명 정규화 처리 추가
                                normalized_brand = normalize_brand(result['brand_name'], collections['brands'], item['author'])
                                
                                update_data = {
                                    '09_feed': result['is_group_buy'],
                                    '09_item': str(result['product_name']),
                                    '09_brand': str(normalized_brand['name']),  # 정규화된 브랜드명 사용
                                    'open_date': start_date,
                                    'end_date': end_date,
                                    '09_item_category': '',
                                    '09_item_category_2': '',
                                    'processed': True
                                }
                                
                                # GPT-4o-mini API를 사용하여 카테고리 분석
                                if result['product_name']:
                                    try:
                                        item_category, item_category2 = analyze_product_category(result['product_name'])
                                        update_data['09_item_category'] = item_category
                                        update_data['09_item_category_2'] = item_category2
                                        
                                        # 카테고리 분석 결과 출력
                                        print(f"{i}번 게시글: 카테고리 분석 결과 - 주 카테고리: {item_category}, 서브 카테고리: {item_category2}")
                                    except Exception as e:
                                        print(f"카테고리 분석 중 오류 발생: {str(e)}")
                                
                                # MongoDB 업데이트
                                feeds_collection.update_one(
                                    {'_id': item['_id']},
                                    {'$set': update_data}
                                )
                                
                                # 인플루언서 데이터 업데이트를 위해 item 업데이트
                                item.update(update_data)
                                update_influencer_data(item, collections)
                            
                            else:
                                update_data = {
                                    '09_feed': 'N' if result['is_group_buy'] == 'N' else '확인필요',
                                    '09_item': '',
                                    '09_brand': '',
                                    'open_date': '',
                                    'end_date': '',
                                    '09_item_category': '',
                                    '09_item_category_2': '',
                                    'processed': True
                                }
                                
                                feeds_collection.update_one(
                                    {'_id': item['_id']},
                                    {'$set': update_data}
                                )
                            
                            # 각 게시글 분석 완료 후 1초 대기
                            print(f"{i}번 게시글: 처리 완료")
                            
                            break  # 성공적으로 처리되면 while 루프 종료
                            
                        except Exception as e:
                            if "429" in str(e):  # Resource exhausted 에러
                                retry_count += 1
                                print(f"\nOpenAI API 할당량 초과. {wait_time}초 후 재시도... (시도 {retry_count})")
                                time.sleep(wait_time)  # 10초 대기
                            else:
                                # 다른 종류의 오류는 기존처럼 처리
                                print(f"{i}번 게시글: 처리 중 오류 발생 - {str(e)}")
                                break  # 다른 오류 발생 시 루프 종료
                
                except Exception as e:
                    print(f"{i}번 게시글: 처리 중 오류 발생 - {str(e)}")
                    continue
            
            processed_count += len(batch)
            print(f"현재까지 {processed_count}/{total_feeds} 게시글 처리 완료")
        
        print("\n모든 분석이 완료되었습니다.")
        mongo_client.close()
        
    except Exception as e:
        print(f"오류 발생: {str(e)}")

def setup_brand_logger():
    """브랜드 정규화 로깅 설정"""
    # 로그 디렉토리 생성
    log_dir = "brand_logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 로그 파일명 생성 (고정된 이름)
    log_filename = os.path.join(log_dir, "brand_normalization.log")
    
    # 로거 설정
    logger = logging.getLogger('brand_normalizer')
    logger.setLevel(logging.INFO)
    
    # 파일 핸들러 (append 모드)
    file_handler = logging.FileHandler(log_filename, encoding='utf-8', mode='a')
    file_handler.setLevel(logging.INFO)
    
    # 포맷 설정
    formatter = logging.Formatter('%(message)s')
    file_handler.setFormatter(formatter)
    
    # 기존 핸들러 제거 (중복 로깅 방지)
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # 핸들러 추가
    logger.addHandler(file_handler)
    
    return logger

def setup_log_directory():
    """로그 디렉토리 생성"""
    log_dir = "brand_logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    return log_dir

def normalize_brand(brand_name, brands_collection, author=None):
    """브랜드명 정규화 및 병합 처리"""
    try:
        logger = setup_brand_logger()
        
        logger.info(f"\n{'='*50}")
        logger.info(f"검사 대상 브랜드: '{brand_name}'")
        if author:
            logger.info(f"작성자: '{author}'")
        logger.info(f"검사 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # None, Unspecified, 빈 값 처리
        if not brand_name or brand_name in ['None', 'Unspecified', '확인필요']:
            logger.info("⚠️ 브랜드명이 없거나 미지정 상태입니다.")
            logger.info(f"{'='*50}\n")
            return {'name': '확인필요', 'category': ''}
        
        # 유사도 높은 브랜드들 1차 추출
        similar_brands = []
        all_brands = list(brands_collection.find())
        
        for brand in all_brands:
            similarity = jaro_winkler_similarity(brand_name.lower(), brand['name'].lower())
            if similarity >= 0.8:
                similar_brands.append((brand['name'], brand, similarity))
        
        # 유사 브랜드 로깅
        if not similar_brands:
            logger.info("📢 유사도 0.8 이상인 브랜드가 없습니다.")
            logger.info(f"{'='*50}\n")
            return {'name': brand_name, 'category': ''}
        
        logger.info("\n📋 유사도 0.8 이상 후보군:")
        for brand, info, similarity in similar_brands:
            logger.info(f"- {brand} (유사도: {similarity:.4f})")
            if info.get('aliases'):
                logger.info(f"  └ 기존 별칭: {', '.join(info['aliases'])}")
        
        # OpenAI GPT-4o-mini 모델을 사용하여 브랜드 관계 분석
        if similar_brands:
            analysis = analyze_brands_with_openai(brand_name, similar_brands[:10])
            
            if analysis and analysis.get('representative_brand'):
                logger.info("\n🤖 OpenAI 분석 결과:")
                logger.info(f"대표 브랜드: '{analysis['representative_brand']}'")
                logger.info(f"별칭으로 처리: {analysis.get('aliases', [])}")
                if analysis.get('different_brands'):
                    logger.info(f"다른 브랜드로 인식: {analysis['different_brands']}")
                
                rep_brand = analysis['representative_brand']
                new_aliases = analysis.get('aliases', [])
                
                # 대표 브랜드 문서 업데이트
                rep_doc = brands_collection.find_one({'name': rep_brand})
                if rep_doc:
                    # 기존 별칭들도 포함하여 병합
                    existing_aliases = []
                    for brand, info, _ in similar_brands:
                        if brand in new_aliases:  # OpenAI가 별칭으로 판단한 브랜드의
                            existing_aliases.extend(info.get('aliases', []))  # 기존 별칭들도 추가
                
                    # merge_aliases 함수를 사용하여 모든 별칭 병합
                    merged_aliases = merge_aliases(
                        rep_doc.get('aliases', []),  # 대표 브랜드의 기존 별칭
                        existing_aliases + new_aliases,  # 새로운 별칭들과 그들의 기존 별칭
                        brand_name  # 현재 처리 중인 브랜드
                    )
                    
                    # '-' 제거 로직 추가
                    cleaned_aliases = [alias.strip('- ') for alias in merged_aliases]

                    # MongoDB 업데이트
                    brands_collection.update_one(
                        {'name': rep_brand},
                        {'$set': {
                            'aliases': cleaned_aliases,  # '-' 제거된 별칭 사용
                            'status': 'done'
                        }}
                    )
                    
                    # 별칭 브랜드들 삭제
                    for alias in new_aliases:
                        brands_collection.delete_one({'name': alias})
                    
                    logger.info(f"최종 병합된 별칭 목록: {cleaned_aliases}")  # 로깅 추가
                    
                    return {
                        'name': rep_brand,
                        'category': rep_doc.get('category', '')
                    }
                
                # rep_doc이 없는 경우의 처리 추가
                else:
                    # 대표 브랜드로 새 문서 생성
                    new_brand = {
                        'name': rep_brand,
                        'aliases': new_aliases,  # 초기화된 별칭 사용
                        'status': 'done'
                    }
                    brands_collection.insert_one(new_brand)
                    
                    # 별칭 브랜드들 삭제
                    for alias in new_aliases:
                        brands_collection.delete_one({'name': alias})
                    
                    logger.info(f"새 브랜드로 등록: {rep_brand} (별칭: {new_aliases})")
                    return {
                        'name': rep_brand,
                        'category': ''
                    }
        
        # 새 브랜드 등록
        new_brand = {
            'name': brand_name,
            'category': '',
            'aliases': [brand_name],
            'level': '',
            'status': 'ready'
        }
        brands_collection.insert_one(new_brand)
        
        logger.info("\n📝 새 브랜드 등록:")
        logger.info(f"브랜드명: '{brand_name}'")
        logger.info(f"{'='*50}\n")
        
        return {'name': brand_name, 'category': ''}
        
    except Exception as e:
        error_message = f"브랜드 정규화 중 오류 발생: {str(e)}"
        logger.error(f"\n❌ {error_message}")
        logger.info(f"{'='*50}\n")
        print(error_message)
        return {'name': brand_name, 'category': ''}

def analyze_brands_with_openai(target_brand, similar_brands):
    """OpenAI GPT-4o-mini 모델을 사용하여 브랜드 관계 분석"""
    max_retries = 10
    retry_count = 0
    wait_time = 10
    
    while retry_count < max_retries:
        try:
            prompt = f"""
당신은 브랜드 분류 전문가입니다. 브랜드의 의미와 맥락을 정확히 이해하고 분석해야 합니다.

이 작업의 목적은 인스타그램에서 수집된 브랜드 데이터를 정리하는 것입니다.
같은 브랜드가 다양한 형태로 표기되어 있어, 이를 하나의 대표 브랜드명으로 통일하고 
나머지는 별칭으로 처리해야 합니다.
지금 검사하는 브랜드는 "{target_brand}"입니다.
이 브랜드와 유사도가 높은 브랜드들은 다음과 같습니다:

{[f"- {brand} (유사도: {similarity:.3f})" for brand, _, similarity in similar_brands]}

주의사항:
1. 검사 대상 브랜드명("{target_brand}")을 절대 변경하지 마세요.
2. 검사 대상 브랜드가 대표 브랜드가 될 수도 있고, 다른 브랜드의 별칭이 될 수도 있습니다.
3. 유사도만으로 판단하지 말고, 브랜드의 의미와 맥락을 고려하세요.
4. 확실하지 않은 경우 다른 브랜드로 분류하세요.

반드시 아래 형식의 JSON으로만 응답해주세요:
{{
    "representative_brand": "string",  # 대표 브랜드로 선정된 이름
    "aliases": ["string"],            # 확실히 같은 브랜드인 경우만 별칭으로 처리
    "different_brands": ["string"]    # 의심스러운 경우 모두 다른 브랜드로 제외
}}
"""
            
            # OpenAI 클라이언트 초기화
            client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
            
            # GPT-4o-mini API 호출
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}]
            )
            
            text = response.choices[0].message.content.strip()
            
            # JSON 형식 정리
            if text.startswith('```json'):
                text = text[7:]
            if text.endswith('```'):
                text = text[:-3]
            text = text.strip()
            
            result = json.loads(text)
            return result
            
        except Exception as e:
            if "429" in str(e):  # API 할당량 초과 오류
                retry_count += 1
                print(f"\nOpenAI API 할당량 초과. {wait_time}초 대기 중... (시도 {retry_count}/{max_retries})")
                time.sleep(wait_time)
                wait_time += 10
                continue
            else:
                print(f"OpenAI API 오류: {e}")
                return None
    
    print(f"\n최대 재시도 횟수({max_retries})를 초과했습니다.")
    return None

def normalize_brand_name(name):
    """브랜드명 정규화 (대소문자 구분 없애고, 공백 제거)"""
    return name.lower().strip()

def merge_aliases(main_aliases, sub_aliases, sub_brand):
    """aliases 병합 시 중복 제거 및 정규화"""
    # 모든 별칭을 소문자로 변환하여 set에 저장
    normalized_aliases = {normalize_brand_name(alias) for alias in main_aliases}
    
    # 새로운 별칭들 추가
    for alias in sub_aliases:
        normalized_alias = normalize_brand_name(alias)
        if normalized_alias not in normalized_aliases:
            normalized_aliases.add(normalized_alias)
    
    # 서브 브랜드명도 별칭으로 추가
    normalized_sub_brand = normalize_brand_name(sub_brand)
    if normalized_sub_brand not in normalized_aliases:
        normalized_aliases.add(normalized_sub_brand)
    
    return list(normalized_aliases)

if __name__ == "__main__":
    analyze_instagram_feed()
