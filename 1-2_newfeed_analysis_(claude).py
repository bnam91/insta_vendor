"""
인스타그램 피드 분석 스크립트

입력 파일:
1. 1-1_newfeed_crawl_data.json
   - 크롤링된 인스타그램 피드 원본 데이터
   - 포함 정보: 게시물 ID, 본문 내용, 작성일, 작성자 정보 등

2. 2-2_influencer_processing_data.json
   - 인플루언서 정보 데이터
   - 포함 정보: 인플루언서 ID, 공구유무, 브랜드/상품 정보 등

3. brand_category.json
   - 브랜드 카테고리 관리 데이터
   - 포함 정보: 브랜드명, 카테고리 분류 정보, 별칭(aliases), 레벨, 상태

출력/업데이트 파일:
1. 1-1_newfeed_crawl_data.json
   - 분석 결과가 추가된 피드 데이터
   - 추가되는 정보: 공구 여부, 상품명, 브랜드명, 공구 시작일/종료일

2. 2-2_influencer_processing_data.json
   - 업데이트되는 인플루언서 정보
   - 변경사항: 공구유무 상태, 새로운 브랜드/상품 정보
   - 브랜드별 상품 이력 관리 (20일 이내 중복 체크)

3. unregistered_influencers.log
   - 미등록 인플루언서 정보 기록
   - 포함 정보: 발견 시간, 인플루언서명, 게시물 링크

4. influencer_update_errors.log
   - 데이터 처리 중 발생하는 오류 로그
   - 포함 정보: 시간, 인플루언서명, 오류 내용

실행 결과:
1. 콘솔 출력
   - 총 분석할 게시글 수 표시
   - 각 게시물 분석 진행 상황 (순차적으로 표시)
   - 중복 상품 발견 시 알림
   - 에러 발생 시 해당 게시물 ID와 에러 내용
   - 전체 분석 완료 메시지

2. JSON 파일 업데이트
   예시) 1-1_newfeed_crawl_data.json:
   {
     "작성자": "인플루언서명",
     "본문": "게시물 내용",
     "작성시간": "2024-03-15T14:30:00",
     "공구피드": "공구오픈",
     "공구상품": "상품명",
     "브랜드": "브랜드명",
     "오픈예정일": "2024-03-15",
     "공구마감일": "2024-03-20"
   }

3. 인플루언서 데이터 업데이트
   예시) 2-2_influencer_processing_data.json:
   {
     "username": "인플루언서명",
     "공구유무": "Y",
     "브랜드": [
       {
         "name": "브랜드명",
         "category": "카테고리",
         "products": [
           {
             "item": "상품명",
             "type": "공구오픈",
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
1. JSON 파일에서 인스타그램 피드 데이터를 읽어옴
2. Claude AI를 사용하여 각 게시물 분석:
   - 공구 게시물 여부 판단 (공구예고/공구오픈/공구리마인드/확인필요/N)
   - 상품명, 브랜드명 추출
   - 공구 시작일/종료일 추출
3. 분석 결과를 JSON 파일에 자동 업데이트
4. 인플루언서 데이터 자동 업데이트
5. 브랜드 카테고리 관리 및 자동 등록

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
   - brand_category.json에서 브랜드 정규화
   - 미등록 브랜드 자동 등록 (status: 'ready')
   - 별칭(aliases) 관리 지원

4. 상품 중복 체크:
   - 동일 상품 20일 이내 중복 등록 방지
   - 중복 발견 시 콘솔에 알림

오류 처리:
- 개별 게시물 처리 실패시 다음 게시물로 진행
- 오류 상세 내용 로그 파일에 기록
- 미등록 인플루언서 별도 로그 관리

주의사항:
- API 호출 제한 방지를 위한 딜레이 포함
- 이미 처리된 데이터는 건너뜀
- 브랜드명 누락시 '확인필요'로 설정
"""

#예정
# 브랜드딕셔너리 만들어서 입력되게
# 아이템 카테고리 만들어서 입력되게

import anthropic
import time
import json
from datetime import datetime
import os
from dotenv import load_dotenv

def update_influencer_data(item):
    try:
        # 브랜드 카테고리 데이터 로드
        with open('brand_category.json', 'r', encoding='utf-8') as f:
            brand_category_data = json.load(f)

        with open('2-2_influencer_processing_data.json', 'r', encoding='utf-8') as file:
            influencer_data = json.load(file)
        
        # 브랜드명 정규화 및 카테고리 찾기 함수
        def normalize_brand(brand_name):
            for main_brand, info in brand_category_data['brands'].items():
                if brand_name == main_brand or brand_name in info['aliases']:
                    return {
                        'name': main_brand,
                        'category': info['category']
                    }
            # 브랜드가 없는 경우 새로 추가
            if brand_name and brand_name != '확인필요':
                brand_category_data['brands'][brand_name] = {
                    'category': '',
                    'aliases': [],
                    'level': '',
                    'status': 'ready'
                }
                # brand_category.json 파일 업데이트
                with open('brand_category.json', 'w', encoding='utf-8') as f:
                    json.dump(brand_category_data, f, ensure_ascii=False, indent=2)
                return {
                    'name': brand_name,
                    'category': ''
                }
            return {
                'name': brand_name,
                'category': ''
            }

        influencer_found = False
        for influencer in influencer_data:
            if influencer['username'] == item['작성자']:
                influencer_found = True
                if influencer.get('공구유무') != 'Y':
                    influencer['공구유무'] = 'Y'
                
                brands = item['브랜드'].split(', ')
                
                for brand in brands:
                    # 브랜드명 정규화 및 카테고리 찾기
                    brand_info = normalize_brand(brand)
                    normalized_brand_name = brand_info['name']
                    
                    # 1-1 JSON 파일의 브랜드명도 업데이트
                    item['브랜드'] = normalized_brand_name
                    
                    # 기존 브랜드 찾기
                    brand_exists = False
                    for existing_brand in influencer['브랜드']:
                        if existing_brand['name'] == normalized_brand_name:
                            # 카테고리 업데이트
                            existing_brand['category'] = brand_info['category']
                            
                            # 중복 체크
                            product_exists = False
                            for product in existing_brand['products']:
                                # 작성시간에서 날짜만 추출하고 datetime 객체로 변환
                                existing_date = datetime.strptime(product['mentioned_date'].split('T')[0], '%Y-%m-%d')
                                new_date = datetime.strptime(item['작성시간'].split('T')[0], '%Y-%m-%d')
                                
                                # 날짜 차이 계산 (절대값)
                                date_diff = abs((new_date - existing_date).days)
                                
                                if (product['item'] == item['공구상품'] and 
                                    date_diff <= 20):  # 20일 이내면 중복으로 처리
                                    product_exists = True
                                    print(f"중복 상품 발견: {item['공구상품']} - 기존:{existing_date.date()} 신규:{new_date.date()} (차이: {date_diff}일)")
                                    break
                            
                            # 중복이 아닌 경우에만 추가
                            if not product_exists:
                                existing_brand['products'].append({
                                    'item': item['공구상품'],
                                    'type': item['공구피드'],
                                    'mentioned_date': item['작성시간'],
                                    'expected_date': item['오픈예정일'],
                                    'end_date': item['공구마감일'],
                                    'item_feed_link': item['게시물링크'],
                                    'preserve': ''
                                })
                            brand_exists = True
                            break
                    
                    # 새로운 브랜드인 경우
                    if not brand_exists:
                        influencer['브랜드'].append({
                            'name': normalized_brand_name,
                            'category': brand_info['category'],
                            'products': [{
                                'item': item['공구상품'],
                                'type': item['공구피드'],
                                'mentioned_date': item['작성시간'],
                                'expected_date': item['오픈예정일'],
                                'end_date': item['공구마감일'],
                                'item_feed_link': item['게시물링크'],
                                'preserve': ''
                            }]
                        })
                break
        
        # username이 없는 경우 로그 기록
        if not influencer_found:
            log_message = f"[{item['작성시간']}] 미등록 인플루언서 발견: {item['작성자']} (게시물링크: {item['게시물링크']})\n"
            with open('unregistered_influencers.log', 'a', encoding='utf-8') as log_file:
                log_file.write(log_message)
            print(f"미등록 인플루언서 로그 기록: {item['작성자']}")
            return
        
        # 업데이트된 데이터 저장
        with open('2-2_influencer_processing_data.json', 'w', encoding='utf-8') as file:
            json.dump(influencer_data, file, ensure_ascii=False, indent=2)
            
        print(f"인플루언서 데이터 업데이트 완료: {item['작성자']}")
        
    except Exception as e:
        error_message = f"[{item['작성시간']}] 오류 발생 - 인플루언서: {item['작성자']}, 오류: {str(e)}\n"
        with open('influencer_update_errors.log', 'a', encoding='utf-8') as error_file:
            error_file.write(error_message)
        print(f"인플루언서 데이터 업데이트 중 오류 발생: {str(e)}")

def analyze_instagram_feed():
    try:
        # 환경 변수에서 API 키 로드
        load_dotenv()  # .env 파일 로드
        client = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))
        
        # JSON 파일 읽기
        with open('1-1_newfeed_crawl_data.json', 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        print(f"총 {len(data)}개의 게시글을 분석합니다...")
        
        # 각 게시글 분석
        for i, item in enumerate(data, start=1):
            print(f"\n[{i}/{len(data)}] {i}번 게시글 분석 중...")
            
            try:
                # 이미 처리된 데이터인지 확인 (09_feed 필드로 변경)
                if item.get('09_feed'):
                    print(f"{i}번 게시글: 이미 처리된 데이터입니다. 건너뜁니다.")
                    continue
                
                # 본문 내용 확인 (content 필드로 변경)
                content = item.get('content')
                if not content or not content.strip():
                    print(f"{i}번 게시글: 본문 내용이 비어있습니다.")
                    continue
                
                print(f"{i}번 게시글: 클로드 분석 요청 중...")
                message = client.messages.create(
                    model="claude-3-5-sonnet-latest",
                    # model="claude-3-5-haiku-latest",
                    max_tokens=500,
                    temperature=0.3,
                    system="""You are analyzing Instagram feed content. Extract information and respond in the following JSON format only:
                    {
                        "is_group_buy": "공구예고"/"공구오픈"/"공구리마인드"/"확인필요"/"N",
                        "product_name": "Include all main product categories in the title",
                        "brand_name": "brand name here",
                        "start_date": "MM-DD format only if year is not specified",
                        "end_date": "MM-DD format only if year is not specified"
                    }
                    
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
                 
                    
                    Do not include any other text in your response.""",
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": content
                                }
                            ]
                        }
                    ]
                )
                
                response = message.content[0].text if isinstance(message.content, list) else message.content
                result = json.loads(response)
                
                # '당일' 처리
                if result['start_date'] == '당일':
                    date_parts = item['작성시간'].split('T')[0].split('-')
                    result['start_date'] = f"{date_parts[1]}-{date_parts[2]}"
                    
                if result['end_date'] == '당일':
                    date_parts = item['작성시간'].split('T')[0].split('-')
                    result['end_date'] = f"{date_parts[1]}-{date_parts[2]}"

                # 브랜드명이 비어있고 상품명이 있는 경우 '확인필요'로 설정
                if not result['brand_name'] and result['product_name']:
                    result['brand_name'] = '확인필요'

                # 변환된 값으로 Claude 응답 출력
                print(f"{i}번 게시글: 클로드 응답 내용: {json.dumps(result, ensure_ascii=False, indent=4)}")
                
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
                created_date = item.get('cr_at')  # 작성시간 필드명 변경
                
                if result['is_group_buy'] in ['공구예고', '공구오픈', '공구리마인드']:
                    # 시작일 처리 - 이제 '당일' 처리는 위에서 완료됨
                    start_date = validate_date(str(result['start_date']), created_date)
                    end_date = validate_date(str(result['end_date']), created_date)
                    
                    # 필드명 변경에 따른 데이터 업데이트
                    item.update({
                        '09_feed': result['is_group_buy'],  # 공구피드 -> 09_feed
                        '09_item': str(result['product_name']),  # 공구상품 -> 09_item
                        '09_brand': str(result['brand_name']),  # 브랜드 -> 09_brand
                        'open_date': start_date,  # 오픈예정일 -> open_date
                        'end_date': end_date,  # 공구마감일 -> end_date
                        '09_item_category': ''  # 새로 추가된 카테고리 필드
                    })
                    
                    # 인플루언서 데이터 업데이트 - item 직접 전달
                    update_influencer_data(item)
                    
                elif result['is_group_buy'] == '확인필요':
                    item.update({
                        '09_feed': '확인필요',
                        '09_item': '',
                        '09_brand': '',
                        'open_date': '',
                        'end_date': '',
                        '09_item_category': ''
                    })
                    
                else:
                    item.update({
                        '09_feed': 'N',
                        '09_item': '',
                        '09_brand': '',
                        'open_date': '',
                        'end_date': '',
                        '09_item_category': ''
                    })
                
                # JSON 파일 저장
                with open('1-1_newfeed_crawl_data.json', 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                
                print(f"{i}번 게시글: 처리 완료")
                time.sleep(1)
                
            except Exception as e:
                print(f"{i}번 게시글: 처리 중 오류 발생 - {str(e)}")
                continue
        
        print("\n모든 분석이 완료되었습니다.")
        
    except Exception as e:
        print(f"오류 발생: {str(e)}")

if __name__ == "__main__":
    analyze_instagram_feed()
