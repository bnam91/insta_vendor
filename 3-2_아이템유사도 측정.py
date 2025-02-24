"""

입력 토큰:
- 상품명 비교 (2개 상품) ≈ 50 토큰
- 시스템 프롬프트 ≈ 100 토큰
총 입력 토큰: 약 150 토큰/비교

1000개 상품 처리 시 예상 비교 횟수:
- 브랜드당 평균 (4 × 3)/2 = 6회 비교
- 총 비교 횟수 ≈ 1000 × 1.5 = 1,500회

비용 계산:
입력: 1,500회 × 150 토큰 = 225,000 토큰
→ $0.150 × (225,000/1,000,000) = $0.034

출력: 1,500회 × 20 토큰 = 30,000 토큰
→ $0.600 × (30,000/1,000,000) = $0.018

총 예상 비용: $0.052 ≈ 70원

===

[파일 처리 정보]
1. 입력 파일 (2-2_influencer_processing_data.json)
   - 구조:
     {
       "username": "인플루언서아이디",
       "name": "인플루언서이름",
       "브랜드": [
         {
           "name": "브랜드명",
           "products": [
             {
               "item": "상품명",
               "mentioned_date": "언급일자",
               "link": "상품링크",
               "preserve": "" // 초기값
             }
           ]
         }
       ]
     }

2. 출력 파일 (3-2_similar_items_result.json)
   - 구조:
     [
       {
         "브랜드": "브랜드명",
         "인플루언서": {
           "username": "인플루언서아이디",
           "name": "인플루언서이름"
         },
         "유사상품군": [
           {
             "상품명": "상품명",
             "언급일자": "언급일자",
             "링크": "상품링크"
           }
         ]
       }
     ]

[데이터 처리 로직]
1. preserve 필드 업데이트
   - 가장 오래된 상품: preserve="True"
   - 20일 이상 차이나는 상품: preserve="True"
   - 그 외 상품: preserve="" (빈 문자열)

2. 중복 상품 처리
   - preserve="True" 상품 중 최신 상품과 비교
   - 유사도 70% 이상 && 20일 이내 차이: 중복으로 처리
   - 중복 상품은 2-2 파일에서 삭제되고 3-2 파일에 기록

[API 사용]
- OpenAI GPT-4 API 사용
- 상품명 유사도 측정에 활용
- 응답값: 0-100 사이의 유사도 점수

[실시간 업데이트]
- 중복 상품 발견 시마다 2-2, 3-2 파일 실시간 업데이트
- 진행상황 및 예상 비용 실시간 출력

[주의사항]
- API 키 필요
- 많은 API 호출로 인한 비용 발생
- 파일 실시간 업데이트로 인한 백업 필요

상품 유사도 측정 및 중복 상품 통합 프로그램

이 프로그램은 다음과 같은 기능을 수행합니다:

1. 상품 유사도 측정
- 각 인플루언서의 브랜드별로 등록된 상품들의 이름 유사도를 측정
- 유사도가 70점 이상인 상품들을 중복 상품으로 판단
- 20일 이상 차이나는 상품은 다른 시즌 상품으로 간주하여 보존

2. 상품 통합 처리
2.1 preserve 필드 관리
    - preserve="True": 보존할 상품 표시
    - 가장 오래된 상품을 우선적으로 preserve="True"로 설정
    - 20일 이상 차이나는 상품도 preserve="True"로 설정

2.2 중복 상품 처리
    - preserve="True"인 상품 중 가장 최근 날짜의 상품과 비교
    - 유사도 70점 이상 && 20일 이내 차이: 중복 상품으로 처리
    - 중복 상품은 2-2 파일에서 삭제되고 3-2 파일에 기록

입력 파일 요구사항:
1. 2-2_influencer_processing_data.json
   - 인플루언서별 브랜드와 상품 정보가 포함된 파일
   - 각 상품에 preserve 필드가 존재해야 함 (빈 문자열로 초기화)

출력 파일:
1. 3-2_similar_items_result.json
   - 유사 상품군 분석 결과 저장
   - 브랜드별, 인플루언서별로 유사 상품 그룹화
   - 각 상품의 언급일자, 링크 등 상세 정보 포함

2. 2-2_influencer_processing_data.json (업데이트)
   - 중복 상품이 제거된 상태로 업데이트
   - preserve 필드가 설정된 상태로 업데이트

주의사항:
1. OpenAI API 키가 필요합니다
2. 실행 전 2-2 파일의 모든 상품에 preserve 필드가 있어야 합니다
3. 많은 API 호출이 발생하므로 비용이 발생할 수 있습니다
4. 실행 중 두 파일이 실시간으로 업데이트되므로 백업을 권장합니다

예상 비용:
- GPT-4 API 사용: 약 $0.03/1K tokens
- 총 비용은 상품 수에 따라 달라짐

실행 시간:
- 상품 수에 따라 다르며, API 호출 대기 시간이 포함됨
- 진행률이 실시간으로 표시됨
"""

import json
import openai
from datetime import datetime
from dotenv import load_dotenv
import os

def extract_product_names(product_name):
    """상품명에서 개별 제품들을 추출"""
    # 괄호 안의 내용 처리
    if '(' in product_name and ')' in product_name:
        items = product_name[product_name.find('(')+1:product_name.find(')')].split(',')
        items = [item.strip() for item in items]
        # 괄호 밖의 메인 상품명도 추가
        main_item = product_name[:product_name.find('(')].strip()
        if main_item:
            items.append(main_item)
    else:
        # 쉼표나 특수문자로 구분된 경우
        items = [item.strip() for item in product_name.replace('/', ',').replace('&', ',').split(',')]
    
    return items

def calculate_similarity(item1, item2, client, api_key):
    """두 상품의 유사도 계산"""
    try:
        print("   ⏳ API 요청 중...")  # API 호출 시작 표시
        
        # OpenAI 클라이언트 초기화
        client = openai.OpenAI(api_key=api_key)
        
        # 타임아웃 설정 (30초)
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
        
        print("   ✓ API 응답 수신")  # API 응답 수신 표시
        
        # 입력/출력 토큰 분리 계산
        total_input_tokens = similarity_response.usage.prompt_tokens
        total_output_tokens = similarity_response.usage.completion_tokens
        
        current_similarity = int(similarity_response.choices[0].message.content.strip())
        max_similarity = current_similarity
        
        return max_similarity, total_input_tokens, total_output_tokens
        
    except openai.error.Timeout:
        print("   ⚠️ API 요청 시간 초과. 다시 시도합니다...")
        return 0, 0, 0
    except Exception as e:
        print(f"   ❌ 에러 발생: {str(e)}")
        return 0, 0, 0

def check_similar_items():
    print("\n" + "="*50)
    print("상품 유사도 측정 프로그램 시작")
    print("="*50)
    
    # .env 파일 로드
    load_dotenv()
    openai.api_key = os.getenv('OPENAI_API_KEY')
    print("[1/4] OpenAI API 키 설정 완료")
    
    # 3-2 파일 초기화
    try:
        with open('3-2_similar_items_result.json', 'r', encoding='utf-8') as f:
            similar_items_results = json.load(f)
            print(f"[2/4] 3-2 파일 로드 완료: {len(similar_items_results)}개의 기존 유사 상품군 발견")
    except (FileNotFoundError, json.JSONDecodeError):
        similar_items_results = []
        print("[2/4] 3-2 파일 새로 생성됨")
    
    # JSON 파일 읽기
    print("\n[3/4] 2-2 JSON 파일 로딩 중...")
    with open('2-2_influencer_processing_data.json', 'r', encoding='utf-8') as file:
        data = json.load(file)
    print(f"✓ 총 {len(data)}명의 인플루언서 데이터 로드 완료")
    
    # 전체 통계 계산
    total_influencers = len(data)
    total_brands = sum(len(inf['브랜드']) for inf in data)
    total_products = sum(len(brand['products']) for inf in data for brand in inf['브랜드'])
    
    print(f"\n[4/4] 데이터 통계:")
    print(f"- 총 인플루언서 수: {total_influencers}")
    print(f"- 총 브랜드 수: {total_brands}")
    print(f"- 총 상품 수: {total_products}")
    
    # 전체 통계 변수 초기화
    total_comparisons = 0
    completed_comparisons = 0
    total_input_tokens = 0
    total_output_tokens = 0
    products_to_remove = set()  # 각 브랜드마다 초기화
    
    # 전체 비교 횟수 계산
    for influencer in data:
        for brand in influencer['브랜드']:
            if not brand['name'] or len(brand['products']) <= 1:
                continue
            products_without_preserve = len([p for p in brand['products'] if p.get('preserve', '') == ''])
            if products_without_preserve > 0:
                total_comparisons += products_without_preserve
    
    print(f"\n분석 시작:")
    print(f"- 예상 비교 횟수: {total_comparisons}개")
    print(f"- 예상 소요 시간: 약 {(total_comparisons * 2) // 60}분")
    print("\n" + "="*50)
    
    # 각 인플루언서의 브랜드 데이터 처리
    for inf_idx, influencer in enumerate(data, 1):
        has_products_to_compare = False
        
        # 먼저 비교할 상품이 있는지 확인
        for brand in influencer['브랜드']:
            if brand['name'] and len(brand['products']) > 1:
                has_products_to_compare = True
                break
        
        # 비교할 상품이 있는 경우에만 출력
        if has_products_to_compare:
            print(f"\n{inf_idx}/{total_influencers} - {influencer['username']} ({influencer.get('name', '이름 없음')})")
            
            for brand_idx, brand in enumerate(influencer['브랜드'], 1):
                if not brand['name'] or len(brand['products']) <= 1:
                    continue
                    
                print(f"\n[{brand['name']}] {len(brand['products'])}개 상품 분석 중...")
                
                products = brand['products']
                products_to_remove = set()  # 각 브랜드마다 초기화
                
                # preserve='True'인 가장 최근 상품 찾기
                latest_preserved_idx = None
                latest_preserved_date = None
                
                for idx, product in enumerate(products):
                    if product.get('preserve', '') == 'True':
                        current_date = datetime.fromisoformat(product['mentioned_date'].replace('Z', '+00:00'))
                        if latest_preserved_date is None or current_date > latest_preserved_date:
                            latest_preserved_date = current_date
                            latest_preserved_idx = idx
                
                # preserve='True'인 상품이 있는 경우에만 비교 진행
                if latest_preserved_idx is not None:
                    latest_product = products[latest_preserved_idx]
                    
                    for i, product in enumerate(products):
                        if product.get('preserve', '') == 'True':
                            continue
                        
                        similarity_score, input_tokens, output_tokens = calculate_similarity(latest_product['item'], product['item'], None, openai.api_key)
                        
                        # 날짜 차이 계산
                        current_date = datetime.fromisoformat(product['mentioned_date'].replace('Z', '+00:00'))
                        latest_date = datetime.fromisoformat(latest_product['mentioned_date'].replace('Z', '+00:00'))
                        days_diff = abs((current_date - latest_date).days)
                        
                        # 중복 아이템 발견 시에만 출력
                        if similarity_score >= 70:
                            print(f"! 유사도 {similarity_score}% - {product['item']}")
                            if days_diff > 20:
                                print("  → 다른 시즌 상품으로 보존")
                                product['preserve'] = 'True'
                            else:
                                print("  → 중복 상품으로 처리")
                                products_to_remove.add(i)
                
                # 브랜드 분석 완료 시 결과만 간단히 표시
                if products_to_remove:
                    print(f"* {len(products_to_remove)}개 중복 상품 처리됨")
                    
                    # 3-2 파일에 기록
                    similar_group = {
                        "브랜드": brand['name'],
                        "인플루언서": {
                            "username": influencer['username'],
                            "name": influencer['name']
                        },
                        "유사상품군": []
                    }
                    
                    # 큰 인덱스부터 삭제
                    for index in sorted(products_to_remove, reverse=True):
                        try:
                            product = brand['products'][index]
                            similar_group["유사상품군"].append({
                                "상품명": product['item'],
                                "언급일자": product['mentioned_date'],
                                "링크": product.get('link', '')
                            })
                            del brand['products'][index]
                        except IndexError as e:
                            print(f"! 삭제 중 에러 발생: {str(e)}")
                            continue
                    
                    # 3-2 파일 업데이트
                    similar_items_results.append(similar_group)
                    with open('3-2_similar_items_result.json', 'w', encoding='utf-8') as f:
                        json.dump(similar_items_results, f, ensure_ascii=False, indent=2)
                    
                    # 2-2 파일 실시간 업데이트
                    with open('2-2_influencer_processing_data.json', 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    print("* 2-2, 3-2 파일 업데이트 완료")
                else:
                    print("\n* 중복 상품 없음")
        
        # 진행률도 변경이 있을 때만 출력
        if completed_comparisons > 0:
            print(f"\n진행률: {completed_comparisons}/{total_comparisons} ({(completed_comparisons/total_comparisons*100):.1f}%)")
            print(f"예상 비용: ${(total_input_tokens * 0.15 + total_output_tokens * 0.60)/1000000:.6f}")

    # 최종 결과 출력
    print("\n" + "="*50)
    print("✓ 분석 완료!")
    print("="*50)
    print(f"총 {len(similar_items_results)}개의 유사 상품군이 3-2 파일에 기록되었습니다.")
    
    return similar_items_results

# 함수 실행
similar_items = check_similar_items()

# 결과가 있을 때만 출력
if similar_items:
    for item in similar_items:
        print(f"\n브랜드: {item['브랜드']}")
        print(f"인플루언서: {item['인플루언서']['username']} ({item['인플루언서']['name']})")
        for product in item['유사상품군']:
            print(f"유사상품: {product['상품명']} ({product['언급일자']})")
