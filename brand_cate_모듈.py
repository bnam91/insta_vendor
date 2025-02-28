import json
from jellyfish import jaro_winkler_similarity
import re
from tqdm import tqdm
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import time  # 추가
import os
import google.generativeai as genai
from dotenv import load_dotenv
from operator import itemgetter
import logging
from datetime import datetime

# .env 파일 로드
load_dotenv()

# Gemini API 설정
genai.configure(api_key=os.getenv('GEMINI_API_KEY'))
model = genai.GenerativeModel('gemini-2.0-flash')

def setup_logger():
    """로깅 설정"""
    log_filename = "brand_merge_log.txt"
    
    # 로거 설정
    logger = logging.getLogger('brand_merger')
    logger.setLevel(logging.INFO)
    
    # 파일 핸들러
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # 콘솔 핸들러 추가
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 포맷 설정 (시간 제거)
    formatter = logging.Formatter('%(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)  # 콘솔에도 같은 포맷 적용
    
    # 핸들러 추가
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)  # 콘솔 핸들러 추가
    
    return logger

# MongoDB 연결 설정
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))
db = client['insta09_database']
collection = db['08_main_brand_category_data']

# 로거 설정
logger = setup_logger()

def is_korean(text):
    # 한글이 포함되어 있는지 확인
    return bool(re.search('[가-힣]', text))

def determine_main_brand(brand1, info1, brand2, info2):
    # 1. 한글 브랜드명 우선
    if is_korean(brand1) and not is_korean(brand2):
        return brand1, info1, brand2, info2
    elif is_korean(brand2) and not is_korean(brand1):
        return brand2, info2, brand1, info1
    
    # 2. 공백이 없는 브랜드명 우선
    if ' ' not in brand1 and ' ' in brand2:
        return brand1, info1, brand2, info2
    elif ' ' not in brand2 and ' ' in brand1:
        return brand2, info2, brand1, info1
    
    # 3. 더 짧은 브랜드명 우선
    if len(brand1) < len(brand2):
        return brand1, info1, brand2, info2
    elif len(brand2) < len(brand1):
        return brand2, info2, brand1, info1
    
    # 4. status가 done인 경우 우선
    if info1.get('status') == 'done' and info2.get('status') != 'done':
        return brand1, info1, brand2, info2
    elif info2.get('status') == 'done' and info1.get('status') != 'done':
        return brand2, info2, brand1, info1
    
    # 5. 모든 조건이 동일한 경우, 사전순으로 앞선 브랜드를 선택
    if brand1.lower() <= brand2.lower():
        return brand1, info1, brand2, info2
    else:
        return brand2, info2, brand1, info1

def get_max_similarity(str1, str2, aliases):
    # 기본 문자열 유사도 계산
    max_similarity = jaro_winkler_similarity(str1.lower(), str2.lower())
    
    # aliases 검사
    for alias in aliases:
        alias_similarity = jaro_winkler_similarity(str1.lower(), alias.lower())
        max_similarity = max(max_similarity, alias_similarity)
    
    return max_similarity

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

def get_top_similar_brands(target_brand, brands_data, top_n=10):
    """유사도가 가장 높은 상위 10개 브랜드 반환"""
    similarities = []
    for brand2, info2 in brands_data.items():
        aliases1 = []  # target_brand의 aliases
        aliases2 = info2.get('aliases', [])
        
        similarity = max(
            get_max_similarity(target_brand, brand2, aliases2),
            get_max_similarity(brand2, target_brand, aliases1)
        )
        
        similarities.append((brand2, info2, similarity))
    
    # 유사도 기준으로 정렬하여 상위 10개 반환
    return sorted(similarities, key=itemgetter(2), reverse=True)[:top_n]

def analyze_brands_with_gemini(target_brand, similar_brands):
    """Gemini API를 사용하여 브랜드 관계 분석"""
    max_retries = 5  # 최대 재시도 횟수
    retry_count = 0
    wait_time = 10  # 기본 대기 시간 10초
    
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
   예: '이유살림'과 '유딘살림'은 철자가 비슷해도 전혀 다른 브랜드입니다.
4. 확실하지 않은 경우 다른 브랜드로 분류하세요.

반드시 아래 형식의 JSON으로만 응답해주세요:
{{
    "representative_brand": "string",  # 대표 브랜드로 선정된 이름 (검사 대상 브랜드 그대로 사용)
    "aliases": ["string"],            # 확실히 같은 브랜드인 경우만 별칭으로 처리
    "different_brands": ["string"]    # 의심스러운 경우 모두 다른 브랜드로 제외
}}
"""
            
            response = model.generate_content(prompt)
            text = response.text.strip()
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
                print(f"\nGemini API 할당량 초과. {wait_time}초 대기 중... (시도 {retry_count}/{max_retries})")
                time.sleep(wait_time)
                wait_time += 10  # 다음 재시도시 10초 추가
                continue  # 다시 시도
            else:
                print(f"Gemini API 오류: {e}")
                return None
    
    print(f"\n최대 재시도 횟수({max_retries})를 초과했습니다. 잠시 후 다시 시도해주세요.")
    proceed = input("계속 진행하려면 엔터키를 누르세요. (종료: q): ")
    if proceed.lower() == 'q':
        return None
    return analyze_brands_with_gemini(target_brand, similar_brands)  # 재귀적으로 다시 시도

def merge_brands():
    try:
        print("MongoDB에서 브랜드 데이터 로딩 중...")
        all_brands = list(collection.find())
        total_brands = len(all_brands)
        print(f"총 {total_brands}개의 브랜드를 처리합니다.\n")
        
        for idx, brand_doc in enumerate(all_brands, 1):
            target_brand = brand_doc['name']
            print(f"\r[{idx}/{total_brands}] 현재 처리 중: {target_brand}", end='')
            
            similar_pairs = []
            for other_doc in all_brands:
                if other_doc['name'] != target_brand:
                    similarity = jaro_winkler_similarity(target_brand.lower(), other_doc['name'].lower())
                    if similarity >= 0.8:
                        similar_pairs.append((other_doc['name'], other_doc, similarity))
            
            similar_pairs = sorted(similar_pairs, key=lambda x: x[2], reverse=True)[:10]
            
            if similar_pairs:
                print()
                print(f"\n'{target_brand}'와(과) 유사도가 높은 브랜드들:")
                for brand2, info2, similarity in similar_pairs:
                    print(f"- {brand2} (유사도: {similarity:.4f})")
                    if info2.get('aliases'):
                        print(f"  별칭: {', '.join(info2['aliases'])}")
                
                # 사용자 입력 없이 자동으로 제미나이 분석 진행
                analysis = analyze_brands_with_gemini(target_brand, similar_pairs)
                
                if analysis and analysis.get('representative_brand'):
                    print(f"\n제미나이 분석 결과:")
                    print(f"검사 대상 브랜드: '{target_brand}'")
                    print(f"대표 브랜드로 선정: '{analysis['representative_brand']}'")
                    print(f"별칭으로 처리: {analysis.get('aliases', [])}")
                    if analysis.get('different_brands'):
                        print(f"다른 브랜드 제외: {analysis['different_brands']}")
                    
                    # MongoDB 업데이트
                    try:
                        representative_brand = analysis['representative_brand']
                        aliases = analysis.get('aliases', [])
                        
                        # 대표 브랜드 문서 업데이트
                        rep_doc = collection.find_one({'name': representative_brand})
                        if rep_doc:
                            # merge_aliases 함수를 사용하여 별칭 병합
                            existing_aliases = rep_doc.get('aliases', [])
                            merged_aliases = merge_aliases(existing_aliases, aliases, target_brand)
                            
                            collection.update_one(
                                {'name': representative_brand},
                                {'$set': {
                                    'aliases': merged_aliases,
                                    'status': 'done'
                                }}
                            )
                        
                        # 별칭 브랜드들 삭제
                        for alias in aliases:
                            collection.delete_one({'name': alias})
                        
                        print(f"\n✅ 브랜드 병합 완료!")
                        print(f"- 대표 브랜드: '{representative_brand}'")
                        print(f"- 최종 별칭 목록: {merged_aliases}")
                        
                    except Exception as e:
                        print(f"MongoDB 업데이트 중 오류 발생: {e}")
                    
                    # 로깅
                    logger.info(f"{'='*50}")
                    logger.info(f"[{idx}/{total_brands}] {target_brand} 검사 결과")
                    logger.info("")
                    logger.info("유사도가 높은 브랜드들과 각 별칭들:")
                    for brand2, info2, similarity in similar_pairs:
                        logger.info(f"- {brand2} (유사도: {similarity:.4f})")
                        if info2.get('aliases'):
                            logger.info(f"  별칭: {', '.join(info2['aliases'])}")
                    logger.info("")
                    logger.info("제미나이 분석:")
                    logger.info(f"검사 대상 브랜드: '{target_brand}'")
                    logger.info(f"대표 브랜드로 선정: '{analysis['representative_brand']}'")
                    logger.info(f"별칭으로 처리: {analysis.get('aliases', [])}")
                    if analysis.get('different_brands'):
                        logger.info(f"다른 브랜드 제외: {', '.join(analysis['different_brands'])}")
                    logger.info("")
                    logger.info(f"{'='*50}")
                    logger.info("")
                
                # 사용자 입력 제거 - 자동으로 다음 브랜드로 진행

        print(f"\n\n모든 브랜드 처리 완료!")

    except Exception as e:
        print(f"치명적인 오류 발생: {e}")

if __name__ == "__main__":
    merge_brands()