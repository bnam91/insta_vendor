import json
from jellyfish import jaro_winkler_similarity
import re
from tqdm import tqdm

def is_korean(text):
    # 한글이 포함되어 있는지 확인
    return bool(re.search('[가-힣]', text))

def determine_main_brand(brand1, info1, brand2, info2):
    # 1. aliases가 있는 브랜드 우선
    if len(info1.get('aliases', [])) > len(info2.get('aliases', [])):
        return brand1, info1, brand2, info2
    elif len(info1.get('aliases', [])) < len(info2.get('aliases', [])):
        return brand2, info2, brand1, info1
    
    # 2. 한글 브랜드명 우선
    if is_korean(brand1) and not is_korean(brand2):
        return brand1, info1, brand2, info2
    elif is_korean(brand2) and not is_korean(brand1):
        return brand2, info2, brand1, info1
    
    # 3. 공백이 없는 브랜드명 우선
    if ' ' not in brand1 and ' ' in brand2:
        return brand1, info1, brand2, info2
    elif ' ' not in brand2 and ' ' in brand1:
        return brand2, info2, brand1, info1
    
    # 4. 더 짧은 브랜드명 우선
    if len(brand1) < len(brand2):
        return brand1, info1, brand2, info2
    elif len(brand2) < len(brand1):
        return brand2, info2, brand1, info1
    
    # 5. status가 done인 경우 우선
    if info1.get('status') == 'done' and info2.get('status') != 'done':
        return brand1, info1, brand2, info2
    elif info2.get('status') == 'done' and info1.get('status') != 'done':
        return brand2, info2, brand1, info1
    
    # 모든 조건이 동일한 경우
    return None, None, None, None

def get_max_similarity(str1, str2, aliases):
    # 기본 문자열 유사도 계산
    max_similarity = jaro_winkler_similarity(str1.lower(), str2.lower())
    
    # aliases 검사
    for alias in aliases:
        alias_similarity = jaro_winkler_similarity(str1.lower(), alias.lower())
        max_similarity = max(max_similarity, alias_similarity)
    
    return max_similarity

def merge_brands():
    try:
        print("브랜드 카테고리 파일 로딩 중...")
        with open('brand_cate_test.json', 'r', encoding='utf-8') as file:
            data = json.load(file)

        # 검사할 브랜드명 입력
        target_brand = input("\n검사할 브랜드명을 입력하세요: ")
        similar_brands = []
        
        print(f"\n브랜드 '{target_brand}' 검사 중...")
        
        # 입력된 브랜드가 json에 있는 경우
        is_new_brand = target_brand not in data['brands']
        target_info = data['brands'].get(target_brand, {'aliases': []})
        
        # 유사한 브랜드 찾기
        for brand2, info2 in data['brands'].items():
            if brand2 == target_brand:
                continue
            
            aliases1 = target_info.get('aliases', [])
            aliases2 = info2.get('aliases', [])
            
            similarity = max(
                get_max_similarity(target_brand, brand2, aliases2),
                get_max_similarity(brand2, target_brand, aliases1)
            )
            
            if similarity >= 0.8:
                similar_brands.append((brand2, info2, similarity))
        
        # 유사한 브랜드가 있으면 결과 출력
        if similar_brands:
            print("\n유사도 0.8 이상인 브랜드들:")
            for brand2, info2, similarity in similar_brands:
                print(f"- {brand2} (유사도: {similarity:.2f})")
                if info2.get('aliases'):
                    print(f"  별칭: {', '.join(info2['aliases'])}")
            
            # 사용자 확인
            proceed = input("\n위 브랜드들을 병합할까요? (y/n): ")
            if proceed.lower() != 'y':
                print("병합을 취소합니다.")
                return
            
            # 병합 진행
            brands_to_delete = set()
            print("\n병합 진행 중...")
            
            for brand2, info2, similarity in similar_brands:
                # 새로운 브랜드인 경우, 기존 브랜드의 aliases에 추가
                if is_new_brand:
                    main_brand, main_info = brand2, info2
                    main_aliases = set(main_info.get('aliases', []))
                    main_aliases.add(target_brand)
                    data['brands'][main_brand]['aliases'] = list(main_aliases)
                    print(f"  ㄴ 별칭 추가: {target_brand} -> {main_brand}의 aliases에 추가")
                else:
                    main_brand, main_info, sub_brand, sub_info = determine_main_brand(
                        target_brand, target_info, brand2, info2
                    )
                    
                    if main_brand is None:
                        main_brand, main_info = target_brand, target_info
                        sub_brand, sub_info = brand2, info2
                    
                    main_aliases = set(main_info.get('aliases', []))
                    main_aliases.add(sub_brand)
                    main_aliases.update(sub_info.get('aliases', []))
                    data['brands'][main_brand]['aliases'] = list(main_aliases)
                    
                    brands_to_delete.add(sub_brand)
                    print(f"  ㄴ 병합 완료: {sub_brand} -> {main_brand}")
            
            # 기존 브랜드 병합의 경우에만 브랜드 삭제
            if not is_new_brand:
                for brand in brands_to_delete:
                    if brand in data['brands']:
                        del data['brands'][brand]
                    
            print(f"\n처리 완료!")
            if not is_new_brand:
                print(f"총 {len(brands_to_delete)}개 브랜드 병합")
        else:
            print("\n유사도 0.8 이상인 브랜드가 없습니다.")
            if is_new_brand:
                print("새로운 브랜드를 추가하시겠습니까?")
                if input("(y/n): ").lower() == 'y':
                    data['brands'][target_brand] = {'aliases': []}
                    print(f"브랜드 '{target_brand}' 추가 완료")
        
    except Exception as e:
        print(f"오류 발생: {e}")

if __name__ == "__main__":
    merge_brands()