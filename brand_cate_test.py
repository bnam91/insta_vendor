import json
from jellyfish import jaro_winkler_similarity
from collections import defaultdict

def search_brand():
    # JSON 파일 읽기
    with open('brand_cate_test.json', 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 전체 브랜드 수 출력
    total_brands = len(data['brands'])
    print(f"\n전체 브랜드 수: {total_brands}")
    
    while True:
        # 브랜드명 입력 받기
        brand_name = input("\n브랜드명을 입력하세요 (종료하려면 'q' 입력): ")
        
        if brand_name.lower() == 'q':
            print("프로그램을 종료합니다.")
            break
        
        similarities = []
        # 유사도 구간별 카운트를 위한 딕셔너리
        similarity_ranges = defaultdict(int)
        
        for brand in data['brands']:
            similarity = jaro_winkler_similarity(brand_name, brand)
            # aliases도 확인
            for alias in data['brands'][brand]['aliases']:
                alias_similarity = jaro_winkler_similarity(brand_name, alias)
                similarity = max(similarity, alias_similarity)
            
            similarities.append((brand, similarity))
            
            # 유사도 구간 카운트
            if similarity >= 0.9:
                similarity_ranges['0.9-1.0'] += 1
            elif similarity >= 0.8:
                similarity_ranges['0.8-0.9'] += 1
            elif similarity >= 0.7:
                similarity_ranges['0.7-0.8'] += 1
            elif similarity >= 0.6:
                similarity_ranges['0.6-0.7'] += 1
            else:
                similarity_ranges['0.0-0.6'] += 1
        
        # 유사도 기준으로 정렬하고 상위 10개 선택
        similarities.sort(key=lambda x: x[1], reverse=True)
        top_matches = similarities[:10]
        
        print("\n유사도 구간별 브랜드 수:")
        for range_name, count in sorted(similarity_ranges.items(), reverse=True):
            print(f"유사도 {range_name}: {count}개")
        
        print(f"\n'{brand_name}'와(과) 가장 유사한 상위 10개 브랜드:")
        for brand, similarity in top_matches:
            brand_info = data['brands'][brand]
            print(f"\n유사도: {similarity:.2f}")
            print(f"""    "{brand}": {{
      "category": "{brand_info['category']}",
      "aliases": {json.dumps(brand_info['aliases'], ensure_ascii=False, indent=6)},
      "level": "{brand_info['level']}",
      "status": "{brand_info['status']}"
    }},""")

if __name__ == "__main__":
    search_brand()