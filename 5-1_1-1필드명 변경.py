import json

# 필드명 매핑 딕셔너리
field_mapping = {
    "작성시간": "cr_at",
    "작성자": "author",
    "본문": "content",
    "게시물링크": "post_url",
    "수집일자": "crawl_date",
    "공구피드": "09_feed",
    "공구상품": "09_item",
    "브랜드": "09_brand",
    "오픈예정일": "open_date",
    "공구마감일": "end_date",
    "처리여부": "processed"
}

# JSON 파일 읽기
with open('1-1_newfeed_crawl_data.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# 변경된 필드 수를 추적하기 위한 카운터
changed_count = 0

# 각 항목의 필드명 변경 및 새 필드 추가
for item in data:
    # processed 값을 false로 설정
    item["processed"] = False
    
    # 새로운 필드 추가
    item["09_item_category"] = ""
    
    # 기존 필드명 변경
    for old_key, new_key in field_mapping.items():
        if old_key in item:
            item[new_key] = item.pop(old_key)
            changed_count += 1
            print(f"필드 변경: {old_key} → {new_key}")

# 변경된 데이터를 파일에 저장
with open('1-1_newfeed_crawl_data.json', 'w', encoding='utf-8') as file:
    json.dump(data, file, ensure_ascii=False, indent=4)

print(f"\n총 {changed_count}개의 필드가 변경되었습니다.")
