import json

with open('brand_category.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
    brand_count = len(data['brands'])
    print(f'브랜드 총 개수: {brand_count}개')