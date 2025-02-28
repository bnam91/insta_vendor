from pymongo import MongoClient
from pymongo.server_api import ServerApi

# MongoDB 연결 설정
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))
db = client['insta09_database']
collection = db['08_main_brand_category_data']

# 사용자로부터 aliases 개수 입력 받기
aliases_input = input("aliases가 몇 개인 브랜드를 찾으시겠습니까? (범위 검색은 '2,5'와 같이 입력): ")

# 입력값 처리
if ',' in aliases_input:
    min_aliases, max_aliases = map(int, aliases_input.split(','))
    print(f"\naliases가 {min_aliases}~{max_aliases}개인 브랜드:")
    count = 0
    for doc in collection.find():
        if doc.get('aliases') and min_aliases <= len(doc['aliases']) <= max_aliases:
            count += 1
            print(f"\n브랜드: {doc['name']}")
            print(f"Aliases 개수: {len(doc['aliases'])}")
            print(f"Aliases: {doc['aliases']}")
            print("-" * 30)
    print(f"\n총 {count}개의 브랜드가 {min_aliases}~{max_aliases}개의 aliases를 가지고 있습니다.")

else:
    min_aliases = int(aliases_input)
    print(f"\naliases가 {min_aliases}개 이상인 브랜드:")
    count = 0
    for doc in collection.find():
        if doc.get('aliases') and len(doc['aliases']) >= min_aliases:
            count += 1
            print(f"\n브랜드: {doc['name']}")
            print(f"Aliases 개수: {len(doc['aliases'])}")
            print(f"Aliases: {doc['aliases']}")
            print("-" * 30)
    print(f"\n총 {count}개의 브랜드가 {min_aliases}개 이상의 aliases를 가지고 있습니다.")
