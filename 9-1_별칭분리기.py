from pymongo import MongoClient
from pymongo.server_api import ServerApi

# MongoDB 연결 설정
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))
db = client['insta09_database']
collection = db['08_main_brand_category_data']

def aliases_separation():
    while True:
        # 브랜드명 입력 받기
        brand_name = input("\n브랜드명을 입력하세요 (종료하려면 'q' 입력): ")
        
        if brand_name.lower() == 'q':
            break

        # 입력받은 브랜드명이 대표명이나 aliases에 있는 도큐먼트 검색
        brand_doc = collection.find_one({
            "$or": [
                {"name": brand_name},
                {"aliases": brand_name}
            ]
        })

        if brand_doc and brand_doc.get('aliases'):
            print(f"\n현재 '{brand_doc['name']}'의 aliases 목록:")
            for alias in brand_doc['aliases']:
                print(f"- {alias}")
            
            # 분리할 alias 입력 받기
            alias_to_separate = input("\n분리할 aliases를 입력하세요: ")
            
            if alias_to_separate in brand_doc['aliases']:
                # 중복 확인
                existing_doc = collection.find_one({"name": alias_to_separate})
                if existing_doc:
                    print(f"\n⚠️ '{alias_to_separate}'이(가) 이미 대표명으로 존재하는 도큐먼트가 있습니다.")
                    print("중복 생성을 방지하기 위해 새로운 도큐먼트 생성이 취소되었습니다.")
                else:
                    # 기존 도큐먼트에서 alias 제거
                    collection.update_one(
                        {"_id": brand_doc['_id']},
                        {"$pull": {"aliases": alias_to_separate}}
                    )
                    
                    # 새로운 도큐먼트 생성
                    new_doc = {
                        "name": alias_to_separate,
                        "category": brand_doc.get('category', ''),
                        "aliases": [alias_to_separate],
                        "level": brand_doc.get('level', ''),
                        "status": brand_doc.get('status', 'ready')
                    }
                    
                    # 새 도큐먼트 삽입
                    collection.insert_one(new_doc)
                    
                    print(f"\n'{alias_to_separate}'가 성공적으로 분리되어 새로운 도큐먼트로 생성되었습니다.")
            else:
                print("입력하신 alias가 목록에 없습니다.")
        else:
            print("해당 브랜드를 찾을 수 없거나 aliases가 없습니다. 다시 시도해주세요.")

def change_main_brand():
    while True:
        # 브랜드명 입력 받기
        search_name = input("\n브랜드명을 입력하세요 (종료하려면 'q' 입력): ")
        
        if search_name.lower() == 'q':
            break

        # 입력받은 브랜드명이 대표명이나 aliases에 있는 도큐먼트 검색
        brand_doc = collection.find_one({
            "$or": [
                {"name": search_name},
                {"aliases": search_name}
            ]
        })

        if brand_doc:
            print("\n[변경 전 정보]")
            print(f"대표명: {brand_doc['name']}")
            print("aliases 목록:")
            for alias in brand_doc['aliases']:
                print(f"- {alias}")
            
            # 새로운 대표명으로 지정할 브랜드명 입력 받기
            new_main = input("\n대표명으로 지정할 브랜드명을 입력하세요: ")
            
            if new_main in brand_doc['aliases']:
                # 다른 도큐먼트에서 동일한 대표명이 있는지 확인
                existing_doc = collection.find_one({
                    "_id": {"$ne": brand_doc['_id']},  # 현재 도큐먼트 제외
                    "name": new_main
                })
                
                if existing_doc:
                    print(f"\n⚠️ '{new_main}'이(가) 이미 다른 도큐먼트의 대표명으로 존재합니다.")
                    print("중복을 방지하기 위해 대표명 변경이 취소되었습니다.")
                    continue
                
                old_name = brand_doc['name']
                
                # aliases 리스트에서 새로운 대표명 제거
                new_aliases = [alias for alias in brand_doc['aliases'] if alias != new_main]
                
                # 기존 대표명이 aliases 목록에 없을 경우에만 추가
                if old_name not in new_aliases:
                    new_aliases.append(old_name)
                
                # 도큐먼트 업데이트
                collection.update_one(
                    {"_id": brand_doc['_id']},
                    {
                        "$set": {
                            "name": new_main,
                            "aliases": new_aliases,
                            "status": "done"
                        }
                    }
                )
                
                # 변경된 도큐먼트 재조회
                updated_doc = collection.find_one({"_id": brand_doc['_id']})
                
                print("\n[🚩변경 결과]")
                print("➡️ 대표명 변경:")
                print(f"   {brand_doc['name']} → {updated_doc['name']}")
                print("\n➡️ aliases 목록 변경:")
                print("   변경 전:", brand_doc['aliases'])
                print("   변경 후:", updated_doc['aliases'])
                print(f"\n➡️ 전체 aliases 개수: {len(updated_doc['aliases'])}개")
                print(f"\n➡️ status 변경: {brand_doc.get('status', 'ready')} → {updated_doc['status']}")
                
            else:
                print("입력하신 브랜드명이 aliases 목록에 없습니다.")
        else:
            print("해당 브랜드를 찾을 수 없습니다. 다시 시도해주세요.")

def change_brand_name():
    while True:
        # 브랜드명 입력 받기
        search_name = input("\n변경할 brand를 입력해주세요 (종료하려면 'q' 입력): ")
        
        if search_name.lower() == 'q':
            break

        # 입력받은 브랜드명이 대표명이나 aliases에 있는 도큐먼트 검색
        brand_doc = collection.find_one({
            "$or": [
                {"name": search_name},
                {"aliases": search_name}
            ]
        })

        if brand_doc:
            print("\n[현재 정보]")
            print(f"대표명: {brand_doc['name']}")
            print("aliases 목록:")
            for alias in brand_doc['aliases']:
                print(f"- {alias}")
            
            # 새로운 브랜드명 입력 받기
            new_name = input("\n브랜드명을 수정해주세요: ")
            
            # 변경 확인
            confirm = input(f"\n'{search_name}'을(를) '{new_name}'(으)로 변경할까요? (y/n): ")
            if confirm.lower() != 'y':
                print("변경이 취소되었습니다.")
                continue
            
            # 기존 도큐먼트 업데이트
            old_name = search_name
            
            if search_name == brand_doc['name']:
                # 대표명인 경우
                collection.update_one(
                    {"_id": brand_doc['_id']},
                    {"$set": {"name": new_name}}
                )
            else:
                # aliases인 경우
                collection.update_one(
                    {"_id": brand_doc['_id']},
                    {"$pull": {"aliases": search_name}}
                )
            
            # 다른 도큐먼트의 aliases에서 제거
            collection.update_many(
                {"_id": {"$ne": brand_doc['_id']}, "aliases": search_name},
                {"$pull": {"aliases": search_name}}
            )
            
            # 변경된 도큐먼트 재조회
            updated_doc = collection.find_one({"_id": brand_doc['_id']})
            
            print("\n[🚩변경 결과]")
            print(f"브랜드명 변경: {old_name} → {new_name}")
            print(f"대표명: {updated_doc['name']}")
            print("aliases 목록:", updated_doc['aliases'])
            
            # aliases 분리 여부 확인
            if search_name in brand_doc['aliases']:
                separate = input("\n변경한 brand명을 aliases에서 분리하여 새로운 도큐먼트로 만드시겠습니까? (y/n): ")
                if separate.lower() == 'y':
                    # 중복 확인
                    existing_doc = collection.find_one({"name": new_name})
                    if existing_doc:
                        print(f"\n⚠️ '{new_name}'이(가) 이미 대표명으로 존재하는 도큐먼트가 있습니다.")
                        print("중복 생성을 방지하기 위해 새로운 도큐먼트 생성이 취소되었습니다.")
                    else:
                        # 새로운 도큐먼트 생성
                        new_doc = {
                            "name": new_name,
                            "category": brand_doc.get('category', ''),
                            "aliases": [new_name],
                            "level": brand_doc.get('level', ''),
                            "status": "ready"
                        }
                        collection.insert_one(new_doc)
                        print(f"\n'{new_name}'가 성공적으로 분리되어 새로운 도큐먼트로 생성되었습니다.")
        
        else:
            print("해당 브랜드를 찾을 수 없습니다. 다시 시도해주세요.")

while True:
    print("\n어떤 작업을 실시할건가요?")
    print("1. aliases 분리")
    print("2. 대표브랜드와 aliases 변경")
    print("3. 브랜드명 변경")
    print("q. 종료")
    
    choice = input("\n💡 원하는 작업의 번호를 입력하세요: ")
    
    if choice == 'q':
        break
    elif choice == '1':
        aliases_separation()
    elif choice == '2':
        change_main_brand()
    elif choice == '3':
        change_brand_name()
    else:
        print("올바른 메뉴를 선택해주세요.")
