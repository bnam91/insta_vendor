import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import datetime
import re
import keyboard  # keyboard 라이브러리 추가

# MongoDB 연결
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    
    # 연결 확인
    client.admin.command('ping')
    print("MongoDB 연결 성공!")

    # 데이터베이스 선택
    db = client['insta09_database']
    
    # 사용자로부터 원본 컬렉션 이름 입력 받기
    collection_name = input("백업할 원본 컬렉션 이름을 입력하세요 (붙여넣기 후 수정 가능): ")
    print(f"입력한 원본 컬렉션 이름: '{collection_name}'입니다. 엔터를 눌러 계속 진행하세요.")

    # 엔터 키가 눌릴 때까지 대기
    keyboard.wait('enter')

    # 사용자로부터 백업 컬렉션 이름 입력 받기
    while True:
        backup_collection_name = input("변경할 백업 컬렉션 이름을 입력하세요 (붙여넣기 후 수정 가능): ")
        if not backup_collection_name:  # 빈 문자열 체크 추가
            print("경고: 백업 컬렉션 이름이 비어 있습니다. 다른 이름을 입력하세요.")
        elif backup_collection_name == collection_name:
            print("경고: 백업 컬렉션 이름이 원본 컬렉션 이름과 동일합니다. 다른 이름을 입력하세요.")
        else:
            break
    print(f"입력한 백업 컬렉션 이름: '{backup_collection_name}'입니다. 엔터를 눌러 계속 진행하세요.")

    # 엔터 키가 눌릴 때까지 대기
    keyboard.wait('enter')
    
    # 백업할 컬렉션 목록
    collections_to_backup = [
        collection_name,
    ]
    
    # 각 컬렉션 백업
    for collection_name in collections_to_backup:
        # 원본 컬렉션
        source_collection = db[collection_name]
        
        # 백업 컬렉션 생성
        backup_collection = db[backup_collection_name]
        
        # 데이터 가져오기
        all_documents = list(source_collection.find({}))
        
        # _id 필드 제거 (새 컬렉션에 삽입 시 자동 생성됨)
        for doc in all_documents:
            if '_id' in doc:
                del doc['_id']
        
        # 백업 컬렉션에 데이터 삽입
        if all_documents:
            backup_collection.insert_many(all_documents)
            print(f"백업 완료! 컬렉션 '{backup_collection_name}'에 {len(all_documents)}개의 문서가 백업되었습니다.")
        else:
            print(f"컬렉션 '{collection_name}'에는 백업할 데이터가 없습니다.")
        
        # 인덱스 복사
        indexes = source_collection.index_information()
        for index_name, index_info in indexes.items():
            if index_name != '_id_':  # 기본 _id 인덱스는 자동 생성되므로 제외
                backup_collection.create_index(index_info['key'], **{k: v for k, v in index_info.items() if k != 'key'})

except Exception as e:
    print(f"에러 발생: {e}")

finally:
    # 연결 종료
    client.close()
