import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import datetime
import re

# MongoDB 연결
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    # 연결 확인
    client.admin.command('ping')
    print("MongoDB 연결 성공!")

    # 데이터베이스 선택
    db = client['insta09_database']
    
    # 백업할 컬렉션 목록
    collections_to_backup = [
        '01_main_newfeed_crawl_data',
        '02_main_influencer_data',
        '08_test_brand_category_data'
    ]
    
    # 현재 날짜 및 시간
    current_date = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    current_datetime = datetime.datetime.now()
    
    # 30일 이상 지난 백업 컬렉션 삭제
    all_collections = db.list_collection_names()
    backup_pattern = re.compile(r'backup_.*_(\d{8})_\d{6}$')
    
    for collection_name in all_collections:
        match = backup_pattern.match(collection_name)
        if match:
            date_str = match.group(1)
            try:
                # 백업 날짜 파싱
                backup_date = datetime.datetime.strptime(date_str, "%Y%m%d")
                
                # 30일 이상 지났는지 확인
                days_difference = (current_datetime - backup_date).days
                if days_difference >= 30:
                    # 30일 이상 지난 백업 삭제
                    db.drop_collection(collection_name)
                    print(f"오래된 백업 삭제: '{collection_name}' (생성일로부터 {days_difference}일 경과)")
            except ValueError:
                # 날짜 형식이 맞지 않는 경우 무시
                continue
    
    # 각 컬렉션 백업
    for collection_name in collections_to_backup:
        # 원본 컬렉션
        source_collection = db[collection_name]
        
        # 백업 컬렉션 이름 생성 (backup_ + 원본 이름 + 날짜)
        backup_collection_name = f"backup_{collection_name}_{current_date}"
        
        # 백업 컬렉션 생성
        backup_collection = db[backup_collection_name]
        
        # 인덱스 복사
        indexes = source_collection.index_information()
        for index_name, index_info in indexes.items():
            # 모든 인덱스 복사 (기본 _id 인덱스 제외)
            if index_name != '_id_':
                backup_collection.create_index(index_info['key'], **{k: v for k, v in index_info.items() if k != 'key'})
                print(f"복사된 인덱스: '{index_name}'")
        
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

except Exception as e:
    print(f"에러 발생: {e}")

finally:
    # 연결 종료
    client.close()
