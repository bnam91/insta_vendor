import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson import ObjectId  # ObjectId를 가져옵니다.

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
        input("json으로 변경할 콜렉션을 입력하세요: ")  # 사용자 입력으로 변경
    ]
    
    # JSON 파일로 저장
    def convert_objectid_to_str(data):
        if isinstance(data, ObjectId):
            return str(data)
        elif isinstance(data, dict):
            # _id 필드 삭제
            data.pop('_id', None)
            return {key: convert_objectid_to_str(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [convert_objectid_to_str(item) for item in data]
        return data

    # 각 컬렉션 백업
    for collection_name in collections_to_backup:
        # 원본 컬렉션
        source_collection = db[collection_name]
        
        # 데이터 가져오기
        all_documents = list(source_collection.find({}))
        all_documents = convert_objectid_to_str(all_documents)  # ObjectId 변환 및 _id 필드 삭제
        
        # JSON 파일로 저장
        json_file_path = f"col_data/{collection_name}.json"  # JSON 파일 경로를 col_data 폴더로 설정
        with open(json_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(all_documents, json_file, ensure_ascii=False, indent=4)  # JSON 파일에 데이터 저장
        print(f"컬렉션 '{collection_name}'의 데이터가 '{json_file_path}'에 저장되었습니다.")

except Exception as e:
    print(f"에러 발생: {e}")

finally:
    # 연결 종료
    client.close()
