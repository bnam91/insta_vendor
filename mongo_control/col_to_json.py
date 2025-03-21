import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson import ObjectId
from datetime import datetime
import os

# ObjectId와 datetime을 문자열로 변환하는 함수
def convert_objectid_to_str(data):
    if isinstance(data, ObjectId):
        return str(data)
    elif isinstance(data, datetime):
        return data.isoformat()
    elif isinstance(data, dict):
        # _id 필드 삭제
        data.pop('_id', None)
        return {key: convert_objectid_to_str(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_objectid_to_str(item) for item in data]
    return data

# 컬렉션을 JSON으로 저장하는 함수
def save_collection_to_json(client, db_name, collection_name):
    try:
        # 데이터베이스 선택
        db = client[db_name]
        
        # 원본 컬렉션
        source_collection = db[collection_name]
        
        # 데이터 가져오기
        all_documents = list(source_collection.find({}))
        all_documents = convert_objectid_to_str(all_documents)  # ObjectId 변환 및 _id 필드 삭제
        
        # 현재 스크립트 파일의 디렉토리 경로 가져오기
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # col_data 디렉토리 경로 (현재 스크립트와 같은 위치)
        col_data_dir = os.path.join(script_dir, "col_data")
        
        # col_data 디렉토리가 없으면 생성
        if not os.path.exists(col_data_dir):
            os.makedirs(col_data_dir)
            
        # JSON 파일로 저장
        json_file_path = os.path.join(col_data_dir, f"{collection_name}.json")
        with open(json_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(all_documents, json_file, ensure_ascii=False, indent=4)
        
        print(f"컬렉션 '{collection_name}'의 데이터가 '{json_file_path}'에 저장되었습니다.")
        return True
        
    except Exception as e:
        print(f"에러 발생: {e}")
        return False

# 직접 실행 시 사용되는 코드
if __name__ == "__main__":
    # MongoDB 연결
    uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri, server_api=ServerApi('1'))

    try:
        # 연결 확인
        client.admin.command('ping')
        print("MongoDB 연결 성공!")

        # 데이터베이스 선택
        db_name = 'insta09_database'
        
        # 백업할 컬렉션 목록
        collection_name = input("json으로 변경할 콜렉션을 입력하세요: ")
        
        # JSON으로 저장
        save_collection_to_json(client, db_name, collection_name)

    except Exception as e:
        print(f"에러 발생: {e}")

    finally:
        # 연결 종료
        client.close()
