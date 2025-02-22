import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# MongoDB 연결
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    # 연결 확인
    client.admin.command('ping')
    print("MongoDB 연결 성공!")

    # 데이터베이스와 컬렉션 선택
    db = client['insta09_database']
    collection = db['01_test_newfeed_crawl_data']

    # JSON 파일 읽기
    with open('1-1_newfeed_crawl_data.json', 'r', encoding='utf-8') as file:
        data = json.load(file)

    # 데이터가 리스트인 경우 insert_many 사용, 단일 객체인 경우 insert_one 사용
    if isinstance(data, list):
        result = collection.insert_many(data)
        print(f"데이터 {len(result.inserted_ids)}개 삽입 성공! 데이터베이스: {db.name}, 컬렉션: {collection.name}")
    else:
        result = collection.insert_one(data)
        print(f"데이터 삽입 성공! 문서 ID: {result.inserted_id} 데이터베이스: {db.name}, 컬렉션: {collection.name}")

except FileNotFoundError:
    print("JSON 파일을 찾을 수 없습니다.")
except json.JSONDecodeError:
    print("JSON 파일 형식이 올바르지 않습니다.")
except Exception as e:
    print(f"에러 발생: {e}")

finally:
    # 연결 종료
    client.close()
