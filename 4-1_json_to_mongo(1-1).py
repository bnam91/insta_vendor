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
    #  collection = db['02_test_influencer_data']
   
    # 기존 인덱스 삭제
    try:
        collection.drop_indexes()
        print("모든 인덱스 삭제 완료!")
    except Exception as e:
        print(f"인덱스 삭제 중 오류 발생: {e}")
        pass

    # post_url에 유니크 인덱스 생성
    try:
        collection.create_index('post_url', unique=True)
        print("post_url 필드에 유니크 인덱스 생성 완료!")
    except Exception as e:
        print(f"인덱스 생성 중 오류 발생: {e}")
        pass

    # JSON 파일 읽기
    with open('1-1_newfeed_crawl_data.json', 'r', encoding='utf-8') as file:
    # with open('2-2_influencer_processing_data.json', 'r', encoding='utf-8') as file:
        data = json.load(file)

    # 데이터 처리 전에 중복 확인
    post_urls = [doc['post_url'] for doc in data]
    duplicate_urls = [url for url in post_urls if post_urls.count(url) > 1]
    
    # 데이터가 리스트인 경우 insert_many 사용, 단일 객체인 경우 insert_one 사용
    if isinstance(data, list):
        success_count = 0
        failed_documents = []
        duplicate_documents = []
        
        for doc in data:
            try:
                result = collection.update_one(
                    {'post_url': doc['post_url']},
                    {'$set': doc},
                    upsert=True
                )
                if result.modified_count > 0 or result.upserted_id:
                    success_count += 1
                else:
                    # 변경되지 않은 문서 기록
                    duplicate_documents.append(doc['post_url'])
            except Exception as e:
                failed_documents.append({
                    'document': doc,
                    'error': str(e)
                })
                print(f"문서 업데이트 중 오류 발생: {e}")
        
        # 실제 MongoDB의 문서 수 확인
        actual_count = collection.count_documents({})
        print(f"\n처리 완료!")
        print(f"입력된 데이터 수: {len(data)}")
        print(f"성공적으로 처리된 작업: {success_count}")
        print(f"MongoDB의 실제 문서 수: {actual_count}")
        print(f"실패: {len(failed_documents)}/{len(data)} 문서")
        
        # 실패한 문서 로그 저장
        if failed_documents:
            with open('failed_documents.json', 'w', encoding='utf-8') as f:
                json.dump(failed_documents, f, ensure_ascii=False, indent=2)
            print("실패한 문서들이 'failed_documents.json'에 저장되었습니다.")
            
        # 맨 마지막에 중복 URL 정보 출력
        if duplicate_urls:
            print(f"중복 URL: {duplicate_documents}")
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
