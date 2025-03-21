import json
import tkinter as tk
from tkinter import filedialog
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# MongoDB 연결
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    # 연결 확인
    client.admin.command('ping')
    print("MongoDB 연결 성공!")
    
    # GUI로 JSON 파일 선택하기
    root = tk.Tk()
    root.withdraw()  # GUI 창 숨기기
    json_file_name = filedialog.askopenfilename(
        title="업로드할 JSON 파일을 선택하세요",
        filetypes=(("JSON 파일", "*.json"), ("모든 파일", "*.*"))
    )
    
    if not json_file_name:
        print("파일 선택이 취소되었습니다.")
        exit()
        
    print(f"선택된 파일: {json_file_name}")
    
    # 데이터베이스 이름 입력 받기
    db_name = input("사용할 데이터베이스 이름을 입력하세요: ")
    
    # 컬렉션 이름 입력 받기
    collection_name = input("사용할 컬렉션 이름을 입력하세요: ")
    
    # 데이터베이스 선택
    db = client[db_name]
    
    # 컬렉션 선택
    collection = db[collection_name]
    
    # JSON 파일 읽기
    with open(json_file_name, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 데이터 타입 확인 및 삽입
    if isinstance(data, list):
        # 리스트인 경우 insert_many 사용
        result = collection.insert_many(data)
        print(f"{len(result.inserted_ids)}개의 문서가 {collection_name} 컬렉션에 삽입되었습니다.")
    else:
        # 딕셔너리인 경우 insert_one 사용
        result = collection.insert_one(data)
        print(f"문서가 {collection_name} 컬렉션에 삽입되었습니다. ID: {result.inserted_id}")
    
    print("JSON 파일 업로드 완료!")
    
except Exception as e:
    print(f"오류 발생: {e}")

finally:
    # MongoDB 연결 종료
    client.close()
    print("MongoDB 연결 종료")
