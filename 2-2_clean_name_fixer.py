'''
[프로그램 설명]
MongoDB에서 clean_name이 빈칸인 프로필 데이터를 찾아서
full_name 기반으로 clean_name을 추출하여 업데이트하는 프로그램

[데이터 흐름]
1. MongoDB에서 clean_name이 비어있는 문서 검색
2. full_name이 있는 경우 Claude API를 사용하여 clean_name 추출
3. 추출된 clean_name으로 MongoDB 문서 업데이트

[주요 처리 로직]
1. MongoDB 연결 및 데이터 검색
2. Claude API를 사용한 이름 정제
3. MongoDB 데이터 업데이트
'''

import os
import time
import anthropic
import sys
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

def extract_clean_name(display_name):
    """Claude API를 사용하여 표시된 이름에서 대표 닉네임/이름 추출"""
    try:
        max_retries = 3
        retry_count = 0
        retry_delay = 2
        
        while retry_count < max_retries:
            try:
                print(f"\nClaude API에 이름 추출 요청 중... (입력: {display_name})")
                client = anthropic.Anthropic(
                    api_key=os.getenv('ANTHROPIC_API_KEY')  # API 키를 .env에서 불러옴
                )
                
                prompt = f"""다음 인스타그램 프로필 이름에서 가장 적절한 대표 닉네임을 추출해주세요:
                
                프로필 이름: {display_name}
                
                규칙:
                1. 이모지, 특수문자 제거
                2. 실명보다는 계정을 대표하는 닉네임을 우선 선택
                3. 여러 요소가 있다면 계정의 성격을 가장 잘 나타내는 것을 선택
                4. 실명과 닉네임이 함께 있다면 닉네임 우선
                5. 추출한 닉네임만 답변 (다른 설명 없이)
                6. 가능한 한글로 변환하여 표현
                
                예시:
                입력: "소소홈 (뭉야미)"
                출력: 뭉야미
                
                입력: "유네미니 ㅣ🎧 듣는살림 📻 살림라디오"
                출력: 유네미니
                
                입력: "Kim Ji Eun | 우남매 홈"
                출력: 우남매 홈
                
                입력: "12시에맛나요-YummyAt12"
                출력: 12시에맛나요
                
                입력: "the끌림 | 강보람"
                출력: the끌림
                
                입력: "B텔라 | Home Diary	B텔라"
                출력: B텔라

                입력: "Ji Hye Park"
                출력: 박지혜

                입력: "마로마트 | 다이어트먹트 이채은"
                출력: 마로마트"""

                message = client.messages.create(
                    model="claude-3-5-haiku",
                    max_tokens=50,
                    temperature=0,
                    system="대표 닉네임이나 이름만 정확히 추출하여 답변하세요. 다른 설명은 하지 마세요.",
                    messages=[
                        {"role": "user", "content": prompt}
                    ]
                )
                
                # message.content에서 순수 텍스트만 추출
                clean_name = message.content
                if hasattr(clean_name, 'text'):  # TextBlock 객체인 경우
                    clean_name = clean_name.text
                elif isinstance(clean_name, list):  # 리스트인 경우
                    clean_name = clean_name[0].text if clean_name else display_name
                
                # 문자열로 변환하고 앞뒤 공백 제거
                clean_name = str(clean_name).strip()
                
                print(f"추출 전: {display_name}")
                print(f"추출 후: {clean_name}")
                
                return clean_name
                
            except Exception as e:
                retry_count += 1
                print(f"\nClaude API 호출 {retry_count}번째 시도 실패: {str(e)}")
                
                if retry_count < max_retries:
                    print(f"{retry_delay}초 후 재시도합니다...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    print("최대 재시도 횟수를 초과했습니다. 원본 이름을 그대로 사용합니다.")
                    return display_name
                
    except Exception as e:
        print(f"\n이름 추출 중 오류 발생: {str(e)}")
        return display_name

def main():
    # MongoDB 연결
    mongo_uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    try:
        client = MongoClient(mongo_uri, server_api=ServerApi('1'))
        client.admin.command('ping')
        print("MongoDB에 성공적으로 연결되었습니다.")
        
        db = client['insta09_database']
        collection = db['02_test_influencer_data']  # 컬렉션 이름 변경
        
        # clean_name이 비어 있고, full_name이 있고, 09_is 필드가 'Y'인 문서 검색
        query = {
            '$and': [
                {'clean_name': {'$in': ['', None]}},  # clean_name이 비어있거나 없음
                {'full_name': {'$nin': ['', '-', None]}},  # full_name이 비어있지 않고 '-'가 아님
                {'09_is': 'Y'}  # 09_is 필드가 'Y'인 경우만 처리
            ]
        }
        
        # 필요한 필드만 가져오기
        projection = {
            'username': 1,
            'full_name': 1,
            'clean_name': 1,
            '_id': 1
        }
        
        # 데이터 조회
        cursor = collection.find(query, projection)
        docs = list(cursor)
        
        if not docs:
            print("처리할 데이터가 없습니다. 모든 프로필에 clean_name이 설정되어 있습니다.")
            return
            
        print(f"총 {len(docs)}개의 프로필에 clean_name을 설정해야 합니다.")
        
        # 각 문서에 대해 clean_name 업데이트
        updated_count = 0
        failed_count = 0
        
        for doc in docs:
            try:
                doc_id = doc['_id']
                full_name = doc['full_name']
                username = doc.get('username', '알 수 없음')
                
                print(f"\n처리 중: {username} / {full_name}")
                
                # clean_name 추출
                clean_name = extract_clean_name(full_name)
                
                if clean_name:
                    # MongoDB 업데이트
                    collection.update_one(
                        {'_id': doc_id},
                        {'$set': {'clean_name': clean_name}}
                    )
                    print(f"업데이트 완료: {username} / {full_name} -> {clean_name}")
                    updated_count += 1
                else:
                    print(f"추출 실패: {username} / {full_name}")
                    failed_count += 1
                
                # API 호출 간 간격 두기
                time.sleep(1)
                
            except Exception as e:
                print(f"처리 중 오류 발생: {str(e)}")
                failed_count += 1
        
        print(f"\n작업 완료!")
        print(f"업데이트된 프로필: {updated_count}개")
        print(f"실패한 프로필: {failed_count}개")
        
    except Exception as e:
        print(f"MongoDB 연결 또는 처리 실패: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()