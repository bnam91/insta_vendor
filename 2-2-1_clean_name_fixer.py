'''
[프로그램 설명]
MongoDB에서 clean_name이 빈칸인 프로필 데이터를 찾아서
full_name 기반으로 clean_name을 추출하여 업데이트하는 프로그램

[데이터 흐름]
1. MongoDB에서 clean_name이 비어있는 문서 검색
2. full_name이 있는 경우 OpenAI GPT-4o-mini API를 사용하여 clean_name 추출
3. 추출된 clean_name으로 MongoDB 문서 업데이트

[주요 처리 로직]
1. MongoDB 연결 및 데이터 검색
2. OpenAI GPT-4o-mini API를 사용한 이름 정제
3. MongoDB 데이터 업데이트
'''

import os
import time
import sys
import openai  # OpenAI 라이브러리로 변경
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

def extract_clean_name(display_name):
    """OpenAI GPT-4o-mini API를 사용하여 표시된 이름에서 대표 닉네임/이름 추출"""
    try:
        retry_count = 0
        retry_delay = 15  # 고정 15초 대기
        
        while True:  # 무한 재시도
            try:
                print(f"\nGPT-4o-mini API에 이름 추출 요청 중... (입력: {display_name})")
                
                # OpenAI API 설정
                client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
                
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

                입력: "진재영 Actress 🌹"
                출력: 진재영
                
                입력: "유네미니 ㅣ🎧 듣는살림 📻 살림라디오"
                출력: 유네미니
                
                입력: "Kim Ji Eun | 우남매 홈"
                출력: 우남매
                
                입력: "12시에맛나요-YummyAt12"
                출력: 12시에맛나요
                
                입력: "the끌림 | 강보람"
                출력: the끌림
                
                입력: "B텔라 | Home Diary   B텔라"
                출력: B텔라
                
                입력: "Ji Hye Park"
                출력: 박지혜

                입력: "코코앞줌마 마켓🛒"
                출력: 코코앞줌마
                
                입력: "마로마트 | 다이어트먹트 이채은"
                출력: 마로마트"""

                # API 호출 (gpt-4o-mini 모델 사용)
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "당신은 프로필 이름에서 올바른 닉네임이나 대표 이름을 추출하는 전문가입니다."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0,
                    max_tokens=50,
                )
                
                # 응답에서 텍스트 추출
                clean_name = response.choices[0].message.content.strip()
                
                print(f"full_name: {display_name}")
                print(f"clean_name: {clean_name}")
                
                return clean_name
                
            except Exception as e:
                retry_count += 1
                print(f"\nGPT-4o-mini API 호출 {retry_count}번째 시도 실패: {str(e)}")
                print(f"[{retry_count}] INFO: 재시도까지 15초 대기 중...")
                time.sleep(retry_delay)  # 항상 15초 대기
                
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
        
        # 로그 디렉토리 생성
        log_dir = "2-2_log"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            print(f"로그 디렉토리 생성: {log_dir}")
        
        # 고정된 로그 파일 (추가 모드로 열기)
        log_file_path = os.path.join(log_dir, "clean_name_fixer.log")
        log_file = open(log_file_path, "a", encoding="utf-8")
        
        # 로그 실행 시작 헤더
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
        log_file.write("\n\n" + "=" * 80 + "\n")
        log_file.write(f"[{current_time}] Clean Name Fixer 실행 시작\n")
        log_file.write("=" * 80 + "\n\n")
        log_file.flush()  # 즉시 디스크에 쓰기
        
        db = client['insta09_database']
        collection = db['02_main_influencer_data']  # 컬렉션 이름 변경
        
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
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            message = f"[{timestamp}] INFO: 처리할 데이터가 없습니다. 모든 프로필에 clean_name이 설정되어 있습니다."
            print(message)
            log_file.write(message + "\n")
            log_file.flush()  # 즉시 디스크에 쓰기
            log_file.close()
            return
            
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        message = f"[{timestamp}] INFO: 총 {len(docs)}개의 프로필에 clean_name을 설정해야 합니다."
        print(message)
        log_file.write(message + "\n")
        log_file.flush()  # 즉시 디스크에 쓰기
        
        # 각 문서에 대해 clean_name 업데이트
        updated_count = 0
        failed_count = 0
        
        for idx, doc in enumerate(docs, 1):
            try:
                doc_id = doc['_id']
                full_name = doc['full_name']
                username = doc.get('username', '알 수 없음')
                
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                message = f"\n[{timestamp}] PROCESS [{idx}/{len(docs)}]: {username}"
                print(message)
                log_file.write(message + "\n")
                log_file.write(f"  full_name: {full_name}\n")
                log_file.flush()
                
                # clean_name 추출
                clean_name = extract_clean_name(full_name)
                
                if clean_name:
                    # MongoDB 업데이트
                    collection.update_one(
                        {'_id': doc_id},
                        {'$set': {'clean_name': clean_name}}
                    )
                    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                    message = f"[{timestamp}] SUCCESS: {username} 업데이트 완료"
                    print(message)
                    log_file.write(message + "\n")
                    log_file.write(f"  full_name: {full_name}\n")
                    log_file.write(f"  clean_name: {clean_name}\n")
                    log_file.write("-" * 60 + "\n")
                    log_file.flush()
                    updated_count += 1
                else:
                    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                    message = f"[{timestamp}] FAILED: {username} 추출 실패"
                    print(message)
                    log_file.write(message + "\n")
                    log_file.write(f"  full_name: {full_name}\n")
                    log_file.write("-" * 60 + "\n")
                    log_file.flush()
                    failed_count += 1
                
                # 성공적인 API 호출 후에는 대기하지 않음
                
            except Exception as e:
                retry_count += 1
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                print(f"\n[{timestamp}] ERROR: GPT-4o-mini API 호출 {retry_count}번째 시도 실패: {str(e)}")
                print(f"[{timestamp}] INFO: 재시도까지 15초 대기 중...")
                log_file.write(f"[{timestamp}] ERROR: GPT-4o-mini API 호출 {retry_count}번째 시도 실패: {str(e)}\n")
                log_file.write(f"[{timestamp}] INFO: 재시도까지 15초 대기 중...\n")
                log_file.flush()
                time.sleep(15)  # API 호출 실패 시에만 15초 대기
                failed_count += 1
        
        # 요약 정보
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        log_file.write("\n" + "=" * 80 + "\n")
        log_file.write(f"[{timestamp}] 작업 완료 요약\n")
        log_file.write("-" * 60 + "\n")
        log_file.write(f"총 처리 대상: {len(docs)}개\n")
        log_file.write(f"업데이트 성공: {updated_count}개\n")
        log_file.write(f"업데이트 실패: {failed_count}개\n")
        log_file.write("=" * 80 + "\n\n")
        log_file.flush()  # 즉시 디스크에 쓰기
        
        # 콘솔 출력
        print("\n" + "=" * 40)
        print(f"작업 완료!")
        print(f"업데이트된 프로필: {updated_count}개")
        print(f"실패한 프로필: {failed_count}개")
        print("=" * 40)
        
        # 로그 파일 닫기
        log_file.close()
        
    except Exception as e:
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        message = f"[{timestamp}] CRITICAL: MongoDB 연결 또는 처리 실패: {e}"
        print(message)
        try:
            log_file.write(message + "\n")
            log_file.write("=" * 80 + "\n\n")
            log_file.flush()  # 즉시 디스크에 쓰기
            log_file.close()
        except:
            pass
        sys.exit(1)

if __name__ == "__main__":
    main()