import json

def count_usernames(json_str):
    # JSON 문자열을 파이썬 객체로 변환
    data = json.loads(json_str)
    
    # username이 있는 항목 수 계산
    username_count = sum(1 for item in data if "username" in item)
    
    return username_count

# JSON 파일 내용을 문자열로 읽기
with open('2-2_influencer_processing_data.json', 'r', encoding='utf-8') as file:
    json_content = file.read()

# username 개수 계산
total_usernames = count_usernames(json_content)
print(f"총 username 개수: {total_usernames}개")