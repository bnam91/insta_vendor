import json

# 필드명 매핑 딕셔너리
field_mapping = {
    "id": "num",
    "추가날짜": "add_date",
    "팔로우여부": "is_following",
    "게시물": "posts",
    "팔로워": "followers",
    "팔로우": "following",
    "이름": "full_name",
    "이름추출": "clean_name",
    "소개": "bio",
    "외부프로필링크": "out_link",
    "공구유무": "09_is",
    "카테고리": "category",
    "키워드": "keywords",
    "릴스평균조회수(최근 15개)": "reels_views(15)",
    "이미지url": "image_url",
    "브랜드": "brand",
    # 추가된 점수 관련 필드
    "콘텐츠점수(5점)": "content_score",
    "팔로워점수": "follower_score",
    "게시물점수": "post_score",
    "릴스점수": "reels_score",
    "콘텐츠가산점": "content_bonus_score",
    "최종점수": "final_score",
    "등급": "grade",
    "이전등급": "pre_grades"
}

# JSON 파일 읽기
with open('2-2_influencer_processing_data.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# 변경된 필드 수를 추적하기 위한 카운터
changed_count = 0

# 각 항목의 필드명 변경
for item in data:
    for old_key, new_key in field_mapping.items():
        if old_key in item:
            item[new_key] = item.pop(old_key)
            changed_count += 1
            print(f"필드 변경: {old_key} → {new_key}")

# 변경된 데이터를 파일에 저장
with open('2-2_influencer_processing_data.json', 'w', encoding='utf-8') as file:
    json.dump(data, file, ensure_ascii=False, indent=4)

print(f"\n총 {changed_count}개의 필드가 변경되었습니다.")
