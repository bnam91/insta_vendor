"""
파일 설명:
이 스크립트는 인플루언서의 등급을 계산하고 업데이트하는 프로그램입니다.

입력 파일:
- 2-2_influencer_processing_data.json
  필요한 데이터 필드:
  - username: 인플루언서 사용자명
  - 팔로워: 팔로워 수
  - 게시물: 게시물 수
  - 릴스평균조회수(최근 15개): 최근 15개 릴스의 평균 조회수
  - 콘텐츠점수(5점): 콘텐츠 품질 점수 (1-5점)
  - 이전등급: 과거 등급 기록 배열

출력/업데이트:
- 동일한 JSON 파일에 다음 필드들이 추가 또는 업데이트됨:
  - 최종점수: 전체 점수 합계
  - 팔로워점수: 팔로워 수에 따른 점수 (0-30점)
  - 게시물점수: 게시물 수에 따른 점수 (2-30점)
  - 릴스점수: 릴스 조회수에 따른 점수 (2-40점)
  - 콘텐츠가산점: 콘텐츠 품질 점수 × 4 (최대 20점)
  - 등급: 최종 등급 (R, D, C, B, A, S)
  - 이전등급: 최근 5개까지의 등급 기록

처리 과정:
1. JSON 파일에서 데이터 로드
2. 숫자 데이터 정제 (쉼표 제거, 빈 값 처리)
3. 각 항목별 점수 계산
4. 최종 점수 합산
5. 등급 산정
6. 결과를 원본 JSON 파일에 저장
7. 통계 정보 출력

인플루언서 등급 산정 기준

1. 등급 분류
   - R등급: 팔로워 5,000 미만
   - D등급: 팔로워 5,000 이상 ~ 10,000 미만
   - C/B/A/S등급: 점수 계산 후 백분위 기준으로 분류
     * S등급: 상위 15% (85백분위수 이상)
     * A등급: 상위 15~40% (60백분위수 이상)
     * B등급: 상위 40~70% (30백분위수 이상)
     * C등급: 나머지 (하위 30%)

2. 점수 산정 기준 (최대 120점)
   A) 팔로워 점수 (최대 30점)
      - 20만 이상: 30점 (절대점수)
      - 나머지: 상대평가 (5% 단위)
        * 상위 5%: 29점
        * 상위 5~10%: 28점
        * 상위 10~15%: 27점
        ...
        * 상위 90~95%: 2점
        * 하위 5%: 1점

   B) 게시물 점수 (최대 30점, 상대평가)
      * 상위 5%: 30점
      * 상위 5~10%: 29점
      * 상위 10~15%: 28점
      ...
      * 상위 90~95%: 3점
      * 하위 5%: 2점

   C) 릴스 점수 (최대 40점, 상대평가)
      * 상위 5%: 40점
      * 상위 5~10%: 38점
      * 상위 10~15%: 36점
      ...
      * 상위 90~95%: 4점
      * 하위 5%: 2점

   D) 콘텐츠 가산점 (최대 20점)
      - 콘텐츠점수(1~5점) × 4
      - 예: 콘텐츠점수 5점 = 20점 가산
"""

import json
import pandas as pd
import numpy as np
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os

def load_data_from_mongodb():
    """MongoDB에서 인플루언서 데이터를 로드"""
    try:
        print("MongoDB 연결 시도 중...")
        uri = os.getenv('MONGODB_URI', "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
        client = MongoClient(uri, server_api=ServerApi('1'))
        
        # 연결 테스트
        client.admin.command('ping')
        print("MongoDB 연결 성공!")
        
        # 데이터베이스와 컬렉션 선택
        db = client['insta09_database']
        collection = db['02_test_influencer_data']
        
        # 모든 문서 가져오기
        print("데이터 로드 중...")
        data = list(collection.find())
        print(f"총 {len(data)}개의 문서를 로드했습니다.")
        
        if len(data) > 0:
            print(f"첫 번째 문서 샘플: {data[0].keys()}")
        
        return pd.DataFrame(data), collection
    except Exception as e:
        print("MongoDB 연결 또는 데이터 로드 오류:", str(e))
        print("자세한 오류 정보:", repr(e))
        return None, None

def clean_numeric_data(df):
    """숫자 데이터 정제"""
    print("데이터 정제 시작...")
    
    # 필드 존재 여부 확인
    existing_columns = df.columns.tolist()
    print(f"데이터프레임 컬럼: {existing_columns}")
    
    # 영어 필드명으로 변경된 컬럼들에 대해 처리
    for col in ['followers', 'posts', 'reels_views(15)', 'content_score']:
        if col not in df.columns:
            print(f"경고: '{col}' 컬럼이 데이터프레임에 없습니다.")
            continue
            
        print(f"'{col}' 컬럼 정제 중...")
        # 빈 문자열이나 누락된 값을 0으로 변환
        df[col] = df[col].replace('', '0')
        # NaN 값을 0으로 변환
        df[col] = df[col].fillna('0')
        # 쉼표 제거 후 숫자로 변환
        df[col] = df[col].astype(str).str.replace(',', '')
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
    print("데이터 정제 완료")
    return df

def calculate_follower_score(followers):
    """팔로워 수에 따른 점수 계산"""
    if followers >= 200000:
        return 30
    return 0  # 나머지는 상대평가에서 처리

def calculate_percentile_score(value, percentiles, scores):
    """백분위 기반 점수 계산"""
    for threshold, score in zip(percentiles, scores):
        if value >= threshold:
            return score
    return scores[-1]

def calculate_grades(df):
    """인플루언서 등급 및 점수 계산"""
    # 모든 점수 컬럼 초기화
    df['final_score'] = 0
    df['follower_score'] = 0
    df['post_score'] = 0
    df['reels_score'] = 0
    df['content_bonus_score'] = 0
    
    # R, D 등급 필터링
    df['grade'] = 'C'
    mask_r = df['followers'] < 5000
    mask_d = (df['followers'] >= 5000) & (df['followers'] < 10000)
    df.loc[mask_r, 'grade'] = 'R'
    df.loc[mask_d, 'grade'] = 'D'
    
    # 점수 계산 대상
    scoring_mask = ~(mask_r | mask_d)
    
    # 1. 팔로워 점수
    # 먼저 20만 이상 계정에 30점 부여
    df.loc[scoring_mask, 'follower_score'] = df.loc[scoring_mask, 'followers'].apply(calculate_follower_score)
    
    # 20만 미만 계정들에 대해 상대평가
    follower_relative_mask = (scoring_mask) & (df['followers'] < 200000)
    follower_percentiles = np.percentile(df.loc[follower_relative_mask, 'followers'], 
                                       [95, 90, 85, 80, 75, 70, 65, 60, 55, 50, 
                                        45, 40, 35, 30, 25, 20, 15, 10, 5])
    follower_scores = list(range(29, 0, -1))  # 29점부터 1점까지
    df.loc[follower_relative_mask, 'follower_score'] = df.loc[follower_relative_mask, 'followers'].apply(
        lambda x: calculate_percentile_score(x, follower_percentiles, follower_scores)
    )
    
    # 2. 게시물 점수 (상대평가)
    post_percentiles = np.percentile(df.loc[scoring_mask, 'posts'], 
                                   [95, 90, 85, 80, 75, 70, 65, 60, 55, 50, 
                                    45, 40, 35, 30, 25, 20, 15, 10, 5])
    post_scores = list(range(30, 1, -1))  # 30점부터 2점까지
    df.loc[scoring_mask, 'post_score'] = df.loc[scoring_mask, 'posts'].apply(
        lambda x: calculate_percentile_score(x, post_percentiles, post_scores)
    )
    
    # 3. 릴스 점수 (상대평가)
    reels_percentiles = np.percentile(df.loc[scoring_mask, 'reels_views(15)'], 
                                    [95, 90, 85, 80, 75, 70, 65, 60, 55, 50, 
                                     45, 40, 35, 30, 25, 20, 15, 10, 5])
    reels_scores = list(range(40, 1, -2))  # 40점부터 2점까지 2점 간격
    df.loc[scoring_mask, 'reels_score'] = df.loc[scoring_mask, 'reels_views(15)'].apply(
        lambda x: calculate_percentile_score(x, reels_percentiles, reels_scores)
    )
    
    # 4. 콘텐츠 가산점
    df.loc[scoring_mask, 'content_bonus_score'] = df.loc[scoring_mask, 'content_score'] * 4
    
    # 최종 점수 계산
    df.loc[scoring_mask, 'final_score'] = (df.loc[scoring_mask, 'follower_score'] + 
                                    df.loc[scoring_mask, 'post_score'] + 
                                    df.loc[scoring_mask, 'reels_score'] + 
                                    df.loc[scoring_mask, 'content_bonus_score'])
    
    # 기존 등급을 이전등급 배열에 추가
    for idx in df.index:
        current_grade = df.at[idx, 'grade']
        previous_grades = df.at[idx, 'pre_grades']
        
        # 이전등급이 문자열로 되어있거나 빈 문자열이나 None인 경우 빈 리스트로 초기화
        if not isinstance(previous_grades, list) or previous_grades is None:
            previous_grades = []
            
        # 현재 등급을 이전등급 배열의 앞쪽에 추가
        previous_grades.insert(0, current_grade)
        
        # 최대 5개까지만 유지
        if len(previous_grades) > 5:
            previous_grades = previous_grades[:5]
            
        df.at[idx, 'pre_grades'] = previous_grades
    
    # 최종 등급 부여
    score_percentiles = np.percentile(df.loc[scoring_mask, 'final_score'], [85, 60, 30])
    conditions = [
        df.loc[scoring_mask, 'final_score'] >= score_percentiles[0],
        df.loc[scoring_mask, 'final_score'] >= score_percentiles[1],
        df.loc[scoring_mask, 'final_score'] >= score_percentiles[2]
    ]
    choices = ['S', 'A', 'B']
    df.loc[scoring_mask, 'grade'] = np.select(conditions, choices, default='C')
    
    return df

def update_mongodb(df, collection):
    """계산된 결과를 MongoDB에 업데이트"""
    for _, row in df.iterrows():
        # 업데이트할 필드만 추출
        update_data = {
            'final_score': row['final_score'],
            'follower_score': row['follower_score'],
            'post_score': row['post_score'],
            'reels_score': row['reels_score'],
            'content_bonus_score': row['content_bonus_score'],
            'grade': row['grade'],
            'pre_grades': row['pre_grades']
        }
        
        # _id로 문서 찾아서 업데이트
        collection.update_one({'_id': row['_id']}, {'$set': update_data})
    
    print("MongoDB 업데이트 완료")

def main():
    print("프로그램 시작")
    
    # MongoDB에서 데이터 로드
    df, collection = load_data_from_mongodb()
    if df is None:
        print("데이터를 로드할 수 없어 프로그램을 종료합니다.")
        return
    
    print(f"데이터프레임 크기: {df.shape}")
    
    # 데이터 정제
    df = clean_numeric_data(df)
    
    # 등급 및 점수 계산
    print("등급 및 점수 계산 시작...")
    result_df = calculate_grades(df)
    print("등급 및 점수 계산 완료")
    
    # MongoDB 업데이트
    print("MongoDB 업데이트 시작...")
    update_mongodb(result_df, collection)
    
    # 결과 출력
    print("\n=== 등급별 통계 ===")
    print("\n1. 등급별 인플루언서 수:")
    grade_counts = result_df['grade'].value_counts().sort_index()
    for grade, count in grade_counts.items():
        print(f"{grade}등급: {count}명")
    
    print("\n2. 등급별 평균 점수:")
    grade_scores = result_df.groupby('grade')['final_score'].mean().round(2).sort_index()
    for grade, score in grade_scores.items():
        if grade not in ['R', 'D']:
            print(f"{grade}등급: {score}점")
    
    print("\n3. 인플루언서별 상세 점수:")
    for _, row in result_df.iterrows():
        print(f"\n[{row['username']}]")
        print(f"팔로워: {row['followers']:,}명")
        if row['grade'] not in ['R', 'D']:
            print(f"팔로워점수: {row.get('follower_score', 0)}점")
            print(f"게시물점수: {row.get('post_score', 0)}점")
            print(f"릴스점수: {row.get('reels_score', 0)}점")
            print(f"콘텐츠가산점: {row.get('content_bonus_score', 0)}점")
        print(f"최종점수: {row['final_score']}점")
        print(f"최종등급: {row['grade']}")

if __name__ == "__main__":
    main()
