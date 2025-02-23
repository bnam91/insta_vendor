import streamlit as st
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd

# 페이지 레이아웃을 wide 모드로 설정
st.set_page_config(layout="wide", page_title="인스타그램 데이터 분석 대시보드")

# 페이지 여백 줄이기
st.markdown("""
    <style>
        .block-container {
            padding-top: 1.5rem;
            padding-bottom: 0rem;
            padding-left: 1rem;
            padding-right: 1rem;
        }
        /* 라디오 버튼 그룹의 마진 조정 */
        .st-emotion-cache-1n76uvr {
            margin-top: -15px;
        }
        /* 라디오 버튼 그룹 중앙 정렬 */
        div[role="radiogroup"] {
            display: flex;
            justify-content: center;
            gap: 1rem;
        }
        /* 모든 입력 요소 중앙 정렬 */
        .stTextInput, .collection-select, .category-select, .stButton {
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100%;
        }
        /* 입력 필드 너비 조정 */
        .stTextInput > div {
            width: 100%;
            margin-top: 25px;
        }
        /* 컬렉션 selectbox 너비 조정 */
        .collection-select > div {
            width: 100%;
            margin-top: 15px;
        }
        /* 카테고리 selectbox 너비 조정 */
        .category-select > div {
            width: 100%;
            margin-top: 25px;
        }
        /* 버튼 너비 조정 */
        .stButton > button {
            width: 100%;
            margin-top: 25px;
        }
        [data-testid=stSidebar] {
            background-color: #f0f2f6;
            padding-top: 1rem;
        }
        .sidebar-button {
            width: 100%;
            margin: 0.2rem 0;
            padding: 0.5rem;
        }
        /* 데이터프레임 스타일링 */
        .stDataFrame {
            margin-top: -10px;
        }
    </style>
    """, unsafe_allow_html=True)

# MongoDB 연결 설정 전에 사이드바 추가
with st.sidebar:
    st.header("메뉴")
    
    # 데이터 분석 섹션
    with st.expander("📊데이터 분석"):
        st.button("오늘의 피드", key="today_feed")
        st.button("브랜드 추출", key="brand_extract")
        st.button("브랜드 중복체크", key="brand_check")
        st.button("아이템 중복체크", key="item_check")
        st.button("오늘의 아이템 찾기", key="today_item")
    
    # SNS 분석 섹션
    with st.expander("📱SNS 분석"):
        st.button("팔로잉 추출", key="following_extract")
        st.button("인플루언서 분석", key="influencer_analysis")
        st.button("비전 분석", key="vision_analysis")
        st.button("등급 분류", key="grade_classification")
    
    # DM 관리 섹션
    with st.expander("💌DM 관리"):
        st.button("DM팔로우시트 열기", key="dm_sheet")
        st.button("DM보내기", key="send_dm")
        st.button("팔로우하기", key="follow")

# MongoDB 연결 설정
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))

# 데이터베이스 및 컬렉션 선택
db = client['insta09_database']
collections = ['02_test_influencer_data', '01_test_newfeed_crawl_data']

# 페이지 제목
st.title('인스타그램 데이터 분석 대시보드')

# 컬렉션 선택 콤보박스
selected_collection = st.selectbox('컬렉션 선택', collections, key="collection_select")

# 좌우 여백을 위한 컬럼 추가하여 중앙 정렬 (빈 칼럼 추가)
left_space, col1, col2, col3, empty1, col4, col5, col6, empty2, col7, col8, right_space = st.columns(
    [
        0.1,  # 왼쪽 여백
        2.8,    # 첫 번째 컬럼 (검색 타입)
        2,    # 두 번째 컬럼 (검색 입력)
        1,    # 검색 버튼
        0.5, # 빈 공간
        1.5,  # 다섯 번째 컬럼 (카테고리 선택)
        1,    # 여섯 번째 컬럼 (카테고리 검색 입력)
        1.5,  # 카테고리 검색 버튼
        2,   # 빈 공간
        1,  # 아홉 번째 컬럼 (엑셀 저장 버튼)
        1,    # 열 번째 컬럼 (초기화 버튼)
        0.1   # 오른쪽 여백
    ])

with left_space:
    st.empty()

with col1:
    search_type = st.radio(
        "",
        ["브랜드", "아이템", "인플루언서"],
        horizontal=True
    )

with col2:
    search_input = st.text_input("", placeholder="검색어 입력")

with col3:
    search_button = st.button("검색")

with empty1:
    st.empty()

with col4:
    category = st.selectbox('', ['카테고리1', '카테고리2', '카테고리3'], 
                          placeholder="카테고리 선택",
                          key="category_select")

with col5:
    category_input = st.text_input("", placeholder="퍼센트 입력")

with col6:
    category_search = st.button("카테고리 검색")

with empty2:
    st.empty()

with col7:
    excel_button = st.button("엑셀저장")

with col8:
    reset_button = st.button("초기화")

with right_space:
    st.empty()

# 데이터 표시 영역
st.divider()

try:
    # 선택된 컬렉션에서 데이터 가져오기
    collection = db[selected_collection]
    data = list(collection.find({}, {'_id': 0}))
    
    if data:
        # 데이터를 DataFrame으로 변환
        df = pd.DataFrame(data)
        
        # 숫자형 컬럼의 빈 문자열을 NaN으로 변환
        for col in df.columns:
            if 'views' in col or 'likes' in col or 'comments' in col:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # DataFrame을 테이블로 표시
        st.dataframe(df, use_container_width=True, height=600)
        st.write(f"총 {len(data)}개의 데이터가 있습니다.")
    else:
        st.warning("데이터가 없습니다.")

except Exception as e:
    st.error(f"데이터 로딩 중 오류 발생: {e}")

# MongoDB 연결 확인
try:
    client.admin.command('ping')
    st.success("MongoDB 연결 성공!")
except Exception as e:
    st.error(f"MongoDB 연결 실패: {e}")
