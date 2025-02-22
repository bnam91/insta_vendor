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
            padding-top: 1rem;
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
        .stTextInput, .stSelectbox, .stButton {
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
        /* selectbox 너비 조정 */
        .stSelectbox > div {
            width: 100%;
            margin-top: 25px;
        }
        /* 버튼 너비 조정 */
        .stButton > button {
            width: 100%;
            margin-top: 25px;
        }
    </style>
    """, unsafe_allow_html=True)

# MongoDB 연결 설정
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))

# 데이터베이스 및 컬렉션 선택
db = client['insta09_database']
collections = ['02_test_influencer_data', '01_test_newfeed_crawl_data']

# 페이지 제목
st.title('인스타그램 데이터 분석 대시보드')

# 컬렉션 선택 콤보박스
selected_collection = st.selectbox('컬렉션 선택', collections)

# 좌우 여백을 위한 컬럼 추가하여 중앙 정렬 (빈 칼럼 추가)
left_space, col1, col2, col3, empty1, col4, col5, col6, empty2, col7, col8, right_space = st.columns([0.1, 3, 2, 1, 2, 1.5, 3, 1.5, 1, 1.5, 2, 0.1])

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
    category = st.selectbox('', ['카테고리1', '카테고리2', '카테고리3'], placeholder="카테고리 선택")

with col5:
    category_input = st.text_input("", placeholder="카테고리 검색어")

with col6:
    category_search = st.button("카테고리 검색")

with empty2:
    st.empty()

with col7:
    excel_button = st.button("엑셀 저장")

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
        st.dataframe(df, use_container_width=True)
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
