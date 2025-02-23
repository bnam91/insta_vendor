import streamlit as st
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd

# 페이지 레이아웃을 wide 모드로 설정하고 사이드바를 초기에 닫힌 상태로 설정
st.set_page_config(
    layout="wide", 
    page_title="🚀 인스타그램 데이터 분석 대시보드",
    initial_sidebar_state="collapsed"  # 사이드바 초기 상태를 닫힌 상태로 설정
)

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
            margin-top: -18px;
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
    with st.expander("👥SNS 분석"):
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
st.title('🚀인스타그램 데이터 분석 대시보드')

# 컬렉션 선택 콤보박스
selected_collection = st.selectbox('컬렉션 선택', 
                                 options=['empty_data'] + collections,  # 'empty_data'를 첫 번째 옵션으로 추가
                                 key="collection_select")

# 좌우 여백을 위한 컬럼 추가하여 중앙 정렬 (빈 칼럼 추가)
left_space, col1, col2, col3, empty1, col4, col5, col6, empty2, col7, col8, right_space = st.columns(
    [
        0.01,  # 왼쪽 여백
        3,    # 첫 번째 컬럼 (검색 타입)
        1.5,    # 두 번째 컬럼 (검색 입력)
        0.8,    # 검색 버튼
        0.5, # 빈 공간
        1.5,  # 다섯 번째 컬럼 (카테고리 선택)
        0.7,    # 여섯 번째 컬럼 (퍼센트 입력)
        1,  # 카테고리 검색 버튼
        0.5,   # 빈 공간
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
    category = st.selectbox('', 
                             ['뷰티', '패션', '홈/리빙', '푸드', '육아', 
                              '건강', '맛집탐방', '전시/공연', '반려동물', '기타'], 
                             placeholder="카테고리 선택",
                             key="category_select")

with col5:
    category_input = st.text_input("", placeholder="%")

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

# 빈 데이터프레임 생성
empty_df = pd.DataFrame()
display_df = empty_df  # 초기값 설정

# 카테고리 검색 처리
if category_search:
    try:
        # 입력값 검증
        if not category:
            st.warning("카테고리를 선택해주세요.")
        elif not category_input:
            st.warning("퍼센트를 입력해주세요.")
        else:
            # 퍼센트 값을 숫자로 변환
            try:
                percent_threshold = float(category_input)
            except ValueError:
                st.error("올바른 숫자를 입력해주세요.")
                percent_threshold = 0

            # 02_test_influencer_data 컬렉션에서 데이터 가져오기
            collection = db['02_test_influencer_data']
            data = list(collection.find({}, {'_id': 0}))
            
            if data:
                df = pd.DataFrame(data)
                filtered_data = []
                
                for _, row in df.iterrows():
                    if pd.notna(row.get('category')) and row['category']:
                        categories = row['category'].split(',')
                        for cat in categories:
                            if category in cat:
                                try:
                                    percent_str = cat.split('(')[1].rstrip('%)')
                                    percent = float(percent_str)
                                    if percent >= percent_threshold:
                                        filtered_data.append(row)
                                        break
                                except Exception as e:
                                    continue
                
                filtered_df = pd.DataFrame(filtered_data)
                
                if not filtered_data:
                    st.warning(f"{category} 카테고리에서 {percent_threshold}% 이상인 데이터가 없습니다.")
                    display_df = empty_df
                else:
                    display_df = filtered_df
                    st.write(f"총 {len(filtered_data)}개의 데이터가 있습니다.")
            else:
                st.warning("데이터가 없습니다.")
                display_df = empty_df
                
    except Exception as e:
        st.error(f"카테고리 검색 중 오류 발생: {e}")
        display_df = empty_df

# 검색 버튼 클릭 처리
elif search_button:
    try:
        if search_type == "브랜드":
            # 02_test_influencer_data 컬렉션에서 검색
            collection = db['02_test_influencer_data']
            
            # brand.name에 검색어가 포함된 경우 검색 (대소문자 구분 없이)
            query = {
                "brand": {
                    "$elemMatch": {
                        "name": {"$regex": search_input, "$options": "i"}
                    }
                }
            }
            
            # find()를 사용하여 모든 일치하는 문서 검색
            brand_data_list = list(collection.find(query))
            
            if brand_data_list:
                items_data = []
                
                # 각 인플루언서의 데이터를 처리
                for influencer_data in brand_data_list:
                    for brand in influencer_data.get('brand', []):
                        # 검색한 브랜드명과 일치하는 경우만 처리
                        if search_input.lower() in brand.get('name', '').lower():
                            brand_name = brand.get('name', '')
                            
                            for product in brand.get('products', []):
                                item_data = {
                                    '인플루언서': influencer_data.get('clean_name', ''),
                                    '아이디': influencer_data.get('username', ''),
                                    '팔로워': influencer_data.get('followers', ''),
                                    '카테고리': influencer_data.get('category', ''),
                                    '등급': influencer_data.get('grade', ''),
                                    '브랜드명': brand_name,
                                    '아이템명': product.get('item', ''),
                                    '타입': product.get('type', ''),
                                    '언급일자': product.get('mentioned_date', ''),
                                    '예상일자': product.get('expected_date', ''),
                                    '종료일자': product.get('end_date', ''),
                                    '피드링크': product.get('item_feed_link', ''),
                                    '릴스조회수': influencer_data.get('reels_views(15)', '0')
                                }
                                items_data.append(item_data)
                
                if items_data:
                    display_df = pd.DataFrame(items_data)
                    columns_order = [
                        '인플루언서', '아이디', '팔로워', '릴스조회수', '카테고리', '등급',
                        '브랜드명', '아이템명', '타입', '언급일자', '예상일자', 
                        '종료일자', '피드링크'
                    ]
                    display_df = display_df[columns_order]
                    st.write(f"브랜드 '{search_input}'에 대해 총 {len(items_data)}개의 아이템이 검색되었습니다.")
                else:
                    display_df = empty_df
                    st.warning("해당 브랜드의 아이템 정보가 없습니다.")
            else:
                display_df = empty_df
                st.warning("검색어와 일치하는 브랜드를 찾을 수 없습니다.")
        
        elif search_type == "인플루언서":
            # 02_test_influencer_data 컬렉션에서 검색
            collection = db['02_test_influencer_data']
            
            # username 또는 clean_name에 검색어가 포함된 경우 검색 (대소문자 구분 없이)
            query = {
                "$or": [
                    {"username": {"$regex": search_input, "$options": "i"}},
                    {"clean_name": {"$regex": search_input, "$options": "i"}}
                ]
            }
            
            # find()를 사용하여 모든 일치하는 문서 검색
            influencer_data_list = list(collection.find(query))
            
            if influencer_data_list:  # influencer_data를 influencer_data_list로 변경
                # 브랜드별 아이템 리스트 추출
                items_data = []
                
                # 각 인플루언서의 데이터를 처리
                for influencer_data in influencer_data_list:
                    for brand in influencer_data.get('brand', []):
                        brand_name = brand.get('name', '')
                        
                        for product in brand.get('products', []):
                            item_data = {
                                '인플루언서': influencer_data.get('clean_name', ''),
                                '아이디': influencer_data.get('username', ''),
                                '팔로워': influencer_data.get('followers', ''),
                                '카테고리': influencer_data.get('category', ''),
                                '등급': influencer_data.get('grade', ''),
                                '브랜드명': brand_name,
                                '아이템명': product.get('item', ''),
                                '타입': product.get('type', ''),
                                '언급일자': product.get('mentioned_date', ''),
                                '예상일자': product.get('expected_date', ''),
                                '종료일자': product.get('end_date', ''),
                                '피드링크': product.get('item_feed_link', ''),
                                '릴스조회수': influencer_data.get('reels_views(15)', '0')
                            }
                            items_data.append(item_data)
                
                if items_data:
                    display_df = pd.DataFrame(items_data)
                    columns_order = [
                        '브랜드명', '아이템명', '등급', '인플루언서', '아이디', 
                        '팔로워', '릴스조회수', '카테고리', 
                        '언급일자', '타입', '예상일자', '종료일자', '피드링크'
                    ]
                    display_df = display_df[columns_order]
                    st.write(f"검색어 '{search_input}'에 대해 총 {len(items_data)}개의 아이템이 검색되었습니다.")
                else:
                    display_df = empty_df
                    st.warning("검색된 인플루언서의 아이템 정보가 없습니다.")
            else:
                display_df = empty_df
                st.warning("검색어와 일치하는 인플루언서를 찾을 수 없습니다.")
        
        elif search_type == "아이템":
            # 02_test_influencer_data 컬렉션에서 검색
            collection = db['02_test_influencer_data']
            
            # products.item에 검색어가 포함된 경우 검색 (대소문자 구분 없이)
            query = {
                "brand": {
                    "$elemMatch": {
                        "products": {
                            "$elemMatch": {
                                "item": {"$regex": search_input, "$options": "i"}
                            }
                        }
                    }
                }
            }
            
            # find()를 사용하여 모든 일치하는 문서 검색
            item_data_list = list(collection.find(query))
            
            if item_data_list:
                items_data = []
                
                # 각 인플루언서의 데이터를 처리
                for influencer_data in item_data_list:
                    for brand in influencer_data.get('brand', []):
                        brand_name = brand.get('name', '')
                        
                        for product in brand.get('products', []):
                            # 검색한 아이템명과 일치하는 경우만 처리
                            if search_input.lower() in product.get('item', '').lower():
                                item_data = {
                                    '아이템명': product.get('item', ''),
                                    '브랜드명': brand_name,
                                    '타입': product.get('type', ''),
                                    '인플루언서': influencer_data.get('clean_name', ''),
                                    '아이디': influencer_data.get('username', ''),
                                    '팔로워': influencer_data.get('followers', ''),
                                    '릴스조회수': influencer_data.get('reels_views(15)', '0'),
                                    '카테고리': influencer_data.get('category', ''),
                                    '등급': influencer_data.get('grade', ''),
                                    '언급일자': product.get('mentioned_date', ''),
                                    '예상일자': product.get('expected_date', ''),
                                    '종료일자': product.get('end_date', ''),
                                    '피드링크': product.get('item_feed_link', '')
                                }
                                items_data.append(item_data)
                
                if items_data:
                    display_df = pd.DataFrame(items_data)
                    columns_order = [
                        '브랜드명', '아이템명', '등급', '인플루언서', '아이디', 
                        '팔로워', '릴스조회수', '카테고리', 
                        '언급일자', '타입', '예상일자', '종료일자', '피드링크'
                    ]
                    display_df = display_df[columns_order]
                    st.write(f"아이템 '{search_input}'에 대해 총 {len(items_data)}개의 결과가 검색되었습니다.")
                else:
                    display_df = empty_df
                    st.warning("해당 아이템 정보가 없습니다.")
            else:
                display_df = empty_df
                st.warning("검색어와 일치하는 아이템을 찾을 수 없습니다.")
        
        else:
            # 기존의 브랜드, 아이템 검색 로직
            if selected_collection and selected_collection != 'empty_data':
                collection = db[selected_collection]
                data = list(collection.find({}, {'_id': 0}))
                
                if data:
                    df = pd.DataFrame(data)
                    for col in df.columns:
                        if 'views' in col or 'likes' in col or 'comments' in col:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    display_df = df
                    st.write(f"총 {len(data)}개의 데이터가 있습니다.")
                else:
                    display_df = empty_df
                    st.warning("데이터가 없습니다.")
            else:
                display_df = empty_df
    except Exception as e:
        st.error(f"검색 중 오류가 발생했습니다: {e}")
        display_df = empty_df

# 컬렉션 선택 처리
elif selected_collection and selected_collection != 'empty_data':
    try:
        collection = db[selected_collection]
        data = list(collection.find({}, {'_id': 0}))
        
        if data:
            df = pd.DataFrame(data)
            for col in df.columns:
                if 'views' in col or 'likes' in col or 'comments' in col:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            display_df = df
            st.write(f"총 {len(data)}개의 데이터가 있습니다.")
        else:
            display_df = empty_df
            st.warning("데이터가 없습니다.")
    except Exception as e:
        st.error(f"데이터 로딩 중 오류 발생: {e}")
        display_df = empty_df

# 최종 데이터프레임 표시 (한 번만 호출)
st.dataframe(
    display_df,
    use_container_width=True,
    height=600,
    key="main_data_frame",
    column_config={
        "피드링크": st.column_config.LinkColumn("피드링크")
    }
)

# MongoDB 연결 확인
try:
    client.admin.command('ping')
    st.success("MongoDB 연결 성공!")
except Exception as e:
    st.error(f"MongoDB 연결 실패: {e}")
