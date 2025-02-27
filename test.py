import streamlit as st
from notion_client import Client
import pandas as pd
import plotly.figure_factory as ff
import plotly.express as px
import datetime
import traceback
import altair as alt

# 페이지 설정
st.set_page_config(page_title="노션 데이터베이스 뷰어", layout="wide")

# 노션 API 연결 설정
notion = Client(auth="")
database_id = "1a7111a57788812e9e60fb87b833d345"

# 노션 데이터를 판다스 데이터프레임으로 변환
def notion_to_dataframe(response):
    if not response or not response.get('results'):
        return pd.DataFrame()
    
    data = []
    for page in response['results']:
        item = {}
        properties = page.get('properties', {})
        
        for prop_name, prop_data in properties.items():
            prop_type = prop_data.get('type')
            
            if prop_type == 'title':
                title_objects = prop_data.get('title', [])
                item[prop_name] = ''.join([t.get('plain_text', '') for t in title_objects])
            elif prop_type == 'rich_text':
                text_objects = prop_data.get('rich_text', [])
                item[prop_name] = ''.join([t.get('plain_text', '') for t in text_objects])
            elif prop_type == 'number':
                item[prop_name] = prop_data.get('number')
            elif prop_type == 'select':
                select_data = prop_data.get('select')
                item[prop_name] = select_data.get('name') if select_data else None
            elif prop_type == 'multi_select':
                multi_select = prop_data.get('multi_select', [])
                item[prop_name] = ', '.join([s.get('name', '') for s in multi_select])
            elif prop_type == 'date':
                date_data = prop_data.get('date')
                item[prop_name] = date_data.get('start') if date_data else None
            elif prop_type == 'checkbox':
                item[prop_name] = prop_data.get('checkbox')
            elif prop_type == 'url':
                item[prop_name] = prop_data.get('url')
            elif prop_type == 'email':
                item[prop_name] = prop_data.get('email')
            elif prop_type == 'phone_number':
                item[prop_name] = prop_data.get('phone_number')
            elif prop_type == 'formula':
                formula_data = prop_data.get('formula', {})
                formula_type = formula_data.get('type')
                if formula_type == 'string':
                    item[prop_name] = formula_data.get('string')
                elif formula_type == 'number':
                    item[prop_name] = formula_data.get('number')
                elif formula_type == 'boolean':
                    item[prop_name] = formula_data.get('boolean')
                elif formula_type == 'date':
                    date_info = formula_data.get('date')
                    item[prop_name] = date_info.get('start') if date_info else None
                else:
                    item[prop_name] = str(formula_data)
            elif prop_type == 'relation':
                relation_data = prop_data.get('relation', [])
                item[prop_name] = ', '.join([r.get('id', '') for r in relation_data])
            elif prop_type == 'rollup':
                rollup_data = prop_data.get('rollup', {})
                rollup_type = rollup_data.get('type')
                if rollup_type == 'number':
                    item[prop_name] = rollup_data.get('number')
                elif rollup_type == 'date':
                    date_info = rollup_data.get('date')
                    item[prop_name] = date_info.get('start') if date_info else None
                elif rollup_type == 'array':
                    array_data = rollup_data.get('array', [])
                    item[prop_name] = str(array_data)
                else:
                    item[prop_name] = str(rollup_data)
            elif prop_type == 'created_time':
                item[prop_name] = prop_data.get('created_time')
            elif prop_type == 'created_by':
                created_by = prop_data.get('created_by', {})
                item[prop_name] = created_by.get('name', '') if created_by else None
            elif prop_type == 'last_edited_time':
                item[prop_name] = prop_data.get('last_edited_time')
            elif prop_type == 'last_edited_by':
                last_edited_by = prop_data.get('last_edited_by', {})
                item[prop_name] = last_edited_by.get('name', '') if last_edited_by else None
            elif prop_type == 'files':
                files = prop_data.get('files', [])
                file_names = []
                for file in files:
                    file_type = file.get('type')
                    if file_type == 'file':
                        file_info = file.get('file', {})
                        file_names.append(file_info.get('name', '') or file_info.get('url', ''))
                    elif file_type == 'external':
                        external_info = file.get('external', {})
                        file_names.append(external_info.get('name', '') or external_info.get('url', ''))
                item[prop_name] = ', '.join(file_names) if file_names else None
            elif prop_type == 'people':
                people = prop_data.get('people', [])
                item[prop_name] = ', '.join([p.get('name', '') for p in people if 'name' in p])
            elif prop_type == 'status':
                status_data = prop_data.get('status', {})
                item[prop_name] = status_data.get('name') if status_data else None
            else:
                # 알 수 없는 타입의 경우 원본 데이터를 문자열로 변환
                item[prop_name] = f"{prop_type}: {str(prop_data)}"
        
        data.append(item)
    
    return pd.DataFrame(data)


# 탭 생성
tab1, tab2, tab3, tab4 = st.tabs(["타임라인 보기", "데이터베이스 보기", "데이터 분석", "설정"])

with tab2:
    st.header("데이터베이스 보기")
    try:
        # 데이터베이스 쿼리
        response = notion.databases.query(database_id=database_id)
        
        # 데이터프레임으로 변환
        df = notion_to_dataframe(response)
        
        if not df.empty:
            st.dataframe(df)
        else:
            st.info("데이터베이스에 데이터가 없습니다.")
    except Exception as e:
        st.error(f"데이터베이스 조회 오류: {e}")

with tab1:
    st.header("타임라인 보기")
    try:
        # 데이터베이스 쿼리
        response = notion.databases.query(database_id=database_id)
        
        # 데이터프레임으로 변환
        df = notion_to_dataframe(response)
        
        if not df.empty:
            # 시작일과 종료일 열 자동 찾기
            start_date_column = None
            end_date_column = None
            
            for col in df.columns:
                # 시작일 관련 키워드
                if any(keyword in col.lower() for keyword in ['시작일', '시작 일', '시작날짜', '시작 날짜', 'start']):
                    start_date_column = col
                    break
            
            for col in df.columns:
                # 종료일/마감일/보고예정일 관련 키워드
                if any(keyword in col.lower() for keyword in ['종료일', '마감일', '완료일', '보고예정일', '예정일', 'end', 'due']):
                    end_date_column = col
                    break
            
            # 상태 열 자동 찾기
            status_column = None
            for col in df.columns:
                if any(keyword in col.lower() for keyword in ['상태', '진행', 'status', 'state']):
                    status_column = col
                    break
            
            # 품목/플랫폼 열 자동 찾기
            platform_column = None
            for col in df.columns:
                if any(keyword in col.lower() for keyword in ['품목', '플랫폼', '제품', 'platform', 'product']):
                    platform_column = col
                    break
            
            # 제목 열 자동 찾기
            title_column = None
            for col in df.columns:
                if any(keyword in col.lower() for keyword in ['제목', '이름', '항목', 'title', 'name']):
                    title_column = col
                    break
            
            # 제목 열이 없으면 첫 번째 열을 사용
            if not title_column and df.columns.size > 0:
                title_column = df.columns[0]
            
            # 시작일이 있는 경우에만 진행
            if start_date_column:
                # 날짜 변환
                df['시작일'] = pd.to_datetime(df[start_date_column], errors='coerce')
                
                if end_date_column:
                    df['종료일'] = pd.to_datetime(df[end_date_column], errors='coerce')
                else:
                    df['종료일'] = df['시작일']  # 종료일이 없으면 시작일과 동일하게 설정
                
                # 유효한 날짜가 있는 행만 필터링
                df_filtered = df.dropna(subset=['시작일'])
                
                if df_filtered.empty:
                    st.warning("데이터베이스에 유효한 날짜 데이터가 없습니다.")
                else:
                    # 날짜 기준으로 정렬
                    df_filtered = df_filtered.sort_values('시작일')
                    
                    # 시간 범위 선택 옵션 추가
                    time_range_options = {
                        "2주": 14,
                        "1개월": 30,
                        "3개월": 90,
                        "6개월": 180,
                        "1년": 365,
                        "전체 기간": None
                    }
                    
                    selected_range = st.selectbox(
                        "시간 범위 선택",
                        options=list(time_range_options.keys()),
                        index=1  # 기본값: 1개월
                    )
                    
                    # 현재 날짜 기준 시작일과 종료일 계산
                    today = datetime.datetime.now().date()
                    days = time_range_options[selected_range]
                    
                    if days:
                        # 시간 범위에 따라 표시 기간 조정 (선택한 기간의 2배로 확장)
                        view_start = today - datetime.timedelta(days=days)  # 현재 날짜에서 선택한 기간만큼 과거
                        view_end = today + datetime.timedelta(days=days)    # 현재 날짜에서 선택한 기간만큼 미래
                    else:
                        # 전체 기간인 경우 데이터의 최소/최대 날짜 사용
                        min_date = df_filtered['시작일'].min().date()
                        max_date = df_filtered['종료일'].max().date()
                        
                        # 여유 공간 추가 (10% 정도)
                        date_range = (max_date - min_date).days
                        padding = max(date_range * 0.1, 7)  # 최소 7일 또는 전체 기간의 10%
                        
                        view_start = min_date - datetime.timedelta(days=int(padding))
                        view_end = max_date + datetime.timedelta(days=int(padding))
                    
                    # 상태가 있는 경우 필터링
                    if status_column:
                        # 결측값 처리
                        df_filtered[status_column] = df_filtered[status_column].fillna('상태 미지정')
                        
                        # 상태가 '진행중' 또는 '시작전'인 항목만 필터링
                        filtered_status_df = df_filtered[
                            df_filtered[status_column].str.contains('진행중|진행 중|시작전|시작 전', case=False, na=False)
                        ]
                        
                        if filtered_status_df.empty:
                            st.info("'진행중' 또는 '시작전' 상태의 항목이 없습니다.")
                        else:
                            # 간트 차트용 데이터 준비
                            chart_data = []
                            
                            for _, row in filtered_status_df.iterrows():
                                # 시작일과 종료일
                                start = row['시작일']
                                end = row['종료일'] if pd.notna(row['종료일']) else start + datetime.timedelta(days=1)
                                
                                # 제목과 플랫폼 정보
                                title = row[title_column]
                                platform = row[platform_column] if platform_column and pd.notna(row[platform_column]) else "미분류"
                                
                                # 상태 정보 추가
                                status = row[status_column] if status_column and pd.notna(row[status_column]) else "상태 미지정"
                                
                                # 날짜 포맷팅
                                start_formatted = start.strftime('%m/%d')
                                end_formatted = end.strftime('%m/%d')
                                date_range = f"{start_formatted}~{end_formatted}"
                                
                                # 간트 차트 데이터 추가
                                chart_data.append({
                                    "Platform": platform,  # 품목/플랫폼 정보를 Y축으로 사용
                                    "Task": title,         # 작업 제목
                                    "Start": start,
                                    "End": end,
                                    "DateRange": date_range,
                                    "DisplayText1": f"{title} ({date_range})",  # 첫 번째 줄: 제목
                                    "DisplayText2": f"{platform} ({date_range})",  # 두 번째 줄: 품목 + 날짜
                                    "Status": status  # 상태 정보 추가
                                })
                            
                            # 데이터프레임으로 변환
                            gantt_df = pd.DataFrame(chart_data)
                            
                            # 플랫폼별 가장 빠른 시작일 계산
                            platform_earliest_dates = gantt_df.groupby('Platform')['Start'].min().reset_index()
                            platform_earliest_dates = platform_earliest_dates.sort_values('Start')
                            
                            # 정렬된 플랫폼 순서 생성
                            platform_order = platform_earliest_dates['Platform'].tolist()
                            
                            # 플랫폼 이름을 시작일 순서에 따라 새로운 이름으로 변경 (01_플랫폼명, 02_플랫폼명 등)
                            platform_rename = {platform: f"{i+1:02d}_{platform}" for i, platform in enumerate(platform_order)}
                            gantt_df['SortedPlatform'] = gantt_df['Platform'].map(platform_rename)
                            
                            # 원래 플랫폼 이름 저장 (표시용)
                            gantt_df['OriginalPlatform'] = gantt_df['Platform']
                            
                            # 정렬을 위한 플랫폼 필드로 변경
                            gantt_df['Platform'] = gantt_df['SortedPlatform']
                            
                            # 오늘 날짜 계산
                            today = pd.Timestamp(datetime.datetime.now().date())
                            today_formatted = today.strftime('%m/%d')  # 오늘 날짜 포맷팅 (월/일)
                            
                            # 상태에 따른 색상 조건 추가
                            color_condition = alt.condition(
                                "datum.Status == '시작전' || datum.Status == '시작 전'",
                                alt.value('#E8E8E8'),  # 시작전인 경우 더 연한 회색으로 변경
                                alt.value('#ADD8E6')   # 그 외의 경우 연한 파란색
                            )
                            
                            # 차트 생성 부분에서 정렬 순서 변경
                            bars = alt.Chart(gantt_df).mark_bar(
                                height=40  # 바의 높이 증가 (2줄 텍스트를 위해)
                            ).encode(
                                x=alt.X('Start:T', 
                                        title='날짜',
                                        scale=alt.Scale(domain=[view_start, view_end]),
                                        axis=alt.Axis(
                                            orient='top',  # 날짜 축을 위로 이동
                                            grid=True,     # 격자선 추가
                                            format='%m/%d', # 날짜 형식
                                            tickCount=10   # 눈금 개수 설정
                                        )),
                                x2='End:T',
                                y=alt.Y('Platform:N', 
                                       title='품목/플랫폼',
                                       sort=None),  # 정렬 없음 - 데이터 자체가 이미 정렬됨
                                color=color_condition,  # 상태에 따른 색상 조건 적용
                                tooltip=['OriginalPlatform', 'Task', 'DateRange', 'Status']  # 기본 필드만 포함
                            )
                            
                            # 첫 번째 줄 텍스트 추가 (제목)
                            text1 = alt.Chart(gantt_df).mark_text(
                                align='left',
                                baseline='middle',
                                dx=5,  # 텍스트 위치 조정
                                dy=-8,  # 위쪽으로 조정
                                color='black',
                                fontSize=12,
                                fontWeight='bold'  # 제목은 굵게 표시
                            ).encode(
                                x='Start:T',
                                y='Platform:N',
                                text='DisplayText1'  # 제목 표시
                            )
                            
                            # 두 번째 줄 텍스트 추가 (품목 + 날짜)
                            text2 = alt.Chart(gantt_df).mark_text(
                                align='left',
                                baseline='middle',
                                dx=5,  # 텍스트 위치 조정
                                dy=8,   # 아래쪽으로 조정
                                color='black',
                                fontSize=11
                            ).encode(
                                x='Start:T',
                                y='Platform:N',
                                text='DisplayText2'  # 품목 + 날짜 표시
                            )
                            
                            # 오늘 날짜를 표시하는 세로선 추가 - 얇은 실선으로 변경
                            today_rule = alt.Chart(
                                pd.DataFrame({'today': [today]})
                            ).mark_rule(
                                color='red',
                                strokeWidth=1.0,  # 더 얇은 선 두께
                                strokeDash=[]     # 빈 배열로 설정하여 실선으로 변경
                            ).encode(
                                x='today:T'
                            )
                            
                            # 오늘 날짜 레이블 추가
                            today_label = alt.Chart(
                                pd.DataFrame({'today': [today]})
                            ).mark_text(
                                align='center',
                                baseline='top',
                                dy=-5,  # 위치 조정
                                color='red',
                                fontSize=11,
                                fontWeight='bold',
                                text=today_formatted
                            ).encode(
                                x='today:T',
                                y=alt.value(0)  # 차트 상단에 고정
                            )
                            
                            # 차트 결합
                            gantt_chart = (bars + text1 + text2 + today_rule + today_label).properties(
                                width=800,
                                height=700,  # 차트 높이 600에서 800으로 증가
                                title=f'노션 데이터베이스 간트 차트 (품목별) - {selected_range} 보기'
                            ).configure_view(
                                strokeWidth=0
                            ).configure_axis(
                                grid=True,
                                gridOpacity=0.6,  # 격자선 투명도 설정
                                gridWidth=1       # 격자선 두께 설정
                            )
                            
                            # 차트 표시
                            st.altair_chart(gantt_chart, use_container_width=True)
                    else:
                        st.warning("데이터베이스에 상태 열을 찾을 수 없습니다.")
            else:
                st.error("데이터베이스에 시작일 열을 찾을 수 없습니다.")
        else:
            st.info("데이터베이스에 데이터가 없습니다.")
    except Exception as e:
        st.error(f"타임라인 생성 오류: {str(e)}")
        # 디버깅을 위한 상세 오류 정보
        st.error(traceback.format_exc())

with tab3:
    st.header("데이터 분석")
    st.write("이 탭에서는 데이터 분석 기능을 구현할 수 있습니다.")
    
    try:
        # 데이터베이스 쿼리
        response = notion.databases.query(database_id=database_id)
        
        # 데이터프레임으로 변환
        df = notion_to_dataframe(response)
        
        if not df.empty:
            # 기본 통계 표시
            st.subheader("기본 통계")
            st.write(df.describe())
            
            # 열 선택 옵션
            if len(df.columns) > 0:
                selected_column = st.selectbox("분석할 열 선택", df.columns)
                
                # 선택한 열의 데이터 분포 표시
                st.subheader(f"{selected_column} 데이터 분포")
                try:
                    st.bar_chart(df[selected_column].value_counts())
                except:
                    st.write("이 열은 차트로 표시할 수 없습니다.")
        else:
            st.info("데이터베이스에 데이터가 없습니다.")
    except Exception as e:
        st.error(f"데이터 분석 오류: {e}")

with tab4:
    st.header("설정")
    st.write("앱 설정을 관리하는 탭입니다.")
    
    # 데이터베이스 정보 표시
    st.subheader("데이터베이스 정보")
    st.write(f"데이터베이스 ID: {database_id}")
    
    # 데이터베이스 속성 정보 표시
    try:
        db_info = notion.databases.retrieve(database_id=database_id)
        st.subheader("데이터베이스 속성")
        for prop_name, prop_data in db_info.get('properties', {}).items():
            st.write(f"- {prop_name}: {prop_data.get('type')}")
    except Exception as e:
        st.error(f"데이터베이스 정보 조회 오류: {e}")
