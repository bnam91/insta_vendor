#v2
# 검색박스 추가 완료 (인플검색기능, 브랜드검색기능)

'''
v3

- 구글스프레드시트 열기 버튼
(https://drive.google.com/drive/u/0/folders/1O2hq8127zZUcW1izEFTRDB1J8LIWoLW2)
(https://docs.google.com/spreadsheets/d/1lxb0LbH5VBPm49tIj1w3AHqwzTqqhKrqxxtX_fGNNeE/edit?gid=1477585691#gid=1477585691)
- 설명서 버튼 > 노션링크
(https://www.notion.so/193111a5778880f7a64df50ec8526a80#193111a577888073b7c7efc7e4802f6f)
- status 표시
- log 볼수있게
- id 어떻게"?

- 기능 9버튼에 2-4_점수계산.py 추가하기(임시)
- 기능 버튼 호버 기능으로 간략 설명 추가하기.
- 
* 현재 팔로잉리스트를 추출하는 건 완료했음



기능2 * 1-1 오늘의 피드 * 1-1_newfeed_crawl.py
기능3 * 1-2 브랜드 추출 * 1-2_newfeed_analysis_(claude).py
기능4 * 1-3 오늘의 아이템 * 1-3_item_today.py
기능5


(완)기능6 * 2-1. 팔로잉 추출  >>>>>>>>>>>>> 팔로우도 동시에 할까요?
(완)기능7 * 2-2. 팔로잉 리스트 전처리1>
(완)기능8 * 2-3. 팔로잉 리스트 전처리2(비전)>
(완)기능9 * 2-4. 등급분석

기능10 * 
기능11 * 팔로우 매크로
기능12 * DM 보내기

===
- 로그 파일열기
- 엑셀 저장하면 저장한 폴더 열기 기능




v4

v5
- 최초 로그인시 아이디 입력 화면
- 검색어 추천
- 이미지url 더블클릭시 열기기능

'''


import sys
import json
import os
import platform  # platform 모듈 추가
import pandas as pd  # 파일 상단에 추가
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                           QHBoxLayout, QPushButton, QTableWidget, QTableWidgetItem, QInputDialog, QLineEdit, QComboBox, QRadioButton, QSpacerItem, QSizePolicy, QShortcut, QMessageBox)
from PyQt5.QtCore import QTimer
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QKeySequence
import subprocess
from googleapiclient.discovery import build
from auth import get_credentials

class JSONViewer(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('JSON 뷰어')
        self.setGeometry(100, 100, 1500, 600)  # x위치, y위치, 너비, 높이
        
        # 실행 취소를 위한 스택 추가
        self.undo_stack = []
        
        # 키보드 단축키 추가
        self.shortcut = QShortcut(QKeySequence("Ctrl+Z"), self)
        self.shortcut.activated.connect(self.undo_delete)
        
        # 메인 위젯 생성
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        
        # 수평 레이아웃 생성
        layout = QHBoxLayout()
        main_widget.setLayout(layout)
        
        # 왼쪽 패널 생성 (콤보박스 + 테이블)
        left_panel = QWidget()
        left_layout = QVBoxLayout()
        left_panel.setLayout(left_layout)
        
        # JSON 파일 선택 콤보박스 추가
        self.file_combo = QComboBox()
        self.update_json_files()  # JSON 파일 목록 업데이트
        self.file_combo.currentTextChanged.connect(self.on_file_selected)
        left_layout.addWidget(self.file_combo)
        
        # 검색 패널 생성
        search_panel = QWidget()
        search_layout = QHBoxLayout()
        search_panel.setLayout(search_layout)
        
        # NEW 버튼 추가
        new_btn = QPushButton('NEW')
        new_btn.setFixedWidth(60)
        new_btn.clicked.connect(lambda: self.show_new_items())  # NEW 버튼 이벤트 연결
        search_layout.addWidget(new_btn)
        
        # 첫 번째 검색 그룹 (브랜드/아이템/인플루언서)
        search_group1 = QWidget()
        search_layout1 = QHBoxLayout()
        search_group1.setLayout(search_layout1)
        
        # 라디오 버튼 생성 - 순서 변경 및 아이템 추가
        self.brand_radio = QRadioButton("브랜드")
        self.brand_radio.setChecked(True)  # 기본값으로 브랜드 선택
        self.item_radio = QRadioButton("아이템")
        self.influencer_radio = QRadioButton("인플루언서")
        
        # 검색창 생성
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("검색어를 입력하세요")
        self.search_input.setMinimumWidth(200)  # 최소 너비 설정
        self.search_input.setFixedWidth(300)    # 고정 너비 설정
        
        # 검색 버튼 생성
        search_button1 = QPushButton("검색")
        search_button1.clicked.connect(self.perform_search)
        search_button1.setFixedWidth(60)  # 검색 버튼 너비 설정
        
        # 첫 번째 그룹에 위젯 추가 - 순서 변경
        search_layout1.addWidget(self.brand_radio)
        search_layout1.addWidget(self.item_radio)
        search_layout1.addWidget(self.influencer_radio)
        search_layout1.addWidget(self.search_input)
        search_layout1.addWidget(search_button1)
        
        # 두 번째 검색 그룹 (카테고리)
        search_group2 = QWidget()
        search_layout2 = QHBoxLayout()
        search_group2.setLayout(search_layout2)
        
        # 카테고리 콤보박스 추가 및 크기 조정
        self.category_combo = QComboBox()
        categories = ['뷰티', '패션', '홈/리빙', '푸드', '육아', '건강', '맛집탐방', '전시/공연', '반려동물', '기타']
        self.category_combo.addItems(categories)
        self.category_combo.setFixedWidth(120)
        
        # 퍼센트 입력 필드 추가
        self.percent_input = QLineEdit()
        self.percent_input.setPlaceholderText("퍼센트 입력 (예: 30)")
        self.percent_input.setFixedWidth(100)
        
        # 검색 관련 위젯들을 담을 서브 그룹
        search_subgroup = QWidget()
        search_sublayout = QHBoxLayout()
        search_sublayout.setContentsMargins(0, 0, 0, 0)  # 내부 여백 제거
        search_subgroup.setLayout(search_sublayout)
        
        # 카테고리 검색 버튼
        search_button2 = QPushButton("검색")
        search_button2.clicked.connect(self.perform_category_search)
        search_button2.setFixedWidth(60)
        
        # 서브 그룹에 검색 관련 위젯들 추가
        search_sublayout.addWidget(self.category_combo)
        search_sublayout.addWidget(self.percent_input)
        search_sublayout.addSpacing(10)
        search_sublayout.addWidget(search_button2)
        
        # 검색 버튼과 엑셀/초기화 버튼 사이에 신축성 있는 공간 추가
        spacer = QSpacerItem(40, 20, QSizePolicy.Expanding, QSizePolicy.Minimum)
        search_layout2.addWidget(search_subgroup)
        search_layout2.addItem(spacer)  # 여기에 공간 추가
        
        # 초기화 버튼과 엑셀 저장 버튼을 담을 컨테이너
        reset_excel_container = QWidget()
        reset_excel_layout = QHBoxLayout()
        reset_excel_container.setLayout(reset_excel_layout)
        
        # 엑셀 저장 버튼
        excel_btn = QPushButton("엑셀저장")
        excel_btn.clicked.connect(self.save_to_excel)
        excel_btn.setFixedWidth(80)
        
        # 초기화 버튼
        reset_button = QPushButton("초기화")
        reset_button.clicked.connect(self.reset_search)
        reset_button.setFixedWidth(60)
        
        reset_excel_layout.addWidget(excel_btn)
        reset_excel_layout.addWidget(reset_button)
        
        # 메인 검색 패널에 위젯들 추가
        search_layout2.addWidget(reset_excel_container)
        
        # 메인 검색 패널에 두 그룹 추가
        search_layout.addWidget(search_group1)
        search_layout.addWidget(search_group2)
        
        # 검색 패널을 왼쪽 레이아웃에 추가
        left_layout.insertWidget(1, search_panel)
        
        # 테이블 위젯 생성
        self.table = QTableWidget()
        left_layout.addWidget(self.table)
        
        # 레이아웃에 왼쪽 패널 추가
        layout.addWidget(left_panel)
        
        # 테이블 스타일 설정
        self.table.setStyleSheet("""
            QTableWidget {
                background-color: white;
                gridline-color: #d0d0d0;
            }
            QHeaderView::section {
                background-color: #f0f0f0;
                padding: 6px;
                font-weight: bold;
                border: 1px solid #d0d0d0;
            }
            QTableWidget::item {
                padding: 5px;
                border-bottom: 1px solid #d0d0d0;
            }
        """)
        
        # 테이블 설정
        self.table.setAlternatingRowColors(True)  # 행 색상 번갈아가며 표시
        self.table.verticalHeader().setVisible(False)  # 행 번호 숨기기
        self.table.setShowGrid(True)  # 그리드 라인 표시
        
        # 버튼 컨테이너 생성
        button_container = QWidget()
        button_layout = QVBoxLayout()
        button_container.setLayout(button_layout)
        
        # 우측 버튼 패널 재구성
        self.buttons2 = []  # 버튼 리스트 초기화
        
        # 설명서 버튼 생성 및 이벤트 연결
        manual_btn = QPushButton('설명서')
        manual_btn.clicked.connect(self.open_manual)
        button_layout.addWidget(manual_btn)
        button_layout.addSpacing(30)  # 간격 증가
        
        sheet_open_btn = QPushButton('시트 열기')
        sheet_open_btn.clicked.connect(self.open_sheet_and_data)
        button_layout.addWidget(sheet_open_btn)
        
        # 시트 동기화 버튼 추가
        sheet_sync_btn = QPushButton('시트 동기화')
        sheet_sync_btn.clicked.connect(self.sync_from_sheet)
        button_layout.addWidget(sheet_sync_btn)
        
        # 피드/브랜드/아이템 관련 버튼들
        feed_btn = QPushButton('오늘의 피드')
        feed_btn.clicked.connect(lambda: self.run_script('1-1_newfeed_crawl.py'))
        
        brand_extract_btn = QPushButton('브랜드 추출')
        brand_extract_btn.clicked.connect(lambda: self.run_script('1-2_newfeed_analysis_(claude).py'))
        
        brand_check_btn = QPushButton('브랜드 중복체크')
        brand_check_btn.clicked.connect(lambda: self.run_script('3-1.브랜드유사도_측정.py'))
        
        item_check_btn = QPushButton('아이템 중복체크')
        item_check_btn.clicked.connect(lambda: self.run_script('3-2_아이템유사도 측정.py'))
        
        item_today_btn = QPushButton('오늘의 아이템 찾기')
        item_today_btn.clicked.connect(lambda: self.run_item_today('1-3_item_today.py'))
        
        button_layout.addWidget(feed_btn)
        button_layout.addWidget(brand_extract_btn)
        button_layout.addWidget(brand_check_btn)
        button_layout.addWidget(item_check_btn)
        button_layout.addWidget(item_today_btn)
        button_layout.addSpacing(30)  # 간격 증가
        
        # 인플루언서 관련 버튼
        following_btn = QPushButton('팔로잉 추출')
        influencer_btn = QPushButton('인플루언서 분석')
        vision_btn = QPushButton('인플루언서 분석(비전)')
        level_btn = QPushButton('등급 분류')
        
        self.buttons2.extend([following_btn, influencer_btn, vision_btn, level_btn])
        
        button_layout.addWidget(following_btn)
        button_layout.addWidget(influencer_btn)
        button_layout.addWidget(vision_btn)
        button_layout.addWidget(level_btn)
        button_layout.addSpacing(30)  # 간격 증가
        
        # DM/팔로우 관련 버튼
        dm_sheet_btn = QPushButton('DM&팔로우시트 열기')
        dm_sheet_btn.clicked.connect(self.open_dm_sheet)
        button_layout.addWidget(dm_sheet_btn)
        button_layout.addWidget(QPushButton('DM보내기'))
        button_layout.addWidget(QPushButton('팔로우하기'))
        
        # 버튼 컨테이너를 메인 레이아웃에 추가
        layout.addWidget(button_container)
        
        # 데이터 수정 여부를 추적하는 플래그 추가
        self.data_modified = False
        
        # 타이머 설정 추가
        self.timer = QTimer()
        self.timer.timeout.connect(self.check_file_changes)
        self.timer.start(1000)  # 1초마다 갱신
        
        # 마지막 수정 시간 저장
        self.last_modified_time = 0
        
        # JSON 파일 경로 저장
        self.json_file_path = '0-0_clean.json'
        # self.json_file_path = '2-2_influencer_processing_data.json'
        
        # 테이블 초기화
        self.table.setRowCount(0)
        self.table.setColumnCount(0)

        # 라디오 버튼 이벤트 연결
        self.brand_radio.clicked.connect(self.on_radio_button_clicked)
        self.item_radio.clicked.connect(self.on_radio_button_clicked)
        self.influencer_radio.clicked.connect(self.on_radio_button_clicked)

        # 상태 표시줄 추가
        self.statusBar = self.statusBar()
        self.statusBar.showMessage('준비')

        # 버튼 이벤트 연결
        self.buttons2[0].clicked.connect(self.run_following_extract)  # 팔로잉 추출
        self.buttons2[1].clicked.connect(self.run_influencer_processing)  # 인플루언서 분석
        self.buttons2[2].clicked.connect(self.run_vision_mod)  # 인플루언서 분석(비전)
        self.buttons2[3].clicked.connect(self.run_level_classification)  # 등급 분류 추가

        # 스프레드시트 ID와 범위 설정
        self.SPREADSHEET_ID = '1W5Xz4uaqSPysGLk28w6ybFHkGAPcz19_1BHdOih0Hoc'
        self.RANGE_NAME = 'brand_today!A:K'  # K열까지 사용

    def update_json_files(self):
        """현재 디렉토리의 JSON 파일 목록을 업데이트"""
        import glob
        json_files = glob.glob('*.json')  # 현재 디렉토리의 모든 JSON 파일
        self.file_combo.clear()
        self.file_combo.addItems(json_files)
        
        # 기본 파일 선택
        default_file = '0-0_clean.json'
        if default_file in json_files:
            self.file_combo.setCurrentText(default_file)
    
    def on_file_selected(self, filename):
        """콤보박스에서 파일이 선택되었을 때"""
        self.json_file_path = filename
        self.load_json_data()  # 선택된 파일로 데이터 로드

    def truncate_text(self, text, max_length=30):
        """텍스트 길이를 제한하고 ... 추가"""
        text = str(text)  # 입력값을 문자열로 변환
        return text if len(text) <= max_length else text[:max_length] + '...'

    def load_json_data(self):
        try:
            current_modified_time = os.path.getmtime(self.json_file_path)
            
            if current_modified_time != self.last_modified_time and not self.data_modified:
                self.last_modified_time = current_modified_time
                
                with open(self.json_file_path, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    
                if data and isinstance(data, list) and len(data) > 0:
                    # 1-3_item_today_data.json 파일인 경우 정렬 로직 적용
                    if '1-3_item_today' in self.json_file_path:
                        # 날짜를 datetime 객체로 변환하여 최신순으로 정렬
                        from datetime import datetime
                        for item in data:
                            try:
                                item['date_obj'] = datetime.strptime(item['수집일자'], '%Y-%m-%d')
                            except:
                                item['date_obj'] = datetime.min
                        
                        # 1. 날짜 내림차순으로 정렬
                        # 2. 같은 날짜 내에서는 브랜드로 정렬
                        data.sort(key=lambda x: (-x['date_obj'].timestamp(), x['브랜드']))
                        
                        # 임시 date_obj 필드 제거
                        for item in data:
                            del item['date_obj']
                        
                        headers = ['삭제', 'NEW', '수집일자', '브랜드', '브랜드 카테고리', '브랜드 등급', 
                                 '아이템', '작성자', '이름', '등급', '카테고리', '게시물링크']  # 카테고리 추가
                    else:
                        headers = ['삭제'] + list(data[0].keys())
                    
                    self.table.setColumnCount(len(headers))
                    self.table.setHorizontalHeaderLabels(headers)
                    self.table.setRowCount(len(data))
                    
                    # 데이터 채우기
                    for row, item in enumerate(data):
                        # 삭제 버튼 추가 - 수정된 부분
                        delete_btn = QPushButton('삭제')
                        delete_btn.clicked.connect(lambda _, btn=delete_btn: self.delete_row(self.table.indexAt(btn.pos()).row()))
                        # 엔터 키 이벤트 추가
                        delete_btn.setFocusPolicy(Qt.StrongFocus)
                        delete_btn.keyPressEvent = lambda e, btn=delete_btn: self.delete_button_key_press(e, btn)
                        self.table.setCellWidget(row, 0, delete_btn)
                        
                        # 나머지 데이터 채우기
                        for col, header in enumerate(headers[1:], 1):
                            value = str(item.get(header, ''))
                            # 텍스트 길이 제한 적용
                            truncated_value = self.truncate_text(value)
                            table_item = QTableWidgetItem(truncated_value)
                            # 전체 텍스트를 툴팁으로 설정
                            if len(value) > 30:  # 원본 텍스트가 길 경우에만 툴팁 추가
                                table_item.setToolTip(value)
                            table_item.setTextAlignment(Qt.AlignCenter)
                            self.table.setItem(row, col, table_item)
                    
                    # 컬럼 너비 자동 조정
                    self.table.resizeColumnsToContents()
                    # 행 높이 조정
                    self.table.verticalHeader().setDefaultSectionSize(40)
                
        except Exception as e:
            print(f"JSON 파일 로드 중 오류 발생: {str(e)}")

    def delete_row(self, row):
        """테이블에서 선택된 행을 삭제하는 함수"""
        try:
            # 현재 행 수 확인
            total_rows = self.table.rowCount()
            
            # 삭제할 행의 데이터를 저장
            row_data = []
            for col in range(self.table.columnCount()):
                item = self.table.item(row, col)
                if col == 0:  # 삭제 버튼 컬럼
                    row_data.append(None)
                elif item is not None:
                    row_data.append(item.text())
                else:
                    row_data.append('')
            
            # 실행 취소 스택에 삭제할 행의 정보 저장
            self.undo_stack.append({
                'row': row,
                'data': row_data
            })
            
            # JSON 파일에서도 해당 데이터 삭제
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            if 0 <= row < len(json_data):
                json_data.pop(row)
                
                # 데이터 수정 플래그 설정
                self.data_modified = True
                self.last_modified_time = os.path.getmtime(self.json_file_path)
                
                with open(self.json_file_path, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            self.table.removeRow(row)
            
            # 다음 행의 삭제 버튼으로 포커스 이동
            if row < total_rows - 1:  # 마지막 행이 아니면 다음 행으로
                next_button = self.table.cellWidget(row, 0)
                if next_button:
                    next_button.setFocus()
            elif row > 0:  # 마지막 행이면 이전 행으로
                prev_button = self.table.cellWidget(row - 1, 0)
                if prev_button:
                    prev_button.setFocus()
            
        except Exception as e:
            print(f"행 삭제 중 오류 발생: {str(e)}")

    def undo_delete(self):
        """삭제 실행 취소 함수"""
        if not self.undo_stack:
            return
            
        # 스택에서 마지막 삭제 정보 가져오기
        last_delete = self.undo_stack.pop()
        row = last_delete['row']
        row_data = last_delete['data']
        
        # JSON 파일에도 데이터 복원
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            # 데이터 딕셔너리 생성
            restored_data = {}
            headers = [self.table.horizontalHeaderItem(i).text() for i in range(1, self.table.columnCount())]
            for i, header in enumerate(headers, 1):
                if row_data[i]:  # 값이 있는 경우에만 저장
                    restored_data[header] = row_data[i]
            
            # 데이터 삽입
            json_data.insert(row, restored_data)
            
            # 데이터 수정 플래그 설정
            self.data_modified = True
            self.last_modified_time = os.path.getmtime(self.json_file_path)
            
            with open(self.json_file_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
        except Exception as e:
            print(f"JSON 파일 복원 중 오류 발생: {str(e)}")
        
        # 행 삽입
        self.table.insertRow(row)
        
        # 삭제 버튼 다시 추가
        delete_btn = QPushButton('삭제')
        delete_btn.clicked.connect(lambda _, btn=delete_btn: self.delete_row(self.table.indexAt(btn.pos()).row()))
        self.table.setCellWidget(row, 0, delete_btn)
        
        # 데이터 복원
        for col in range(1, len(row_data)):
            if row_data[col] is not None:
                item = QTableWidgetItem(str(row_data[col]))
                item.setTextAlignment(Qt.AlignCenter)
                self.table.setItem(row, col, item)

    def delete_button_key_press(self, event, button):
        """삭제 버튼의 키 입력 이벤트 처리"""
        if event.key() in (Qt.Key_Return, Qt.Key_Enter):
            row = self.table.indexAt(button.pos()).row()
            self.delete_row(row)
        else:
            # 다른 키 이벤트는 기본 동작 수행
            QPushButton.keyPressEvent(button, event)

    # 엑셀 저장 함수 추가
    def save_to_excel(self):
        try:
            # 테이블 데이터를 DataFrame으로 변환
            rows = self.table.rowCount()
            cols = self.table.columnCount()
            headers = [self.table.horizontalHeaderItem(i).text() for i in range(1, cols)]  # 삭제 컬럼 제외
            
            df = pd.DataFrame(columns=headers, index=range(rows))
            
            for row in range(rows):
                for col in range(1, cols):  # 1부터 시작하여 삭제 버튼 컬럼 제외
                    item = self.table.item(row, col)
                    if item is not None:
                        df.iloc[row, col-1] = item.text()  # col-1로 인덱스 조정
            
            # 엑셀 파일로 저장
            excel_file = self.json_file_path.replace('.json', '.xlsx')
            df.to_excel(excel_file, index=False)
            print(f'엑셀 파일이 저장되었습니다: {excel_file}')
            
        except Exception as e:
            print(f"엑셀 파일 저장 중 오류 발생: {str(e)}")

    def run_script(self, script_name):
        """Python 스크립트를 실행하는 함수"""
        try:
            # 확인 대화상자 표시
            reply = QMessageBox.question(self, '실행 확인', 
                f'{script_name}을(를) 실행하시겠습니까?',
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            
            if reply == QMessageBox.Yes:
                self.statusBar.showMessage(f'{script_name} 실행 중...')
                import subprocess
                process = subprocess.Popen(['python', script_name])
                
                # 상태 업데이트를 위한 타이머 설정
                self.check_process_timer = QTimer()
                self.check_process_timer.timeout.connect(
                    lambda: self.check_process_status(process, script_name.replace('.py', ''))
                )
                self.check_process_timer.start(1000)
                
                print(f"{script_name} 실행됨")
        except Exception as e:
            self.statusBar.showMessage(f'오류 발생: {str(e)}')

    def on_radio_button_clicked(self):
        """라디오 버튼 클릭 시 검색창 초기화"""
        self.search_input.clear()
        self.percent_input.clear()
        self.load_json_data()  # 전체 데이터 다시 로드

    def perform_search(self):
        try:
            # 검색 시작 전 데이터 초기화
            filtered_data = []
            
            # 2-2 파일에서 데이터 로드
            with open('2-2_influencer_processing_data.json', 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            # 카테고리 검색인 경우 (퍼센트 입력값이 있을 때)
            if self.percent_input.text().strip():
                # 브랜드/아이템/인플루언서 검색창 초기화
                self.search_input.clear()
                
                selected_category = self.category_combo.currentText()
                try:
                    min_percent = float(self.percent_input.text())
                except ValueError:
                    min_percent = 0
                
                for item in data:
                    category_str = item.get('카테고리', '')
                    if category_str:
                        categories = [cat.strip() for cat in category_str.split(',')]
                        for cat in categories:
                            category_parts = cat.split('(')
                            if len(category_parts) == 2 and selected_category in category_parts[0]:
                                try:
                                    percent = float(category_parts[1].rstrip('%)'))
                                    if percent >= min_percent:
                                        filtered_data.append(item)
                                        break
                                except:
                                    continue
            
            # 브랜드/아이템/인플루언서 검색인 경우
            elif self.search_input.text().strip():
                # 카테고리 검색 입력값 초기화
                self.percent_input.clear()
                search_term = self.search_input.text().strip().lower()
                
                if self.brand_radio.isChecked():
                    for item in data:
                        brands = item.get('브랜드', [])
                        for brand in brands:
                            if search_term in brand.get('name', '').lower():
                                for product in brand.get('products', []):
                                    if product.get('item'):  # 빈 아이템 제외
                                        new_item = item.copy()
                                        new_item['표시_브랜드카테고리'] = brand.get('category', '')
                                        new_item['표시_브랜드'] = brand.get('name', '')
                                        new_item['표시_아이템'] = product.get('item', '')
                                        new_item['표시_언급날짜'] = product.get('mentioned_date', '')
                                        new_item['표시_item_feed_link'] = product.get('item_feed_link', '')
                                        filtered_data.append(new_item)
                
                elif self.item_radio.isChecked():
                    for item in data:
                        brands = item.get('브랜드', [])
                        for brand in brands:
                            for product in brand.get('products', []):
                                if search_term in product.get('item', '').lower():
                                    new_item = item.copy()
                                    new_item['표시_브랜드카테고리'] = brand.get('category', '')
                                    new_item['표시_브랜드'] = brand.get('name', '')
                                    new_item['표시_아이템'] = product.get('item', '')
                                    new_item['표시_언급날짜'] = product.get('mentioned_date', '')
                                    new_item['표시_item_feed_link'] = product.get('item_feed_link', '')
                                    filtered_data.append(new_item)
                
                elif self.influencer_radio.isChecked():
                    search_term = search_term.replace('https://www.instagram.com/', '').replace('/', '')
                    for item in data:
                        if search_term in item.get('username', '').lower():
                            brands = item.get('브랜드', [])
                            if brands:
                                for brand in brands:
                                    for product in brand.get('products', []):
                                        if product.get('item'):  # 빈 아이템 제외
                                            new_item = item.copy()
                                            new_item['표시_브랜드카테고리'] = brand.get('category', '')
                                            new_item['표시_브랜드'] = brand.get('name', '')
                                            new_item['표시_아이템'] = product.get('item', '')
                                            new_item['표시_언급날짜'] = product.get('mentioned_date', '')
                                            new_item['표시_item_feed_link'] = product.get('item_feed_link', '')
                                            filtered_data.append(new_item)
                            else:
                                filtered_data.append(item)
            else:
                # 아무 검색어도 입력되지 않은 경우
                filtered_data = data

            # 검색 결과 표시
            if filtered_data:
                # 헤더 순서 변경: 브랜드카테고리를 브랜드 앞으로 이동
                headers = ["삭제", "브랜드카테고리", "브랜드", "아이템", "언급날짜", "등급", "username", "이름추출", 
                          "팔로워", "릴스평균조회수(최근 15개)", "카테고리", "item_feed_link"]
                
                self.table.setColumnCount(len(headers))
                self.table.setHorizontalHeaderLabels(headers)
                self.table.setRowCount(len(filtered_data))
                
                for row, item in enumerate(filtered_data):
                    # 삭제 버튼 추가
                    delete_btn = QPushButton('삭제')
                    delete_btn.clicked.connect(lambda _, btn=delete_btn: self.delete_row(self.table.indexAt(btn.pos()).row()))
                    self.table.setCellWidget(row, 0, delete_btn)
                    
                    # 나머지 데이터 채우기
                    for col, header in enumerate(headers[1:], 1):
                        value = ''
                        if header == "브랜드카테고리":
                            value = item.get('표시_브랜드카테고리', '')
                        elif header == "브랜드":
                            value = item.get('표시_브랜드', '')
                        elif header == "아이템":
                            value = item.get('표시_아이템', '')
                        elif header == "언급날짜":
                            value = item.get('표시_언급날짜', '')
                        elif header == "item_feed_link":
                            value = item.get('표시_item_feed_link', '')
                        else:
                            value = str(item.get(header, ''))
                        
                        table_item = QTableWidgetItem(value)
                        table_item.setTextAlignment(Qt.AlignCenter)
                        self.table.setItem(row, col, table_item)
                
                self.table.resizeColumnsToContents()
            else:
                self.table.setRowCount(0)
                
        except Exception as e:
            print(f"검색 중 오류 발생: {str(e)}")

    def perform_category_search(self):
        """카테고리 검색을 위한 별도 함수"""
        self.brand_radio.setChecked(False)
        self.item_radio.setChecked(False)
        self.influencer_radio.setChecked(False)
        self.perform_search()

    # 기능 7 버튼 - 인플루언서 처리 실행 함수
    def run_influencer_processing(self):
        """인플루언서 분석 스크립트 실행"""
        try:
            reply = QMessageBox.question(self, '실행 확인', 
                '인플루언서 분석을 실행하시겠습니까?',
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            
            if reply == QMessageBox.Yes:
                self.statusBar.showMessage('인플루언서 분석 실행 중...')
                import subprocess
                process = subprocess.Popen(['python', '2-2_influencer_processing_v2.py'])
                
                self.check_process_timer = QTimer()
                self.check_process_timer.timeout.connect(
                    lambda: self.check_process_status(process, "인플루언서 분석")
                )
                self.check_process_timer.start(1000)
                
                print("2-2_influencer_processing_v2.py 실행됨")
        except Exception as e:
            self.statusBar.showMessage(f'오류 발생: {str(e)}')

    # 기능 8 버튼 - 비전 모듈 실행 함수
    def run_vision_mod(self):
        """비전 분석 스크립트 실행"""
        try:
            reply = QMessageBox.question(self, '실행 확인', 
                '비전 분석을 실행하시겠습니까?',
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            
            if reply == QMessageBox.Yes:
                self.statusBar.showMessage('비전 분석 실행 중...')
                import subprocess
                process = subprocess.Popen(['python', '2-3_vision_mod_v1.py'])
                
                self.check_process_timer = QTimer()
                self.check_process_timer.timeout.connect(
                    lambda: self.check_process_status(process, "비전 분석")
                )
                self.check_process_timer.start(1000)
                
                print("2-3_vision_mod_v1.py 실행됨")
        except Exception as e:
            self.statusBar.showMessage(f'오류 발생: {str(e)}')

    # 등급 분류 실행 함수
    def run_level_classification(self):
        """등급 분류 스크립트 실행"""
        try:
            reply = QMessageBox.question(self, '실행 확인', 
                '등급 분류를 실행하시겠습니까?',
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            
            if reply == QMessageBox.Yes:
                self.statusBar.showMessage('등급 분류 실행 중...')
                import subprocess
                process = subprocess.Popen(['python', '2-4_influencer_level.py'])
                
                self.check_process_timer = QTimer()
                self.check_process_timer.timeout.connect(
                    lambda: self.check_process_status(process, "등급 분류")
                )
                self.check_process_timer.start(1000)
                
                print("2-4_influencer_level.py 실행됨")
        except Exception as e:
            self.statusBar.showMessage(f'오류 발생: {str(e)}')

    def check_process_status(self, process, task_name):
        """프로세스 상태를 확인하고 상태 표시줄 업데이트"""
        if process.poll() is not None:  # 프로세스가 종료됨
            self.check_process_timer.stop()
            if process.returncode == 0:
                self.statusBar.showMessage(f'{task_name} 완료됨')
            else:
                self.statusBar.showMessage(f'{task_name} 실행 중 오류 발생')

    # 초기화 버튼 기능 추가
    def reset_search(self):
        """검색 필드 초기화 및 기본 파일 로드"""
        # 검색 필드 초기화
        self.search_input.clear()
        self.percent_input.clear()
        self.category_combo.setCurrentIndex(0)
        self.brand_radio.setChecked(True)
        
        # 기본 파일로 변경
        self.json_file_path = '0-0_clean.json'
        self.file_combo.setCurrentText('0-0_clean.json')
        
        # undo 스택 초기화
        self.undo_stack.clear()
        
        # 데이터 다시 로드
        self.load_json_data()

    def run_following_extract(self):
        """팔로잉 추출 스크립트 실행"""
        try:
            # URL 입력 받기
            url, ok = QInputDialog.getText(self, '프로필 URL 입력', 
                '인스타그램 프로필 URL 또는 사용자명을 입력하세요:',
                QLineEdit.Normal, '')
            
            if ok and url:
                # URL 형식 확인 및 변환
                if not url.startswith('http'):
                    # 사용자명만 입력한 경우 전체 URL로 변환
                    url = url.strip('/')  # 슬래시 제거
                    url = url.replace('instagram.com/', '')  # instagram.com이 포함된 경우 제거
                    url = f'https://www.instagram.com/{url}/'
                
                # 임시 파일에 URL 저장
                with open('temp_profile_url.txt', 'w', encoding='utf-8') as f:
                    f.write(url)
                
                self.statusBar.showMessage('팔로잉 추출 실행 중...')
                import subprocess
                process = subprocess.Popen(['python', '2-1_following_extract.py'])
                
                # 상태 업데이트를 위한 타이머 설정
                self.check_process_timer = QTimer()
                self.check_process_timer.timeout.connect(
                    lambda: self.check_process_status(process, "팔로잉 추출")
                )
                self.check_process_timer.start(1000)
                
                print("2-1_following_extract.py 실행됨")
            else:
                self.statusBar.showMessage('URL 입력이 취소되었습니다.')
            
        except Exception as e:
            self.statusBar.showMessage(f'오류 발생: {str(e)}')

    def run_item_today(self, script_name):
        """오늘의 아이템 스크립트 실행 및 JSON 파일 표시"""
        try:
            reply = QMessageBox.question(self, '실행 확인', 
                '오늘의 아이템을 실행하시겠습니까?',
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            
            if reply == QMessageBox.Yes:
                # 스크립트 실행
                self.statusBar.showMessage(f'{script_name} 실행 중...')
                import subprocess
                process = subprocess.Popen(['python', script_name])
                
                # 상태 업데이트를 위한 타이머 설정
                self.check_process_timer = QTimer()
                self.check_process_timer.timeout.connect(
                    lambda: self.check_process_status(process, script_name.replace('.py', ''))
                )
                self.check_process_timer.start(1000)
                
                print(f"{script_name} 실행됨")
                
                # JSON 파일 콤보박스 업데이트 및 선택
                def update_and_select_json():
                    self.update_json_files()
                    target_file = '1-3_item_today_data.json'
                    index = self.file_combo.findText(target_file)
                    if index >= 0:
                        self.file_combo.setCurrentIndex(index)
                        self.json_file_path = target_file
                        self.load_json_data()
                
                # 파일이 생성될 시간을 주기 위해 약간의 딜레이 후 실행
                QTimer.singleShot(2000, update_and_select_json)
            
        except Exception as e:
            self.statusBar.showMessage(f'오류 발생: {str(e)}')

    # 설명서 열기 함수 추가
    def open_manual(self):
        """설명서 파일 열기"""
        try:
            manual_path = os.path.join(os.getcwd(), 'manual.txt')
            
            if os.path.exists(manual_path):
                if platform.system() == 'Windows':
                    os.startfile(manual_path)
                elif platform.system() == 'Darwin':  # macOS
                    subprocess.run(['open', manual_path])
                else:  # Linux
                    subprocess.run(['xdg-open', manual_path])
                self.statusBar.showMessage('설명서를 열었습니다.')
            else:
                QMessageBox.warning(self, '경고', 'manual.txt 파일을 찾을 수 없습니다.')
                self.statusBar.showMessage('manual.txt 파일이 없습니다.')
        except Exception as e:
            QMessageBox.critical(self, '오류', f'설명서를 여는 중 오류가 발생했습니다: {str(e)}')
            self.statusBar.showMessage('설명서 열기 실패')

    def open_dm_sheet(self):
        """DM&팔로우 스프레드시트 열기"""
        try:
            import webbrowser
            sheet_url = 'https://docs.google.com/spreadsheets/d/1W5Xz4uaqSPysGLk28w6ybFHkGAPcz19_1BHdOih0Hoc/edit?gid=1773602371#gid=1773602371'
            webbrowser.open(sheet_url)
            self.statusBar.showMessage('DM&팔로우 시트를 열었습니다.')
        except Exception as e:
            QMessageBox.critical(self, '오류', f'스프레드시트를 여는 중 오류가 발생했습니다: {str(e)}')
            self.statusBar.showMessage('스프레드시트 열기 실패')

    def show_new_items(self):
        """NEW 버튼 클릭시 오늘의 아이템 데이터 표시"""
        try:
            target_file = '1-3_item_today_data.json'
            if os.path.exists(target_file):
                self.json_file_path = target_file
                index = self.file_combo.findText(target_file)
                if index >= 0:
                    self.file_combo.setCurrentIndex(index)
                self.load_json_data()
                self.statusBar.showMessage('오늘의 신규 아이템을 표시합니다.')
            else:
                QMessageBox.warning(self, '알림', '오늘의 아이템 데이터가 없습니다.\n먼저 오늘의 아이템 찾기를 실행해주세요.')
                self.statusBar.showMessage('오늘의 아이템 데이터가 없습니다.')
        except Exception as e:
            self.statusBar.showMessage(f'오류 발생: {str(e)}')

    def sync_from_sheet(self):
        """스프레드시트의 데이터를 JSON 파일로 동기화"""
        try:
            # 1. 구글 스프레드시트에서 데이터 읽어오기
            creds = get_credentials()
            service = build('sheets', 'v4', credentials=creds)
            
            # 스프레드시트 데이터 가져오기
            result = service.spreadsheets().values().get(
                spreadsheetId=self.SPREADSHEET_ID,
                range=self.RANGE_NAME
            ).execute()
            sheet_data = result.get('values', [])
            
            if not sheet_data:
                raise Exception("스프레드시트에서 데이터를 찾을 수 없습니다.")
            
            # 헤더 추출 및 인덱스 찾기
            headers = sheet_data[0]
            link_idx = headers.index('게시물링크')
            category_idx = headers.index('브랜드 카테고리')
            
            # 스프레드시트 데이터를 딕셔너리로 변환
            sheet_dict = {row[link_idx]: row[category_idx] for row in sheet_data[1:] if len(row) > max(link_idx, category_idx)}
            
            # 2. JSON 파일 읽기
            target_file = '1-3_item_today_data.json'
            with open(target_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            # 3. 데이터 매칭 및 업데이트
            updated_count = 0
            for item in json_data:
                link = item.get('게시물링크')
                if link in sheet_dict:
                    new_category = sheet_dict[link]
                    if item.get('브랜드 카테고리') != new_category:
                        item['브랜드 카테고리'] = new_category
                        updated_count += 1
            
            # 4. 변경된 데이터 JSON 파일에 저장
            with open(target_file, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            # 5. GUI 테이블 새로고침
            self.json_file_path = target_file
            self.load_json_data()
            
            # 완료 메시지 표시
            QMessageBox.information(self, '동기화 완료', 
                f'동기화가 완료되었습니다.\n업데이트된 항목: {updated_count}개')
            self.statusBar.showMessage(f'스프레드시트에서 {updated_count}개 항목 동기화 완료')
            
        except Exception as e:
            self.statusBar.showMessage(f'오류 발생: {str(e)}')
            QMessageBox.critical(self, '오류', f'동기화 중 오류가 발생했습니다:\n{str(e)}')

    def open_sheet_and_data(self):
        """시트 열기 버튼 클릭시 데이터 표시 및 스프레드시트 열기"""
        try:
            # 1. 오늘의 아이템 데이터 표시
            target_file = '1-3_item_today_data.json'
            if os.path.exists(target_file):
                # JSON 파일 읽기
                with open(target_file, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                
                # Google Sheets API 인증 및 서비스 객체 생성
                creds = get_credentials()
                service = build('sheets', 'v4', credentials=creds)
                
                # 데이터 전처리
                sheet_data = []
                headers = ['NEW', '수집일자', '브랜드', '브랜드 카테고리', '브랜드 등급', 
                          '아이템', '작성자', '이름', '등급', '카테고리', '게시물링크']  # 카테고리 추가
                sheet_data.append(headers)
                
                # JSON 데이터를 시트 형식으로 변환
                for item in json_data:
                    row = [
                        item.get('NEW', ''),
                        item.get('수집일자', ''),
                        item.get('브랜드', ''),
                        item.get('브랜드 카테고리', ''),
                        item.get('브랜드 등급', ''),
                        item.get('아이템', ''),
                        item.get('작성자', ''),
                        item.get('이름', ''),
                        item.get('등급', ''),
                        item.get('카테고리', ''),  # 카테고리 추가
                        item.get('게시물링크', '')
                    ]
                    sheet_data.append(row)
                
                # 기존 시트 데이터 모두 삭제
                service.spreadsheets().values().clear(
                    spreadsheetId=self.SPREADSHEET_ID,
                    range=self.RANGE_NAME
                ).execute()
                
                # 새 데이터 업데이트
                body = {
                    'values': sheet_data
                }
                service.spreadsheets().values().update(
                    spreadsheetId=self.SPREADSHEET_ID,
                    range=self.RANGE_NAME,
                    valueInputOption='RAW',
                    body=body
                ).execute()
                
                # 테이블 표시 업데이트
                self.json_file_path = target_file
                index = self.file_combo.findText(target_file)
                if index >= 0:
                    self.file_combo.setCurrentIndex(index)
                self.load_json_data()
                
                # 스프레드시트 열기
                import webbrowser
                sheet_url = f'https://docs.google.com/spreadsheets/d/{self.SPREADSHEET_ID}/edit#gid=1773602371'
                webbrowser.open(sheet_url)
                
                self.statusBar.showMessage('데이터 로드 및 스프레드시트 동기화 완료')
                
        except Exception as e:
            self.statusBar.showMessage(f'오류 발생: {str(e)}')
            QMessageBox.critical(self, '오류', f'데이터 처리 중 오류가 발생했습니다:\n{str(e)}')

    def check_file_changes(self):
        """파일 변경 여부를 확인하고 필요한 경우에만 데이터를 다시 로드"""
        try:
            if not os.path.exists(self.json_file_path):
                return
                
            current_modified_time = os.path.getmtime(self.json_file_path)
            
            # 파일이 외부에서 수정되었고, 내부적인 수정이 아닌 경우에만 리로드
            if current_modified_time != self.last_modified_time and not self.data_modified:
                self.last_modified_time = current_modified_time
                self.load_json_data()
            
            # 수정 플래그 초기화
            self.data_modified = False
            
        except Exception as e:
            print(f"파일 변경 확인 중 오류 발생: {str(e)}")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    viewer = JSONViewer()
    viewer.show()
    sys.exit(app.exec_())
