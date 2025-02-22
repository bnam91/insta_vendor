"""
1-3_item_today.py - 인플루언서 공구 아이템 데이터 처리 및 업데이트

입력 파일:
1. 1-1_newfeed_crawl_data.json: 크롤링된 새로운 피드 데이터
2. brand_category.json: 브랜드 정보 및 카테고리 데이터
3. 2-2_influencer_processing_data.json: 인플루언서 정보 데이터
4. 1-3_item_today_data.json: 기존에 처리된 아이템 데이터

출력 파일:
1. 1-3_item_today_data.json: 업데이트된 최종 아이템 데이터
2. 1-1_newfeed_crawl_data.json: 처리 상태가 업데이트된 피드 데이터

주요 기능:
- 새로운 피드 데이터에서 미처리된 공구 아이템 추출
- 브랜드 정보 매핑 (별칭 처리 포함)
- 인플루언서 정보 연동
- 중복 데이터 필터링 (작성자, 브랜드, 날짜 기준)
- NEW 표시 업데이트 (2일 이내 데이터)
"""

from datetime import datetime
import json

def get_base_author(author_str):
    """작성자 문자열에서 기본 닉네임 추출"""
    return author_str.split('(')[0].strip()

def should_combine_dates(date1, date2):
    """두 날짜가 20일 이내인지 확인
    날짜가 없는 경우 오늘 날짜 기준으로 계산"""
    try:
        # 날짜가 없는 경우 오늘 날짜 사용
        if not date1:
            date1 = datetime.now().strftime('%Y-%m-%d')
        if not date2:
            date2 = datetime.now().strftime('%Y-%m-%d')
            
        d1 = datetime.strptime(date1, '%Y-%m-%d')
        d2 = datetime.strptime(date2, '%Y-%m-%d')
        return abs((d1 - d2).days) < 20
    except:
        return True

def update_data():
    try:
        print("데이터 업데이트를 시작합니다...")
        today_date = datetime.now()
        
        # 기존 데이터 읽기 및 NEW 표시 업데이트
        try:
            with open('1-3_item_today_data.json', 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
            print(f"- 기존 데이터 수: {len(existing_data)}개")
            
            # 기존 데이터의 NEW 표시 업데이트
            for item in existing_data:
                collection_date = datetime.strptime(item.get('수집일자', ''), '%Y-%m-%d')
                days_difference = (today_date - collection_date).days
                item['NEW'] = 'NEW' if days_difference <= 2 else ''
        except (FileNotFoundError, json.JSONDecodeError):
            existing_data = []
            print("- 기존 데이터 파일이 없거나 비어있습니다.")

        # 새로운 피드 데이터 읽기
        try:
            with open('1-1_newfeed_crawl_data.json', 'r', encoding='utf-8') as f:
                newfeed_data = json.load(f)  # 배열 형태로 로드됨
                if not isinstance(newfeed_data, list):
                    print("1-1 파일이 배열 형태가 아닙니다.")
                    newfeed_data = []
                print(f"- 1-1 파일 데이터 수: {len(newfeed_data)}개")
        except json.JSONDecodeError as e:
            print(f"1-1 파일 읽기 오류: {str(e)}")
            newfeed_data = []

        # brand_category.json 파일 읽기
        with open('brand_category.json', 'r', encoding='utf-8') as f:
            brand_category_data = json.load(f)
            
        # 2-2_influencer_processing_data.json 파일 읽기
        with open('2-2_influencer_processing_data.json', 'r', encoding='utf-8') as f:
            influencer_data = json.load(f)
            
        # 인플루언서 데이터를 username으로 빠르게 검색하기 위한 딕셔너리 생성
        influencer_dict = {inf['username']: inf for inf in influencer_data}
        
        # 브랜드 별칭을 실제 브랜드명으로 매핑하는 딕셔너리 생성
        brand_mapping = {}
        for brand_name, brand_info in brand_category_data['brands'].items():  # 'brands' 키 추가
            brand_mapping[brand_name] = {
                'name': brand_name,
                'category': brand_info.get('category', ''),
                'level': brand_info.get('level', '')
            }
            # 별칭이 있는 경우 각 별칭을 대표 브랜드명으로 매핑
            if brand_info.get('aliases'):
                for alias in brand_info['aliases']:
                    brand_mapping[alias] = {
                        'name': brand_name,  # 별칭의 경우 대표 브랜드명 사용
                        'category': brand_info.get('category', ''),
                        'level': brand_info.get('level', '')
                    }

        # 새로운 피드 데이터 처리
        new_items = []
        processed_count = 0

        for item in newfeed_data:
            print(f"\n처리 시작: {item.get('작성자')} - {item.get('브랜드')}")
            print(f"현재 처리여부: {item.get('처리여부')}")
            print(f"브랜드 존재 여부: {bool(item.get('브랜드'))}")
            
            if item.get('처리여부') == False and item.get('브랜드'):
                brand = item['브랜드'].strip()
                print(f"브랜드 처리: {brand}")
                brand_info = brand_mapping.get(brand, {'name': brand, 'category': '', 'level': ''})
                print(f"브랜드 매핑 결과: {brand_info}")
                
                # 인플루언서 정보 가져오기
                influencer = influencer_dict.get(item['작성자'], {})
                
                processed_item = {
                    'NEW': 'NEW',
                    '수집일자': item['수집일자'],
                    '브랜드': brand_info['name'],
                    '브랜드 카테고리': brand_info['category'],
                    '브랜드 등급': brand_info['level'],
                    '아이템': item['공구상품'],
                    '작성자': item['작성자'],
                    '이름': influencer.get('이름추출', ''),
                    '등급': influencer.get('등급', ''),
                    '카테고리': influencer.get('카테고리', ''),
                    '게시물링크': item['게시물링크']
                }

                # 중복 체크
                is_duplicate = False
                for existing_item in existing_data + new_items:
                    same_author = get_base_author(existing_item['작성자']) == get_base_author(processed_item['작성자'])
                    same_brand = existing_item['브랜드'] == processed_item['브랜드']
                    dates_within_range = should_combine_dates(existing_item['수집일자'], processed_item['수집일자'])
                    
                    if same_author and same_brand and dates_within_range:
                        is_duplicate = True
                        print(f"중복 발견: {processed_item['작성자']} - {processed_item['브랜드']}")
                        break

                if not is_duplicate:
                    new_items.append(processed_item)
                    item['처리여부'] = True
                    processed_count += 1
                    print(f"처리 완료: {item['작성자']} - {item['브랜드']}")
                    print(f"처리여부 변경됨: {item['처리여부']}")

        # 기존 데이터와 새로운 데이터 통합
        updated_data = existing_data + new_items

        # 최종 데이터 저장
        with open('1-3_item_today_data.json', 'w', encoding='utf-8') as f:
            json.dump(updated_data, f, ensure_ascii=False, indent=2)
            
        # 1-1 파일 업데이트 - 전체 배열을 저장
        print(f"\n저장 전 처리된 아이템 수: {processed_count}")
        with open('1-1_newfeed_crawl_data.json', 'w', encoding='utf-8') as f:
            json.dump(newfeed_data, f, ensure_ascii=False, indent=2)

        print(f"\n작업이 완료되었습니다!")
        print(f"- 최종 데이터 수: {len(updated_data)}개")
        print(f"- 새로 추가된 데이터 수: {len(new_items)}개")
        print(f"- 처리된 아이템 수: {processed_count}개")

    except Exception as e:
        print(f"\n오류 발생: {str(e)}")

def format_date(date_str):
    """오픈예정일 포맷 변환 (2025-02-10 -> 250210)"""
    if not date_str or date_str.strip() == '':
        return '-'
    try:
        date_obj = datetime.strptime(date_str.strip(), '%Y-%m-%d')
        return date_obj.strftime('%y%m%d')
    except:
        return '-'

if __name__ == "__main__":
    update_data()
