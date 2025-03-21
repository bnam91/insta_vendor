import os
import base64
import time
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By

def save_captcha_image(browser, directory="agent_capture"):
    """
    브라우저에서 캡차 이미지를 저장하는 함수
    """
    # 이미지 저장 디렉토리 확인/생성
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    try:
        # 캡차 이미지 찾기
        captcha_img = browser.find_element(By.CSS_SELECTOR, "img#captchaimg")
        
        # 이미지 저장
        img_src = captcha_img.get_attribute("src")
        img_data = img_src.split(',')[1]
        with open(f"{directory}/captcha.png", "wb") as f:
            f.write(base64.b64decode(img_data))
        print("캡차 이미지를 저장했습니다.")
        
        # 질문 텍스트 가져오기
        question_element = browser.find_element(By.CSS_SELECTOR, "em#captcha_info")
        question_text = question_element.text
        print(f"캡차 질문: {question_text}")
        
        return f"{directory}/captcha.png", question_text
    except Exception as e:
        print(f"캡차 이미지 저장 중 오류 발생: {e}")
        return None, None

def get_captcha_answer(image_path, question, api_key):
    """
    GPT-4o에 이미지 전송하여 캡차 답변 얻기
    """
    # 이미지를 base64로 인코딩
    try:
        with open(image_path, "rb") as image_file:
            encoded_image = base64.b64encode(image_file.read()).decode('utf-8')
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        
        payload = {
            "model": "gpt-4o",
            "messages": [
                {
                    "role": "system",
                    "content": "당신은 이미지를 정확하게 해석하는 AI 도우미입니다. 이미지에 표시된 내용을 보고 질문에 대한 정확한 답변만 숫자 혹은 단어로 제공해주세요."
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": f"다음 이미지를 보고 질문에 답해주세요. 질문: {question}. 필요한 내용만 정확히 답변해주세요."},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{encoded_image}"
                            }
                        }
                    ]
                }
            ],
            "max_tokens": 100
        }
        
        response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
        result = response.json()
        
        if "choices" in result:
            answer = result["choices"][0]["message"]["content"].strip()
            
            # 토큰 사용량 계산 및 출력
            if "usage" in result:
                # 토큰 사용량 계산
                prompt_tokens = result["usage"]["prompt_tokens"]
                completion_tokens = result["usage"]["completion_tokens"]
                total_tokens = result["usage"]["total_tokens"]
                
                # 토큰당 가격 (달러) - GPT-4o 가격
                prompt_price_per_token = 0.00001  # $0.01 / 1K tokens
                completion_price_per_token = 0.00003  # $0.03 / 1K tokens
                
                # 달러 비용 계산
                prompt_cost_usd = prompt_tokens * prompt_price_per_token
                completion_cost_usd = completion_tokens * completion_price_per_token
                total_cost_usd = prompt_cost_usd + completion_cost_usd
                
                # 한화로 환산 (환율: 1 USD = 약 1,350 KRW로 가정)
                exchange_rate = 1350
                total_cost_krw = total_cost_usd * exchange_rate
                
                print("\nGPT-4o 캡챠 토큰 사용량:")
                print(f"프롬프트 토큰: {prompt_tokens}")
                print(f"응답 토큰: {completion_tokens}")
                print(f"총 토큰: {total_tokens}")
                print(f"예상 비용: ${total_cost_usd:.6f} (약 {total_cost_krw:.2f}원)")
            
            # 숫자만 추출
            numeric_answer = ''.join(filter(str.isdigit, answer))
            return numeric_answer if numeric_answer else answer
        else:
            print("API 응답 오류:", result)
            return "3"  # 오류 시 기본값
    except Exception as e:
        print(f"캡차 답변 획득 중 오류 발생: {e}")
        return "3"  # 오류 시 기본값

def input_captcha_answer(browser, captcha_answer):
    """
    캡차 입력 필드에 답변 입력하고 확인 버튼 클릭
    """
    try:
        print("캡차 입력 필드에 입력을 시도합니다...")
        
        # 방법 1: 기본 send_keys 사용 (다양한 셀렉터 시도)
        selectors = [
            "input.RNlBgEir9H", 
            "input#captcha", 
            "input[placeholder='정답을 입력해주세요.']",
            "input[type='text']",
            ".RNlBgEir9H"
        ]
        
        input_success = False
        for selector in selectors:
            try:
                answer_input = browser.find_element(By.CSS_SELECTOR, selector)
                browser.execute_script("arguments[0].scrollIntoView(true);", answer_input)
                time.sleep(1)
                answer_input.clear()
                answer_input.send_keys(captcha_answer)
                print(f"셀렉터 '{selector}'로 입력 성공")
                input_success = True
                break
            except Exception as e:
                print(f"셀렉터 '{selector}' 실패: {e}")
        
        if not input_success:
            print("모든 입력 셀렉터 시도 실패")
            return False
        
        # 확인 버튼 클릭 시도 (다양한 방법)
        time.sleep(1)
        
        # 1. 일반 클릭
        confirm_selectors = [
            "button._1pLxm0thhJ", 
            "button._3H41zekzbw", 
            "button[type='submit']",
            "button.submit",
            "button:contains('확인')"
        ]
        
        click_success = False
        for selector in confirm_selectors:
            try:
                confirm_button = browser.find_element(By.CSS_SELECTOR, selector)
                browser.execute_script("arguments[0].scrollIntoView(true);", confirm_button)
                time.sleep(0.5)
                confirm_button.click()
                print(f"확인 버튼 클릭 성공: {selector}")
                click_success = True
                break
            except Exception as e:
                print(f"확인 버튼 클릭 실패: {selector} - {e}")
        
        # 2. JavaScript 클릭 (일반 클릭 실패 시)
        if not click_success:
            js_click_scripts = [
                "document.querySelector('button._3H41zekzbw').click();",
                "document.querySelector('button[type=\"submit\"]').click();",
                "document.querySelector('button.submit').click();",
                "document.getElementsByTagName('button')[0].click();"
            ]
            
            for script in js_click_scripts:
                try:
                    browser.execute_script(script)
                    print(f"JS 클릭 성공: {script}")
                    click_success = True
                    break
                except Exception as e:
                    print(f"JS 클릭 실패: {script} - {e}")
        
        print("모든 클릭 방법 시도 완료")
        return click_success
    
    except Exception as e:
        print(f"캡차 답변 입력 중 오류 발생: {e}")
        return False

def extract_seller_info(browser):
    """
    판매자 정보 추출 함수
    """
    try:
        print("\n판매자 정보 추출 중...")
        time.sleep(3)  # 페이지 로딩 대기 시간
        
        # 찾은 정보를 저장할 딕셔너리
        seller_info = {
            "상호명": "",
            "고객센터 번호": "",
            "사업장소재지": "",
            "이메일주소": ""
        }
        
        # 정보 유형별 추출 여부
        found_info_types = set()
        
        # 실제 HTML 구조에 맞는 선택자 사용
        selectors = [
            # dt/dd 구조에 맞는 XPath 선택자
            {"type": "xpath", "selector": "//dt[contains(text(), '상호명')]/following-sibling::dd", "name": "상호명"},
            {"type": "xpath", "selector": "//dt[contains(text(), '고객센터')]/following-sibling::dd", "name": "고객센터 번호"},
            {"type": "xpath", "selector": "//dt[contains(text(), '사업장 소재지')]/following-sibling::dd", "name": "사업장소재지"},
            {"type": "xpath", "selector": "//dt[contains(text(), 'e-mail')]/following-sibling::dd", "name": "이메일주소"},
            
            # 클래스 기반 CSS 선택자 - 클래스 이름 정확히 사용
            {"type": "xpath", "selector": "//dt[@class='_1nqckXI-BW'][contains(text(), '상호명')]/following-sibling::dd[@class='EdE67hDR6I']", "name": "상호명"},
            {"type": "xpath", "selector": "//dt[@class='_1nqckXI-BW'][contains(text(), '고객센터')]/following-sibling::dd[@class='EdE67hDR6I']", "name": "고객센터 번호"},
            {"type": "xpath", "selector": "//dt[@class='_1nqckXI-BW'][contains(text(), '사업장 소재지')]/following-sibling::dd[@class='EdE67hDR6I']", "name": "사업장소재지"},
            {"type": "xpath", "selector": "//dt[@class='_1nqckXI-BW'][contains(text(), 'e-mail')]/following-sibling::dd[@class='EdE67hDR6I']", "name": "이메일주소"},
            
            # 상위 div 구조 활용
            {"type": "xpath", "selector": "//div[@class='aAVvlAZ43w'][.//dt[contains(text(), '상호명')]]//dd", "name": "상호명"},
            {"type": "xpath", "selector": "//div[@class='aAVvlAZ43w'][.//dt[contains(text(), '고객센터')]]//dd", "name": "고객센터 번호"},
            {"type": "xpath", "selector": "//div[@class='aAVvlAZ43w'][.//dt[contains(text(), '사업장')]]//dd", "name": "사업장소재지"},
            {"type": "xpath", "selector": "//div[@class='aAVvlAZ43w'][.//dt[contains(text(), 'e-mail')]]//dd", "name": "이메일주소"}
        ]
        
        for selector in selectors:
            info_type = selector["name"]
            
            # 이미 해당 정보를 찾았으면 건너뛰기
            if info_type in found_info_types:
                continue
                
            try:
                if selector["type"] == "xpath":
                    element = browser.find_element(By.XPATH, selector["selector"])
                else:
                    element = browser.find_element(By.CSS_SELECTOR, selector["selector"])
                
                value = element.text.strip()
                
                # 고객센터 번호인 경우 전화번호만 추출
                if info_type == "고객센터 번호":
                    # 공백으로 분할하고 첫 번째 부분(전화번호)만 사용
                    value = value.split()[0]
                
                seller_info[info_type] = value
                found_info_types.add(info_type)
            except Exception:
                pass  # 오류 메시지 출력하지 않음
        
        # 찾은 정보만 반환
        return {k: v for k, v in seller_info.items() if v}
        
    except Exception as e:
        print(f"판매자 정보 추출 중 오류 발생: {e}")
        return {}

def solve_captcha_and_get_info(browser, api_key):
    """
    캡차 해결 후 판매자 정보 가져오기
    """
    try:
        # 1. 캡차 이미지 저장 및 질문 가져오기
        image_path, question = save_captcha_image(browser)
        if not image_path or not question:
            print("캡차 이미지 또는 질문을 가져오는데 실패했습니다.")
            return {}
        
        # 2. GPT-4o로 캡차 답변 얻기
        captcha_answer = get_captcha_answer(image_path, question, api_key)
        print(f"GPT가 제공한 캡차 답변: {captcha_answer}")
        
        # 3. 캡차 답변 입력 및 제출
        input_success = input_captcha_answer(browser, captcha_answer)
        if not input_success:
            print("캡차 답변 입력에 실패했습니다.")
            return {}
        
        # 4. 입력 완료 후 잠시 대기
        time.sleep(3)
        
        # 5. 판매자 정보 추출
        seller_info = extract_seller_info(browser)
        return seller_info
    
    except Exception as e:
        print(f"캡차 해결 및 정보 추출 과정에서 오류 발생: {e}")
        return {} 