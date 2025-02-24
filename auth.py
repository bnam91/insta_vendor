import os
import sys
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

# Google API 접근 범위 설정 (필요한 최소한의 범위만 포함)
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/documents',
    'https://www.googleapis.com/auth/calendar'
]

# 환경 변수에서 클라이언트 정보 가져오기
CLIENT_ID = os.getenv('GOOGLE_CLIENT_ID')
CLIENT_SECRET = os.getenv('GOOGLE_CLIENT_SECRET')

def get_token_path():
    """운영 체제에 따른 토큰 저장 경로 반환"""
    if sys.platform == "win32":
        return os.path.join(os.environ["APPDATA"], "GoogleAPI", "token.json")
    return os.path.join(os.path.expanduser("~"), ".config", "GoogleAPI", "token.json")

def ensure_token_dir():
    """토큰 저장 디렉토리가 없으면 생성"""
    token_dir = os.path.dirname(get_token_path())
    if not os.path.exists(token_dir):
        os.makedirs(token_dir)

def get_credentials():
    """OAuth2 인증을 통해 자격 증명 반환"""
    token_path = get_token_path()
    ensure_token_dir()

    creds = None
    if os.path.exists(token_path):
        # 저장된 토큰이 있으면 로드
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    
    if not creds or not creds.valid:
        # 토큰이 없거나 유효하지 않으면 새로 생성
        if creds and creds.expired and creds.refresh_token:
            # 토큰이 만료되었지만 갱신 가능하면 갱신
            creds.refresh(Request())
        else:
            # 새로 OAuth2 플로우를 통해 인증
            flow = InstalledAppFlow.from_client_config(
                {
                    "installed": {
                        "client_id": CLIENT_ID,
                        "client_secret": CLIENT_SECRET,
                        "redirect_uris": ["http://localhost"],
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                    }
                },
                SCOPES
            )
            creds = flow.run_local_server(port=0)
        
        # 생성된 토큰을 파일에 저장
        with open(token_path, 'w') as token:
            token.write(creds.to_json())
    
    return creds
