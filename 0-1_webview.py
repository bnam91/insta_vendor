import webview
import subprocess
import os

def open_streamlit_gui():
    # Streamlit 앱을 실행하는 명령어
    subprocess.Popen(
        ['streamlit', 'run', '0-0_streamlit_gui.py', '--server.headless', 'true'],
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
    )

    # 웹뷰 창 생성 (초기 크기 설정)
    webview.create_window('Streamlit GUI', 'http://localhost:8501', width=1400, height=800)

    # 웹뷰 시작
    webview.start()

if __name__ == '__main__':
    open_streamlit_gui()
