import pandas as pd
import requests
from bs4 import BeautifulSoup

# 엑셀 파일에서 링크가 포함된 컬럼을 읽기
input_file = 'naver_카페.xlsx'  # 원본 엑셀 파일 경로
output_file = '카페_link_output.xlsx'  # 출력 엑셀 파일 경로
column_name = 'link'  # 링크가 포함된 컬럼 이름

# 엑셀 파일 읽기
df = pd.read_excel(input_file)

# 결과를 저장할 리스트
results = []

for index, row in df.iterrows():
    url = row[column_name]
    try:
        # URL에 접속
        response = requests.get(url)
        response.raise_for_status()
        
        # HTML 파싱
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 클래스가 'answerDetail'인 모든 요소를 찾아서 텍스트 추출
        answer_details = soup.find_all(class_='txt')
        if answer_details:
            # 모든 answerDetail 클래스를 가진 요소의 텍스트를 합쳐서 저장
            text = '\n\n\n'.join(detail.get_text(strip=True) for detail in answer_details)
        else:
            text = "No answerDetail found"
    except requests.exceptions.RequestException as e:
        text = f"Error: {e}"
        # 추가 디버깅 정보
        print(f"Failed to fetch URL: {url}. Error: {e}")
    except Exception as e:
        text = f"Error: Failed parsing ({e})"
    
    # 결과 리스트에 추가
    results.append(text)

# 새로운 컬럼에 결과 추가
df['AnswerDetails'] = results

# 결과를 새로운 엑셀 파일에 저장
df.to_excel(output_file, index=False)

