import pandas as pd
from bs4 import BeautifulSoup
import logging
import os
import argparse


# Set up logging
logging.basicConfig(filename='decompress.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')



# 각 파일에 대응하는 comment 파싱 키 클래스
parsing_classKey_comment = {
    'naver_blog': 'u_cbox_contents',
    'naver_cafe': 'txt',
    'naver_kin': 'answerDetail'
}

# 각 파일에 대응하는 secretComment 파싱 키 클래스
parsing_classKey_secretComment = {
    'naver_blog': 'u_cbox_delete_contents',
    'naver_cafe': 'not_exit_classKey_1446a54sd15sd67s89456123456789',
    'naver_kin': 'not_exit_classKey_1446a54sd15sd67s89456123456789'
}

# 필요한 열만 선택하여 엑셀 파일을 읽어옴
columns_to_extract = ['channel', 'title', 'registered_date', 'detail_content', 'comment_html', 'site_name', 'board_name']

#comment_html' 열의 HTML 내용을 파싱하여 class 요소의 개수를 추출하는 함수
def count_elements(html_content, class_name):
    
    if pd.isna(html_content):  # 셀이 비어 있는 경우 처리
        return 0
    soup = BeautifulSoup(html_content, 'lxml')
    elements = soup.find_all(class_=class_name)
    return len(elements)

#comment_html 열의 HTML 내용을 파싱하여 comment_texts를 추출하는 함수
def extract_contents(html_content, class_name):
    
    
    if pd.isna(html_content):  # 셀이 비어 있는 경우 처리
        return []
    
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        elements = soup.find_all(class_=class_name)
        return [element.get_text(strip=True) for element in elements]
    except Exception as e:
        logging.error(f"Error in extract_contents: {e}")
        logging.exception("Traceback:")  # Log the full stack trace
        return []

#commentN'의 갯수+1 행을 복제하면서 첫 번째 행은 detail_content를 유지하고 나머지는 comment_texts 텍스트를 채우는 함수
def expand_rows(row):
    
    rows = []
    repeat_count = row['commentN']  # comment의 갯수만큼 row 생성
    if repeat_count > 0:
        # 첫 번째 행은 detail_content와 종류를 설정합니다
        row_copy = row.copy()
        row_copy['종류'] = 'detail_content'
        rows.append(row_copy)
        contents_texts = row['comment_texts']
        # 나머지 행들은 comment_texts 텍스트를 채우고 종류를 설정합니다
        for i in range(1, repeat_count + 1):
            new_row = row.copy()
            if i <= len(contents_texts):
                new_row['detail_content'] = contents_texts[i - 1]
            else:
                new_row['detail_content'] = "비밀댓글입니다"  # comment_texts 외에 나머지는 모두 비밀댓글로 간주
            new_row['종류'] = 'comment'
            rows.append(new_row)
    return rows

def get_file_path_and_keys(path ,file_name, key):
    file_path = f"{path}{file_name}"
    
    try:
        comment_class_key = parsing_classKey_comment[key]
        secret_comment_class_key = parsing_classKey_secretComment[key]
        return file_path, comment_class_key, secret_comment_class_key
    except KeyError:
        print(f"Error: '{key}'에 대응하는 파일이 존재하지 않습니다.")
        return None, None, None


def process_excel_file(input_path, file_name, output_path, output_file_name=None, type=None):
    file_path, comment_class_key, secret_comment_class_key = get_file_path_and_keys(input_path , file_name ,type)
    if not file_path:
        return
    

    df = pd.read_excel(file_path, usecols=columns_to_extract)
    # comment의 갯수를 계산하기 위해 열에 대해 count_elements 함수를 적용하여 새로운 열에 저장합니다
    df['comment_class_key'] = df['comment_html'].apply(lambda x: count_elements(x, comment_class_key))
    df['secret_comment_class_key'] = df['comment_html'].apply(lambda x: count_elements(x, secret_comment_class_key))
    df['commentN'] = df['comment_class_key'] + df['secret_comment_class_key']
    # 'comment_html' 열에서 comment_class_key 텍스트를 추출하여 새로운 열에 저장합니다
    df['comment_texts'] = df['comment_html'].apply(lambda x: extract_contents(x, comment_class_key))

    # 새로운 데이터프레임 생성
    expanded_rows = []
    for _, row in df.iterrows():
        expanded_rows.extend(expand_rows(row))

    new_df = pd.DataFrame(expanded_rows)
    # 새로운 열 추가
    new_df['사용여부'] = '0'
    new_df['category'] = '회생파산'

    # 새로운 엑셀 파일로 저장
    if not output_file_name:
        base_file_name = os.path.splitext(file_name)[0]  # 확장자를 제거한 파일 이름
        output_file_name = f"{base_file_name}_decompress.xlsx"
            
    output_file_path = f'{output_path}{output_file_name}'
    new_df.to_excel(output_file_path, index=False, columns=['사용여부', 'channel', 'category', 'title', 'detail_content', '종류', 'registered_date', 'site_name', 'board_name'])

    print(f"New Excel file saved to {output_file_path}")

def main():
    file_input = input("input 경로와 파일 이름을 입력하세요 (예:./naver_카페.xlsx): ")
    input_path, file_name = os.path.split(file_input)
    input_path += '/'
    if not file_name:
        print("Error: 파일 이름을 입력해야 합니다.")
        return

    
    file_output = input("output 경로와 파일 이름을 입력하세요 (예:./naver_카페.xlsx, 이름 생략 시 기본 decompress로 지정):")
    output_path , output_file_name =os.path.split(file_output)
    output_path += '/'
    if not output_file_name:
        output_file_name = None
    
    type = input("타입을 입력하세요.")
    if not type:
        print("Error: 파일 종류를 입력해야 합니다.")
        return
    
    print("파일 입력 경로 확인:", input_path)
    print("파일 출력 경로 확인:", output_path)



    print("변환 작업중입니다. 잠시만 기다려주세요...")
    process_excel_file(input_path, file_name, output_path,output_file_name, type)



if __name__ == "__main__":
    main()
