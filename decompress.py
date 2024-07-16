import pandas as pd
from bs4 import BeautifulSoup
import logging
import os
import argparse
import platform


#엑셀에서 한 cell당 저장할 수 있는 comment_html    1cell당 값이 32000을 넘어서 html파일에 접근하여 그곳에서 parsing해야함.
#column값들을 쉽게 다룰 수 있게 따로 변수로 뺴서 가져옴
#register-data값을 그대로 가져오는 것이 아니라 형식을 변경해서 가져와야함. ex) 24-07-13


# Set up logging
logging.basicConfig(filename='decompress.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


# cloumn 값을 지정함.
column_filed = {
    1 : 'channel' ,
    2 : 'title',
    3 : 'registered_date',
    4 : 'detail_content',
    5 : 'comment_html',
    6 : 'site_name',
    7 : 'board_name'
}
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
columns_to_extract = [column_filed[1], column_filed[2], column_filed[3], column_filed[4], column_filed[5], column_filed[6], column_filed[7]]


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
    parser = argparse.ArgumentParser(description='Process Excel file.')
    parser.add_argument('-input', required=True, help='input 경로와 파일 이름 (예: ./inputexcelfile.xlsx)')
    parser.add_argument('-output', required=True, help='output 경로와 파일 이름 (예: ./outputexcelfile.xlsx)')
    parser.add_argument('-type', required=True, help='파일 종류 (예: naver_blog)')
    args = parser.parse_args()

    input_path, input_file_name = os.path.split(args.input)
    output_path, output_file_name = os.path.split(args.output)
    
    
    
    if  os.path.isabs(input_path) and platform.system() == "Windows":  #상대경로가 아니라면
        print("input_path가 절대경로 입니다. input_path: "  + str(input_path))
        input_path += '\\'
    else:
        input_path += '/'
    

    if  os.path.isabs(output_path) and  platform.system() == "Windows":  #상대경로가 아니라면
        print("output_path가 절대경로 입니다. output_path: "  + str(output_path))
        output_path += '\\'        
    else:
        output_path += '/' 



    if not input_file_name:
        print("Error: 파일 이름을 입력해야 합니다.")
        return

    if not output_file_name: #output file명을 입력하지 않으면, _decompress이름이 붙은 파일이 생성.
        output_file_name = None


    if not args.type:
        print("Error: 파일 종류를 입력해야 합니다.")
        return

    print("파일 입력 경로 확인:", input_path)
    print("파일 출력 경로 확인:", output_path)

    print("변환 작업중입니다. 잠시만 기다려주세요...")
    process_excel_file(input_path, input_file_name, output_path, output_file_name, args.type)

if __name__ == "__main__":
    main()
