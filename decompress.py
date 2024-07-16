import pandas as pd
from bs4 import BeautifulSoup


#파일명 경로 매핑
file_paths = {
    '1234': '../완성예시/1234.xlsx',
    'naver_카페': '../완성예시/naver_카페.xlsx',
    'naver_지식인': '../완성예시/naver_지식인.xlsx'
}

# 각 파일에 대응하는 comment 파싱 키 클래스
parsing_classKey_comment = {
    '1234': 'u_cbox_contents',
    'naver_카페': 'txt',
    'naver_지식인' : 'answerDetail'
}

# 각 파일에 대응하는 secretComment 파싱 키 클래스
parsing_classKey_secretComment = {
    '1234': 'u_cbox_delete_contents',
    'naver_카페': 'not_exit_classKey_1446a54sd15sd67s89456123456789' ,  
    'naver_지식인' : 'not_exit_classKey_1446a54sd15sd67s89456123456789'
}

# 파일 이름을 입력받음
file_name = input("파일 이름을 입력하세요: ")


# 입력받은 파일 이름으로 파일 경로, 각 파일에 대응하는 classKey들을 가져옴
try:
    file_path = file_paths[file_name]
    comment_class_key = parsing_classKey_comment[file_name]
    secret_comment_class_key = parsing_classKey_secretComment[file_name]
except KeyError:
    print(f"Error: '{file_name}'에 대응하는 파일이 존재하지 않습니다.")
    exit()



# 필요한 열만 선택하여 엑셀 파일을 읽어옴
columns_to_extract = ['channel', 'title', 'registered_date', 'detail_content', 'comment_html', 'site_name', 'board_name']
df = pd.read_excel(file_path, usecols=columns_to_extract)

# 'comment_html' 열의 HTML 내용을 파싱하여 class 요소의 개수를 추출하는 함수
def count_elements(html_content, class_name):
    if pd.isna(html_content):  # 셀이 비어 있는 경우 처리
        return 0
    soup = BeautifulSoup(html_content, 'lxml')
    elements = soup.find_all(class_=class_name)
    return len(elements)

# 'comment_html' 열의 HTML 내용을 파싱하여 comment_texts를 추출하는 함수
def extract_contents(html_content, class_name):
    if pd.isna(html_content):  # 셀이 비어 있는 경우 처리
        return []
    soup = BeautifulSoup(html_content, 'lxml')
    elements = soup.find_all(class_=class_name)
    return [element.get_text(strip=True) for element in elements]

# comment의 갯수를 계산하기 위해 열에 대해 count_elements함수를적용하여 새로운 열에 저장합니다
df['comment_class_key'] = df['comment_html'].apply(lambda x: count_elements(x, comment_class_key))
df['secret_comment_class_key'] = df['comment_html'].apply(lambda x: count_elements(x, secret_comment_class_key))
df['commentN'] = df['comment_class_key'] + df['secret_comment_class_key']

# 'comment_html' 열에서 comment_class_key 텍스트를 추출하여 새로운 열에 저장합니다
df['comment_texts'] = df['comment_html'].apply(lambda x: extract_contents(x, comment_class_key))

# 'commentN'의 갯수+1 행을 복제하면서 첫 번째 행은 detail_content를 유지하고 나머지는 comment_texts 텍스트를 채우는 함수(종료행과 detail_content행 채움)
def expand_rows(row):
    rows = []
    repeat_count = row['commentN'] #comment의 갯수만큼 row생성
    if repeat_count > 0:
        # 첫 번째 행은 detail_content와 종류를 설정합니다
        row['종류'] = 'detail_content'
        rows.append(row)
        contents_texts = row['comment_texts']
        # 나머지 행들은 comment_texts 텍스트를 채우고 종류를 설정합니다
        for i in range(1, repeat_count + 1):
            new_row = row.copy()
            if i <= len(contents_texts):
                new_row['detail_content'] = contents_texts[i - 1]
            else:
                new_row['detail_content'] = "비밀댓글입니다"  # comment_texts외에 나머지는 모두 비밀댓글로 간주.
            new_row['종류'] = 'comment'
            rows.append(new_row)
    return rows


# 새로운 데이터프레임 생성
expanded_rows = []
for _, row in df.iterrows():
    expanded_rows.extend(expand_rows(row))

new_df = pd.DataFrame(expanded_rows)

# 새로운 열 추가
new_df['사용여부'] = '0'
new_df['category'] = '회생파산'

# 새로운 엑셀 파일로 저장

output_file_path = f'../완성예시/{file_name}_output.xlsx'
new_df.to_excel(output_file_path, index=False, columns=['사용여부', 'channel', 'category', 'title', 'detail_content', '종류', 'registered_date', 'site_name', 'board_name'])

print(f"New Excel file saved to {output_file_path}")
