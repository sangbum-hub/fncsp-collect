# 2. 네이버 블로그
# 유형: WEB CRAWLING
# 데이터타입 칼럼명: jobplanet_statistic
import json
import datetime
from pickle import FALSE
import sys
from os import path
from xmlrpc.client import DateTime

# 상위 폴더 위치
# dir = path.abspath('../../..')
# dir = path.abspath('./')
# sys.path.append(dir)

from libraries import *
# from Funcs import funcs, check_data_pattern, write_log, timeout, retry_function
from .Funcs import funcs

DataType = "naver_blog"
SearchID = "autoSystem"

FlagPrintData = True  # True: 저장할 최종 데이터 출력
FlagSaveData = True  # True: 엘라스틱에 데이터 저장
MAX_RETRY = 5
TIMEOUT = 30
PAGE_DELAY_TIME = 2

"""
@retry_function.retry(MAX_RETRY, str)
def result_save_function(data):
    flag1 = funcs.save_data_to_es(index=index_name, data=data)
    if flag1 == "Success":
        return flag1
    elif flag1 == "Fail":
        raise Exception("save_data_to_es_error!!")


@retry_function.retry(MAX_RETRY, str)
def update_searchdate_function(biz_no, flag1):
    if flag1 == "Success":
        today = datetime.datetime.today().strftime("%Y-%m-%d")
        biz_no_list = [(biz_no, today)]
        flag2 = funcs.update_searchDate_mysql(DataType, biz_no_list)
        if flag1 == flag2:
            return "Success"
        elif flag1 != flag2:
            raise Exception("update_searchDate_mysql_error!!")
    else:
        return "Fail"
"""

def _get_head(biz_no,SearchDate,SearchID):
    return {
        "BusinessNum": biz_no,
        "DataType": DataType,
        "SearchDate": SearchDate,
        "SearchID": SearchID
    }

# URL Query 생성
def _naverBlogUrl(pageNo, keyword, date_date=None, SearchDate=None):
    base_url = "https://section.blog.naver.com/Search/Post.nhn?"
    if date_date is None:
        queryParams = urlencode({
            quote_plus('pageNo'): pageNo,
            quote_plus('rangeType'): 'ALL',
            quote_plus('orderBy'): 'sim',
            quote_plus('keyword'): unquote(keyword)
        }, encoding='utf-8')
    else:
        queryParams = urlencode({
            quote_plus('pageNo'): pageNo,
            quote_plus('rangeType'): 'ALL',
            quote_plus('orderBy'): 'sim',
            quote_plus('keyword'): unquote(keyword),
            quote_plus('startDate'): unquote(date_date),
            quote_plus('endDate'): unquote(SearchDate[:10]),
        }, encoding='utf-8')

    url = base_url + queryParams
    return url

# 페이지에 기록된 시간 표시를 표준시간 (연,월,일) 표시로 변경
# 예) "1시간전" → [현재시간] - 1시간 → 2022-07-05
def calc_date(date_str:str, SearchDate):
    # 정규표현식으로 문장 내 숫자 존재 확인
    matched_lst = re.findall("\d+", date_str)
    if len(matched_lst)==3:
        return f"{matched_lst[0]}-{matched_lst[1].zfill(2)}-{matched_lst[2].zfill(2)}"
    
    delta = None
    delta_int = int(re.findall("\d+", date_str)[0])
    if "시간" in date_str:
        delta = datetime.timedelta(hours=delta_int)
    elif "분" in date_str:
        delta = datetime.timedelta(minutes=delta_int)
    elif "초" in date_str:
        delta = datetime.timedelta(seconds=delta_int)
    
    result = datetime.datetime.strptime(SearchDate, '%Y.%m.%d') - delta

    return result.strftime("%Y-%m-%d")

def explore_pages(company_name, ceo_name, webDriver, start_date=None, console_print=False, search_date=None):
    """
    # date_date가 없으면 초기 적재 → 날짜 기간 조건 없이 검색


    # date_date가 있으면 업데이트 적재 → 특정 기간 조건 검색
    """
    results = []

    search_date = search_date if search_date else datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

    keyword = funcs.re_type(company_name)

    SrchKeyword = f'"{keyword}"'

    # 포스팅 개수 계산
    url = _naverBlogUrl(pageNo=1, keyword=SrchKeyword) if start_date is None else _naverBlogUrl(pageNo=1, keyword=SrchKeyword, date_date=start_date, SearchDate=search_date)
    webDriver.get(url) # 셀레니움 HTTP/GET method 실행
    time.sleep(PAGE_DELAY_TIME) # 1초 대기

    # "css selector" 로 검색 결과 "건" 수 수집
    css_selector = '#content > section > div.category_search > div.search_information > span > span > em'
    tot_num_post = webDriver.find_element_by_css_selector(css_selector).text.replace("건", "")
    while True:
        if ',' not in tot_num_post:
            break
        tot_num_post = tot_num_post.replace(',', '')
    tot_num_post = int(tot_num_post)

    # 데이터가 없는 경우
    if tot_num_post < 1:
        return None
    else:
        # 페이지 당 7개씩 존재 → "전체 건 수"/7 = 검색결과 페이지 수
        tot_num_page = math.ceil(tot_num_post / 7)
        finger_print = set() # Hash Set 을 사용하여 직전 루프에서 반복 수집되는 블로그 방지
        for pageNo in range(1, tot_num_page + 1):
            # if pageNo > 300:
            if pageNo > 10:
                break
            
            url = _naverBlogUrl(pageNo=pageNo, keyword=SrchKeyword) if start_date is None else _naverBlogUrl(pageNo=pageNo, keyword=SrchKeyword, date_date=start_date, SearchDate=search_date)
            webDriver.get(url)
            time.sleep(PAGE_DELAY_TIME)

            css_selector = 'div.info_post > div.writer_info > span.name_blog'
            blog_name = [i.text for i in webDriver.find_elements_by_css_selector(css_selector)] # 블로그 이름 수집
            css_selector = 'div.info_post > div.writer_info > a > em.name_author'
            author_name = [i.text for i in webDriver.find_elements_by_css_selector(css_selector)] # 블로그 저자 이름 수집
            css_selector = 'div.info_post > div.writer_info > span.date'
            post_date = [i.text for i in webDriver.find_elements_by_css_selector(css_selector)] # 포스트 날짜 수집
            css_selector = 'div.desc > a.desc_inner > strong > span.title'
            post_title = [i.text for i in webDriver.find_elements_by_css_selector(css_selector)] # 포스트 이름 수집
            
            # 직전 페이지와 같은 타이틀의 게시글이 반복될 경우 루프 종료
            checker_lst = list(map(lambda o: o in finger_print, post_title))
            if sum(checker_lst)>=3:
                break
            """
            checker = True
            for x in checker_lst:
                checker = checker and x
            if checker:
                break
            """
            finger_print = set(post_title)
            
            doc_id = [i.get_property('href').split('/')[-1] for i in webDriver.find_elements_by_xpath('//*[@id="content"]/section/div[2]/div/div/div[1]/div[1]/a[1]')] # 문서 ID 경로
            upload_id = [f'{i}/{j}' for i,j in zip(author_name, doc_id)]

            post_content = []
            for i in range(1, len(blog_name) + 1):
                # 블로그 내용 수집
                css_selector = '#content > section > div.area_list_search > div:nth-child(' + str(
                    i) + ') > div > div.info_post > div.desc > a.text'
                try:
                    post = webDriver.find_element_by_css_selector(css_selector).text
                except Exception as e:
                    post = ""
                post_content.append(post)

            # JSON 형태로 저장
            for idx in range(len(blog_name)):
                naver_blog_data = {
                        'BlogName':blog_name[idx],
                        'AuthorName':author_name[idx],
                        'PostDate':calc_date(post_date[idx], search_date),
                        'PostTitle':post_title[idx],
                        'PostContent':post_content[idx],
                    }
                results.append((upload_id[idx], naver_blog_data))
    
    return results

def get_result(biz_no, company_name, ceo_name, webDriver, start_date=None, console_print=False, err_msg=None,search_date=None):
    SearchDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    head = _get_head(biz_no, SearchDate, SearchID)
    # Data_lst = explore_pages(company_name, ceo_name, webDriver, start_date, console_print, start_date=start_date)

    Data_lst = explore_pages(
        company_name = company_name,
        ceo_name = ceo_name,
        webDriver = webDriver,
        start_date = start_date,
        console_print = console_print,
        search_date=search_date
    )

    Data_lst = [None] if Data_lst is None else Data_lst
    results = []
    for Data in Data_lst:
        result = head.copy()
        result['Data'] = Data[1]
        results.append(result)
    return results

"""
def main(biz_no, keyword:list):
    try:
        webDriver = webdriver.Chrome(
            options=WEBDRIVER_OPTIONS, executable_path=WEBDRIVER_PATH
        )

        head = _get_head(
            biz_no,
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            SearchID
            )

        Data = explore_pages(keyword[0],keyword[1],webDriver)
    except Exception as e:
        write_log.write_log(BIZ_NO=biz_no, DATA_TYPE=DataType, ERR_LOG=e)
    finally:
        if Data is None:
            head["Data"] = None
            flag1 = result_save_function(head)
            update_searchdate_function(biz_no, flag1)
        else:
            for data in Data:
                result = head.copy()
                result['Data'] = data
                flag1 = result_save_function(result)
                update_searchdate_function(biz_no, flag1)


if __name__=="__main__":
    webDriver = webdriver.Chrome(
        options=WEBDRIVER_OPTIONS, executable_path=WEBDRIVER_PATH
    )
    explore_pages("(주)레몬엠","강기단",webDriver)
"""