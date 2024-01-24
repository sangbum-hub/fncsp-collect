# 2. 네이버 카페
# 유형: WEB CRAWLING
# 데이터타입 칼럼명: naver_cafe
import datetime
from re import search

# 상위 폴더 위치
# dir = path.abspath('../../..')
# dir = path.abspath('./')
# sys.path.append(dir)

from libraries import *
from .Funcs import funcs, check_data_pattern, write_log, timeout, retry_function

DataType = "naver_cafe"
SearchID = "autoSystem"

FlagPrintData = True  # True: 저장할 최종 데이터 출력
FlagSaveData = True  # True: 엘라스틱에 데이터 저장
MAX_RETRY = 5
TIMEOUT = 30
PAGE_DELAY_TIME = 2

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

def _get_head(biz_no,SearchDate,SearchID):
    return {
        "BusinessNum": biz_no,
        "DataType": DataType,
        "SearchDate": SearchDate,
        "SearchID": SearchID
    }

def naverCafeUrl(pageNo, keyword, SearchDate, date_date=None):
    base_url = "https://cafe.naver.com/ca-fe/home/search/articles?"
    if date_date is None:
        queryParams = urlencode({
            quote_plus('q'): unquote(keyword),
            quote_plus('p'): pageNo
        }, encoding='utf-8')
    else:
        queryParams = urlencode({
            quote_plus('q'): unquote(keyword),
            quote_plus('pr'):7,
            quote_plus('p'): pageNo,
            quote_plus('ps'):unquote(str(date_date)),
            quote_plus('pe'):unquote(SearchDate[:10].replace('-', '')),
        }, encoding='utf-8')
    url = base_url + queryParams
    return url

def get_current_date(date_str:str,SearchDate):
    dt = datetime.timedelta()
    if '초' in date_str:
        lst = re.findall('(\d?\d)초', date_str)
        if len(lst)==0:
            raise "there is no time pattern"
        dt = datetime.timedelta(seconds=int(lst[0]))
    elif '분' in date_str:
        lst = re.findall('(\d?\d)분', date_str)
        if len(lst)==0:
            raise "there is no time pattern"
        dt = datetime.timedelta(minutes=int(lst[0]))
    elif '시간' in date_str:
        lst = re.findall('(\d?\d)시간', date_str)
        if len(lst)==0:
            raise "there is no time pattern"
        dt = datetime.timedelta(hours=int(lst[0]))
    else:
        return date_str.replace('.','-')
    
    return (datetime.datetime.strptime(SearchDate, '%Y-%m-%d %H:%M:%S.%f') - dt).strftime("%Y-%m-%d")

def explore_pages(company_name, ceo_name, webDriver, start_date=None, console_print=False,SearchDate=None):
    """
    # date_date가 없으면 초기 적재
    if date_date is None:


    # date_date가 있으면 업데이트 적재
    else:


    ########################################################
    """

    reuslt_lst = []

    SearchDate = SearchDate if SearchDate else datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

    # 검색어 리스트 생성 [기업명, 기업명&제품명, 기업명&서비스명, ...]
    srch_keyword_list = [company_name]

    for keyword in srch_keyword_list:
        SrchKeyword = f'"{keyword}"'

        # 포스팅 개수 계산
        url = naverCafeUrl(pageNo=1, keyword=SrchKeyword, SearchDate=SearchDate) if start_date is None else naverCafeUrl(pageNo=1, keyword=SrchKeyword, date_date=start_date, SearchDate=SearchDate)
        webDriver.get(url)
        time.sleep(1)

        tot_num_post = webDriver.find_elements_by_class_name('total_count')[0].text.replace("건", "")
        while True:
            if ',' not in tot_num_post:
                break
            tot_num_post = tot_num_post.replace(',', '')
        tot_num_post = int(tot_num_post)

        tot_num_page = math.ceil(tot_num_post / 12)
        finger_print = set()
        for pageNo in range(1, tot_num_page + 1):
            if pageNo>100:
                break
            url = naverCafeUrl(pageNo=pageNo, keyword=SrchKeyword, SearchDate=SearchDate) if start_date is None else naverCafeUrl(pageNo=pageNo, keyword=SrchKeyword, date_date=start_date, SearchDate=SearchDate)
            webDriver.get(url)
            time.sleep(1)

            css_selector = 'p.cafe_info > a > span'
            cafe_name = [i.text for i in webDriver.find_elements_by_css_selector(css_selector)]
            css_selector = 'p.cafe_info > span'
            cafe_date = [i.text for i in webDriver.find_elements_by_css_selector(css_selector)]
            
            css_selector = 'div.detail_area > a'
            cafe_title = [i.text for i in webDriver.find_elements_by_css_selector(css_selector)]
            # 직전 페이지와 같은 타이틀의 게시글이 반복될 경우 루프 종료
            checker_lst = list(map(lambda o: o in finger_print, cafe_title))
            if sum(checker_lst)>=5:
                break
            """
            checker = True
            for x in checker_lst:
                checker = checker and x
            if checker:
                break
            """
            finger_print = set(cafe_title)

            css_selector = 'div.detail_area > p.item_content'
            cafe_content = [i.text for i in webDriver.find_elements_by_css_selector(css_selector)]
            xpath = '//*[@id="mainContainer"]/div/div[1]/div[3]/div/div[3]/ul/li/div/div/div/a'
            cafe_id = []
            for i in webDriver.find_elements_by_xpath(xpath):
                href = i.get_property('href')
                # reg = search('https://cafe\.naver\.com/(\w+)/(\d+)\?', href)
                reg = search('https://cafe\.naver\.com/([^\?]+)', href)
                # cafe_id.append('/'.join(reg.groups()))
                cafe_id.append(reg.groups()[0])

            for i in range(len(cafe_name)):
                naver_cafe_data = {
                        'CafeName':cafe_name[i],
                        'CafeDate':get_current_date(cafe_date[i], SearchDate),
                        'CafeTitle':cafe_title[i],
                        'CafeContent':cafe_content[i],
                    }
                #flag = funcs.save_data_to_es(index=IndexName, id=cafe_id[i], data=naver_cafe_data)
                reuslt_lst.append((cafe_id[i], naver_cafe_data))

    return reuslt_lst

def get_result(biz_no, company_name, ceo_name, webDriver, console_print=False, err_msg=None, start_date=None,SearchDate=None):
    # SearchDate = SearchDate if SearchDate else datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    SearchDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    head = _get_head(biz_no, SearchDate, SearchID)
    # Data_lst = explore_pages(company_name, ceo_name, webDriver, console_print, err_msg, start_date=start_date)

    Data_lst = explore_pages(
        company_name=company_name,
        ceo_name=ceo_name,
        webDriver=webDriver,
        start_date=start_date,
        console_print=console_print,
        SearchDate=SearchDate
        )

    Data_lst = [None] if Data_lst is None else Data_lst
    results = []
    for Data in Data_lst:
        result = head.copy()
        result['Data'] = Data[1]
        results.append(result)
    return results

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
    explore_pages("정원건설","김재원",webDriver)