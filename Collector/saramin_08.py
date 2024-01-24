# 8. 사람인
# 유형: WEB CRAWLING
# 데이터타입 칼럼명: saramin
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
from .Funcs import funcs, check_data_pattern, write_log, timeout, retry_function

DataType = "saramin"
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

def explore_pages(company_name, ceo_name, webDriver, start_date=None, console_print=False, err_log=None):
    
    url = "https://www.saramin.co.kr/zf_user/"
    id = "gabriel0718"
    pwd = "white2mind!"
    Data = None

    # 1.홈페이지 열기
    webDriver.get(url)
    time.sleep(PAGE_DELAY_TIME)
    # 2.로그인하기
    try:
        css_selector = '#sri_header > div.wrap_header > div.utility > a.btn_sign.signin'
        btn_login = webDriver.find_element_by_css_selector(css_selector)
        btn_login.click()
        # 2.1 아이디, 패스워드 입력
        ## ID가 user_email인 element가 로딩될 때 까지 1초 대기
        input_id = WebDriverWait(webDriver, 5).until(EC.presence_of_element_located((By.ID, 'id')))
        input_id.send_keys(id)
        input_pwd = WebDriverWait(webDriver, 5).until(EC.presence_of_element_located((By.ID, 'password')))
        input_pwd.send_keys(pwd)
        # 2.2 로그인 실행
        css_selector = '#login_frm > div > div > div.login-form > button'
        try:
            btn_login = WebDriverWait(webDriver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, css_selector)))
            btn_login.click()
            time.sleep(PAGE_DELAY_TIME)
        except Exception as e:
            msg = f"{str(e)}\n로그인 실패"
            if console_print:
                print(msg)
            if err_log is not None:
                err_log[0] = msg
            return None
    except:
        pass

    # 기업명 검색
    company_name = funcs.re_type(company_name)
    url = "https://www.saramin.co.kr/zf_user/company-review/company-search?page=1&recruitCheck=&order=favor&searchWord={}&reviewTags=&revenue=&salary=&employees=&operatingRevenue=&startingSalary=&establishment=&netRevenue=&order=favor&service_comment=".format(company_name)
    webDriver.get(url)
    time.sleep(PAGE_DELAY_TIME)
    
    css_selector = "#company_search_form > div.list_company_search.type_review > div.total_sort > span > span"
    count = int(webDriver.find_element_by_css_selector(css_selector).text)
    

    if count <= 0:
        msg = f"saramin \"{company_name}\" 정보 없음"
        if console_print:
            print(msg)
        if err_log is not None:
            err_log[0] = msg
        return None
        
    css_selector = "#company_search_form > div.list_company_search.type_review > div.wrap_list.type_review > div"
    cpn_card = webDriver.find_elements_by_css_selector(css_selector)
    len_cpn_card = len(cpn_card) - 2
    
    company_detected = "fail" if len_cpn_card < 1 else "success"

    if company_detected == "fail":
        msg = f"saramin \"{company_name}\" 정보 없음"
        if console_print:
            print(msg)
        if err_log is not None:
            err_log[0] = msg
        return None
    
    for i in range(1, len_cpn_card+1):
        css_selector = f"#company_search_form > div.list_company_search.type_review > div.wrap_list.type_review > div:nth-child({i}) > div > div.area_info > div.text_info"
        cp_info = webDriver.find_element_by_css_selector(css_selector).text.split(" ")
        ceoName = cp_info[-1]

        css_selector = f"#company_search_form > div.list_company_search.type_review > div.wrap_list.type_review > div:nth-child({i}) > div > div.area_info > strong > a"
        cpnName = webDriver.find_element_by_css_selector(css_selector).text
        cpnName = funcs.re_type(cpnName)
        
        if ceo_name != ceoName or company_name != cpnName:
            continue

        company_detected = "success"
        cpnUrl = webDriver.find_element_by_css_selector(css_selector)
        cpnUrl = cpnUrl.get_attribute("href")
        webDriver.get(cpnUrl)
        time.sleep(PAGE_DELAY_TIME)

        css_selector = "#content > div.wrap_review_detail > div > div.wrap_info > div.area_info > div.text_info > span"
        cpn_card = webDriver.find_elements_by_css_selector(css_selector)
        len_cpn_card = len(cpn_card)

        Data = {}
        Industry = None
        CompSize = None
        NumEmp = None
        NumYears = None
        Ceo = None
        Review = None
        Data["Industry"] = Industry
        Data["CompSize"] = CompSize
        Data["NumEmp"] = NumEmp
        Data["NumYears"] = NumYears
        Data["Ceo"] = Ceo
        Data["Review"] = Review

        for j in range(1, len_cpn_card+1):
            css_selector = f"#content > div.wrap_review_detail > div > div.wrap_info > div.area_info > div.text_info > span:nth-child({j})"
            cpn_info2 = webDriver.find_element_by_css_selector(css_selector).text
            if j == 1:
                Industry = cpn_info2
            m = re.findall(r'[ㄱ-ㅎ|가-힣]+기업', cpn_info2)
            if m:
                CompSize = m[0]
            m = re.findall(r'[0-9]+명', cpn_info2)
            if m:
                NumEmp = m[0]
            m = re.findall(r'[0-9]+년', cpn_info2)
            if len(m):
                NumYears = m[0]
            if j == len_cpn_card:
                Ceo = cpn_info2
        Data["Industry"] = Industry
        Data["CompSize"] = CompSize
        Data["NumEmp"] = NumEmp
        Data["NumYears"] = NumYears
        Data["Ceo"] = Ceo

        try:
            css_selector = "#btn_list_card"
            btn = webDriver.find_element_by_css_selector(css_selector)
            btn.click()
            time.sleep(PAGE_DELAY_TIME)
        except:
            pass
        
        try:
            css_selector = "#content > div.wrap_review_detail > div > div.wrap_review > div.box_left > div.review_statistics.js-review-statistics > div.wrap_card.js-wrap-card > div"
            qst_card = webDriver.find_elements_by_css_selector(css_selector)
            len_qst_card = len(qst_card) - 2

            Review = []
            for i in range(1, len_qst_card+1):
                Response = []
                question_dic = {}
                css_selector = f"#content > div.wrap_review_detail > div > div.wrap_review > div.box_left > div.review_statistics.js-review-statistics > div.wrap_card.js-wrap-card.more > div:nth-child({i})"
                qst = webDriver.find_element_by_css_selector(css_selector).text
                qst_lst = re.split('[\n%]', qst)
                while '' in qst_lst:
                    qst_lst.remove('')
                if ' 미만' in qst_lst:
                    qst_lst.remove(' 미만')
                    qst_lst.remove(" 초과")

                    for idx in range(2, 9, 2):
                        qst_lst[idx] = qst_lst[idx] + "%"
                        if idx == 4:
                            qst_lst[idx] = qst_lst[idx] + " 미만"
                        elif idx == 8:
                            qst_lst[idx] = qst_lst[idx] + " 초과"
                    
                elif ' 이하' in qst_lst:
                    qst_lst.remove(' 이하')
                    qst_lst.remove(' 이상')
                    for idx in range(2, 9, 2):
                        qst_lst[idx] = qst_lst[idx] + "%"
                        if idx == 2:
                            qst_lst[idx] = qst_lst[idx] + " 이하"
                        elif idx == 4:
                            qst_lst[idx] = qst_lst[idx] + " 이상"

                question_dic["Question"] = qst_lst[0].strip("Q ")
                Answers = qst_lst[2::2]
                Percentages = qst_lst[1::2]
                
                for j in range(len(Answers)):
                    answer_dic = {}
                    answer_dic["Answer"] = Answers[j].strip("\n")
                    answer_dic["Percentange"] = float(Percentages[j])
                    Response.append(answer_dic)

                question_dic['Response'] = Response
                Review.append(question_dic)
                Data["Review"] = Review
            return Data
        
        except:
            return Data

    return Data

def get_result(biz_no, company_name, ceo_name, webDriver, start_date=None, console_print=False, err_msg=None):
    SearchDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    head = _get_head(biz_no, SearchDate, SearchID)
    Data = explore_pages(company_name, ceo_name, webDriver, start_date, console_print, err_msg)
    head['Data'] = Data
    return head
    """
    results = []
    Data_lst = [None] if Data_lst is None else Data_lst
    for Data in Data_lst:
        result = head.copy()
        result['Data'] = Data
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
    explore_pages("이노그리드", "김명진",webDriver)