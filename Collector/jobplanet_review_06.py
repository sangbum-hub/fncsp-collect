# 6. 잡플래닛 리뷰
# 유형: WEB CRAWLING
# 데이터타입 칼럼명: jobplanet_statistic
import datetime

# 상위 폴더 위치
# dir = path.abspath('../../..')
# dir = path.abspath('./')
# sys.path.append(dir)

from libraries import *
from .Funcs import funcs, check_data_pattern, write_log, timeout, retry_function

DataType = "jobplanet_review"
SearchID = "autoSystem"

FlagPrintData = True  # True: 저장할 최종 데이터 출력
FlagSaveData = True  # True: 엘라스틱에 데이터 저장
MAX_RETRY = 5
TIMEOUT = 30
PAGE_DELAY_TIME = 2

JOBPLANET_ID = "bax@bax.co.kr"
JOBPLANET_PW = "bax!5380058"

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

# jobplanet, login
def _login(webDriver, id, pw):
    webDriver.get("https://www.jobplanet.co.kr/users/sign_in")
    time.sleep(PAGE_DELAY_TIME)

    input_id = webDriver.find_element_by_css_selector("#user_email")
    input_pw = webDriver.find_element_by_css_selector("#user_password")

    input_id.send_keys(id)
    input_pw.send_keys(pw)

    login_btn = webDriver.find_element_by_css_selector("#signInSignInCon > div.signInsignIn_wrap > div > section.section_email.er_r > fieldset > button")

    login_btn.click()
    time.sleep(PAGE_DELAY_TIME)

def _get_head(biz_no,SearchDate,SearchID):
    return {
        "BusinessNum": biz_no,
        "DataType": DataType,
        "SearchDate": SearchDate,
        "SearchID": SearchID
    }

def _find_matched_company(webDriver,company_name, ceo_name, console_print, err_msg):
    btn = webDriver.find_element_by_css_selector("#mainContents > div:nth-child(1) > div > div.result_hd > div > a")
    btn.click()
    time.sleep(PAGE_DELAY_TIME)

    elm = webDriver.find_element_by_css_selector("#listCompaniesTitle > span")
    content_cnt = int(elm.text)
    page_num = content_cnt//10
    page_num += 1 if (content_cnt%10) > 0 else 0

    for i in range(page_num):
        elms = webDriver.find_elements_by_css_selector("#listCompanies > div > div.section_group > section")
        cmp_num = len(elms)
        for j in range(cmp_num):
            webDriver.get(f"https://www.jobplanet.co.kr/search/companies/{funcs.re_type(company_name)}?page={i+1}")
            time.sleep(PAGE_DELAY_TIME)

            btn = webDriver.find_element_by_css_selector(f"#listCompanies > div > div.section_group > section:nth-child({j+1}) > div > div > dl.content_col2_3.cominfo > dt > a")
            btn.click()
            time.sleep(PAGE_DELAY_TIME)

            cpn_id = re.findall("/(\d+)/",webDriver.current_url)[0]

            # 팝업화면 표시 감지 및 닫기
            try:
                popup_window = webDriver.find_element_by_css_selector("div.layer_popup_box.layer_popup_box_on > div.layer_popup_bg")
                btn = webDriver.find_element_by_css_selector("div.premium_modal_header > button")
                btn.click()
            except:
                pass

            # 뉴스룸 페이지 이동
            webDriver.get(f"https://www.jobplanet.co.kr/companies/{cpn_id}/landing/{funcs.re_type(company_name)}")
            time.sleep(PAGE_DELAY_TIME)

            # CEO 이름 확인
            ceoName = None
            try:
                try:
                    webDriver.find_element_by_css_selector(
                        "#contents_wrap > div.jply_layout > div:nth-child(1) > div > div > div.basic_info_sec > div > div > button.btn_info_more.ic_arrow_light_down"
                        ).click()
                except:
                    pass
                elm = webDriver.find_element_by_css_selector("div.basic_info_sec > div > ul.basic_info_more > li:nth-child(1) > dl > dd")
                ceoName = elm.text
            except:
                msg = "Jobplanet dosen't have CEO Name"
                if console_print:
                    print(msg)
                if err_msg is not None:
                    err_msg[0] = msg
                raise Exception("from \"_find_matched_company\"")

            if ceo_name in ceoName:
                return cpn_id
    raise Exception()


def explore_pages(company_name, ceo_name, webDriver, stop_date=None, console_print=False, err_msg=None):
    # 로그인
    try:
        _login(webDriver,JOBPLANET_ID,JOBPLANET_PW)
    except:
        msg = "jobplanet, login 과정 오류"
        if console_print:
            print(msg)
        if err_msg is not None:
            err_msg[0] = msg

    Data_list = None
    try:
        Data_list = explore_pages_no_login(company_name, ceo_name, webDriver, stop_date, console_print, err_msg)
    except:
        msg = "jobplanet_review, 화면 탐색중 오류"
        if console_print:
            print(msg)
        if err_msg is not None:
            err_msg[0] = msg

    return Data_list    

def explore_pages_no_login(company_name, ceo_name, webDriver, stop_date=None, console_print=False, err_msg=None):
    # 기업이름으로 검색
    url = 'https://www.jobplanet.co.kr/search?category=&query=' + funcs.re_type(company_name)
    webDriver.get(url)
    time.sleep(PAGE_DELAY_TIME)

    # 팝업 닫기
    try:
        close_button = webDriver.find_element_by_css_selector("#jobRestructureModal > div > div > div.layer_popup.jply_modal_contents_ty > div > div > div.modal_bottom > div > button.jply_btn_md.ty_default_solid")
        close_button.click()
    except:
        pass

    try:
        tmp = webDriver.find_element_by_css_selector("div.no_result_hd")
        msg = f"Jobplanet dosen't have \"{company_name}\" information"
        if console_print:
            print(msg)
        if err_msg is not None:
            err_msg[0] = msg
        return None
    except:
        pass

    try:
        cpn_id = _find_matched_company(webDriver,company_name,ceo_name,console_print,err_msg)
    except Exception as e:
        msg = "CEO name dosen't match"
        if console_print:
            print(msg)
        if err_msg is not None:
            err_msg[0] = msg
        return None

    # 리뷰 페이지 이동
    webDriver.get(f"https://www.jobplanet.co.kr/companies/{cpn_id}/reviews/{funcs.re_type(company_name)}")
    time.sleep(PAGE_DELAY_TIME)

    # 리뷰 개수 확인
    elm = webDriver.find_element_by_css_selector("#viewReviewsTitle > span")
    review_cnt = int(elm.text.replace(",",""))
    # 리뷰가 존재 하지 않을 경우 종료
    if review_cnt < 1:
        msg = "No review"
        if console_print:
            print(msg)
        if err_msg is not None:
            err_msg[0] = msg
        return None

    Data_lst = explore_personal_reviews(cpn_id, company_name, review_cnt, stop_date, webDriver)

    return Data_lst

def explore_personal_reviews(cpn_id, company_name, review_cnt, stop_date, webDriver):
    results = []

    total_page = review_cnt // 5
    total_page = total_page + 1 if total_page % 5 > 0 else total_page

    # 리뷰 페이지 반복문
    for i in range(1, total_page+1):
        webDriver.get(f"https://www.jobplanet.co.kr/companies/{cpn_id}/reviews/{funcs.re_type(company_name)}?page={i}")
        time.sleep(PAGE_DELAY_TIME)

        elms = webDriver.find_elements_by_css_selector("section.content_ty4.video_ad_content")
        display_review_count = len(elms)

        j = 1
        # 리뷰 반복문
        while display_review_count>0:
            Data = {}
            # 작성자 기본정보
            try:
                elm = webDriver.find_element_by_css_selector(f"#viewReviewsList > div > div > div > section:nth-child({j}) > div > div")
            except:
                j+=1
                continue
            txt = elm.text
            txt = txt.replace("\n", "")
            txts = txt.split("|")

            Data["ReviewDate"] = txts.pop(-1).replace(', ', '-')

            if stop_date is not None:
                if int(stop_date) >= int(Data["ReviewDate"].replace('-','')):
                    return results

            tmp = txts.pop(-1)
            Data["EmployeeStatus"] = txts.pop(-1)
            Data["Job"] = txts.pop(-1) if txts else None

            # 평점
            elm = webDriver.find_element_by_css_selector(f"#viewReviewsList > div > div > div > section:nth-child({j}) > div > div.ctbody_col2 > dl > dd.sta_box > div > div")
            elm_attribute = elm.get_attribute("style")
            if elm_attribute == "width: 100%;":
                Data['TotalScore'] = 100.0
            elif elm_attribute == "width: 80%;":
                Data['TotalScore'] = 80.0
            elif elm_attribute == "width: 60%;":
                Data['TotalScore'] = 60.0
            elif elm_attribute == "width: 40%;":
                Data['TotalScore'] = 40.0
            else:
                Data['TotalScore'] = 20.0

            for col, k in zip(["PromotionScore","WelfareScore","BalanceScore","CultureScore","ExecutiveScore"], range(3, 12, 2)):
                elm = webDriver.find_element_by_css_selector(f"#viewReviewsList > div > div > div > section:nth-child({j}) > div > div.ctbody_col2 > dl > dd:nth-child({k}) > div > div")
                elm_attribute = elm.get_attribute("style")
                if elm_attribute == "width: 100%;":
                    Data[col] = 100.0
                elif elm_attribute == "width: 80%;":
                    Data[col] = 80.0
                elif elm_attribute == "width: 60%;":
                    Data[col] = 60.0
                elif elm_attribute == "width: 40%;":
                    Data[col] = 40.0
                else:
                    Data[col] = 20.0

            # 리뷰
            col_n_selector = [
                ("ReviewTitle",f"#viewReviewsList > div > div > div > section:nth-child({j}) > div > div.ctbody_col2 > div > div.us_label_wrap > h2"),
                ("Advantage",f"#viewReviewsList > div > div > div > section:nth-child({j}) > div > div.ctbody_col2 > div > dl > dd:nth-child(2) > span"),
                ("Disadvantage",f"#viewReviewsList > div > div > div > section:nth-child({j}) > div > div.ctbody_col2 > div > dl > dd:nth-child(4) > span"),
                ("ForExecutive",f"#viewReviewsList > div > div > div > section:nth-child({j}) > div > div.ctbody_col2 > div > dl > dd:nth-child(6) > span"),
            ]

            for col, selector in col_n_selector:
                elm = webDriver.find_element_by_css_selector(selector)
                Data[col] = elm.text

            try:
                col_n_selector = [
                    ("GrowthYN", f"#viewReviewsList > div > div > div > section:nth-child({j}) > div > div.ctbody_col2 > div > p:nth-child(3) > strong"),
                    ("RecommendYN", f"#viewReviewsList > div > div > div > section:nth-child({j}) > div > div.ctbody_col2 > div > p.txt.recommend.etc_box"),
                ]
                for col, selector in col_n_selector:
                    elm = webDriver.find_element_by_css_selector(selector)
                    Data[col] = elm.text
            except:
                elm = webDriver.find_element_by_css_selector(f"#viewReviewsList > div > div > div > section:nth-child({j}) > div > div.ctbody_col2 > div > p.txt.recommend.etc_box")
                Data["RecommendYN"] = elm.text
                Data["GrowthYN"] = None

            results.append(Data)
            display_review_count -= 1
            j+=1

    return results

def get_result(biz_no, company_name, ceo_name, webDriver, console_print=False, err_msg=None):
    SearchDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    head = _get_head(biz_no, SearchDate, SearchID)
    Data = explore_pages(
        company_name = company_name,
        ceo_name = ceo_name,
        webDriver = webDriver,
        console_print = console_print,
        err_msg = err_msg
        )
    head["Data"] = Data
    return head

def get_result_no_login(biz_no, company_name, ceo_name, webDriver, start_date=None, console_print=False, err_msg=None):
    SearchDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    head = _get_head(biz_no, SearchDate, SearchID)
    Data = explore_pages_no_login(
        company_name = company_name,
        ceo_name = ceo_name,
        webDriver = webDriver,
        console_print = console_print,
        err_msg = err_msg
        )
    head["Data"] = Data
    return head

def main(biz_no, keyword:list):
    try:
        webDriver = webdriver.Chrome(
            options=WEBDRIVER_OPTIONS, executable_path=WEBDRIVER_PATH
        )
        webDriver.find_element_by_css_selector()
        head = _get_head(
            biz_no,
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            SearchID
            )

        Data_lst = explore_pages(keyword[0],keyword[1],webDriver)
    except Exception as e:
        write_log.write_log(BIZ_NO=biz_no, DATA_TYPE=DataType, ERR_LOG=e)
    finally:
        for Data in Data_lst:
            head["Data"] = Data if Data else None
            flag1 = result_save_function(head)
        update_searchdate_function(biz_no, flag1)

if __name__=="__main__":
    webDriver = webdriver.Chrome(
        options=WEBDRIVER_OPTIONS, executable_path=WEBDRIVER_PATH
    )

    Data = explore_pages("카카오","홍은택",webDriver)