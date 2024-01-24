# 5. 잡플래닛 프리미엄 리뷰
# 유형: WEB CRAWLING
# 데이터타입 칼럼명: jobplanet_statistic
import datetime

# 상위 폴더 위치
# dir = path.abspath('../../..')
# dir = path.abspath('./')
# sys.path.append(dir)

from libraries import *
from .Funcs import funcs, check_data_pattern, write_log, timeout, retry_function

DataType = "jobplanet_premium"
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


def explore_pages(company_name, ceo_name, biz_no, webDriver, stop_date=None, console_print=False, err_msg=None):
    # 로그인
    try:
        _login(webDriver,JOBPLANET_ID,JOBPLANET_PW)
    except:
        msg = "jobplanet, login 과정 오류"
        if console_print:
            print(msg)
        if err_msg is not None:
            err_msg[0] = msg
        return None

    Data_list = None
    try:
        Data_list = explore_pages_no_login(company_name, ceo_name, biz_no, webDriver, stop_date, console_print, err_msg)
    except:
        msg = "jobplanet_premium, 화면 탐색중 오류"
        if console_print:
            print(msg)
        if err_msg is not None:
            err_msg[0] = msg
        return None

    return Data_list


def explore_pages_no_login(company_name, ceo_name, biz_no, webDriver, stop_date=None, console_print=False, err_msg=None):
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

    # 프리미엄 리뷰 페이지 이동
    webDriver.get(f"https://www.jobplanet.co.kr/companies/{cpn_id}/premium_reviews/{funcs.re_type(company_name)}")
    time.sleep(PAGE_DELAY_TIME)

    tmp = webDriver.find_element_by_css_selector('li.viewPremiumReviews > a > span.num')
    premium_review_cnt = int(tmp.text)

    if premium_review_cnt < 1:
        return None

    Datas = []
    for i in range(6):
        # css_selector = f"#premium_detail_box > div.unit_menu_box.bottom > ul > li:nth-child('{i+1}') > button"
        css_selector = f"#premium_detail_box > div.unit_menu_box.bottom > ul > li:nth-child({i+1}) > button"
        btn_next = WebDriverWait(webDriver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
            )
        btn_next.click()
        time.sleep(PAGE_DELAY_TIME)

        # 등록된 리뷰가 없을 때 다음 loop 돌기
        try:
            css_selector = '#premium_detail_box > div.premium_detail_wrap > div.unit_chart_review_box > div > div.review_no_data'
            webDriver.find_element_by_css_selector(css_selector)
            continue
        except:
            pass

        # 현재 페이지 html에 담기
        html = webDriver.find_element_by_tag_name('html')

        # 페이지 다운 스크롤
        for _ in range(8):
            html.send_keys(Keys.PAGE_DOWN)
            time.sleep(0.5)
        html.send_keys(Keys.END)
        time.sleep(PAGE_DELAY_TIME)
        push_btn(webDriver, i)

        tag_name = 'div.unit_chart_review_item'
        pre_review_list = webDriver.find_elements_by_tag_name(tag_name)

        # 각 리뷰 받아오기
        for r in pre_review_list:
            Datas.append(crawl_single_review(r, biz_no))
            

    return Datas

def crawl_single_review(r, biz_no):
    Data = {}
    Data['Category'] = None
    Data['PreReviewType'] = None
    Data['Question'] = None
    Data['Response'] = None
    Category = r.find_element_by_tag_name('div.category_tag').text
    Question = r.find_element_by_tag_name('div.questions_text').text
    try:
        Answer_lst = [i.text.split('\n') for i in r.find_elements_by_tag_name(
            'div.unit_premium_chart_wrap > div.unit_statistic_chart > ul')][0]
        Answers = Answer_lst[::2]
        Percentages = list(map(float, Answer_lst[1::2]))
        Response = []
        for num in range(len(Answers)):
            answer_dic = {}
            answer_dic["Answer"] = Answers[num]
            answer_dic["Percentage"] = Percentages[num]
            Response.append(answer_dic)

        Data["Category"] = Category
        Data["PreReviewType"] = "그래프형"
        Data["Question"] = Question
        Data["Response"] = Response

    except Exception as e:
        # print("Error:", e)
        Answers = [i.text for i in
                r.find_elements_by_tag_name('div.answer_text > span')]
        Response = []
        # print("Answers :", Answers)
        # print("###1")
        last_premium_data = get_last_premium_data(Question, biz_no)
        for num in range(len(Answers)):
            answer_dic = {}
            # if Answers[num] == last_premium_data(Answers[num], biz_no):
            #     break
            try:
                if Answers[num] == last_premium_data:
                    break
            except:
                pass
            answer_dic["Answer"] = Answers[num]
            answer_dic["Percentage"] = None
            Response.append(answer_dic)
        # print("###2")
        Data["Category"] = Category
        Data["PreReviewType"] = "질의응답형"
        Data["Question"] = Question
        Data["Response"] = Response
        # print("###3")

        # print("###Data :", Data)
    return Data

def get_last_premium_data(Question, biz_num, DataType="jobplanet_premium"):
    body={
        "query": {
            "bool":{
            "must":[
                {"match": {"DataType": DataType}},
                {"match": {"BusinessNum": biz_num}},
                {"match": {"Data.Question" :Question}}
                ]
            }
        }
    }

    results = funcs.get_data_from_es("source_data", body)
    try:
        last_stored_answer = results[0]["_source"]['Data']['Response'][0]['Answer']
    except:
        last_stored_answer = None
    return last_stored_answer

def push_btn(webDriver, num):
    if num == 0:
        try:
            while True:
                css_selector = "#premium_detail_box > div.premium_detail_wrap > div.unit_chart_review_box > div > div.unit_chart_review_item.open.id_620 > div > div.review_status_box.success.long > div > div > div.btn_more_box > button"
                btn = webDriver.find_element_by_css_selector(css_selector)
                btn.click()
                time.sleep(1)
        except:
            pass
        try:
            while True:
                css_selector = "#premium_detail_box > div.premium_detail_wrap > div.unit_chart_review_box > div > div.unit_chart_review_item.open.id_623 > div > div.review_status_box.success.long > div > div > div.btn_more_box > button"
                btn = webDriver.find_element_by_css_selector(css_selector)
                btn.click()
                time.sleep(1)
        except:
            pass
        try:
            while True:
                css_selector = "#premium_detail_box > div.premium_detail_wrap > div.unit_chart_review_box > div > div.unit_chart_review_item.open.id_628 > div > div.review_status_box.success.long > div > div > div.btn_more_box > button"
                btn = webDriver.find_element_by_css_selector(css_selector)
                btn.click()
                time.sleep(1)
        except:
            pass
    elif num == 1:
        try:
            while True:
                css_selector = "#premium_detail_box > div.premium_detail_wrap > div.unit_chart_review_box > div > div.unit_chart_review_item.open.id_633 > div > div.review_status_box.success.long > div > div > div.btn_more_box > button"
                btn = webDriver.find_element_by_css_selector(css_selector)
                btn.click()
                time.sleep(1)
        except:
            pass
        try:
            while True:
                css_selector = "#premium_detail_box > div.premium_detail_wrap > div.unit_chart_review_box > div > div.unit_chart_review_item.open.id_638 > div > div.review_status_box.success.long > div > div > div.btn_more_box > button"
                btn = webDriver.find_element_by_css_selector(css_selector)
                btn.click()
                time.sleep(1)
        except:
            pass
    elif num == 2:
        try:
            while True:
                css_selector = "#premium_detail_box > div.premium_detail_wrap > div.unit_chart_review_box > div > div.unit_chart_review_item.open.id_640 > div > div.review_status_box.success.long > div > div > div.btn_more_box > button"
                btn = webDriver.find_element_by_css_selector(css_selector)
                btn.click()
                time.sleep(1)
        except:
            pass
        try:
            while True:
                css_selector = "#premium_detail_box > div.premium_detail_wrap > div.unit_chart_review_box > div > div.unit_chart_review_item.open.id_643 > div > div.review_status_box.success.long > div > div > div.btn_more_box > button"
                btn = webDriver.find_element_by_css_selector(css_selector)
                btn.click()
                time.sleep(1)
        except:
            pass
        try:
            while True:
                css_selector = "#premium_detail_box > div.premium_detail_wrap > div.unit_chart_review_box > div > div.unit_chart_review_item.open.id_648 > div > div.review_status_box.success.long > div > div > div.btn_more_box > button"
                btn = webDriver.find_element_by_css_selector(css_selector)
                btn.click()
                time.sleep(1)
        except:
            pass
    elif num == 3:
        try:
            while True:
                css_selector = "#premium_detail_box > div.premium_detail_wrap > div.unit_chart_review_box > div > div.unit_chart_review_item.open.id_657 > div > div.review_status_box.success.long > div > div > div.btn_more_box > button"
                btn = webDriver.find_element_by_css_selector(css_selector)
                btn.click()
                time.sleep(1)
        except:
            pass
        try:
            while True:
                css_selector = "#premium_detail_box > div.premium_detail_wrap > div.unit_chart_review_box > div > div.unit_chart_review_item.open.id_658 > div > div.review_status_box.success.long > div > div > div.btn_more_box > button"
                btn = webDriver.find_element_by_css_selector(css_selector)
                btn.click()
                time.sleep(1)
        except:
            pass
    elif num == 4:
        try:
            while True:
                css_selector = "#premium_detail_box > div.premium_detail_wrap > div.unit_chart_review_box > div > div.unit_chart_review_item.open.id_666 > div > div.review_status_box.success.long > div > div > div.btn_more_box > button"
                btn = webDriver.find_element_by_css_selector(css_selector)
                btn.click()
                time.sleep(1)
        except:
            pass
        try:
            while True:
                css_selector = "#premium_detail_box > div.premium_detail_wrap > div.unit_chart_review_box > div > div.unit_chart_review_item.open.id_660 > div > div.review_status_box.success.long > div > div > div.btn_more_box > button"
                btn = webDriver.find_element_by_css_selector(css_selector)
                btn.click()
                time.sleep(1)
        except:
            pass
    elif num == 5:
        try:
            while True:
                css_selector = "#premium_detail_box > div.premium_detail_wrap > div.unit_chart_review_box > div > div.unit_chart_review_item.open.id_668 > div > div.review_status_box.success.long > div > div > div.btn_more_box > button"
                btn = webDriver.find_element_by_css_selector(css_selector)
                btn.click()
                time.sleep(1)
        except:
            pass

def get_result(biz_no, company_name, ceo_name, webDriver, console_print=False, err_msg=None):
    results = []
    SearchDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    head = _get_head(biz_no, SearchDate, SearchID)
    Datas = explore_pages(
        company_name = company_name,
        ceo_name = ceo_name,
        webDriver = webDriver,
        console_print = console_print,
        err_msg = err_msg,
        biz_no = biz_no
        )
    # Datas = [None] if Datas is None else Datas
    for x in Datas:
        result = head.copy()
        result['Data'] = x
        results.append(result)
    return results

def get_result_no_login(biz_no, company_name, ceo_name, webDriver, start_date=None, console_print=False, err_msg=None):
    results = []
    SearchDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    head = _get_head(biz_no, SearchDate, SearchID)
    Datas = explore_pages_no_login(
        company_name = company_name,
        ceo_name = ceo_name,
        webDriver = webDriver,
        console_print = console_print,
        err_msg = err_msg,
        biz_no = biz_no
        )
    Datas = [None] if Datas is None else Datas
    for x in Datas:
        result = head.copy()
        result['Data'] = x
        results.append(result)
    return results

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

        Data_lst = explore_pages(keyword[0],keyword[1],keyword[2],webDriver)
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

    Data = explore_pages("카카오","홍은택","93880",webDriver)