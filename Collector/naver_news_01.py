# 1. 네이버 뉴스
# 유형: WEB CRAWLING
# 데이터타입 칼럼명: naver_news
import copy
import datetime
import json
import re
import sys
import unicodedata
from os import path

from .Funcs import funcs, write_log

# 상위 폴더 위치
# dir = path.abspath('../../..')
# dir = path.abspath('./')
# sys.path.append(dir)

today = datetime.datetime.today()
DataType = "naver_news"
FlagPrintData = True  # True: 저장할 최종 데이터 출력
FlagSaveData = True  # True: 엘라스틱에 데이터 저장
MAX_RETRY = 5
TIMEOUT = 30
# 오늘날짜부터 n일전까지의 뉴스 탐색
SearchDays = 100


# @retry_function.retry(MAX_RETRY, str)
def result_save_function(data, index_name="source_data"):
    flag1 = funcs.save_data_to_es(index=index_name, data=data)
    if flag1 == "Success":
        return flag1
    elif flag1 == "Fail":
        raise Exception("save_data_to_es_error!!")


# @retry_function.retry(MAX_RETRY, str)
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
# 기업명+대표자명 리스트
def main(biz_no, keyword: list,webdriver,WEBDRIVER_OPTIONS,WEBDRIVER_PATH):
    Data_list = []
    if keyword:
        try:
            webDriver = webdriver.Chrome(
                options=WEBDRIVER_OPTIONS, executable_path=WEBDRIVER_PATH
            )
            div_no = 1
            while True:
                flag, url_naver_news, log = get_url(keyword, div_no)
                # url 있으면
                if flag:
                    webDriver.get(url_naver_news)
                    try:
                        # 데이터가 없는 경우 혹은 마지막페이지라면
                        data_len = webDriver.find_elements_by_css_selector(
                            "div.not_found02"
                        )
                        if data_len:
                            # 마지막페이지라면 종료
                            # if div_no > 1:
                            break
                        else:
                            # else:
                            #     print("데이터없음")
                            #     빈데이터저장
                            # 뉴스 목록 가져오기
                            for news in webDriver.find_elements_by_tag_name(
                                "ul.list_news > li.bx"
                            ):
                                naver_news_yn = news.find_elements_by_tag_name(
                                    "div.info_group > a.info"
                                )[-1].text
                                # 네이버뉴스라면 저장하기
                                if str(naver_news_yn).strip() == "네이버뉴스":
                                    url = news.find_elements_by_tag_name(
                                        "div.info_group > a.info"
                                    )[-1].get_attribute("href")
                                    flag, Data, log = crawling_news(url)
                                    # 데이터를 가져왔다면
                                    if flag:
                                        final_data = {
                                            "BusinessNum": biz_no,
                                            "DataType": DataType,
                                            "SearchDate": datetime.datetime.now().strftime(
                                                "%Y-%m-%d %H:%M:%S.%f"
                                            ),
                                            "SearchID": "autoSystem",
                                            "Data": Data,
                                        }

                                        final_data = unicodedata.normalize(
                                            "NFKD",
                                            json.dumps(final_data, ensure_ascii=False),
                                        )  # 유니코드 normalize
                                        final_data = unicodedata.normalize(
                                            "NFC", final_data
                                        )  # 한글 자음모음 합치기
                                        final_data = json.loads(final_data)
                                        Data_list.append(copy.deepcopy(final_data))
                                        # 데이터 확인용
                                        if FlagPrintData:
                                            print(
                                                json.dumps(
                                                    final_data,
                                                    indent=2,
                                                    ensure_ascii=False,
                                                )
                                            )
                                    else:
                                        # 로그적재하기
                                        write_log.write_log(
                                            BIZ_NO=biz_no,
                                            DATA_TYPE=DataType,
                                            ERR_LOG=log,
                                        )
                                        break
                    except Exception as e:
                        # 로그적재하기
                        write_log.write_log(
                            BIZ_NO=biz_no, DATA_TYPE=DataType, ERR_LOG=e
                        )
                        pass
                    div_no += 10
                else:
                    # 로그적재하기
                    write_log.write_log(BIZ_NO=biz_no, DATA_TYPE=DataType, ERR_LOG=e)
            webDriver.close()
        except Exception as e:
            # 로그적재하기
            write_log.write_log(BIZ_NO=biz_no, DATA_TYPE=DataType, ERR_LOG=e)
        finally:
            if Data_list:
                if bool(Data_list) and FlagSaveData:
                    for data in Data_list:
                        flag1 = result_save_function(data)
                        update_searchdate_function(biz_no, flag1)
            else:
                empty_data = {
                    "BusinessNum": biz_no,
                    "DataType": DataType,
                    "SearchDate": datetime.datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    "SearchID": "autoSystem",
                    "Data": None,
                }
                flag1 = result_save_function(empty_data)
                update_searchdate_function(biz_no, flag1)
"""
"""
explore_pages() 기반으로 전체 수집
"""
def get_Data_list(keyword,webDriver,biz_no,console_print=False,do_write_log=False):
    Data_list = []
    for results in explore_pages(keyword,webDriver,biz_no,console_print,do_write_log):
        flag = results[0]
        result = results[1]
        if flag:
            Data_list.append(
                copy.deepcopy(result)
            )
    return Data_list

def _get_head(biz_no, SearchDate=None, SearchID=None):
    return {
                    "BusinessNum": biz_no,
                    "DataType": DataType,
                    "SearchDate": datetime.datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    "SearchID": "autoSystem",
                    "Data": None,
                }

def get_result(
    biz_no, 
    company_name, 
    ceo_name, 
    webDriver, 
    console_print=False, 
    err_msg=None,
    secondWebDriver=None,
    start_date=None,
    end_date=None
    ):
    
    head = _get_head(biz_no)
    Data_lst = explore_pages(
        keyword = [company_name, ceo_name],
        webDriver = webDriver,
        biz_no = biz_no,
        console_print = console_print,
        do_write_log = False,
        secondWebDriver = secondWebDriver,
        start_date=start_date,
        end_date=end_date,
    )
    # Data_lst = [None] if Data_lst is None else Data_lst
    if Data_lst is None:
        return []
    results = []
    for Data in Data_lst:
        result = head.copy()
        result['Data'] = Data[1]
        results.append(result)
    return results

"""
yield문으로 기사 1건당 리턴,
webDriver 생성 부분 분리
"""
def explore_pages(
    keyword,
    webDriver,
    biz_no,
    console_print=False,
    do_write_log=False,
    secondWebDriver=None,
    start_date=None,
    end_date=None,
    ):

    div_no = 1
    finger_print = []
    usable_keyword_cnt = len(list(filter(lambda o: o!="", keyword)))
    limit_news_cnt = 10
    limit_page_len = 10
    limit_crawl = usable_keyword_cnt<2
    while True:
        if end_date is None or start_date is None:
            flag, url_naver_news, log = get_url(keyword, div_no)
        else:
            flag, url_naver_news, log = get_url(keyword, div_no, start_date, end_date)
        if limit_crawl and (div_no>limit_page_len or limit_news_cnt<1):
            flag = False
        # url 있으면
        if flag:
            webDriver.get(url_naver_news)
            try:
                # 데이터가 없는 경우 혹은 마지막페이지라면
                data_len = webDriver.find_elements_by_css_selector(
                    "div.not_found02"
                )
                if data_len:
                    # 마지막페이지라면 종료
                    # if div_no > 1:
                    yield False, None
                    return
                else:
                    # else:
                    #     print("데이터없음")
                    #     빈데이터저장
                    # 뉴스 목록 가져오기
                    new_finger_print = [x.text for x in webDriver.find_elements_by_tag_name("#main_pack > section > div > div.group_news > ul > li > div > div > a")]
                    lst = list(map(lambda o: o in finger_print, new_finger_print))
                    if sum(lst)>=5:
                        break
                    finger_print = new_finger_print

                    for news in webDriver.find_elements_by_tag_name(
                        "ul.list_news > li.bx"
                    ):
                        naver_news_yn = news.find_elements_by_tag_name(
                            "div.info_group > a.info"
                        )[-1].text
                        # 네이버뉴스라면 저장하기
                        if str(naver_news_yn).strip() == "네이버뉴스":
                            url = news.find_elements_by_tag_name(
                                "div.info_group > a.info"
                            )[-1].get_attribute("href")
                            flag, Data, log = crawling_news(url,secondWebDriver)
                            # 데이터를 가져왔다면
                            if flag:

                                # 메타데이터 중복해서 넣으므로 주석 처리함
                                # final_data = {
                                #     "BusinessNum": biz_no,
                                #     "DataType": DataType,
                                #     "SearchDate": datetime.datetime.now().strftime(
                                #         "%Y-%m-%d %H:%M:%S.%f"
                                #     ),
                                #     "SearchID": "autoSystem",
                                #     "Data": Data,
                                # }
                                final_data = Data

                                final_data = unicodedata.normalize(
                                    "NFKD",
                                    json.dumps(final_data, ensure_ascii=False),
                                )  # 유니코드 normalize
                                final_data = unicodedata.normalize(
                                    "NFC", final_data
                                )  # 한글 자음모음 합치기
                                final_data = json.loads(final_data)

                                # 데이터 확인용
                                if console_print:
                                    print(
                                        json.dumps(
                                            final_data,
                                            indent=2,
                                            ensure_ascii=False,
                                        )
                                    )

                                limit_news_cnt -= 1
                                # 엘라스틱서치에 데이터 저장
                                yield True, final_data
                            else:
                                # 로그적재하기
                                if do_write_log:
                                    write_log.write_log(
                                        BIZ_NO=biz_no,
                                        DATA_TYPE=DataType,
                                        ERR_LOG=log,
                                    )
                                break
            except Exception as e:
                # 로그적재하기
                if do_write_log:
                    write_log.write_log(
                        BIZ_NO=biz_no, DATA_TYPE=DataType, ERR_LOG=e
                    )
                pass
            div_no += 10
        else:
            # 로그적재하기
            if do_write_log:
                write_log.write_log(BIZ_NO=biz_no, DATA_TYPE=DataType, ERR_LOG=e)
            break

# 네이버뉴스기사 내용 크롤링
# def crawling_news(url,webdriver,WEBDRIVER_OPTIONS,WEBDRIVER_PATH,secondWebDriver=None,):  <-- 성준님이 매개변수 5개 넣은 이유 고민 필요
def crawling_news(url,secondWebDriver=None,):
    flag, Data, log = False, naver_news_templete(), None
    try:
        close_browser = False
        # if secondWebDriver==None:
        #     close_browser = True
        #     secondWebDriver = webdriver.Chrome(
        #         options=WEBDRIVER_OPTIONS, executable_path=WEBDRIVER_PATH
        #     )
        secondWebDriver.get(url)
        if "entertain.naver.com" in secondWebDriver.current_url:
            try:
                secondWebDriver.set_page_load_timeout(20)
                PressName = secondWebDriver.find_element_by_css_selector(
                    "#content > div.end_ct > div > div.press_logo > a > img"
                ).get_attribute("alt")
                NewsDate = secondWebDriver.find_element_by_css_selector(
                    "#content > div.end_ct > div > div.article_info > span > em"
                ).text
                NewsDate = NewsDate.split(" ")[0].split(".")
                NewsDate = NewsDate[0] + "-" + NewsDate[1] + "-" + NewsDate[2]
                NewsTitle = secondWebDriver.find_element_by_css_selector(
                    "#content > div.end_ct > div > h2"
                ).text
                NewsTitle = re.sub(
                    "[-=+#/\?:^@*\"※~ㆍ!』‘|`'…》\”\“\’·\n,.]", "", NewsTitle
                )
                NewsContent = secondWebDriver.find_element_by_css_selector(
                    "#articeBody"
                ).text
                NewsContent = re.sub(
                    "[-=+#/\?:^@*\"※~ㆍ!』‘|`'…》\”\“\’·\n]", "", NewsContent
                )
                NewsContent = ". ".join(
                    [line.strip() for line in NewsContent.split(".")]
                ).strip()
                JstName = secondWebDriver.find_elements_by_tag_name("p.byline_p")
                if JstName:
                    JstName = JstName[0].text.split(" ")[0]
                else:
                    JstName = None
                UrlLink = url
                Data = {
                    "PressName": PressName,
                    "NewsDate": NewsDate,
                    "NewsTitle": NewsTitle,
                    "NewsContent": NewsContent,
                    "JstName": JstName,
                    "UrlLink": UrlLink,
                }
                flag = True
                if close_browser:
                    secondWebDriver.close()
            except Exception as e:
                Data = None
                log = e
                if close_browser:
                    secondWebDriver.close()
        elif "sports.news.naver.com" in secondWebDriver.current_url:
            try:
                secondWebDriver.set_page_load_timeout(20)
                PressName = secondWebDriver.find_element_by_tag_name(
                    "#pressLogo > a > img"
                ).get_attribute(
                    "alt"
                )  # 언론사명
                NewsDate = secondWebDriver.find_element_by_css_selector(
                    "#content > div > div.content > div > div.news_headline > div > span:nth-child(1)"
                ).text  # 날짜
                NewsDate = NewsDate.split(" ")[1].split(".")
                NewsDate = NewsDate[0] + "-" + NewsDate[1] + "-" + NewsDate[2]
                NewsTitle = secondWebDriver.find_element_by_tag_name(
                    "#content > div > div.content > div > div.news_headline > h4"
                ).text  # 제목
                NewsTitle = re.sub(
                    "[-=+#/\?:^@*\"※~ㆍ!』‘|`'…》\”\“\’·\n,.]", "", NewsTitle
                )
                NewsContent = secondWebDriver.find_element_by_css_selector(
                    "#newsEndContents"
                ).text  # 본문
                NewsContent = re.sub(
                    "[-=+#/\?:^@*\"※~ㆍ!』‘|`'…》\”\“\’·\n]", "", NewsContent
                )
                NewsContent = ". ".join(
                    [line.strip() for line in NewsContent.split(".")]
                ).strip()
                JstName = secondWebDriver.find_elements_by_tag_name("p.byline")
                if JstName:
                    JstName = JstName[0].text.split(" ")[0]
                else:
                    JstName = None
                UrlLink = url
                Data = {
                    "PressName": PressName,
                    "NewsDate": NewsDate,
                    "NewsTitle": NewsTitle,
                    "NewsContent": NewsContent,
                    "JstName": JstName,
                    "UrlLink": UrlLink,
                }
                flag = True
                if close_browser:
                    secondWebDriver.close()
            except Exception as e:
                Data = None
                log = e
                if close_browser:
                    secondWebDriver.close()
        else:
            try:
                secondWebDriver.set_page_load_timeout(20)
                PressName = secondWebDriver.find_element_by_tag_name(
                    "img.media_end_head_top_logo_img"
                ).get_attribute("title")
                NewsDate = secondWebDriver.find_element_by_tag_name(
                    "span.media_end_head_info_datestamp_time"
                ).text
                NewsDate = NewsDate.split(" ")[0].split(".")
                NewsDate = NewsDate[0] + "-" + NewsDate[1] + "-" + NewsDate[2]
                NewsTitle = secondWebDriver.find_element_by_tag_name(
                    "h2.media_end_head_headline"
                ).text
                NewsTitle = re.sub(
                    "[-=+#/\?:^@*\"※~ㆍ!』‘|`'…》\”\“\’·\n,.]", "", NewsTitle
                )
                NewsContent = secondWebDriver.find_element_by_id("dic_area").text
                NewsContent = re.sub(
                    "[-=+#/\?:^@*\"※~ㆍ!』‘|`'…》\”\“\’·\n]", "", NewsContent
                )
                NewsContent = ". ".join(
                    [line.strip() for line in NewsContent.split(".")]
                ).strip()
                JstName = secondWebDriver.find_elements_by_tag_name("p.byline_p")
                if JstName:
                    JstName = JstName[0].text.split(" ")[0]
                else:
                    JstName = None
                UrlLink = url
                Data = {
                    "PressName": PressName,
                    "NewsDate": NewsDate,
                    "NewsTitle": NewsTitle,
                    "NewsContent": NewsContent,
                    "JstName": JstName,
                    "UrlLink": UrlLink,
                }
                flag = True
                if close_browser:
                    secondWebDriver.close()
            except Exception as e:
                Data = None
                log = e
                if close_browser:
                    secondWebDriver.close()
    except Exception as e:
        Data = None
        log = e
    return flag, Data, log

# naver_news url예시
# https://search.naver.com/search.naver?where=news&query=벡스인텔리전스|최재호&sort=1&pd=3&ds=2022.11.01&de=2022.12.31&start=1

# keyword=["기업명", "대표자명"], div_no=검색 시작 게시글 순번 (1페이지당 10개씩)
def get_url(
    keyword, 
    div_no, 
    start_date=(datetime.datetime.today()).strftime("%Y.%m.%d"), 
    end_date = (
        datetime.datetime.today() - datetime.timedelta(days=SearchDays)
        ).strftime("%Y.%m.%d")
    ):
    flag, url, log = False, None, None
    try:
        query = f'"{keyword[1]}"%7C"{keyword[0]}"'
        """
        start_date = (datetime.datetime.today()).strftime("%Y.%m.%d")
        end_date = (
            datetime.datetime.today() - datetime.timedelta(days=SearchDays)
        ).strftime("%Y.%m.%d")
        """
        # 기업명+대표자명 / 오늘-100일 / 오늘 / 게시글순번
        url = (
            "https://search.naver.com/search.naver?where=news&query="
            + query
            + "&sort=1&pd=3&ds="
            + end_date
            + "&de="
            + start_date
            + "&start="
            + str(div_no)
        )
        # print(url)
        flag = True
    except Exception as e:
        log = e
    return flag, url, log


def naver_news_templete():
    naver_news = {
        "PressName": None,  # 언론사명
        "NewsDate": None,  # 기사날짜
        "NewsTitle": None,  # 기사제목
        "NewsContent": None,  # 기사내용
        "JstName": None,  # 기자명
        "UrlLink": None,  # URL 링크
    }
    return naver_news


if __name__ == "__main__":
    main("1000861574", ["성진섬유", "김진열"])
    # main("2208736743", ["이노그리드", "김명진"])
    # webDriver1 = webdriver.Chrome(options=WEBDRIVER_OPTIONS, executable_path=WEBDRIVER_PATH)
    # webDriver2 = webdriver.Chrome(options=WEBDRIVER_OPTIONS, executable_path=WEBDRIVER_PATH)
    # res_lst = get_result(None,"가을","",webDriver1,True,secondWebDriver=webDriver2)
    pass