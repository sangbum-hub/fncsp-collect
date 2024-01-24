import datetime
import sys
from os import path
from libraries import *
from .Funcs import funcs, check_data_pattern, write_log, timeout, retry_function

today = datetime.datetime.today()
DataType = "kisti_article"
SearchDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
SearchID = "autoSystem"
FlagPrintData = True  # True: 저장할 최종 데이터 출력
FlagSaveData = True  # True: 엘라스틱에 데이터 저장
MAX_RETRY = 5
TIMEOUT = 30


def process(biz_no, company_name, start_date):
    result_list = []
    try:
        Data = api_kisti_article(biz_no, company_name, start_date)
        if Data:
            Data = [i for i in Data if i["journalInfo"]["kistiID"] != "NJOU0000null"]
            # Data = [i for i in Data if i["journalInfo"]["kistiID"]]

        if len(Data) == 0:
            kisti_article_data = {
                "BusinessNum": biz_no,
                "DataType": DataType,
                "SearchDate": SearchDate,
                "SearchID": SearchID,
                "Data": None,
            }
            result_list.append(copy.deepcopy(kisti_article_data))
        else:
            for data in Data:
                kisti_article_data = {
                    "BusinessNum": biz_no,
                    "DataType": DataType,
                    "SearchDate": SearchDate,
                    "SearchID": SearchID,
                    "Data": None if len(data) == 0 else data,
                }
                result_list.append(copy.deepcopy(kisti_article_data))
        return result_list
    except Exception as e:
        write_log.write_log(BIZ_NO=biz_no, DATA_TYPE=DataType, ERR_LOG=e)


def get_api_data(biz_no, companyName, startPosition, displayCount):
    result = call_api_kisti_article(biz_no, companyName, startPosition, displayCount)
    if type(result) == dict:
        return result
    else:
        return None


# 논문: <저자소속기관>을 기업명으로 검색
def api_kisti_article(biz_no, company_name, start_date):
    Data = []
    start_date = start_date
    data = get_api_data(biz_no, company_name, startPosition=1, displayCount=1)
    displayCount = 50
    if data:
        total_count = int(data["resultSummary"]["totalCount"])
        if total_count > 0:
            if total_count < 10:
                displayCount = 1
            elif total_count < 100:
                displayCount = 5
            elif total_count < 1000:
                displayCount = 10
            else:
                displayCount = 50
            
            # 논문 최대 개수 10개 제한 (삼성같은 경우 너무 많이 출력됨)
            if total_count > 10:
                total_count = 10
                displayCount = 1

            # for i in tqdm(range(0, math.ceil(total_count / displayCount))):
            for i in tqdm(range(total_count)):
                # startPosition = i * displayCount + 1
                startPosition = i + 1
                # 만개 이상의 데이터 호출 불가
                if startPosition > 9999:
                    break
                # print("{}%".format(int((startPosition / total_count) * 100)), end=" ")
                data = get_api_data(
                    biz_no, company_name, startPosition=1, displayCount=1
                )
                # print('result -> {} / {}'.format(flag, data))

                # 날짜 비교하여 특허 중복 수집 방지
                if int(start_date.replace('.', '')) > int(data['outputData'][0]['journalInfo']['pdate']) :
                    break

                if data:
                    # 불러온 데이터 파싱
                    result = parser_kisti_article_json(company_name, data)
                    if result:
                        Data.extend(result)
                else:
                    continue
        return Data

    else:
        return ""


# api 리턴된 json 파싱
def parser_kisti_article_json(company_name, data):
    Data = []
    try:
        for item in data["outputData"]:
            if not item.get("articleInfo"):
                continue
            ArticleInfo = item["articleInfo"]
            affiliation_data = ArticleInfo["authorInfo"]["affiliation"]
            if affiliation_data == [[]]:
                affiliation_list = []
            elif type(affiliation_data) == str:
                affiliation_list = [affiliation_data]
            else:
                affiliation_data = [x for x in affiliation_data if x != []]
                # if len(affiliation_data) > 1:
                affiliation_data = [ap["#text"] for ap in affiliation_data]
                affiliation_list = list(set(affiliation_data))

            affiliation_ = [funcs.re_type(i) for i in affiliation_list]

            # Data.articleInfo.affiliation(소속기관) 데이터는 기업명을 포함해야한다
            if company_name not in affiliation_:
                # 소속기관에 기업명이 없을 경우, pass
                continue

            author_data = ArticleInfo["authorInfo"]["author"]
            if author_data == [[]]:
                author_list = []
            elif type(author_data) == str:
                author_list = [author_data]
            else:
                author_data = [x for x in author_data if x != []]
                if len(author_data) > 1:
                    author_data = [ap["#text"] for ap in author_data]
                author_list = list(set(author_data))

            abstractInfo = ArticleInfo["abstractInfo"]
            if abstractInfo == [[]]:
                abstractInfo = []
            articleInfo = {
                "kistiID": ArticleInfo["@kistiID"] if ArticleInfo["@kistiID"] else None,
                "articleTitleInfo": ArticleInfo["articleTitleInfo"]["articleTitle"]
                if ArticleInfo["articleTitleInfo"]["articleTitle"]
                else None,
                "articleTitleInfo2": ArticleInfo["articleTitleInfo"]["articleTitle2"]
                if ArticleInfo["articleTitleInfo"]["articleTitle2"]
                else None,
                "abstractInfo": "\n".join(abstractInfo)
                if abstractInfo
                else None,  # list → text, 줄 바꿈으로 하나의 text로 변환
                "author": author_list if author_list else None,
                "affiliation": affiliation_list if affiliation_list else None,
                "page": ArticleInfo["page"] if ArticleInfo["page"] else None,
                "deeplink": ArticleInfo["deeplink"]
                if ArticleInfo["deeplink"]
                else None,
                "keyword": ArticleInfo["keyword"].split(" . ")
                if type(ArticleInfo["keyword"]) == str
                else None,
            }

            JournalInfo = item["journalInfo"]

            issninfo = JournalInfo["issninfo"]
            if issninfo == [[]]:
                issninfo = []
            issninfo = [x for x in issninfo if x != []]

            isbninfo = JournalInfo["isbninfo"]
            if isbninfo == [[]]:
                isbninfo = []
            isbninfo = [x for x in isbninfo if x != []]

            journalInfo = {
                "kistiID": JournalInfo["@kistiID"] if JournalInfo["@kistiID"] else None,
                "publisher": JournalInfo["publisher"]
                if JournalInfo["publisher"]
                else None,
                "journalTitle": "\n".join(JournalInfo["journalTitleInfo"])
                if JournalInfo["journalTitleInfo"]
                else None,  # list → text, 줄 바꿈으로 하나의 text로 변환
                "issninfo": "\n".join(issninfo) if issninfo else None,
                "isbninfo": "\n".join(isbninfo) if isbninfo else None,
                "volume": JournalInfo["volume"]["#text"]
                if JournalInfo["volume"]["#text"]
                else None,
                "issue": JournalInfo["issue"] if JournalInfo["issue"] else None,
                "year": JournalInfo["year"] if JournalInfo["year"] else None,
                "pdate": f"{JournalInfo['pdate'][:4]}-{JournalInfo['pdate'][4:6]}-{JournalInfo['pdate'][6:]}"
                if JournalInfo["pdate"]
                else None,
            }

            Data.append({"journalInfo": journalInfo, "articleInfo": articleInfo})
    except Exception as e:
        print("parser_kisti_article_json Error : {}".format(e))
        Data = []
    return Data


# API 호출 > 검색 결과 json 리턴
"""
@retry_function.retry(MAX_RETRY, dict)
@timeout.timeout(TIMEOUT)
"""
# kisti_article 수집 api url
# http://openapi.ndsl.kr/itemsearch.do?keyValue=08497314&target=ARTI&searchField=AO&sortby=pubYear&query=대가파우더시스템
def call_api_kisti_article(biz_no, companyName, startPosition, displayCount):
    companyName = funcs.re_type(companyName)
    url = "http://openapi.ndsl.kr/itemsearch.do?"
    queryParams = urlencode(
        {
            quote_plus("keyValue"): unquote("08497314"),  # 인증키
            quote_plus("target"): "ARTI",  # 논문전체
            quote_plus("searchField"): "AO",  # 저자소속기관
            quote_plus("displayCount"): displayCount,  # 10 ~ 100
            quote_plus("startPosition"): startPosition,  # 1 ~
            quote_plus("sortby"): "pubYear",  # 발행일 (Data.journalInfo.year 발행연도 필드 내림차순)
            quote_plus("returnType"): "json",  # xml/json
            quote_plus("query"): companyName,  # 검색어 입력
            quote_plus("callback"): "callback",  # json일 경우 필수
        },
        encoding="utf-8",
    )
    req = requests.get(url + queryParams)
    if 200 <= req.status_code < 300:
        response_body = req.text
        response_body = response_body[
            int(response_body.find("{")) : int(response_body.rfind("}") + 1)
        ]
        try:
            response_body = json.loads(response_body)
        except:
            return None

        return response_body
    else:
        raise Exception(f"STATUS_CODE_{req.status_code}_API_SERVER_ERROR")


# @retry_function.retry(MAX_RETRY, str)
def result_save_function(data):
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

def get_result(biz_no, company_name, ceo_name=None, webDriver=None, console_print=False, start_date=None, err_msg=None):
    result = []
    result.append(process(biz_no, company_name, start_date))
    result = list(itertools.chain(*result))

    return result

def main(biz_no: str, companyName: str):

    result = []
    result.append(process(biz_no, companyName))
    result = list(itertools.chain(*result))

    if FlagPrintData:
        print(result)

    if bool(result) and FlagSaveData:
        for data in result:
            flag1 = result_save_function(data)
            update_searchdate_function(biz_no, flag1)


if __name__ == "__main__":
    main("5058211726", "한국정보화진흥원")
    main("2208736743", "이노그리드")

