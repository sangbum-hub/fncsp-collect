import datetime
import sys
from os import path
from libraries import *
from .Funcs import funcs, check_data_pattern, write_log, timeout, retry_function

today = datetime.datetime.today()
DataType = "kci"
SearchDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
SearchID = "autoSystem"
FlagPrintData = True  # True: 저장할 최종 데이터 출력
FlagSaveData = True  # True: 엘라스틱에 데이터 저장
MAX_RETRY = 5
TIMEOUT = 30

"""
@retry_function.retry(MAX_RETRY, dict)
@timeout.timeout(TIMEOUT)
"""
def call_api_kci(biz_no, query, pageNo):
    url = "https://open.kci.go.kr/po/openapi/openApiSearch.kci?"
    queryParams = urlencode(
        {
            quote_plus("key"): unquote("13784621"),
            quote_plus("apiCode"): "articleSearch",
            quote_plus("displayCount"): 100,  # 1 ~ 100
            quote_plus("page"): pageNo,
            quote_plus("affiliation"): query,  # 검색어 입력
            quote_plus("sortNm"): "pubiYr",  # 발행일자 정렬
            quote_plus("sortDir"): "desc",  # 내림차순
        },
        encoding="utf-8",
    )
    req = requests.get(url + queryParams)
    if 200 <= req.status_code < 300:
        response_body = json.loads(
            json.dumps(
                xmltodict.parse(str(bs(req.text, "html.parser"))), ensure_ascii=False,
            )
        )
        return response_body
    else:
        raise Exception(f"STATUS_CODE_{req.status_code}_API_SERVER_ERROR")


def get_api_data(biz_no, query, pageNo):
    result = call_api_kci(biz_no, query, pageNo)
    if type(result) == dict:
        return result
    else:
        return None


# 논문: <저자소속기관>을 기업명으로 검색
def api_kci(biz_no, company_name, start_date, limit=None):
    pageNo = 1
    company_name = funcs.re_type(company_name)
    result_list = []
    Data = []
    api_data = get_api_data(biz_no, query=company_name, pageNo=pageNo)
    if (not api_data) or (api_data["metadata"]["outputdata"].get("record") == None):
        if (api_data) and (
            api_data["metadata"]["outputdata"].get("result") == {"resultmsg": "No Data"}
        ):
            return [
                {
                    "BusinessNum": biz_no,
                    "DataType": DataType,
                    "SearchDate": SearchDate,
                    "SearchID": SearchID,
                    "Data": None,
                }
            ]
        else:
            return ""
    else:
        if type(api_data["metadata"]["outputdata"]["record"]) == dict:
            Data.append(api_data["metadata"]["outputdata"]["record"])

        elif type(api_data["metadata"]["outputdata"]["record"]) == list:
            [Data.append(i) for i in api_data["metadata"]["outputdata"]["record"]]
            if int(api_data["metadata"]["outputdata"]["result"]["total"]) > 100:
                loop = math.ceil(
                    int(api_data["metadata"]["outputdata"]["result"]["total"]) / 100
                )
                for i in tqdm(range(loop - 1)):
                    pageNo += 1
                    result = get_api_data(biz_no, company_name, pageNo)
                    if not result:
                        pass

                    elif result["metadata"]["outputdata"].get("record") == None:
                        pass

                    elif type(result["metadata"]["outputdata"]["record"]) == dict:
                        Data.append(result["metadata"]["outputdata"]["record"])

                    elif result["metadata"]["outputdata"]["record"]:
                        [
                            Data.append(i)
                            for i in result["metadata"]["outputdata"]["record"]
                        ]
        for i in tqdm(Data):
            try:
                if i["journalinfo"]:
                    pubyear = i["journalinfo"].get("pub-year")
                    pubmon = i["journalinfo"].get("pub-mon")

                    # 날짜 년도 비교하여 중복 수집 방지
                    if int(pubyear) < int(start_date[0:4]) : 
                        break
                    # print("pubyear : ", pubyear)
                    # print("start_date : ", start_date)

                    jornaldata = {
                        "JournalName": i["journalinfo"].get("journal-name"),
                        "PubName": i["journalinfo"].get("publisher-name"),
                        "PubDate": f"{pubyear}-{pubmon}"
                        if pubyear and pubmon
                        else None,
                        "Volume": i["journalinfo"].get("volume"),
                        "Issue": i["journalinfo"].get("issue"),
                    }
                else:
                    jornaldata = None

                if i["articleinfo"]:
                    titlegroup = i["articleinfo"].get("title-group")

                    if titlegroup:
                        if (
                            type(i["articleinfo"]["title-group"]["article-title"])
                            == dict
                        ):
                            ArticleTitle = [
                                i["articleinfo"]["title-group"]["article-title"]
                            ]
                        else:
                            ArticleTitle = i["articleinfo"]["title-group"][
                                "article-title"
                            ]
                    else:
                        ArticleTitle = ""

                    Abstractgroup = i["articleinfo"].get("abstract-group")

                    if Abstractgroup:
                        if type(i["articleinfo"]["abstract-group"]["abstract"]) == dict:
                            Abstract = [i["articleinfo"]["abstract-group"]["abstract"]]
                        else:
                            Abstract = i["articleinfo"]["abstract-group"]["abstract"]
                    else:
                        Abstractgroup = ""

                    ArticleTitleOR = [
                        k.get("#text") for k in ArticleTitle if k["@lang"] == "original"
                    ]
                    ArticleTitleEN = [
                        k.get("#text") for k in ArticleTitle if k["@lang"] == "english"
                    ]
                    AbstractOR = [
                        k.get("#text") for k in Abstract if k["@lang"] == "original"
                    ]
                    AbstractEN = [
                        k.get("#text") for k in Abstract if k["@lang"] == "english"
                    ]
                    articledata = {
                        "ArticleID": i["articleinfo"].get("@article-id"),
                        "ArticleCategory": i["articleinfo"].get("article-categories"),
                        "ArticleTitleOR": ArticleTitleOR if ArticleTitleOR else None,
                        "ArticleTitleEN": ArticleTitleEN if ArticleTitleEN else None,
                        "AuthorGroup": i["articleinfo"]["author-group"].get("author")
                        if i["articleinfo"]["author-group"]
                        else None,
                        "AbstractOR": AbstractOR if AbstractOR else None,
                        "AbstractEN": AbstractEN if AbstractEN else None,
                        "CitationCount": i["articleinfo"].get("citation-count"),
                    }
                else:
                    articledata = None

                final_result = {
                    "BusinessNum": biz_no,
                    "DataType": DataType,
                    "SearchDate": SearchDate,
                    "SearchID": SearchID,
                    "Data": {"JournalInfo": jornaldata, "ArticleInfo": articledata},
                }
                result_list.append(copy.deepcopy(final_result))
                if len(result_list)>=limit:
                    return result_list
            except:
                pass
        return result_list


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

def get_result(biz_no, company_name, ceo_name=None, webDriver=None, console_print=False, start_date=None, err_msg=None, limit=None):
    result = []
    result.append(api_kci(biz_no, company_name, start_date, limit=None))
    result = list(itertools.chain(*result))

    return result

def main(biz_no: str, companyName: str):

    result = []
    result.append(api_kci(biz_no, companyName))
    result = list(itertools.chain(*result))

    if FlagPrintData:
        print(result)

    if bool(result) and FlagSaveData:
        for data in result:
            flag1 = result_save_function(data)
            update_searchdate_function(biz_no, flag1)

    # flag = funcs.bulk_api(result)
    # if flag == "Success":
    #     today = datetime.datetime.today().strftime("%Y-%m-%d")
    #     biz_no_list = [(biz_no, today)]
    #     flag2 = funcs.update_searchDate_mysql(DataType, biz_no_list)
    #     if flag != flag2:
    #         write_log.write_log(
    #             BIZ_NO=biz_no,
    #             DATA_TYPE="kipris_utility_main",
    #             ERR_LOG="update_searchDate_mysql_error",
    #         )


if __name__ == "__main__":
    main("1000861574", "성진섬유")
    # main("1010134270", "한국과학")
    # main("5058211726", "한국정보화진흥원")
    main("2208736743", "이노그리드")
