import datetime
import sys
from os import path
from tracemalloc import start
from libraries import *
from .Funcs import funcs, check_data_pattern, write_log, timeout, retry_function

today = datetime.datetime.today()
DataType = "ntis_rnd_paper"
FlagPrintData = True  # True: 저장할 최종 데이터 출력
FlagSaveData = True  # True: 엘라스틱에 데이터 저장
NTIS_KEY = "uj4ln084p9ta4wua90zx"
MAX_RETRY = 5
TIMEOUT = 30


@retry_function.retry(MAX_RETRY, dict)
@timeout.timeout(TIMEOUT)
def call_api_ntis_rnd_paper(biz_no, companyName, startPosition, displayCnt, start_date):
    url = "https://www.ntis.go.kr/rndopen/openApi/rresearchpdf?"
    apprvKey = unquote(NTIS_KEY)
    queryParams = urlencode(
        {
            quote_plus("apprvKey"): apprvKey,
            quote_plus("query"): '"' + companyName + '"',
            quote_plus("collection"): "researchpdf",
            quote_plus("searchField"): "PB",
            quote_plus("sortBy"): "DATE/DESC",
            quote_plus("startPosition"): startPosition,
            quote_plus("displayCnt"): displayCnt,
        },
        encoding="utf-8",
    )

    req = requests.get(url + queryParams)
    if 200 <= req.status_code < 300:
        result = json.loads(
            json.dumps(
                xmltodict.parse(
                    str(bs(req.text, "html.parser"))
                    .replace('<span class="search_word">', "")
                    .replace("</span>", "")
                ),
                ensure_ascii=False,
            )
        )
        if startPosition == 1:
            if result.get("result") == None or result.get("error"):
                raise Exception("API_RESPONSE_ERROR")
            else:
                return result
        else:
            if (
                (result.get("result") == None)
                or (result.get("error"))
                or (result["result"]["resultset"] == None)
            ):
                raise Exception("API_RESPONSE_ERROR")
            else:
                return result
    else:
        raise Exception(f"STATUS_CODE_{req.status_code}_API_SERVER_ERROR")


def get_api_data(biz_no, companyName, startPosition, displayCnt, start_date):
    result = call_api_ntis_rnd_paper(biz_no, companyName, startPosition, displayCnt, start_date)
    if type(result) == dict:
        return result
    else:
        return None


def api_ntis_rnd_paper(biz_no, companyName, startPosition, displayCnt, start_date):

    ntis_rnd_paper = ntis_rnd_paper_template()
    rnd_paper_result = []
    result_list = []

    result = get_api_data(biz_no, companyName, startPosition, displayCnt, start_date)

    if not result:
        return ""

    else:
        if int(result["result"]["totalhits"]) == 0:
            data = {
                "BusinessNum": str(biz_no),
                "DataType": DataType,
                "SearchDate": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": None,
            }
            result_list.append(copy.deepcopy(data))
            return result_list

        else:
            if type(result["result"]["resultset"]["hit"]) == dict:
                rnd_paper_result.append(result["result"]["resultset"]["hit"])
            elif type(result["result"]["resultset"]["hit"]) == list:
                [
                    rnd_paper_result.append(i)
                    for i in result["result"]["resultset"]["hit"]
                ]
                if int(result["result"]["totalhits"]) > displayCnt:
                    loop = math.ceil(int(result["result"]["totalhits"]) / displayCnt)
                    for i in tqdm(range(loop - 1)):
                        startPosition += displayCnt
                        result = get_api_data(
                            biz_no, companyName, startPosition, displayCnt
                        )
                        if not result:
                            pass
                        else:
                            [
                                rnd_paper_result.append(i)
                                for i in result["result"]["resultset"]["hit"]
                            ]

            for i in tqdm(rnd_paper_result):

                # 날짜 비교하여 중복 수집 방지(년도만 비교, 1년단위로 수집)
                if int(i['publicationyear']) < int(start_date[0:4]) :
                    # print("publicationyear : ", i['publicationyear'])
                    # print("start_date : ", start_date[0:4])
                    break

                if funcs.re_type(i["publicationagency"]) == companyName:
                    for z in i:
                        if i[z] is None:
                            i[z] = {}
                    ntis_rnd_paper["PublicationYear"] = i["publicationyear"]
                    ntis_rnd_paper["ResearchPublicNo"] = i["researchpublicno"]
                    ntis_rnd_paper["PublicationAgency"] = i["publicationagency"]
                    ntis_rnd_paper["ResultTitleKR"] = i["resulttitle"].get("korean")
                    ntis_rnd_paper["ResultTitleEN"] = i["resulttitle"].get("english")
                    ntis_rnd_paper["AbstractKR"] = i["abstract"].get("korean")
                    ntis_rnd_paper["AbstractEN"] = i["abstract"].get("english")
                    ntis_rnd_paper["KeywordKR"] = i["keyword"].get("korean")
                    ntis_rnd_paper["KeywordEN"] = i["keyword"].get("english")
                    ntis_rnd_paper["Contents"] = i["contents"]
                    ntis_rnd_paper["PublicationCountry"] = i["publicationcountry"]
                    ntis_rnd_paper["PublicationLanguage"] = i["publicationlanguage"]
                    ntis_rnd_paper["DocUrl"] = i["docurl"]
                    ntis_rnd_paper["ProjectNumber"] = i["projectnumber"]
                    ntis_rnd_paper["ProjectTitle"] = i["projecttitle"]
                    ntis_rnd_paper["LeadAgency"] = i["leadagency"]
                    ntis_rnd_paper["ManagerName"] = i["managername"]

                    for i in ntis_rnd_paper:
                        if bool(ntis_rnd_paper[i]) == False:
                            ntis_rnd_paper[i] = None

                    data = {
                        "BusinessNum": str(biz_no),
                        "DataType": DataType,
                        "SearchDate": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S.%f"
                        ),
                        "SearchID": "autoSystem",
                        "Data": ntis_rnd_paper,
                    }
                    result_list.append(copy.deepcopy(data))
                else:
                    continue
            if result_list:
                return result_list
            else:
                data = {
                    "BusinessNum": str(biz_no),
                    "DataType": DataType,
                    "SearchDate": str(datetime.datetime.now()),
                    "SearchID": "autoSystem",
                    "Data": None,
                }
                result_list.append(copy.deepcopy(data))
            return result_list


def ntis_rnd_paper_template():
    paper = {
        "PublicationYear": None,
        "ResearchPublicNo": None,
        "PublicationAgency": None,
        "ResultTitleKR": None,
        "ResultTitleEN": None,
        "AbstractKR": None,
        "AbstractEN": None,
        "KeywordKR": None,
        "KeywordEN": None,
        "Contents": None,
        "PublicationCountry": None,
        "PublicationLanguage": None,
        "DocUrl": None,
        "ProjectNumber": None,
        "ProjectTitle": None,
        "LeadAgency": None,
        "ManagerName": None,
    }
    return paper


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

def get_result(biz_no, company_name, ceo_name=None, webDriver=None, console_print=False, start_date=None, err_msg=None):
    displayCnt = 1000

    result = []

    CompanyName = funcs.re_type(company_name)
    result.append(api_ntis_rnd_paper(biz_no, CompanyName, 1, displayCnt, start_date))
    result = list(itertools.chain(*result))

    return result

def main(biz_no, CompanyName):

    displayCnt = 1000

    result = []

    CompanyName = funcs.re_type(CompanyName)
    result.append(api_ntis_rnd_paper(biz_no, CompanyName, 1, displayCnt))
    result = list(itertools.chain(*result))

    if FlagPrintData:
        print(result)

    if bool(result) and FlagSaveData:
        for data in result:
            flag1 = result_save_function(data)
            update_searchdate_function(biz_no, flag1)


if __name__ == "__main__":
    main("2208736743", "이노그리드")
