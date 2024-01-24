import datetime
import sys
from os import path
from libraries import *
from .Funcs import funcs, check_data_pattern, write_log, timeout, retry_function

today = datetime.datetime.today()
DataType = "ntis_accomp"
FlagPrintData = True  # True: 저장할 최종 데이터 출력
FlagSaveData = True  # True: 엘라스틱에 데이터 저장
NTIS_KEY = "uj4ln084p9ta4wua90zx"
MAX_RETRY = 5
TIMEOUT = 30


@retry_function.retry(MAX_RETRY, dict)
@timeout.timeout(TIMEOUT)
def call_api_ntis_accomp(biz_no, companyName, startPosition, displayCnt, start_date):
    # 과제검색
    url = "https://www.ntis.go.kr/rndopen/openApi/public_result?"
    apprvKey = unquote(NTIS_KEY)
    queryParams = urlencode(
        {
            quote_plus("apprvKey"): apprvKey,
            quote_plus("SRWR"): '"' + companyName + '"',
            quote_plus("searchFd"): "OG",
            quote_plus("collection"): "requip",
            quote_plus("searchRnkn"): "DATE/DESC",
            quote_plus("startPosition"): startPosition,
            quote_plus("displayCnt"): displayCnt,
        },
        encoding="utf-8",
    )

    req = requests.get(url + queryParams)
    if 200 <= req.status_code < 300:
        result = json.loads(
            json.dumps(
                xmltodict.parse(str(bs(req.text, "html.parser"))), ensure_ascii=False,
            )
            .replace('<span class=\\"search_word\\">', "")
            .replace("</span>", "")
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
    result = call_api_ntis_accomp(biz_no, companyName, startPosition, displayCnt, start_date)
    if type(result) == dict:
        return result
    else:
        return None


def api_ntis_accomp(biz_no, companyName, startPosition, displayCnt, start_date):

    ntis_accomp = ntis_accomp_template()

    accomp_result = []

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
                accomp_result.append(result["result"]["resultset"]["hit"])
            elif type(result["result"]["resultset"]["hit"]) == list:
                [accomp_result.append(i) for i in result["result"]["resultset"]["hit"]]
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
                                accomp_result.append(i)
                                for i in result["result"]["resultset"]["hit"]
                            ]

            for i in tqdm(accomp_result):
                
                # 날짜 비교하여 중복 수집 방지(년도만 비교, 1년단위로 수집)
                if int(i['projectyear']) < int(start_date[0:4]) :
                    # print("projectyear : ", i['projectyear'])
                    # print("start_date : ", start_date[0:4])
                    break

                if funcs.re_type(i["keeporganization"].get("name")) == companyName:
                    for z in i:
                        if i[z] is None:
                            i[z] = {}
                    ntis_accomp["SitId"] = i["@sitid"]
                    ntis_accomp["ProjectName"] = i["projectname"]
                    ntis_accomp["ProjectCode"] = i["projectcode"]
                    ntis_accomp["EquipId"] = i["equipid"]
                    ntis_accomp["EquipNo"] = i["equipno"]
                    ntis_accomp["Year"] = i["year"]
                    ntis_accomp["ProjectYear"] = i["projectyear"]
                    ntis_accomp["MinistryCode"] = i["ministryname"].get("@code")
                    ntis_accomp["MinistryName"] = i["ministryname"].get("#text")
                    ntis_accomp["BudgetProjectNo"] = i["budgetprojectnumber"]
                    ntis_accomp["BudgetName"] = i["budgetproject"]
                    ntis_accomp["PerformAgentCode"] = i["performagent"].get("@code")
                    ntis_accomp["PerformAgent"] = i["performagent"].get("#text")
                    ntis_accomp["6TCode"] = i["sixtechnology"].get("@code")
                    ntis_accomp["6TName"] = i["sixtechnology"].get("#text")
                    ntis_accomp["TechnologyRoadMapCode"] = i["technologyroadmap"].get(
                        "@code"
                    )
                    ntis_accomp["TechnologyRoadMapName"] = i["technologyroadmap"].get(
                        "#text"
                    )
                    ntis_accomp["ScienceClassCode1"] = i["scienceclass1"].get("@code")
                    ntis_accomp["ScienceClassName1"] = i["scienceclass1"].get("#text")
                    ntis_accomp["ScienceClassCode2"] = i["scienceclass2"].get("@code")
                    ntis_accomp["ScienceClassName2"] = i["scienceclass2"].get("#text")
                    ntis_accomp["ScienceClassCode3"] = i["scienceclass3"].get("@code")
                    ntis_accomp["ScienceClassName3"] = i["scienceclass3"].get("#text")

                    for i in ntis_accomp:
                        if bool(ntis_accomp[i]) == False:
                            ntis_accomp[i] = None
                    data = {
                        "BusinessNum": str(biz_no),
                        "DataType": DataType,
                        "SearchDate": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S.%f"
                        ),
                        "SearchID": "autoSystem",
                        "Data": ntis_accomp,
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


def ntis_accomp_template():
    accomp = {
        "SitId": None,
        "ProjectName": None,
        "ProjectCode": None,
        "EquipId": None,
        "EquipNo": None,
        "Year": None,
        "ProjectYear": None,
        "MinistryCode": None,
        "MinistryName": None,
        "BudgetProjectNo": None,
        "BudgetName": None,
        "PerformAgentCode": None,
        "PerformAgent": None,
        "6TCode": None,
        "6TName": None,
        "TechnologyRoadMapCode": None,
        "TechnologyRoadMapName": None,
        "ScienceClassCode1": None,
        "ScienceClassName1": None,
        "ScienceClassCode2": None,
        "ScienceClassName2": None,
        "ScienceClassCode3": None,
        "ScienceClassName3": None,
    }
    return accomp


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
    result.append(api_ntis_accomp(biz_no, CompanyName, 1, displayCnt, start_date))
    result = list(itertools.chain(*result))

    return result

def main(biz_no, CompanyName):

    displayCnt = 1000

    result = []

    CompanyName = funcs.re_type(CompanyName)
    result.append(api_ntis_accomp(biz_no, CompanyName, 1, displayCnt))
    result = list(itertools.chain(*result))

    if FlagPrintData:
        print(result)

    if bool(result) and FlagSaveData:
        for data in result:
            flag1 = result_save_function(data)
            update_searchdate_function(biz_no, flag1)


if __name__ == "__main__":
    main("2208736743", "이노그리드")
