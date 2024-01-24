import datetime
import sys
from os import path
from libraries import *
from .Funcs import funcs, check_data_pattern, write_log, timeout, retry_function

today = datetime.datetime.today()
DataType = "ntis_org_info"
FlagPrintData = True  # True: 저장할 최종 데이터 출력
FlagSaveData = True  # True: 엘라스틱에 데이터 저장
NTIS_KEY = "uj4ln084p9ta4wua90zx"
MAX_RETRY = 5
TIMEOUT = 30

"""
@retry_function.retry(MAX_RETRY, dict)
@timeout.timeout(TIMEOUT)
"""
def call_api_ntis_org_info(biz_no, companyName, startPosition, displayCnt, start_date):
    url = "https://www.ntis.go.kr/rndopen/openApi/orgRndInfo?"
    apprvKey = unquote(NTIS_KEY)
    queryParams = urlencode(
        {
            quote_plus("apprvKey"): apprvKey, 
            quote_plus("reqOrgBno"): biz_no,
        },
        encoding="utf-8",
    )

    req = requests.get(url + queryParams)
    if 200 <= req.status_code < 300:
        result = json.loads(
            json.dumps(xmltodict.parse(str(bs(req.text, "xml"))), ensure_ascii=False,)
        )
        if result.get("response") == None or result.get("error"):
            raise Exception("API_RESPONSE_ERROR")
        else:
            return result
    else:
        raise Exception(f"STATUS_CODE_{req.status_code}_API_SERVER_ERROR")


def get_api_data(biz_no, companyName, startPosition, displayCnt, start_date):
    result = call_api_ntis_org_info(biz_no, companyName, startPosition, displayCnt, start_date)
    if type(result) == dict:
        return result
    else:
        return None


def api_ntis_org_info(biz_no, companyName, startPosition, displayCnt, start_date):
    ntis_org = ntis_org_template()

    result_list = []

    result = get_api_data(biz_no, companyName, startPosition, displayCnt, start_date)

    if not result:
        return ""

    elif not result["response"].get("body"):
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
        org_result = result["response"].get("body")

        ntis_org["orgName"] = org_result["orgName"]
        ntis_org["orgPageInfo"] = org_result["orgPageInfo"]

        if type(org_result.get("rndKorKeword")) == str:
            ntis_org["rndKorKeyword"] = [org_result["rndKorKeword"]]
        else:
            pass

        if type(org_result.get("rndEngKeword")) == str:
            ntis_org["rndEngKeyword"] = [org_result["rndEngKeword"]]
        else:
            pass

        if type(org_result.get("rndCategory")) == str:
            ntis_org["rndCategory"] = [org_result["rndCategory"]]
        else:
            pass

        ntis_org["rndStatusList"] = org_result.get("rndStatusList")

        if type(ntis_org["rndStatusList"]) == list:
            ntis_org["rndStatusList"] = org_result["rndStatusList"]
            for k in ntis_org["rndStatusList"]:
                k["pjtCnt"] = int(k["pjtCnt"])
                k["rndBudget"] = int(int(k["rndBudget"]) / 1000)
                k["govBudget"] = int(int(k["govBudget"]) / 1000)
                k["paperCnt"] = int(k["paperCnt"])
                k["patentCnt"] = int(k["patentCnt"])
                k["reportCnt"] = int(k["reportCnt"])
        elif type(ntis_org["rndStatusList"]) == dict:
            ntis_org["rndStatusList"] = []
            ntis_org["rndStatusList"].append(org_result["rndStatusList"])
            for k in ntis_org["rndStatusList"]:
                k["pjtCnt"] = int(k["pjtCnt"])
                k["rndBudget"] = int(int(k["rndBudget"]) / 1000)
                k["govBudget"] = int(int(k["govBudget"]) / 1000)
                k["paperCnt"] = int(k["paperCnt"])
                k["patentCnt"] = int(k["patentCnt"])
                k["reportCnt"] = int(k["reportCnt"])

        data = {
            "BusinessNum": str(biz_no),
            "DataType": DataType,
            "SearchDate": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "SearchID": "autoSystem",
            "Data": ntis_org,
        }
        result_list.append(copy.deepcopy(data))
        return result_list


def ntis_org_template():
    org = {
        "orgName": None,
        "orgPageInfo": None,
        "rndKorKeyword": None,
        "rndEngKeyword": None,
        "rndCategory": None,
        "rndStatusList": None,
    }
    return org


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
    displayCnt = 1000

    result = []

    CompanyName = funcs.re_type(company_name)
    result.append(api_ntis_org_info(biz_no, CompanyName, 1, displayCnt, start_date))
    result = list(itertools.chain(*result))

    return result

def main(biz_no, CompanyName):

    displayCnt = 1000

    result = []

    CompanyName = funcs.re_type(CompanyName)
    result.append(api_ntis_org_info(biz_no, CompanyName, 1, displayCnt))
    result = list(itertools.chain(*result))

    if FlagPrintData:
        print(result)

    if bool(result) and FlagSaveData:
        for data in result:
            flag1 = result_save_function(data)
            update_searchdate_function(biz_no, flag1)


if __name__ == "__main__":
    main("2208736743", "이노그리드")
