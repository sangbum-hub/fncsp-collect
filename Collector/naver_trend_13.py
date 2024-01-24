import datetime
import sys
from os import path
from libraries import *
from .Funcs import funcs, check_data_pattern, write_log, timeout, retry_function

today = datetime.datetime.today()
DataType = "naver_trend"
FlagPrintData = True  # True: 저장할 최종 데이터 출력
FlagSaveData = True  # True: 엘라스틱에 데이터 저장
MAX_RETRY = 5
TIMEOUT = 30

class Auth_Info:
    def __init__(self):
        self.auth_list = [
            {"id":"5Z6LFF9LL821FNE9fUmE", "secret":"RCBO6gF8tE"},
            {"id":"4ZQu5OewzVjDSJ1cdSdA", "secret":"PrWJ55zRIn"},
            {"id":"FeCSP_QG9MBu_xfCirex", "secret":"RZBikPtMv9"},
            {"id":"5NgRqK_gWGT_5ZSPbLrg", "secret":"bM00_C5bUB"},
            {"id":"jE6R7hyS50Z4PklkKsnx", "secret":"e4Nm_y0c4N"},
            {"id":"ejfNJ43j1uYqVmzDebUL", "secret":"ErqevqGIq5"},
            {"id":"85RVPmnN1TjaYpYZLp2u", "secret":"PeZ99DlMTh"},
            {"id":"qX1a_btOZrCFqikMhCvq", "secret":"ZyQTpai51h"},
            {"id":"_TrXXSd3zQS8FdMW0sqX", "secret":"7H6_XK9iZl"},
            {"id":"vwN1Jpzb6tscNtr2dVEA", "secret":"O2n7exZQzk"},
            {"id":"aJ_17_xFhkOmyXWh3EbH", "secret":"ptPJNXECoN"},
            {"id":"hooF3Z_8ZBtZFLU624T8", "secret":"f1rY6nGbuT"},
            {"id":"mXRSqmSGfXiIK0uzFa2Q", "secret":"I8ydyFesmW"},
            {"id":"QvhnEXV0OeIvz3pHnXqM", "secret":"Z7sMI5JnZT"},
            {"id":"GZkay1kFpZjj8inSzroE", "secret":"zzu1hd5Bam"},
            {"id":"oZ4cFLBk_DdYpyLJf3OV", "secret":"6kF1E2mZz7"},
            {"id":"MLOH3AMDFqHIGd_tUdfV", "secret":"tCnDbDXp0P"},
            {"id":"VcSzKBRYkdgU7CkVbArq", "secret":"GT7dDQQq3H"},
            {"id":"HqUF2LkcAgOEyBqe_Nx6", "secret":"cHTN2ZtOiU"},
            {"id":"Bk3mikSGvjJwDTaGwm55", "secret":"cLYyMnyKtv"},
            {"id":"Sc5J0K5fTHNIOQrRJboR", "secret":"DolfNTfLks"},
            {"id":"ujkfY2gLms6WOh7xvQYL", "secret":"8NC0Pf8QCZ"},
            {"id":"UdFQ5GtC6y3Iq1vjIyTZ", "secret":"iziA9V1dna"},
            {"id":"YxWXwbCkmz8AoocOHuZO", "secret":"CJRbFE0Zn7"},
            {"id":"y55EPGcfrUPW5SdIvBpz", "secret":"kIiCCBv3zv"},
            {"id":"PTkVCkWPbNW7AvkZoAHK", "secret":"ECMYCJgYQO"},
            {"id":"SWUVoqXrGzRCaAmAivDD", "secret":"xAsMnkCyKo"},
            {"id":"M7nYHdcOIfm6r82az8CV", "secret":"1c5R_C3OxL"},
        ]
        self.current_idx = 0
        self.call_count = 0
    
    def get_auth(self):
        self.call_count += 1
        if self.call_count > 1000:
            self.current_idx += 1
            self.current_idx = (self.current_idx - len(self.auth_list)) if self.current_idx >= len(self.auth_list) else self.current_idx
        return self.auth_list[self.current_idx]
    
    def callable_count(self):
        return 1000 * len(self.auth_list)

"""
@retry_function.retry(MAX_RETRY, dict)
@timeout.timeout(TIMEOUT)
"""
def call_api_naver_trend(biz_no, company_name, start_date=None, auth_info=None):
    # 네이버트랜드는 초기적재/업데이트적재 상관없이 2016년부터 모든 구간으로 적재
    timeUnit = "month"
    startDate = "2016-01-01" if start_date is None else start_date  # 가능한 최소 날짜
    endDate = datetime.datetime.today().strftime("%Y-%m-%d")
    keywordGroups = [{"groupName": company_name, "keywords": [company_name]}]
    if auth_info is None:
        client_id = "M7nYHdcOIfm6r82az8CV"
        client_secret = "1c5R_C3OxL"
    else:
        auth = auth_info.get_auth()
        client_id = auth['id']
        client_secret = auth['secret']
    url = "https://openapi.naver.com/v1/datalab/search"
    body = json.dumps(
        {
            "startDate": startDate,
            "endDate": endDate,
            "timeUnit": timeUnit,
            "keywordGroups": keywordGroups,
        },
        ensure_ascii=False,
    )
    request = urllib.request.Request(url)
    request.add_header("X-Naver-Client-Id", client_id)
    request.add_header("X-Naver-Client-Secret", client_secret)
    request.add_header("Content-Type", "application/json")
    response = urllib.request.urlopen(request, data=body.encode("utf-8"))
    return json.loads(response.read())


def get_api_data(biz_no, company_name, start_date=None, auth_info=None):
    result = call_api_naver_trend(biz_no, company_name, start_date, auth_info)
    if type(result) == dict:
        return result
    else:
        return None


def api_naver_trend(biz_no, company_name, start_date=None, auth_info=None):
    result_list = []
    data_lst = []
    result = get_api_data(biz_no, company_name, start_date, auth_info)
    if not result:
        return ""
    else:
        for l in result["results"]:
            data_lst.extend(
                list(
                    map(
                        lambda o: {"date": o["period"], "ratio": float(o["ratio"]),},
                        l["data"],
                    )
                )
            )
        naver_trend_data = {
            "BusinessNum": biz_no,
            "DataType": DataType,
            "SearchDate": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "SearchID": "autoSystem",
            "Data": {
                "startDate": result["startDate"],
                "endDate": result["endDate"],
                "timeUnit": result["timeUnit"],
                "data": data_lst,
            },
        }
        result_list.append(naver_trend_data)
        return result_list


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


def main(biz_no, CompanyName):
    result = []
    CompanyName = funcs.re_type(CompanyName)
    result.append(api_naver_trend(biz_no, CompanyName))
    result = list(itertools.chain(*result))

    if FlagPrintData:
        print(result)

    if bool(result) and FlagSaveData:
        for data in result:
            flag1 = result_save_function(data)
            update_searchdate_function(biz_no, flag1)


if __name__ == "__main__":
    main("2208736743", "이노그리드")
