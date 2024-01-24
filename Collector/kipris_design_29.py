import datetime
import sys
from os import path
from libraries import *
from Funcs import funcs, check_data_pattern, write_log, timeout, retry_function

today = datetime.datetime.today()
DataType = "kipris_design"
FlagPrintData = True  # True: 저장할 최종 데이터 출력
FlagSaveData = True  # True: 엘라스틱에 데이터 저장
KIPRIS_KEY = "bKjesNzLQsxnzovfzxW7Pf=On12EubsLZI0OOICx8Yk="
MAX_RETRY = 5
TIMEOUT = 30


@retry_function.retry(MAX_RETRY, dict)
@timeout.timeout(TIMEOUT)
def call_api_kipris_design(biz_no, companyName, pageNo):
    url = "http://plus.kipris.or.kr/kipo-api/kipi/designInfoSearchService/getAdvancedSearch?"
    apprvKey = unquote(KIPRIS_KEY)
    queryParams = urlencode(
        {
            quote_plus("ServiceKey"): apprvKey,
            quote_plus("applicantName"): companyName,
            quote_plus("open"): "true",
            quote_plus("rejection"): "true",
            quote_plus("destory"): "true",
            quote_plus("cancle"): "true",
            quote_plus("notice"): "true",
            quote_plus("registration"): "true",
            quote_plus("invalid"): "true",
            quote_plus("abandonment"): "true",
            quote_plus("simi"): "true",
            quote_plus("part"): "true",
            quote_plus("etc"): "true",
            quote_plus("destroy"): "true",
            quote_plus("numOfRows"): 500,
            quote_plus("pageNo"): pageNo,
        },
        encoding="utf-8",
    )

    req = requests.get(url + queryParams)
    if 200 <= req.status_code < 300:
        result = json.loads(
            json.dumps(xmltodict.parse(str(bs(req.text, "xml"))), ensure_ascii=False)
        )
        if result.get("response") == None:
            raise Exception("API_RESPONSE_ERROR")
        else:
            return result
    else:
        raise Exception(f"STATUS_CODE_{req.status_code}_API_SERVER_ERROR")


def get_api_data(biz_no, companyName, pageNo):
    result = call_api_kipris_design(biz_no, companyName, pageNo)
    if type(result) == dict:
        return result
    else:
        return None


def api_kipris_design(biz_no, companyName, pageNo):

    kipris_design = kipris_design_template()

    design_result = []

    result_list = []

    api_data = get_api_data(biz_no, companyName, pageNo)

    if not api_data:
        return ""

    if api_data["response"].get("body"):
        if int(api_data["response"]["count"]["totalCount"]) == 0:  # 데이터가 없을때 실행되는 단락
            KIPRIS_DESIGN = {
                "BusinessNum": str(biz_no),
                "DataType": DataType,
                "SearchDate": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": None,
            }

            result_list.append(copy.deepcopy(KIPRIS_DESIGN))
            return result_list

        else:
            if type(api_data["response"]["body"]["items"]["item"]) == dict:
                design_result.append(api_data["response"]["body"]["items"]["item"])

            elif type(api_data["response"]["body"]["items"]["item"]) == list:
                [
                    design_result.append(i)
                    for i in api_data["response"]["body"]["items"]["item"]
                ]
                if int(api_data["response"]["count"]["totalCount"]) > 500:
                    loop = math.ceil(
                        int(api_data["response"]["count"]["totalCount"]) / 500
                    )

                    for i in tqdm(range(loop - 1)):
                        pageNo += 1
                        result = get_api_data(biz_no, companyName, pageNo)
                        if not result:
                            pass

                        elif result["response"].get("body") == None:
                            pass

                        elif type(result["response"]["body"]["items"]["item"]) == dict:
                            design_result.append(
                                result["response"]["body"]["items"]["item"]
                            )

                        elif result["response"].get("body").get("items"):
                            [
                                design_result.append(i)
                                for i in result["response"]["body"]["items"]["item"]
                            ]
    else:
        pass

    for i in design_result:
        for z in list(kipris_design_template().keys()):
            kipris_design[z] = i[z]
            if z in [
                "agentName",
                "applicantName",
                "designMainClassification",
                "dsShpClssCd",
                "inventorName",
            ] and bool(i[z]):
                kipris_design[z] = i[z].split("|")
            if z in [
                "applicationDate",
                "openDate",
                "priorityDate",
                "publicationDate",
                "registrationDate",
            ] and bool(i[z]):
                kipris_design[z] = "{}-{}-{}".format(i[z][:4], i[z][4:6], i[z][6:])

        kipris_design["applicationDate"] = validate_date(
            kipris_design["applicationDate"]
        )
        kipris_design["openDate"] = validate_date(kipris_design["openDate"])
        kipris_design["priorityDate"] = validate_date(kipris_design["priorityDate"])
        kipris_design["publicationDate"] = validate_date(
            kipris_design["publicationDate"]
        )
        kipris_design["registrationDate"] = validate_date(
            kipris_design["registrationDate"]
        )

        if funcs.re_type(companyName) not in [
            funcs.re_type(i) for i in kipris_design["applicantName"]
        ]:
            continue
        else:
            KIPRIS_DESIGN = {
                "BusinessNum": str(biz_no),
                "DataType": DataType,
                "SearchDate": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": kipris_design,
            }
            result_list.append(copy.deepcopy(KIPRIS_DESIGN))

    if result_list == []:
        KIPRIS_DESIGN = {
            "BusinessNum": str(biz_no),
            "DataType": DataType,
            "SearchDate": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "SearchID": "autoSystem",
            "Data": None,
        }

        result_list.append(copy.deepcopy(KIPRIS_DESIGN))
    return result_list


def validate_date(date_text):
    if date_text == None:
        return None
    date_text = date_text.split("|")[0]

    try:
        datetime.datetime.strptime(date_text, "%Y-%m-%d")
        return date_text
    except ValueError:
        print("Incorrect data format({0}), should be YYYY-MM-DD".format(date_text))
        return None


def kipris_design_template():
    design = {
        "agentName": None,
        "appReferenceNumber": None,
        "applicantName": None,
        "applicationDate": None,
        "applicationNumber": None,
        "applicationStatus": None,
        "articleName": None,
        "designMainClassification": None,
        "designNumber": None,
        "dsShpClssCd": None,
        "internationalRegisterDate": None,
        "internationalRegisterNumber": None,
        "inventorName": None,
        "openDate": None,
        "openNumber": None,
        "priorityDate": None,
        "priorityNumber": None,
        "publicationDate": None,
        "publicationNumber": None,
        "regReferenceNumber": None,
        "registrationDate": None,
        "registrationNumber": None,
    }

    return design


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

def get_result(biz_no, company_name, ceo_name=None, webDriver=None, console_print=False, err_msg=None):
    result = []
    pageNo = 1
    result.append(api_kipris_design(biz_no, company_name, pageNo))
    result = list(itertools.chain(*result))

    return result

def main(biz_no: str, companyName: str):

    result = []
    pageNo = 1
    result.append(api_kipris_design(biz_no, companyName, pageNo))
    result = list(itertools.chain(*result))

    if FlagPrintData:
        print(result)

    if bool(result) and FlagSaveData:
        for data in result:
            flag1 = result_save_function(data)
            update_searchdate_function(biz_no, flag1)


if __name__ == "__main__":
    main("2208736743", "이노그리드")
