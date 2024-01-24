import datetime
import sys
from os import path
from libraries import *
from Funcs import funcs, check_data_pattern, write_log, timeout, retry_function

today = datetime.datetime.today()
DataType = "kipris_mark"
FlagPrintData = True  # True: 저장할 최종 데이터 출력
FlagSaveData = True  # True: 엘라스틱에 데이터 저장
KIPRIS_KEY = "bKjesNzLQsxnzovfzxW7Pf=On12EubsLZI0OOICx8Yk="
MAX_RETRY = 5
TIMEOUT = 30


@retry_function.retry(MAX_RETRY, dict)
@timeout.timeout(TIMEOUT)
def call_api_kipris_mark(biz_no, companyName, pageNo):
    if companyName:
        companyName = companyName.replace(",", "")

    url = "http://plus.kipris.or.kr/kipo-api/kipi/trademarkInfoSearchService/getAdvancedSearch?"
    apprvKey = unquote("bKjesNzLQsxnzovfzxW7Pf=On12EubsLZI0OOICx8Yk=")
    queryParams = urlencode(
        {
            quote_plus("ServiceKey"): apprvKey,
            quote_plus("applicantName"): companyName,
            quote_plus("application"): "true",
            quote_plus("registration"): "true",
            quote_plus("refused"): "true",
            quote_plus("expiration"): "true",
            quote_plus("refused"): "true",
            quote_plus("expiration"): "true",
            quote_plus("withdrawal"): "true",
            quote_plus("publication"): "true",
            quote_plus("cancel"): "true",
            quote_plus("abandonment"): "true",
            quote_plus("trademark"): "true",
            quote_plus("serviceMark"): "true",
            quote_plus("trademarkServiceMark"): "true",
            quote_plus("businessEmblem"): "true",
            quote_plus("collectiveMark"): "true",
            quote_plus("internationalMark"): "true",
            quote_plus("character"): "true",
            quote_plus("figure"): "true",
            quote_plus("compositionCharacter"): "true",
            quote_plus("figureComposition"): "true",
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
    result = call_api_kipris_mark(biz_no, companyName, pageNo)
    if type(result) == dict:
        return result
    else:
        return None


def api_kipris_mark(biz_no, companyName, pageNo):

    kipris_mark = kipris_mark_template()

    mark_result = []

    result_list = []

    api_data = get_api_data(biz_no, companyName, pageNo)

    if not api_data:
        return ""

    if api_data["response"].get("body"):
        if int(api_data["response"]["count"]["totalCount"]) == 0:  # 데이터가 없을때 실행되는 단락
            KIPRIS_MARK = {
                "BusinessNum": str(biz_no),
                "DataType": DataType,
                "SearchDate": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": None,
            }

            result_list.append(copy.deepcopy(KIPRIS_MARK))
            return result_list

        else:
            if type(api_data["response"]["body"]["items"]["item"]) == dict:
                mark_result.append(api_data["response"]["body"]["items"]["item"])

            elif type(api_data["response"]["body"]["items"]["item"]) == list:
                [
                    mark_result.append(i)
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
                            mark_result.append(
                                result["response"]["body"]["items"]["item"]
                            )

                        elif result["response"].get("body").get("items"):
                            [
                                mark_result.append(i)
                                for i in result["response"]["body"]["items"]["item"]
                            ]
    else:
        pass

    for i in mark_result:
        for z in list(kipris_mark_template().keys()):
            kipris_mark[z] = i[z]
            if z in [
                "agentName",
                "applicantName",
                "classificationCode",
                "regPrivilegeName",
                "viennaCode",
            ] and bool(i[z]):
                kipris_mark[z] = i[z].split("|")
            if z in [
                "applicationDate",
                "priorityDate",
                "publicationDate",
                "registrationDate",
            ] and bool(i[z]):
                kipris_mark[z] = "{}-{}-{}".format(i[z][:4], i[z][4:6], i[z][6:])

        kipris_mark["applicationDate"] = validate_date(kipris_mark["applicationDate"])
        kipris_mark["priorityDate"] = validate_date(kipris_mark["priorityDate"])
        kipris_mark["publicationDate"] = validate_date(kipris_mark["publicationDate"])
        kipris_mark["registrationDate"] = validate_date(kipris_mark["registrationDate"])

        if funcs.re_type(companyName) not in [
            funcs.re_type(i) for i in kipris_mark["applicantName"]
        ]:
            continue
        else:
            KIPRIS_MARK = {
                "BusinessNum": str(biz_no),
                "DataType": DataType,
                "SearchDate": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": kipris_mark,
            }
            result_list.append(copy.deepcopy(KIPRIS_MARK))

    if result_list == []:
        KIPRIS_MARK = {
            "BusinessNum": str(biz_no),
            "DataType": DataType,
            "SearchDate": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "SearchID": "autoSystem",
            "Data": None,
        }

        result_list.append(copy.deepcopy(KIPRIS_MARK))
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


def kipris_mark_template():
    mark = {
        "agentName": None,
        "applicantName": None,
        "applicationDate": None,
        "applicationNumber": None,
        "applicationStatus": None,
        "classificationCode": None,
        "priorityDate": None,
        "priorityNumber": None,
        "publicationDate": None,
        "publicationNumber": None,
        "regPrivilegeName": None,
        "registrationDate": None,
        "registrationNumber": None,
        "title": None,
        "viennaCode": None,
    }

    return mark


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
    result.append(api_kipris_mark(biz_no, company_name, pageNo))
    result = list(itertools.chain(*result))

    return result

def main(biz_no, companyName):

    result = []
    pageNo = 1
    result.append(api_kipris_mark(biz_no, companyName, pageNo))
    result = list(itertools.chain(*result))

    if FlagPrintData:
        print(result)

    if bool(result) and FlagSaveData:
        for data in result:
            flag1 = result_save_function(data)
            update_searchdate_function(biz_no, flag1)


if __name__ == "__main__":
    main("2208736743", "이노그리드")
