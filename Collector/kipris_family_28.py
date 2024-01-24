import datetime
import sys
from os import path
from libraries import *
from Funcs import funcs, check_data_pattern, write_log, timeout, retry_function

today = datetime.datetime.today()
DataType = "kipris_family"
FlagPrintData = True  # True: 저장할 최종 데이터 출력
FlagSaveData = True  # True: 엘라스틱에 데이터 저장
KIPRIS_KEY = "bKjesNzLQsxnzovfzxW7Pf=On12EubsLZI0OOICx8Yk="
MAX_RETRY = 5
TIMEOUT = 30


@retry_function.retry(MAX_RETRY, dict)
@timeout.timeout(TIMEOUT)
def call_api_kipris_family(biz_no, KorNumber, pageNo):
    url = "http://plus.kipris.or.kr/kipo-api/kipi/patFamInfoSearchService/getAppNoPatFamInfoSearch?"
    apprvKey = unquote(KIPRIS_KEY)
    queryParams = urlencode(
        {
            quote_plus("ServiceKey"): apprvKey,
            quote_plus("applicationNumber"): KorNumber,
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


def get_api_data(biz_no, KorNumber, pageNo):
    result = call_api_kipris_family(biz_no, KorNumber, pageNo)
    if type(result) == dict:
        return result
    else:
        return None


def api_kipris_family(biz_no, KorNumber, pageNo):

    kipris_family = kipris_family_template()

    family_result = []

    result_list = []

    api_data = get_api_data(biz_no, KorNumber, pageNo)

    if not api_data:
        return ""

    if api_data["response"].get("body"):
        if api_data["response"].get("body")["items"] == None:  # 데이터가 없을때 실행되는 단락

            kipris_family["KorNumber"] = str(KorNumber)
            kipris_family["docdbFamilyID"] = None
            kipris_family["applicationCountryCode"] = None
            kipris_family["applicationDate"] = None
            kipris_family["applicationKindCode"] = None
            kipris_family["applicationNumber"] = None
            kipris_family["publicationCountryCode"] = None
            kipris_family["publicationDate"] = None
            kipris_family["publicationKindCode"] = None
            kipris_family["publicationNumber"] = None

            KIPRIS_FAMILY = {
                "BusinessNum": str(biz_no),
                "DataType": DataType,
                "SearchDate": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": kipris_family,
            }
            result_list.append(copy.deepcopy(KIPRIS_FAMILY))

            return result_list

        else:
            if type(api_data["response"]["body"]["items"]["item"]) == dict:
                family_result.append(api_data["response"]["body"]["items"]["item"])

            elif type(api_data["response"]["body"]["items"]["item"]) == list:
                [
                    family_result.append(i)
                    for i in api_data["response"]["body"]["items"]["item"]
                ]
    else:
        pass

    for i in family_result:
        kipris_family["KorNumber"] = KorNumber
        for z in list(kipris_family_template().keys())[1:]:
            kipris_family[z] = i.get(z)
            if z in ["applicationDate", "publicationDate"] and bool(i.get(z)):
                kipris_family[z] = "{}-{}-{}".format(
                    i.get(z)[:4], i.get(z)[4:6], i.get(z)[6:]
                )

        kipris_family["applicationDate"] = validate_date(
            kipris_family["applicationDate"]
        )
        kipris_family["publicationDate"] = validate_date(
            kipris_family["publicationDate"]
        )

        KIPRIS_FAMILY = {
            "BusinessNum": str(biz_no),
            "DataType": DataType,
            "SearchDate": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "SearchID": "autoSystem",
            "Data": kipris_family,
        }
        result_list.append(copy.deepcopy(KIPRIS_FAMILY))

    return result_list


def validate_date(date_text):  # 날짜 형식 체크
    if date_text == None:
        return None
    date_text = date_text.split("|")[0]
    try:
        datetime.datetime.strptime(date_text, "%Y-%m-%d")
        return date_text
    except ValueError:
        print("Incorrect data format({0}), should be YYYY-MM-DD".format(date_text))
        return None


def kipris_family_template():
    family = {
        "KorNumber": None,
        "docdbFamilyID": None,
        "applicationCountryCode": None,
        "applicationDate": None,
        "applicationKindCode": None,
        "applicationNumber": None,
        "publicationCountryCode": None,
        "publicationDate": None,
        "publicationKindCode": None,
        "publicationNumber": None,
    }

    return family


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

def get_result(biz_no, company_name=None, ceo_name=None, webDriver=None, console_print=False, err_msg=None):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"DataType": "kisti_patent"}},
                    {"exists": {"field": "Data.kistiID"}},
                    {"match": {"BusinessNum": biz_no}},
                ]
            }
        }
    }

    es_list = funcs.get_data_from_es("source_data", query)
    
    head = {
        "BusinessNum": str(biz_no),
        "DataType": DataType,
        "SearchDate": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "SearchID": "autoSystem",
        }
        
    if not es_list:
        head['Data'] = None
        return [head]
    
    es_list = [i["_source"]["Data"]["kistiID"] for i in es_list]

    results = []
    for k in es_list:
        result = []
        KorNumber = k.replace("KOR", "")
        pageNo = 1
        result.append(api_kipris_family(biz_no, KorNumber, pageNo))
        result = list(itertools.chain(*result))
        results.extend(result)
    return results

def main(biz_no):

    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"DataType": "kisti_patent"}},
                    {"exists": {"field": "Data.kistiID"}},
                    {"match": {"BusinessNum": biz_no}},
                ]
            }
        }
    }

    es_list = funcs.get_data_from_es("source_data", query)

    if es_list:
        es_list = [i["_source"]["Data"]["kistiID"] for i in es_list]

        for k in tqdm(es_list):
            result = []
            KorNumber = k.replace("KOR", "")
            pageNo = 1
            result.append(api_kipris_family(biz_no, KorNumber, pageNo))
            result = list(itertools.chain(*result))
            if FlagPrintData:
                print(result)

            if bool(result) and FlagSaveData:
                for data in result:
                    flag1 = result_save_function(data)
                    update_searchdate_function(biz_no, flag1)
    else:
        result = [
            {
                "BusinessNum": str(biz_no),
                "DataType": DataType,
                "SearchDate": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": None,
            }
        ]
        if FlagPrintData:
            print(result)

        if bool(result) and FlagSaveData:
            for data in result:
                flag1 = result_save_function(data)
                update_searchdate_function(biz_no, flag1)


if __name__ == "__main__":
    main("5090340683")
