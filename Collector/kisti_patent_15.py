import datetime
import sys
from os import path
from libraries import *
from .Funcs import funcs, check_data_pattern, write_log, timeout, retry_function

today = datetime.datetime.today()
DataType = "kisti_patent"
SearchDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
SearchID = "autoSystem"
FlagPrintData = True  # True: 저장할 최종 데이터 출력
FlagSaveData = True  # True: 엘라스틱에 데이터 저장
MAX_RETRY = 5
TIMEOUT = 30

# 특허: <출원인>을 기업명으로 검색
def api_kisti_patent(biz_no, company_name, start_date):
    searchField = "PA"  # 출원인:PA
    Data = []
    start_date = start_date
    data = get_api_data(
        biz_no=biz_no,
        startPosition=1,
        displayCount=1,
        searchField=searchField,
        query=company_name,
    )
    if data:
        total_count = int(data["resultSummary"]["totalCount"])
        if total_count > 0:
            # print('total_count: {}'.format(total_count))
            if total_count < 10:
                displayCount = 1
            elif total_count < 100:
                displayCount = 5
            elif total_count < 1000:
                displayCount = 10
            else:
                displayCount = 50
            
            # 특허 최대 개수 10개 제한 (삼성같은 경우 너무 많이 출력됨)
            if total_count > 10:
                total_count = 10
                displayCount = 1

            # for i in tqdm(range(0, math.ceil(total_count / displayCount))):
            for i in tqdm(range(total_count)):
                if i>=196:
                    print("here")
                    
                # startPosition = i * displayCount + 1      # 기존의 방식(모든 특허 가져올 경우)
                startPosition = i + 1   # 특허 최대 개수 10개로 제한했기 때문에 순차적으로 가져오도록 수정

                if startPosition > 9999:
                    break
                data = get_api_data(
                    biz_no=biz_no,
                    startPosition=startPosition,
                    displayCount=displayCount,
                    searchField=searchField,
                    query=company_name,
                )

                # 날짜 확인
                # print('env start_date : ', int(start_date.replace('.', '')))
                # print(type(int(start_date.replace('.', ''))))
                # print("applicationDate : ", int(data['outputData'][0]['patentInfo']['applicationDate']))
                # print(type(int(data['outputData'][0]['patentInfo']['applicationDate'])))
                
                # 날짜 비교하여 특허 중복 수집 방지
                if int(start_date.replace('.', '')) > int(data['outputData'][0]['patentInfo']['applicationDate']) :
                    break

                if data:
                    result = parser_kisti_patent_json(company_name, data)
                    if result:
                        Data += result
                else:
                    continue

        return Data
    else:
        return ""


def parser_kisti_patent_json(company_name, data):
    PatentInfo = []
    try:
        output = data["outputData"]
        for item in output:
            patentInfo = item["patentInfo"]
            tmp = dict()

            applicantsInfo = []
            applicantsInfo_data = patentInfo["applicantsInfo"]
            if len(applicantsInfo_data) <= 1:
                if applicantsInfo_data == [[]]:
                    applicantsInfo = []
                else:
                    applicantsInfo.append(
                        applicantsInfo_data[0].replace(",", "").replace(".", "")
                    )
            else:
                applicantsInfo_data = [x for x in applicantsInfo_data if x != []]
                applicantsInfo = [
                    ap["#text"].replace(",", "").replace(".", "")
                    for ap in applicantsInfo_data
                ]
                applicantsInfo = list(set(applicantsInfo))

            # Data.applicantsInfo(출원인)에 기업명이 있어야한다.
            applicantsInfo_ = [funcs.re_type(i) for i in applicantsInfo]
            break_sign = True
            for ai in applicantsInfo_:
                if company_name in ai:
                    break_sign = False
            if break_sign:
                continue

            tmp["kistiID"] = patentInfo["@kistiID"]
            tmp["patentTitle"] = patentInfo["patentTitle"]
            tmp["abstract"] = patentInfo["abstract"]
            tmp["country"] = patentInfo["country"]
            tmp["nationCode"] = patentInfo["nationCode"]
            tmp["pubReg"] = patentInfo["pubReg"]

            ipcInfo = []
            ipcInfo_data = patentInfo["ipcInfo"]
            if len(ipcInfo_data) <= 1:
                if ipcInfo_data == [[]]:
                    ipcInfo = []
                else:
                    ipcInfo.append(ipcInfo_data[0].replace(",", "").replace(".", ""))
            else:
                ipcInfo_data = [x for x in ipcInfo_data if x != []]
                ipcInfo = [
                    ap["#text"].replace(",", "").replace(".", "") for ap in ipcInfo_data
                ]
                ipcInfo = list(set(ipcInfo))

            tmp["ipcInfo"] = ipcInfo if ipcInfo else None

            koreanauthorinfo = []
            koreanauthorinfo_data = patentInfo["koreanauthorinfo"]
            if len(koreanauthorinfo_data) <= 1:
                if koreanauthorinfo_data == [[]]:
                    koreanauthorinfo = []
                else:
                    koreanauthorinfo.append(
                        koreanauthorinfo_data[0].replace(",", "").replace(".", "")
                    )
            else:
                koreanauthorinfo_data = [x for x in koreanauthorinfo_data if x != []]
                koreanauthorinfo = [
                    ap["#text"].replace(",", "").replace(".", "")
                    for ap in koreanauthorinfo_data
                ]
                koreanauthorinfo = list(set(koreanauthorinfo))

            englishauthorinfo = []
            englishauthorinfo_data = patentInfo["englishauthorinfo"]
            if len(englishauthorinfo_data) <= 1:
                if englishauthorinfo_data == [[]]:
                    englishauthorinfo = []
                else:
                    englishauthorinfo.append(
                        englishauthorinfo_data[0].replace(",", "").replace(".", "")
                    )
            else:
                englishauthorinfo_data = [x for x in englishauthorinfo_data if x != []]
                englishauthorinfo = [
                    ap["#text"].replace(",", "").replace(".", "")
                    for ap in englishauthorinfo_data
                ]
                englishauthorinfo = list(set(englishauthorinfo))

            tmp["koreanauthorinfo"] = koreanauthorinfo if koreanauthorinfo else None
            tmp["englishauthorinfo"] = englishauthorinfo if englishauthorinfo else None
            tmp["applicantsInfo"] = applicantsInfo if applicantsInfo else None
            tmp["applicationDate"] = (
                f'{patentInfo["applicationDate"][0:4]}-{patentInfo["applicationDate"][4:6]}-{patentInfo["applicationDate"][6:8]}'
                if patentInfo["applicationDate"]
                else None
            )
            tmp["applicationNumber"] = (
                patentInfo["applicationNumber"]
                if patentInfo["applicationNumber"]
                else None
            )
            tmp["publicationDate"] = (
                f'{patentInfo["publicationDate"][0:4]}-{patentInfo["publicationDate"][4:6]}-{patentInfo["publicationDate"][6:8]}'
                if patentInfo["publicationDate"]
                else None
            )
            tmp["publicationNumber"] = (
                patentInfo["publicationNumber"]
                if patentInfo["publicationNumber"]
                else None
            )
            tmp["issueDate"] = (
                f'{patentInfo["issueDate"][0:4]}-{patentInfo["issueDate"][4:6]}-{patentInfo["issueDate"][6:8]}'
                if patentInfo["issueDate"]
                else None
            )
            tmp["patentNumber"] = (
                patentInfo["patentNumber"] if patentInfo["patentNumber"] else None
            )
            tmp["noticeDate"] = (
                f'{patentInfo["noticeDate"][0:4]}-{patentInfo["noticeDate"][4:6]}-{patentInfo["noticeDate"][6:8]}'
                if patentInfo["noticeDate"]
                else None
            )
            tmp["noticeNumber"] = (
                patentInfo["noticeNumber"] if patentInfo["noticeNumber"] else None
            )
            tmp["deepLink"] = patentInfo["deepLink"] if patentInfo["deepLink"] else None

            tmp["ipcInfo"] = None if tmp["ipcInfo"] == "" else tmp["ipcInfo"]
            tmp["koreanauthorinfo"] = (
                None if tmp["koreanauthorinfo"] == "" else tmp["koreanauthorinfo"]
            )
            tmp["englishauthorinfo"] = (
                None if tmp["englishauthorinfo"] == "" else tmp["englishauthorinfo"]
            )
            tmp["applicantsInfo"] = (
                None if tmp["applicantsInfo"] == "" else tmp["applicantsInfo"]
            )
            tmp["publicationDate"] = (
                None if tmp["publicationDate"] == "" else tmp["publicationDate"]
            )
            tmp["publicationNumber"] = (
                None if tmp["publicationNumber"] == "" else tmp["publicationNumber"]
            )

            PatentInfo.append(tmp)
    except Exception as e:
        print("parser_kisti_patent_json Error : {}".format(e))
        PatentInfo = []
    return PatentInfo

"""
@retry_function.retry(MAX_RETRY, dict)
@timeout.timeout(TIMEOUT)
"""

# kisti_patent api url 예시
# https://openapi.ndsl.kr/itemsearch.do?keyValue=08497314&target=PATENT&searchField=PA&query=대가파우더시스템
# https://openapi.ndsl.kr/itemsearch.do?keyValue=08497314&target=PATENT&searchField=PA&sortby=adate&returnType=json&query=대가파우더시스템&callback=callback
def call_api_kisti_patent(biz_no, startPosition, displayCount, searchField, query):
    query = funcs.re_type(query)
    url = "http://openapi.ndsl.kr/itemsearch.do?"
    queryParams = urlencode(
        {
            quote_plus("keyValue"): unquote("08497314"),
            # 특허전체:PATENT, 한국특허전체:KPAT, 한국공개특허:KUPA, 한국등록특허:KPTN, 한국공개실용신안:KUUM, 한국등록실용신안:KUMO, 한국의장등록:KODE,
            # 해외특허전체:FPAT, 미국특허전체:UPAT, 미국등록특허:USPA, 미국공개특허:USAP, 일본특허:JEPA, 국제특허:WOPA, 유럽특허:EUPA
            quote_plus("target"): "PATENT",
            # 전체:BI, 발명의 명칭:TI, 출원인:PA, 출원번호:AN, 출원일자:AD, 공개번호:UN, 공개일자:UD, 등록번호:RN,
            # 우선권번호:PRAN, 우선권일자:PRAD, 국제출원번호:IPN, 국제출원일자:IPD, 국제공개번호:IUN, 국제공개일자:IUD,
            # 초록:AB, 발명자:IN, 대리인:AG, IPC분류:IC, USC분류:UC, 대표IPC:ID, 디자인분류:MC
            quote_plus("searchField"): searchField,
            quote_plus("displayCount"): displayCount,  # 10 ~ 100   한페이지에 보여줄 수 있는 특허 개수 조정(10단위로 나눠서 보여줌)
            quote_plus("startPosition"): startPosition,  # 1 ~      특허가 10개라고 예시를 들어보면 몇번째 특허부터 보고싶은지 지정
            # 없으면 정확도 순 정렬
            # 출원일자:adate, 발명의명칭:title, 출원인명:aname, 발명자명:iname, 출원번호:anum,
            # 공개번호:unum, 공개일자:udate, 등록번호:rnum, 등록일자:rdate, 국가:country, IPC:ic
            quote_plus("sortby"): "adate",
            quote_plus("returnType"): "json",  # xml/json
            quote_plus("query"): query,  # 검색어 입력
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


def get_api_data(biz_no, startPosition, displayCount, searchField, query):
    result = call_api_kisti_patent(
        biz_no, startPosition, displayCount, searchField, query
    )
    if type(result) == dict:
        return result
    else:
        return None


def process(biz_no, company_name, start_date):
    result_list = []
    try:
        Data = api_kisti_patent(biz_no, company_name, start_date)
        # time.sleep(1)
        Data = [i for i in Data if i["kistiID"]]

        if len(Data) == 0:
            kisti_patent_data = {
                "BusinessNum": biz_no,
                "DataType": DataType,
                "SearchDate": SearchDate,
                "SearchID": SearchID,
                "Data": None,
            }
            result_list.append(copy.deepcopy(kisti_patent_data))
        else:
            for data in Data:
                kisti_patent_data = {
                    "BusinessNum": biz_no,
                    "DataType": DataType,
                    "SearchDate": SearchDate,
                    "SearchID": SearchID,
                    "Data": None if len(data) == 0 else data,
                }
                result_list.append(copy.deepcopy(kisti_patent_data))
        return result_list
    except Exception as e:
        write_log.write_log(BIZ_NO=biz_no, DATA_TYPE=DataType, ERR_LOG=e)


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

def main(biz_no: str, companyName: str, start_date: str):

    result = []
    result.append(process(biz_no, companyName, start_date))
    result = list(itertools.chain(*result))

    if FlagPrintData:
        print(result)

    if bool(result) and FlagSaveData:
        for data in result:
            flag1 = result_save_function(data)
            update_searchdate_function(biz_no, flag1)


if __name__ == "__main__":
    main("1010826653", "블루")
    # main("1010117344", "썬텔")
    # main("5058211726", "한국정보화진흥원")
    # main("2208736743", "이노그리드")
