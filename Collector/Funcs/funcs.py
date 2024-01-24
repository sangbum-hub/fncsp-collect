import datetime
import operator
import re

import pymysql
from elasticsearch import Elasticsearch


def get_mysql_conn(my_host, my_user, my_passwd, my_database, my_connect_timeout):
    conn = None
    try:
        conn = pymysql.connect(
            host=my_host,
            user=my_user,
            passwd=my_passwd,
            database=my_database,
            connect_timeout=my_connect_timeout,
        )
    except Exception as e:
        print(e)
    return conn


def get_bizNo_mysql(
    DataType, 
    sql_=None, 
    conn = None
    ):  # mysql 에서 사업자 번호 가져오는 함수
    if not conn:
        conn = get_mysql_conn()
    try:
        # conn = get_mysql_conn()
        cur = conn.cursor()
        sql = (
            """
        SELECT BIZ_NO, COMPANY_NAME, CEO_NAME, """
            + DataType
            + """ FROM SOURCE_DATA_STATUS 
            ORDER BY """
            + DataType
            + """ ASC, BIZ_NO ASC"""
        )
        if sql_ is not None:
            sql = sql_
        cur.execute(sql)
        for i in cur:
            yield i
        # res = cur.fetchall()
        cur.close()
        conn.close()
        # return res
    except Exception as e:
        return None


def get_biz_no_list():  # get_bizNo_mysql 함수에서 받아온 리스트 전처리 하는 함수
    try:
        sql = """SELECT BIZ_NO FROM SOURCE_DATA_STATUS ORDER BY BIZ_NO ASC"""
        biz_no_list = get_bizNo_mysql(DataType="", sql_=sql)
        biz_no_list = [str(i[0]) for i in biz_no_list]
        return biz_no_list
    except:
        return []


# 리턴값: 튜플 (사업자번호, 기업명, 대표자명, 타입 데이터 적재 최신 날짜)
# 데이터 적재 x 시, None/Null 값
def get_only_bizNo_mysql(my_host,my_user,my_passwd,my_database,my_connect_timeout):
    try:
        conn = pymysql.connect(
            host=my_host,
            user=my_user,
            passwd=my_passwd,
            database=my_database,
            connect_timeout=my_connect_timeout,
        )
        cur = conn.cursor()
        sql = "SELECT BIZ_NO FROM SOURCE_DATA_STATUS ORDER BY BIZ_NO ASC"
        cur.execute(sql)
        res = cur.fetchall()
        cur.close()
        conn.close()
        return res
    except Exception as e:
        return None


##################### ELASTIC SEARCH #####################
# 엘라스틱서치 연결
def get_es_conn(es_host,es_port,es_http_auth,es_timeout,es_max_retries,es_retry_on_timeout):
    try:
        es = Elasticsearch(
            host=es_host,
            port=es_port,
            http_auth=es_http_auth,
            timeout=es_timeout,
            max_retries=es_max_retries,
            retry_on_timeout=es_retry_on_timeout,
        )
        return es
    except Exception as e:
        print(e)
        return None


def update_searchDate_mysql(DataType, biz_no_lst: list, my_host,my_user,my_passwd,my_database,my_connect_timeout):
    try:
        conn = pymysql.connect(
            host=my_host,
            user=my_user,
            passwd=my_passwd,
            database=my_database,
            connect_timeout=my_connect_timeout,
        )
        cur = conn.cursor()
        for biz_no, search_date in biz_no_lst:
            sql = f"UPDATE SOURCE_DATA_STATUS \
                    SET {DataType} = '{search_date.replace('-','')}' \
                    WHERE BIZ_NO={biz_no}"
            cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
        return "Success"
    except:
        return "Fail"


# 데이터 호출
def get_data_from_es(index, query,es_scroll="60m"):
    import json
    with open("./envs.json", "r", encoding="utf-8") as f:
        envs = json.load(f)
    es_info = {x:envs[x] for x in list(filter(lambda o: "es_"in o, envs.keys()))}
    Data = []
    try:
        es = get_es_conn(
                        es_info["es_host"],
                        es_info["es_port"],
                        (es_info["es_user"],es_info["es_password"]),
                        es_info["es_timeout"],
                        es_info["es_max_retries"],
                        es_info["es_retry_on_timeout"],
                    )
        # 한번에 가져올 데이터 수 (사이즈가 작을수록 빠르게 처리)
        # 엘라스틱서치 호출
        data = es.search(
            index=index, scroll=es_scroll, body=query, track_total_hits=True
        )
        total_num = int(data["hits"]["total"]["value"])
        # print("Total num of Data: {}".format(total_num))

        # import json
        # print(json.dumps(data, indent=2, ensure_ascii=False))

        # 스크롤 시작
        # idx = 0
        sid = data["_scroll_id"]
        while True:
            # idx += 1
            # print("{} Scrolling... {}".format(idx, scroll_size))
            # print(Data[-1])  # 데이터 검수
            Data += data["hits"]["hits"]

            # 스크롤할 id 업데이트
            data = es.scroll(scroll_id=sid, scroll=es_scroll)
            sid = data["_scroll_id"]

            num = len(Data)
            # print(num)

            if num >= total_num:
                break

        es.clear_scroll(scroll_id=sid)
        es.close()

    except Exception as e:
        print(e)

    return Data


def get_data1_from_es(index, query):
    result = None
    try:
        es = get_es_conn()
        res = es.search(index=index, body=query, size=1)
        if res["hits"]["total"]["value"] > 0:
            result = res["hits"]["hits"][0]
        es.close()
    except Exception as e:
        print(e)
    return result


# 데이터 개수 호출
def get_numData_from_es(index, query):
    try:
        es = get_es_conn()
        res = es.search(index=index, body=query, size=1)
        es.close()
        return res["hits"]["total"]
    except Exception as e:
        print(e)
        return None


# 데이터 적재
def save_data_to_es(index, data, id=None, es=None):
    try:
        if es is None:
            es = get_es_conn()
        if id is not None:
            result = es.index(index=index, id=id, body=data)
        else:
            result = es.index(index=index, body=data)
        es.close()
        if result["_shards"]["successful"] > 0:
            return "Success"
        else:
            return "Fail"
    except Exception as e:
        print(e)
        return "Fail"


# 이미 적재된 데이터인지 체크
def check_already_saved(index, id):
    try:
        es = get_es_conn()
        result = es.get(index=index, id=id)
        # print(result)
        es.close()
        if result["found"]:
            return False
        else:
            return True
    except Exception as e:
        # print(e)
        return True


# 엘라스틱서치 인덱스 새로고침
def refresh_es(index):
    try:
        es = get_es_conn()
        es.indices.refresh(index=index)
        es.close()
    except Exception as e:
        print(e)


# 엘라스틱서치 데이터 업데이트
def update_data_from_es(index, id, update_data):
    try:
        es = get_es_conn()
        body = {"doc": update_data}
        response = es.update(index=index, id=id, body=body)
        print(response["result"])
        es.close()
        return response["result"]
    except Exception as e:
        print(e)
        return "fail"


# 사업자번호+뷰아이디로 데이터 호출
def get_view_data(biz_no, ViewID, index_name="view_data"):
    query = {
        "sort": {"CreateDate": "desc"},
        "query": {
            "bool": {
                "must": [
                    {"match": {"BusinessNum": biz_no}},
                    {"match": {"ViewID": ViewID}},
                ]
            }
        },
    }
    data = get_data_from_es(index=index_name, query=query)
    return data


# 기업성장지수(EGI)용 월별 분석데이터 호출
def get_monthly_analysis_data(BusinessNum, DataType, start, end):
    query = {
        "sort": {"InsertDate": "desc"},
        "query": {
            "bool": {
                "must": [
                    {"match": {"DataType": DataType}},
                    {"match": {"BusinessNum": BusinessNum}},
                ],
                "filter": {
                    "range": {
                        "InsertDate": {
                            "gte": start + " 00:00:00",
                            "lt": end + " 00:00:00",
                        }
                    }
                },
            }
        },
    }
    data = get_data_from_es(index="analysis_data", query=query)
    return data


##################### CODE MAPPING #####################
# [기업의 산업분류코드, 10차산업분류코드(2자리), ecos분류(알파벳), istans(4자리) 리스트, 코드세부설명]
# ex) ['47312', '47', 'G', ['2101', '2100'], '도매 및 소매업']
# ex) [None, 'etc', 'etc', ['etc'], '']
def get_indust_code(biz_no):
    CompanyIndustCode, IndustCode, EcosCode, IstansCode, Describe = (
        None,
        "etc",
        "etc",
        [],
        "",
    )

    # 사업자번호로 기업의 산업분류코드 조회
    query = {
        "sort": [{"SearchDate": {"order": "desc"}}],
        "query": {
            "bool": {
                "must": [
                    {"match": {"BusinessNum": biz_no}},
                    {"match": {"DataType": "nicednb_enterprise"}},
                ]
            }
        },
    }
    nicednb_enterprise_data = get_data_from_es(index="source_data", query=query)
    if nicednb_enterprise_data:
        code = nicednb_enterprise_data[0]["_source"]["Data"]["indCd1"]
        if code is not None and code != "":
            CompanyIndustCode = code
            IndustCode = CompanyIndustCode[:2]

    # 기업의 산업분류코드 앞 2자리로 매핑코드 조회
    query = {
        "sort": [{"SearchDate": {"order": "desc"}}],
        "query": {"bool": {"must": [{"match": {"Data.IndustCode": IndustCode}}]}},
    }
    code_mapping = get_data_from_es(index="indust_code", query=query)
    if code_mapping:
        for data in code_mapping:
            Describe = data["_source"]["Data"]["Describe"]
            EcosCode = data["_source"]["Data"]["EcosCode"]
            IstansCode.append(data["_source"]["Data"]["IstansCode"])
    if len(IstansCode) < 1:
        IstansCode = ["etc"]

    return [CompanyIndustCode, IndustCode, EcosCode, IstansCode, Describe]


##################### NICEDNB #####################
# 재무제표 데이터를 파싱하여 5개년도 계정금액 반환
# 반환값: {"연도": ["계정금액", "증감율"], ... }
def find_nicednb_fnl_data(biz_no, target):
    flag, finance_data = "fail", dict()

    this_year = int(datetime.datetime.today().year)
    for Year in range(this_year - 5, this_year):
        Year = str(Year)
        nicednb_fnl_query = {
            "sort": [{"SearchDate": {"order": "desc"}}],
            "query": {
                "bool": {
                    "must": [
                        {"match": {"BusinessNum": biz_no}},
                        {"match": {"DataType": "nicednb_fnl"}},
                        {"match": {"Data.acctNm": re_type(target)}},
                        {"match": {"Data.stYear": Year}},
                    ]
                }
            },
        }
        try:
            # 반환값: []
            nicednb_fnl_data = get_data_from_es(
                index="source_data", query=nicednb_fnl_query
            )

            if nicednb_fnl_data:
                flag = True
                data = nicednb_fnl_data[0]["_source"]["Data"]
                acctAmt, icdcRate = data["acctAmt"], data["icdcRate"]
                if icdcRate is not None and icdcRate >= 9999.99:
                    icdcRate = None
                finance_data[Year] = [acctAmt, icdcRate]
            else:
                finance_data[Year] = [None, None]
        except:
            finance_data[Year] = [None, None]

    return finance_data, flag


##################### ISTANS #####################
# ISTANS 데이터 호출
def get_istans_from_es(IstansGb, IndustCode, IstansYear, Country="국내"):
    result = None
    try:
        query = {
            "sort": [
                {"SearchDate": {"order": "desc"}},
                {"Data.IstansPrice": {"order": "desc"}},
            ],
            "query": {
                "bool": {
                    "must": [
                        {"match": {"Data.IstansGb": IstansGb}},
                        {"terms": {"Data.IndustCode": IndustCode}},
                        {"match": {"Data.IstansYear": IstansYear}},
                        {"match": {"Data.Country": Country}},
                        {"exists": {"field": "Data.IstansPrice"}},
                    ]
                }
            },
        }
        data = get_data1_from_es(index="source_data", query=query)
        if data is not None:
            amount = data["_source"]["Data"]["IstansPrice"]
            unit = data["_source"]["Data"]["DataUnit"]
            nm = data["_source"]["Data"]["IstansNm"]
            if unit not in ["%", "백만달러", "건/십억원"]:
                try:
                    # 사업체수 구하기기
                    query = {
                        "sort": [
                            {"SearchDate": {"order": "desc"}},
                            {"Data.IstansPrice": {"order": "desc"}},
                        ],
                        "query": {
                            "bool": {
                                "must": [
                                    {"match": {"Data.IstansGb": "NB"}},
                                    {"terms": {"Data.IndustCode": IndustCode}},
                                    {"match": {"Data.IstansYear": IstansYear}},
                                    {"exists": {"field": "Data.IstansPrice"}},
                                ]
                            }
                        },
                    }
                    count_data = get_data1_from_es(index="source_data", query=query)
                    if count_data is not None:
                        if "1인당" not in nm:
                            count = count_data["_source"]["Data"]["IstansPrice"]
                            # 산업별 사업체수로 나눠서 평균값 구하기
                            if None not in [amount, count]:
                                result = amount / count
                        if unit == "백만원":
                            # 백만원 > 천원 단위 조정 (백만달러 > 천달러)
                            if result is not None:
                                result = round(result * (10 ** 3), 4)
                except Exception as e:
                    print(e)
                    pass
            else:
                result = amount
    except Exception as e:
        print(e)
    return result


##################### ECOS #####################
# ECOS 데이터 호출
def get_ecos_data(IndustCode, AcctNm, EcosYear):
    result = None
    try:
        query = {
            "sort": [
                {"SearchDate": {"order": "desc"}},
                {"Data.AcctAmt": {"order": "desc"}},
            ],
            "query": {
                "bool": {
                    "must": [
                        {"match": {"Data.IndustCode": IndustCode}},
                        {"match": {"Data.AcctNm": AcctNm}},
                        {"match": {"Data.EcosYear": EcosYear}},
                        {"exists": {"field": "Data.AcctAmt"}},
                    ]
                }
            },
        }
        data = get_data1_from_es(index="source_data", query=query)
        if data is not None:
            amount = data["_source"]["Data"]["AcctAmt"]
            unit = data["_source"]["Data"]["DataUnit"]
            nm = data["_source"]["Data"]["AcctNm"]
            if unit not in ["%", "회", "명"]:
                try:
                    # 사업체수 구하기
                    query = {
                        "sort": [
                            {"SearchDate": {"order": "desc"}},
                            {"Data.AcctAmt": {"order": "desc"}},
                        ],
                        "query": {
                            "bool": {
                                "must": [
                                    {"match": {"Data.IndustCode": IndustCode}},
                                    {"match": {"Data.AcctNm": "사업체수"}},
                                    {"match": {"Data.EcosYear": EcosYear}},
                                    {"exists": {"field": "Data.AcctAmt"}},
                                ]
                            }
                        },
                    }
                    count_data = get_data1_from_es(index="source_data", query=query)
                    if count_data is not None:
                        if "1인당" not in nm:
                            count = count_data["_source"]["Data"]["AcctAmt"]
                            # 산업별 사업체수로 나눠서 평균값 구하기
                            if None not in [amount, count]:
                                result = amount / count
                        if unit == "백만원":
                            # 백만원 > 천원 단위 조정
                            if result is not None:
                                result = round(result * (10 ** 3), 4)
                except Exception as e:
                    print(e)
                    pass
            else:
                result = amount
    except Exception as e:
        print(e)
    return result


##################### ETC #####################
# dict(json)형의 val값을 None -> "" / 숫자 -> str 반환
def change_data_format(dict):
    # print(dict, end=" --------> ")
    try:
        for key, val in dict.items():
            if type(val) is not str:
                dict[key] = str(val)
            if val is None or "None" in str(val):
                dict[key] = ""
    except Exception as e:
        print(e)
    # print(dict)
    return dict


# 기타 부호 제거
def re_type(word):
    if word is not None:
        word = word.replace(" ", "")
        word = "".join(word.split("주식회사")).rstrip().lstrip()
        word = word.replace("㈜", "")
        word = re.sub(r"\([^)]*\)", "", word)
    return word


# 딕셔너리 sorting (reverse:True면 Descending)
def sort_dict(dict_data, reverse=False):
    return dict(sorted(dict_data.items(), key=operator.itemgetter(1), reverse=reverse))


def get_data_agg(index, query):
    result = None
    try:
        es = get_es_conn()
        res = es.search(index=index, body=query, size=1)
        result = res["aggregations"]
        es.close()
    except Exception as e:
        print(e)
    return result


# %%
