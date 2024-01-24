import datetime
import sys
from os import path
from libraries import *
from .Funcs import funcs, check_data_pattern, write_log, timeout, retry_function

today = datetime.datetime.today()
DataType = "ntis_assign"
FlagPrintData = True  # True: 저장할 최종 데이터 출력
FlagSaveData = True  # True: 엘라스틱에 데이터 저장
NTIS_KEY = "uj4ln084p9ta4wua90zx"
MAX_RETRY = 5
TIMEOUT = 30


@retry_function.retry(MAX_RETRY, dict)
@timeout.timeout(TIMEOUT)
def call_api_ntis_assign(biz_no, companyName, startPosition, displayCnt, start_date):

    # 과제검색
    url = "https://www.ntis.go.kr/rndopen/openApi/public_project?"
    apprvKey = unquote(NTIS_KEY)
    queryParams = urlencode(
        {
            quote_plus("apprvKey"): apprvKey,
            quote_plus("collection"): "project",
            quote_plus("addQuery"): f"PB01={companyName}",
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
    result = call_api_ntis_assign(biz_no, companyName, startPosition, displayCnt, start_date)
    if type(result) == dict:
        return result
    else:
        return None


def api_ntis_assign(biz_no, companyName, startPosition, displayCnt, start_date):

    ntis_assign = ntis_assign_template()

    assign_result = []

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
                assign_result.append(result["result"]["resultset"]["hit"])
            elif type(result["result"]["resultset"]["hit"]) == list:
                [assign_result.append(i) for i in result["result"]["resultset"]["hit"]]
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
                                assign_result.append(i)
                                for i in result["result"]["resultset"]["hit"]
                            ]

            for i in tqdm(assign_result):

                # 날짜 비교하여 중복 수집 방지(년도만 비교, 1년단위로 수집)
                if int(i['projectyear']) < int(start_date[0:4]) :
                    # print("projectyear : ", i['projectyear'])
                    # print("start_date : ", start_date[0:4])
                    break

                if funcs.re_type(i["researchagency"].get("name")) == companyName:
                    for z in i:
                        if i[z] is None:
                            i[z] = {}
                    for z in i["scienceclass"]:
                        if z["large"] is None:
                            z["large"] = {}
                        if z["medium"] is None:
                            z["medium"] = {}
                        if z["small"] is None:
                            z["small"] = {}
                    for z in i["applyarea"]:
                        if i["applyarea"][z] is None:
                            i["applyarea"][z] = {}
                    # 1
                    ntis_assign["ProjectNo"] = i["projectnumber"]
                    # 2
                    ntis_assign["ProjectNameKR"] = i["projecttitle"].get("korean")
                    ntis_assign["ProjectNameEN"] = i["projecttitle"].get("english")
                    # 3
                    ntis_assign["ManagerName"] = i["manager"].get("name")
                    # 4
                    ntis_assign["ResearchersName"] = i["researchers"].get("name")
                    try:
                        ntis_assign["ResManCount"] = int(
                            i["researchers"].get("mancount")
                        )
                    except:
                        ntis_assign["ResManCount"] = i["researchers"].get("mancount")
                    try:
                        ntis_assign["ResWmanCount"] = int(
                            i["researchers"].get("womancount")
                        )
                    except:
                        ntis_assign["ResWmanCount"] = i["researchers"].get("womancount")
                    # ntis_assign['ResManCount'] = i['researchers'].get('mancount')
                    # ntis_assign['ResWmanCount'] = i['researchers'].get('womancount')
                    # 5
                    ntis_assign["GoalFull"] = i["goal"].get("full")
                    ntis_assign["GoalTeaser"] = i["goal"].get("teaser")
                    # 6
                    ntis_assign["AbstractFull"] = i["abstract"].get("full")
                    ntis_assign["AbstractTeaser"] = i["abstract"].get("teaser")
                    # 7
                    ntis_assign["EffectFull"] = i["effect"].get("full")
                    ntis_assign["EffectTeaser"] = i["effect"].get("teaser")
                    # 8
                    ntis_assign["KeywordKR"] = i["keyword"].get("korean")
                    ntis_assign["KeywordEN"] = i["keyword"].get("english")
                    # 9
                    ntis_assign["OrderagencyName"] = i["orderagency"].get("name")
                    # 10
                    ntis_assign["ResearchagencyName"] = i["researchagency"].get("name")
                    # 11
                    ntis_assign["BudgetProjectName"] = i["budgetproject"].get("name")
                    # 12
                    ntis_assign["BusinessName"] = i["businessname"]
                    # 13
                    ntis_assign["BigprojectTitle"] = i["bigprojecttitle"]
                    # 14
                    ntis_assign["ManageagencyName"] = i["manageagency"].get("name")
                    # 15
                    ntis_assign["MinistryName"] = i["ministry"].get("name")
                    # 16
                    ntis_assign["ProjectYear"] = i["projectyear"]
                    # 17
                    ntis_assign["ProjectStart"] = i["projectperiod"].get("start")
                    ntis_assign["ProjectEnd"] = i["projectperiod"].get("end")

                    if i["projectperiod"].get("totalstart"):
                        ntis_assign["ProjectTotstart"] = (
                            i["projectperiod"].get("totalstart").split()[0].lstrip()
                        )
                    else:
                        ntis_assign["ProjectTotstart"] = i["projectperiod"].get(
                            "totalstart"
                        )

                    if i["projectperiod"].get("totalend"):
                        ntis_assign["ProjectTotend"] = (
                            i["projectperiod"].get("totalend").split()[0].lstrip()
                        )
                    else:
                        ntis_assign["ProjectTotend"] = i["projectperiod"].get(
                            "totalend"
                        )

                    # 18
                    ntis_assign["OrganizationpNo"] = i["organizationpnumber"]
                    # 19
                    ntis_assign["Scienceclass_New_1_Large_code"] = (
                        [z for z in i["scienceclass"] if z.get("@sequence") == "1"][0]
                        .get("large")
                        .get("@code")
                    )
                    ntis_assign["Scienceclass_New_1_Large"] = (
                        [z for z in i["scienceclass"] if z.get("@sequence") == "1"][0]
                        .get("large")
                        .get("#text")
                    )
                    ntis_assign["Scienceclass_New_1_Midium_Code"] = (
                        [z for z in i["scienceclass"] if z.get("@sequence") == "1"][0]
                        .get("medium")
                        .get("@code")
                    )
                    ntis_assign["Scienceclass_New_1_Midium"] = (
                        [z for z in i["scienceclass"] if z.get("@sequence") == "1"][0]
                        .get("medium")
                        .get("#text")
                    )
                    ntis_assign["Scienceclass_New_1_Small_Code"] = (
                        [z for z in i["scienceclass"] if z.get("@sequence") == "1"][0]
                        .get("small")
                        .get("@code")
                    )
                    ntis_assign["Scienceclass_New_1_Small"] = (
                        [z for z in i["scienceclass"] if z.get("@sequence") == "1"][0]
                        .get("small")
                        .get("#text")
                    )
                    # 20
                    ntis_assign["Ministryscience_Class_Large"] = i[
                        "ministryscienceclass"
                    ].get("large")
                    ntis_assign["Ministryscience_Class_Midium"] = i[
                        "ministryscienceclass"
                    ].get("medium")
                    ntis_assign["Ministryscience_Class_Small"] = i[
                        "ministryscienceclass"
                    ].get("small")
                    # 21
                    ntis_assign["Tempscience_Class_Large"] = i["tempscienceclass"].get(
                        "large"
                    )
                    if i["tempscienceclass"].get("large"):
                        ntis_assign["Tempscience_Class_Large"] = (
                            i["tempscienceclass"].get("large").get("@code")
                        )

                    ntis_assign["Tempscience_Class_Midium"] = i["tempscienceclass"].get(
                        "medium"
                    )
                    if i["tempscienceclass"].get("medium"):
                        ntis_assign["Tempscience_Class_Midium"] = (
                            i["tempscienceclass"].get("medium").get("@code")
                        )

                    ntis_assign["Tempscience_Class_Small"] = i["tempscienceclass"].get(
                        "small"
                    )
                    if i["tempscienceclass"].get("small"):
                        ntis_assign["Tempscience_Class_Small"] = (
                            i["tempscienceclass"].get("small").get("@code")
                        )

                    # 22
                    ntis_assign["PerformagentCode"] = i["performagent"].get("@code")
                    ntis_assign["Performagent"] = i["performagent"].get("#text")
                    # 23
                    ntis_assign["DevelopmentPhasesCode"] = i["developmentphases"].get(
                        "@code"
                    )
                    ntis_assign["DevelopmentPhases"] = i["developmentphases"].get(
                        "#text"
                    )
                    # 24
                    ntis_assign["TechlifecycleCode"] = i["technologylifecycle"].get(
                        "@code"
                    )
                    ntis_assign["TechLifecycle"] = i["technologylifecycle"].get("#text")
                    # 25
                    ntis_assign["RegionCode"] = i["region"].get("@code")
                    ntis_assign["Region"] = i["region"].get("#text")
                    # 26
                    if type(i["economicsocialgoal"]) == dict:
                        ntis_assign["EconomicSocialGoal"] = i["economicsocialgoal"].get(
                            "#text"
                        )
                    else:
                        ntis_assign["EconomicSocialGoal"] = i["economicsocialgoal"]
                    # 27
                    ntis_assign["SixtechCode"] = i["sixtechnology"].get("@code")
                    ntis_assign["Sixtech"] = i["sixtechnology"].get("#text")
                    # 28
                    ntis_assign["ApplyareaFirstCode"] = i["applyarea"]["first"].get(
                        "@code"
                    )
                    ntis_assign["ApplyareaFirst"] = i["applyarea"]["first"].get("#text")

                    if type(i["applyarea"]["second"]) == dict:
                        ntis_assign["ApplyareaSecond"] = i["applyarea"]["second"].get(
                            "#text"
                        )
                    else:
                        ntis_assign["ApplyareaSecond"] = i["applyarea"]["second"]

                    if type(i["applyarea"]["third"]) == dict:
                        ntis_assign["ApplyareaThird"] = i["applyarea"]["third"].get(
                            "#text"
                        )
                    else:
                        ntis_assign["ApplyareaThird"] = i["applyarea"]["third"]

                    # ntis_assign['ApplyareaThird'] = i['ApplyArea']['Third']
                    # 29
                    ntis_assign["ConrinuousFlag"] = i["continuousflag"]
                    # 30
                    ntis_assign["PolicyProjectFlag"] = i["policyprojectflag"]
                    # 31
                    ntis_assign["GovernFunds"] = i["governmentfunds"]
                    # 32
                    ntis_assign["SbusinessFunds"] = i["sbusinessfunds"]
                    # 33
                    ntis_assign["TotFunds"] = i["totalfunds"]
                    # 34
                    ntis_assign["CorporateRegistrationNo"] = i[
                        "corporateregistrationnumber"
                    ]
                    # 35
                    ntis_assign["SeriesProject"] = i["seriesproject"]

                    for i in ntis_assign:
                        if bool(ntis_assign[i]) == False:
                            ntis_assign[i] = None
                    for i in ntis_assign:
                        if "SECRET PROJECT" == ntis_assign[i]:
                            ntis_assign[i] = None

                    if ntis_assign["ProjectStart"]:
                        ntis_assign["ProjectStart"] = "{}-{}-{}".format(
                            ntis_assign["ProjectStart"][:4],
                            ntis_assign["ProjectStart"][4:6],
                            ntis_assign["ProjectStart"][6:],
                        )
                    if ntis_assign["ProjectEnd"]:
                        ntis_assign["ProjectEnd"] = "{}-{}-{}".format(
                            ntis_assign["ProjectEnd"][:4],
                            ntis_assign["ProjectEnd"][4:6],
                            ntis_assign["ProjectEnd"][6:],
                        )
                    if ntis_assign["ResearchersName"]:
                        ntis_assign["ResearchersName"] = ntis_assign[
                            "ResearchersName"
                        ].split(";")
                    if ntis_assign["GovernFunds"]:
                        ntis_assign["GovernFunds"] = int(
                            int(ntis_assign["GovernFunds"]) / 1000
                        )
                    if ntis_assign["SbusinessFunds"]:
                        ntis_assign["SbusinessFunds"] = int(
                            int(ntis_assign["SbusinessFunds"]) / 1000
                        )
                    if ntis_assign["TotFunds"]:
                        ntis_assign["TotFunds"] = int(
                            int(ntis_assign["TotFunds"]) / 1000
                        )

                    data = {
                        "BusinessNum": str(biz_no),
                        "DataType": DataType,
                        "SearchDate": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S.%f"
                        ),
                        "SearchID": "autoSystem",
                        "Data": ntis_assign,
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


def ntis_assign_template():
    assign = {
        "ProjectNo": None,
        "ProjectNameKR": None,
        "ProjectNameEN": None,
        "ManagerName": None,
        "ResearchersName": None,
        "ResManCount": None,
        "ResWmanCount": None,
        "GoalFull": None,
        "GoalTeaser": None,
        "AbstractFull": None,
        "AbstractTeaser": None,
        "EffectFull": None,
        "EffectTeaser": None,
        "KeywordKR": None,
        "KeywordEN": None,
        "OrderagencyName": None,
        "ResearchagencyName": None,
        "BudgetProjectName": None,
        "BusinessName": None,
        "BigprojectTitle": None,
        "ManageagencyName": None,
        "MinistryName": None,
        "ProjectYear": None,
        "ProjectStart": None,
        "ProjectEnd": None,
        "ProjectTotstart": None,
        "ProjectTotend": None,
        "OrganizationpNo": None,
        "Scienceclass_New_1_Large_code": None,
        "Scienceclass_New_1_Large": None,
        "Scienceclass_New_1_Midium_Code": None,
        "Scienceclass_New_1_Midium": None,
        "Scienceclass_New_1_Small_Code": None,
        "Scienceclass_New_1_Small": None,
        "Ministryscience_Class_Large": None,
        "Ministryscience_Class_Midium": None,
        "Ministryscience_Class_Small": None,
        "Tempscience_Class_Large": None,
        "Tempscience_Class_Midium": None,
        "Tempscience_Class_Small": None,
        "PerformagentCode": None,
        "Performagent": None,
        "DevelopmentPhasesCode": None,
        "DevelopmentPhases": None,
        "TechlifecycleCode": None,
        "TechLifecycle": None,
        "RegionCode": None,
        "Region": None,
        "EconomicSocialGoal": None,
        "SixtechCode": None,
        "Sixtech": None,
        "ApplyareaFirstCode": None,
        "ApplyareaFirst": None,
        "ApplyareaSecond": None,
        "ApplyareaThird": None,
        "ConrinuousFlag": None,
        "PolicyProjectFlag": None,
        "GovernFunds": None,
        "SbusinessFunds": None,
        "TotFunds": None,
        "CorporateRegistrationNo": None,
        "SeriesProject": None,
    }
    return assign


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

def get_result(biz_no, company_name, ceo_name=None, webDriver=None, start_date=None, console_print=False, err_msg=None):
    displayCnt = 1000

    result = []

    CompanyName = funcs.re_type(company_name)
    result.append(api_ntis_assign(biz_no, CompanyName, 1, displayCnt, start_date))
    result = list(itertools.chain(*result))

    return result

def main(biz_no, CompanyName):

    displayCnt = 1000

    result = []

    CompanyName = funcs.re_type(CompanyName)
    result.append(api_ntis_assign(biz_no, CompanyName, 1, displayCnt))
    result = list(itertools.chain(*result))

    if FlagPrintData:
        print(result)

    if bool(result) and FlagSaveData:
        for data in result:
            flag1 = result_save_function(data)
            update_searchdate_function(biz_no, flag1)


if __name__ == "__main__":
    main("2208736743", "한국과학")  # 이노그리드

