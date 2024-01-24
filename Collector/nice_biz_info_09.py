import datetime
import sys
from os import path
from libraries import *
from .Funcs import funcs, check_data_pattern, write_log, timeout, retry_function

today = datetime.datetime.today()
DataType = "nice_biz_info"
FlagPrintData = True  # True: 저장할 최종 데이터 출력
FlagSaveData = True  # True: 엘라스틱에 데이터 저장
SearchDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
SearchID = "autoSystem"
MAX_RETRY = 5
TIMEOUT = 30


def crawler_nice_biz_info_NumEmpMonth(page_source):
    matched = re.findall(
        "calChart1[\(\) {}\n$'#\w\.:,;\t]+(xAxis)|(categories)[: {}\n'\t]+(\[[\n '\d년월,\t]+\])",
        page_source,
    )
    temp = matched[1][-1]
    temp = temp.replace(" ", "").replace("\n", "").replace("\t", "")
    matched = re.findall("(\d{2})년(\d{2})월", temp)
    MonthDate_lst = [f"20{x[0]}-{x[1]}" for x in matched]

    lst_dic = {}

    for idx_name, lst_name in [
        ("입사자", "JoinNum"),
        ("퇴사자", "ResignNum"),
        ("총인원수", "TotNum"),
    ]:
        matched = re.findall(
            f"'{idx_name}[\(\)왼오른쪽',\n \t\w:]+([\[\n \t\d,]+\])", page_source
        )
        temp = matched[0]
        temp = temp.replace(" ", "").replace("\n", "").replace("\t", "")
        matched = re.findall("(\d+)", temp)
        lst_dic[lst_name] = [int(x) for x in matched]

    result = [
        {"MonthDate": x[0], "JoinNum": x[1], "ResignNum": x[2], "TotNum": x[3],}
        for x in zip(
            MonthDate_lst, lst_dic["JoinNum"], lst_dic["ResignNum"], lst_dic["TotNum"]
        )
    ]

    return result


def crawler_nice_biz_info_current_status1(page_source):
    idx_name = ["평가기준일", "산업평가 종합등급"]
    css_selecter = "</th>[\n\t ]+<td>[\n \t]+(.+)[\n \t]+</td>"
    res = []
    for idx in idx_name:
        lst = re.findall(idx + css_selecter, page_source)
        res.append(lst[0] if len(lst) != 0 else "")
    return res


def crawler_nice_biz_info_current_status2(page_source):
    css_selecter1 = "조회된 데이터가 없습니다\."
    # css_selecter2 = '"(최?[상중하]위)"'
    res = re.findall(css_selecter1, page_source)
    if len(res) > 0:
        return [None] * 5
    # res = re.findall(css_selecter2, page_source)
    res = bs(page_source, "lxml").find("div", {"class": "sangweGraph"})
    if res:
        res = res.text.split()[0:5]
        return res
    else:
        return [None] * 5


def nice_biz_info_template():
    data = {
        "Ceo": None,
        "CompType": None,
        "IndustryCode": None,
        "Industry": None,
        "GroupName": None,
        "EstDate": None,
        "StockDate": None,
        "IndustStatus": None,
        "WageInfo": None,
    }
    return data


@retry_function.retry(5, webdriver.Chrome)
@timeout.timeout(30)
def wd(biz_no):
    webDriver = webdriver.Chrome(
        options=WEBDRIVER_OPTIONS, executable_path=WEBDRIVER_PATH
    )
    return webDriver


def get_wd(biz_no, webdriver):
    result = wd(biz_no)
    if isinstance(result, webdriver.Chrome):
        return result
    else:
        return None


def get_url(biz_no, company_name, webDriver, start_date):
    url1 = f"https://www.nicebizinfo.com/ep/EP0100M001GE.nice?itgSrch={biz_no}"
    url2 = f"https://www.nicebizinfo.com/ep/EP0100M002GE.nice?kiscode="

    webDriver = webDriver
    # webDriver = get_wd(biz_no, webDriver)

    if not webDriver:
        print("webdriver not available")
        return webDriver

    webDriver.get(url1)
    time.sleep(1)
    num = (
        webDriver.find_element_by_css_selector("body > div.cSection > div > p")
        .text.split("총")[-1]
        .replace("건", "")
        .replace(")", "")
        .rstrip()
        .lstrip()
    )

    if num == "-":
        # get_url(biz_no)
        Data = None
        return Data

    elif int(num) == 0:
        Data = None
        return Data

    elif int(num) < 0:
        # get_url(biz_no)
        Data = None
        return Data

    else:
        ceo_sample = (
            webDriver.find_element_by_css_selector(
                "body > div.cSection > div > div.cTable.sp3.mb60 > table > tbody > tr.bg > th > span.ml25"
            )
            .text.replace("ㅣ", "")
            .rstrip()
            .lstrip()
        )
        ceo_sample = [i.strip() for i in ceo_sample.split(",")]
        try:
            kiscode = (
                webDriver.find_element_by_css_selector("span.fz14.fwb.ml10.fErr > a")
                .get_attribute("onclick")
                .split("(")[-1]
                .replace(")", "")
                .replace("'", "")
            )
            webDriver.get(url2 + kiscode)
            time.sleep(1)
            Data = nice_biz_info_template()

            data_set = [
                i.text
                for i in webDriver.find_elements_by_css_selector(
                    "div.cTable.sp2 > table > tbody > tr.bg > td"
                )
            ]

            summary_list = data_set[:9]
            summary_list = [i.split("\n") for i in summary_list]
            summary_list = {i[0]: i[1] for i in summary_list if len(i) > 1}
            for k, v in summary_list.items():
                if summary_list[k] == "-":
                    summary_list[k] = None
                elif summary_list[k] == "":
                    summary_list[k] = None

            Data["Ceo"] = summary_list.get("대표자")
            if Data["Ceo"]:
                Data["Ceo"] = [i.strip() for i in Data["Ceo"].split(",")]

            Data["CompType"] = summary_list.get("기업형태")
            if Data["CompType"]:
                Data["CompType"] = [i.strip() for i in Data["CompType"].split(",")]

            Data["IndustryCode"] = summary_list.get("산업")
            if Data["IndustryCode"]:
                Data["IndustryCode"] = Data["IndustryCode"].split(")")[0][1:]

            Data["Industry"] = summary_list.get("산업")
            if Data["Industry"]:
                Data["Industry"] = Data["Industry"].split(")")[-1]

            Data["GroupName"] = summary_list.get("그룹명")

            Data["EstDate"] = summary_list.get("설립일자")
            if Data["EstDate"]:
                Data["EstDate"] = Data["EstDate"].replace(".", "-")

            Data["StockDate"] = summary_list.get("상장일자")
            if Data["StockDate"]:
                Data["StockDate"] = Data["StockDate"].replace(".", "-")

            # IndustStatus
            IndustStatus_lst1 = crawler_nice_biz_info_current_status1(
                webDriver.page_source
            )  # evdate, evindex
            IndustStatus_lst2 = crawler_nice_biz_info_current_status2(
                webDriver.page_source
            )  # 활동성, 수익성, 안정성, 성장성, 규모

            if not [i for i in (IndustStatus_lst1 + IndustStatus_lst2) if i]:
                indust_status = None

            else:
                indust_status = {
                    "EvDate": IndustStatus_lst1[0].replace(".", "-"),
                    "EvIndex": IndustStatus_lst1[1],
                    "Activity": IndustStatus_lst2[0],
                    "Profit": IndustStatus_lst2[1],
                    "Stability": IndustStatus_lst2[2],
                    "Growth": IndustStatus_lst2[3],
                    "Size": IndustStatus_lst2[4],
                }

                for k, v in indust_status.items():
                    if not indust_status[k]:
                        indust_status[k] = None

            Data["IndustStatus"] = indust_status

            if len(data_set) > 9:
                wage_list = data_set[9:]
                wage_dict = {}

                wage_dict["기준 날짜"] = (
                    [i for i in wage_list if "종업원수" in i][0]
                    .split("\n")[0]
                    .split("(")[-1]
                    .replace("기준", "")
                    .replace(")", "")
                    .rstrip()
                    .lstrip()
                    .replace(".", "-")
                )
                wage_dict["예상평균연봉"] = (
                    [i for i in wage_list if "예상 평균연봉" in i][0]
                    .split("\n")[-1]
                    .replace("~", "")
                    .rstrip()
                    .lstrip()
                )
                wage_dict["올해입사자평균연봉"] = (
                    [i for i in wage_list if "올해 입사자 평균연봉" in i][0]
                    .split("\n")[-1]
                    .replace("~", "")
                    .rstrip()
                    .lstrip()
                )
                wage_dict["종업원수 기준날짜"] = (
                    [i for i in wage_list if "종업원수" in i][0]
                    .split("\n")[0]
                    .split("(")[-1]
                    .replace("기준", "")
                    .replace(")", "")
                    .rstrip()
                    .lstrip()
                    .replace(".", "-")
                )
                wage_dict["종업원수"] = (
                    [i for i in wage_list if "종업원수" in i][0]
                    .split("\n")[-1]
                    .replace("~", "")
                    .rstrip()
                    .lstrip()
                )
                wage_dict["입사율"] = (
                    [i for i in wage_list if "입사율" in i][0]
                    .split("\n")[-1]
                    .split("%")[0]
                    .rstrip()
                    .lstrip()
                )
                wage_dict["연간입사자"] = (
                    [i for i in wage_list if "입사율" in i][0]
                    .split("\n")[-1]
                    .split("%")[-1]
                    .replace("(", "")
                    .replace(")", "")
                    .replace("명", "")
                    .rstrip()
                    .lstrip()
                )
                wage_dict["퇴사율"] = (
                    [i for i in wage_list if "퇴사율" in i][0]
                    .split("\n")[-1]
                    .split("%")[0]
                    .rstrip()
                    .lstrip()
                )
                wage_dict["연간퇴사자"] = (
                    [i for i in wage_list if "퇴사율" in i][0]
                    .split("\n")[-1]
                    .split("%")[-1]
                    .replace("(", "")
                    .replace(")", "")
                    .replace("명", "")
                    .rstrip()
                    .lstrip()
                )
                wage_dict["업력"] = (
                    [i for i in wage_list if "업력" in i][0]
                    .split("\n")[-1]
                    .replace("년", "")
                    .rstrip()
                    .lstrip()
                )

                for k, v in wage_dict.items():
                    if wage_dict[k] == "-":
                        wage_dict[k] = None

                wage_info = {
                    "InfoDate": None,
                    "AvgWage": None,
                    "NewerAvgWage": None,
                    "NumEmpDate": None,
                    "NumEmp": None,
                    "JoinRate": None,
                    "JoinNum": None,
                    "ResignRate": None,
                    "ResignNum": None,
                    "YearsInfo": None,
                    "NumEmpMonth": None,
                    "CmpWage": None,
                    "FieldWage": None,
                    "FieldTop1Wage": None,
                    "FieldMidWage": None,
                }

                money = re.findall(
                    "name: \['금액'\][, \n]+data: \[(.+)\]", webDriver.page_source
                )
                money = money[0].split(",")
                money = [int(i) * 10 for i in money]
                wage_info = {
                    "InfoDate": wage_dict["기준 날짜"],
                    "AvgWage": wage_dict["예상평균연봉"],
                    "NewerAvgWage": wage_dict["올해입사자평균연봉"],
                    "NumEmpDate": wage_dict["종업원수 기준날짜"],
                    "NumEmp": wage_dict["종업원수"],
                    "JoinRate": wage_dict["입사율"],
                    "JoinNum": wage_dict["연간입사자"],
                    "ResignRate": wage_dict["퇴사율"],
                    "ResignNum": wage_dict["연간퇴사자"],
                    "YearsInfo": wage_dict["업력"],
                    "NumEmpMonth": None,
                    "CmpWage": money[0],
                    "FieldWage": money[1],
                    "FieldTop1Wage": money[2],
                    "FieldMidWage": money[3],
                }

                wage_info_NumEmpMonth = crawler_nice_biz_info_NumEmpMonth(
                    webDriver.page_source
                )

                if wage_info_NumEmpMonth:
                    numemp_list = []
                    for n in wage_info_NumEmpMonth:
                        numemp_list.append(
                            {
                                "MonthDate": n["MonthDate"],
                                "JoinNum": n["JoinNum"],
                                "ResignNum": n["ResignNum"],
                                "TotNum": n["TotNum"],
                            }
                        )
                    wage_info["NumEmpMonth"] = numemp_list

                Data["WageInfo"] = wage_info
            else:
                Data["WageInfo"] = None
            # webDriver.close()
            # webDriver.quit()

            if not [v for k, v in Data.items() if v]:
                Data = None

            # 평가년도와 start년도가 같지않으면 None값으로 대체
            # print("평가날짜 : ", Data["IndustStatus"]["EvDate"][0:4])
            # print("start_date : ", int(start_date[0:4]))
            if int(Data["IndustStatus"]["EvDate"][0:4]) != int(start_date[0:4]):
                Data = None

            return Data

        except Exception as e:
            print(f"{biz_no}_Server_error")
            webDriver.close()
            webDriver.quit()
            write_log.write_log(BIZ_NO=biz_no, DATA_TYPE=DataType, ERR_LOG=str(e))
            return None


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
    SearchDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    head = {
        "BusinessNum": str(biz_no),
        "DataType": DataType,
        "SearchDate": SearchDate,
        "SearchID": "autoSystem",
    }
    Data = get_url(biz_no, company_name, webDriver, start_date)
    Data = None if Data is None else Data
    result = head.copy()
    result['Data'] = Data
    return result

def main(biz_no, company_name):
    result = []
    Data = get_url(biz_no)

    NICE_BIZ_INFO = {
        "BusinessNum": str(biz_no),
        "DataType": DataType,
        "SearchDate": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "SearchID": "autoSystem",
        "Data": Data,
    }
    result.append(copy.deepcopy(NICE_BIZ_INFO))

    if FlagPrintData:
        print(result)

    if bool(result) and FlagSaveData:
        for data in result:
            flag1 = result_save_function(data)
            update_searchdate_function(biz_no, flag1)


if __name__ == "__main__":
    main("2208736743", "이노그리드")
