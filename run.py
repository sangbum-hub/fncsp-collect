import argparse
import json
import os
import re
from datetime import datetime, timedelta
from multiprocessing import pool

from selenium import webdriver

from Collector import (
    kisti_article_14,
    naver_news_01, 
    naver_blog_02, 
    naver_cafe_03, 
    jobplanet_statistic_05, 
    jobplanet_review_06,
    jobplanet_premium_07,
    saramin_08,
    nice_biz_info_09,
    naver_trend_13,
    kisti_patent_15,
    kci_16,
    ntis_assign_17,
    ntis_accomp_18,
    ntis_rnd_paper_19,
    ntis_org_info_20,
)
from Collector.Funcs.funcs import (get_bizNo_mysql, get_es_conn,
                                   get_mysql_conn, save_data_to_es)

LOG_PATH = "./logs"
DATA_FILES_PATH = None

def get_dir():
    now_date = datetime.now().strftime("%Y%m%d")
    if "data_files" not in os.listdir(os.getcwd()):
        os.mkdir(f"{os.getcwd()}/data_files")
    folder = f"{os.getcwd()}/data_files/{now_date}"
    if now_date not in os.listdir(os.getcwd()+"/data_files"):
        os.mkdir(folder)
    return folder
        
def get_fail_log_dir():
    now_date = datetime.now().strftime("%Y%m%d")
    if "logs(failure)" not in os.listdir(os.getcwd()):
        os.mkdir(f"{os.getcwd()}/logs(failure)")
    folder = f"{os.getcwd()}/logs(failure)/{now_date}"
    if now_date not in os.listdir(os.getcwd()+"/logs(failure)"):
        os.mkdir(folder)
    return folder

def make_set(tag_name):
    results = set()
    file_names = list(filter(lambda o: tag_name in o, os.listdir(LOG_PATH)))
    for file_name in file_names:
        with open(f"{LOG_PATH}/{file_name}", "r", encoding="utf-8") as f:
            text = f.read()
        for txt in re.findall("\d{10}", text):
            results.add(txt)
    return results

def update_logs(file_name, massage, log_path=None):
    folder_path = log_path if log_path else LOG_PATH
    with open(f"{folder_path}/{file_name}", "a+", encoding="utf-8") as f:
        f.seek(0)
        data = f.read(10)
        if len(data)>0:
            f.write("\n")
        f.write(massage)

def get_webdriver(head_display=False):
    try:
        option = webdriver.ChromeOptions()
        if head_display==False:
            option.add_argument("headless")
        webDriver = webdriver.Chrome("./Collector/chromedriver", options=option)
        return webDriver
    except Exception as e:
        print(e)
        print("Selenium, not ready")
        exit(1)

# selenium stealth 옵션추가 ( cloudflare 우회용 )
# stealth(driver,
#         languages=["en-US", "en"],
#         vendor="Google Inc.",
#         platform="Win32",
#         webgl_vendor="Intel Inc.",
#         renderer="Intel Iris OpenGL Engine",
#         fix_hairline=True,
#         )


# 100개 기업만 임시방편으로 생성하기
with open('bsn_list.txt', 'r') as file:
    bsn_list_100 = file.readlines()
# 리스트의 각 요소에서 줄 바꿈 문자 제거
bsn_list_100 = tuple((line.strip()) for line in bsn_list_100)



def get_biz_no_list(conn):
    biz_no_list = get_bizNo_mysql(
        ## fncsp 데이터베이스에서 가져올 경우
        # DataType="", sql_="SELECT * FROM SOURCE_DATA_STATUS ORDER BY BIZ_NO ASC",conn=conn
        
        ## kms 데이터베이스에서 가져올 경우
        # DataType="", sql_="SELECT * FROM TB_COMPANY_MASTER ORDER BY BIZ_NO ASC",conn=conn
        DataType="", sql_="SELECT * FROM TB_COMPANY_MASTER WHERE BASE_YEAR = 2020 ORDER BY BIZ_NO ASC",conn=conn
        # DataType="", sql_="SELECT * FROM TB_COMPANY_MASTER WHERE BASE_YEAR = 2020 and BIZ_NO IN (1138102583, 1338126804, 1068177204) ORDER BY BIZ_NO ASC",conn=conn

        # fncsp2 데이터베이스에서 가져올 경우
        # DataType="", sql_="SELECT * FROM SOURCE_DATA_STATUS ORDER BY BIZ_NO ASC",conn=conn
        # DataType="", sql_="SELECT * FROM SOURCE_DATA_STATUS WHERE BIZ_NO IN (1138102583, 1338126804, 5048700575)",conn=conn
        # DataType="", sql_="SELECT * FROM SOURCE_DATA_STATUS WHERE BIZ_NO IN (1138102583, 1338126804, 1068177204)",conn=conn
        # DataType="", sql_=f"SELECT * FROM SOURCE_DATA_STATUS WHERE BIZ_NO IN {bsn_list_100}",conn=conn    # 100개 기업리스트
        
    )
    return biz_no_list

def crawl_naver_news(biz_no, company_name, ceo_name, webDriver, console_print, secondWebDriver, start_date, end_date):
    result = naver_news_01.get_result(
        biz_no=biz_no,
        company_name=company_name,
        ceo_name=ceo_name,
        webDriver=webDriver,
        console_print=console_print,
        secondWebDriver=secondWebDriver,
        start_date=start_date,
        end_date=end_date,
    )

    return result

def crawl_naver_blog(biz_no, company_name, ceo_name, webDriver, console_print, secondWebDriver, start_date, end_date):
    result = naver_blog_02.get_result(
        biz_no=biz_no,
        company_name=company_name,
        ceo_name=ceo_name,
        webDriver=webDriver,
        start_date=start_date,
        console_print=console_print,
        search_date = end_date
    )

    return result

def crawl_naver_cafe(biz_no, company_name, ceo_name, webDriver, console_print, secondWebDriver, start_date, end_date):
    result = naver_cafe_03.get_result(
        biz_no=biz_no, 
        company_name=company_name, 
        ceo_name=ceo_name, 
        webDriver=webDriver, 
        console_print=console_print, 
        err_msg=None, 
        start_date=start_date,
        SearchDate=end_date
    )
    
    return result

def crawl_jobplanet(biz_no, company_name, ceo_name, webDriver, console_print, secondWebDriver, start_date, end_date, run_first_time=True):
    result=[]
    if run_first_time:
        pass
        # result.append(jobplanet_statistic_05.get_result(biz_no, company_name, ceo_name, webDriver, console_print=console_print, err_msg=None))
        # print("result:", result)
    else:
        pass
    result.append(jobplanet_statistic_05.get_result(biz_no, company_name, ceo_name, webDriver, start_date=start_date, console_print=console_print, err_msg=None))
        # print("result:", result)
    result.append(jobplanet_review_06.get_result_no_login(biz_no, company_name, ceo_name, webDriver, start_date=start_date, console_print=console_print, err_msg=None))
    # print("result:", result)
    result.extend(jobplanet_premium_07.get_result_no_login(biz_no, company_name, ceo_name, webDriver, start_date=start_date, console_print=console_print, err_msg=None))
    # result.extend(jobplanet_premium_07.get_result(biz_no, company_name, ceo_name, webDriver, console_print=console_print, err_msg=None))
    
    return result

def crawl_saramin(biz_no, company_name, ceo_name, webDriver, console_print, secondWebDriver, start_date, end_date):
    result = saramin_08.get_result(
        biz_no=biz_no, 
        company_name=company_name,  
        ceo_name=ceo_name, 
        webDriver=webDriver, 
        start_date=start_date, 
        console_print=console_print, 
        err_msg=None
    )
    
    return result

def crawl_nice_biz_info(biz_no, company_name, ceo_name, webDriver, console_print, secondWebDriver, start_date, end_date):
    result = nice_biz_info_09.get_result(
        biz_no=biz_no, 
        company_name=company_name, 
        ceo_name=ceo_name, 
        webDriver=webDriver, 
        console_print=console_print, 
        start_date=start_date,
        err_msg=None
    )
    
    return result

def crawl_kisti_article(biz_no, company_name, ceo_name, webDriver, console_print, secondWebDriver, start_date, end_date):
    result = kisti_article_14.get_result(
        biz_no=biz_no, 
        company_name=company_name, 
        ceo_name=None, 
        webDriver=None, 
        console_print=False, 
        start_date=start_date,
        err_msg=None
    )
    
    return result

def crawl_kisti_patent(biz_no, company_name, ceo_name, webDriver, console_print, secondWebDriver, start_date, end_date):
    result = kisti_patent_15.get_result(
        biz_no=biz_no, 
        company_name=company_name, 
        ceo_name=None, 
        webDriver=None, 
        console_print=False, 
        start_date=start_date,
        err_msg=None
    )
    
    return result

def crawl_ntis_assign(biz_no, company_name, ceo_name, webDriver, console_print, secondWebDriver, start_date, end_date):
    result = ntis_assign_17.get_result(
        biz_no=biz_no, 
        company_name=company_name, 
        ceo_name=None, 
        webDriver=None, 
        console_print=False, 
        start_date=start_date,
        err_msg=None
    )
    
    return result

def crawl_ntis_accomp(biz_no, company_name, ceo_name, webDriver, console_print, secondWebDriver, start_date, end_date):
    result = ntis_accomp_18.get_result(
        biz_no=biz_no, 
        company_name=company_name, 
        ceo_name=None, 
        webDriver=None, 
        console_print=False,
        start_date=start_date,
        err_msg=None
    )
    
    return result

def crawl_ntis_rnd_paper(biz_no, company_name, ceo_name, webDriver, console_print, secondWebDriver, start_date, end_date):
    result = ntis_rnd_paper_19.get_result(
        biz_no=biz_no, 
        company_name=company_name, 
        ceo_name=None, 
        webDriver=None, 
        console_print=False,
        start_date=start_date,
        err_msg=None
    )
    
    return result

def crawl_ntis_org_info(biz_no, company_name, ceo_name, webDriver, console_print, secondWebDriver, start_date, end_date):
    result = ntis_org_info_20.get_result(
        biz_no=biz_no, 
        company_name=company_name, 
        ceo_name=None, 
        webDriver=None, 
        console_print=False,
        start_date=start_date,
        err_msg=None
    )
    
    return result

def crawl_naver_trend(biz_no, company_name, ceo_name, webDriver, console_print, secondWebDriver, start_date, end_date,auth_info=None):
    tmp = datetime.strptime(start_date,"%Y.%m.%d").strftime("%Y-%m-%d")
    result = naver_trend_13.api_naver_trend(
        biz_no, 
        company_name, 
        tmp, 
        auth_info
    )
    return result    

def crawl_kci(biz_no, company_name, ceo_name, webDriver, console_print, secondWebDriver, start_date, end_date):
    result = kci_16.get_result(
        biz_no=biz_no,
        company_name=company_name,
        start_date=start_date,
        limit=None
    )
    return result

def async_group_work(bsn_lst, cmp_names, ceo_names, tag, console_print, es_info, start_date=None, end_date=None,file_save=False,es_save=False, process_num=0,display_browser=False):
    webDriver = None
    secondWebDriver = None

    first_run = True

    # naver_trend, 1000회 제한
    counter1 = 0

    # webDriver context 할당
    if tag in ["saramin","naver_news","naver_blog","naver_cafe"]:
        webDriver = get_webdriver(display_browser)
    elif "jobplanet" in tag:
        webDriver = get_webdriver(True)
    elif "nice_biz_info" in tag:
        webDriver = get_webdriver(True)
    # 2번째 webDriver context 할당
    if tag in ["naver_news"]:
        secondWebDriver = get_webdriver(display_browser)
    if tag in ["naver_trend"]:
        auth_info = naver_trend_13.Auth_Info()
        max_call_count = auth_info.callable_count()

    try:
        for bsn, cmp_name, ceo_name in zip(bsn_lst, cmp_names, ceo_names):
            try:
                if tag == "naver_blog":
                    result = crawl_naver_blog(bsn, cmp_name, ceo_name, webDriver, console_print,secondWebDriver=secondWebDriver,start_date=start_date,end_date=end_date)
                elif tag == "naver_news":
                    result = crawl_naver_news(bsn, cmp_name, ceo_name, webDriver, console_print,secondWebDriver=secondWebDriver,start_date=start_date,end_date=end_date)
                elif tag == "naver_cafe":
                    result = crawl_naver_cafe(bsn, cmp_name, ceo_name, webDriver, console_print,secondWebDriver=secondWebDriver,start_date=start_date,end_date=end_date)
                elif tag == "naver_trend":
                    counter1 += 1
                    if counter1 > max_call_count:
                        break
                    result = crawl_naver_trend(bsn, cmp_name, ceo_name, webDriver, console_print,secondWebDriver=secondWebDriver,start_date=start_date,end_date=end_date,auth_info=auth_info)
                elif tag == "jobplanet":
                    result = crawl_jobplanet(bsn, cmp_name, ceo_name, webDriver, console_print,secondWebDriver=secondWebDriver,start_date=start_date,end_date=end_date,run_first_time=first_run)
                    print("result: ", result)
                elif tag == "saramin":
                    result = crawl_saramin(bsn, cmp_name, ceo_name, webDriver, console_print,secondWebDriver=secondWebDriver,start_date=start_date,end_date=end_date)
                elif tag == "kci":
                    result = crawl_kci(bsn, cmp_name, ceo_name, webDriver, console_print,secondWebDriver=secondWebDriver,start_date=start_date,end_date=end_date)
                elif tag == "nice_biz_info":
                    result = crawl_nice_biz_info(bsn, cmp_name, ceo_name, webDriver, console_print,secondWebDriver=secondWebDriver,start_date=start_date,end_date=end_date)
                elif tag == "kisti_article":
                    result = crawl_kisti_article(bsn, cmp_name, ceo_name, webDriver, console_print,secondWebDriver=secondWebDriver,start_date=start_date,end_date=end_date)
                elif tag == "kisti_patent":
                    result = crawl_kisti_patent(bsn, cmp_name, ceo_name, webDriver, console_print,secondWebDriver=secondWebDriver,start_date=start_date,end_date=end_date)
                elif tag == "ntis_assign":
                    result = crawl_ntis_assign(bsn, cmp_name, ceo_name, webDriver, console_print,secondWebDriver=secondWebDriver,start_date=start_date,end_date=end_date)
                elif tag == "ntis_accomp":
                    result = crawl_ntis_accomp(bsn, cmp_name, ceo_name, webDriver, console_print,secondWebDriver=secondWebDriver,start_date=start_date,end_date=end_date)
                elif tag == "ntis_rnd_paper":
                    result = crawl_ntis_rnd_paper(bsn, cmp_name, ceo_name, webDriver, console_print,secondWebDriver=secondWebDriver,start_date=start_date,end_date=end_date)
                elif tag == "ntis_org_info":
                    result = crawl_ntis_org_info(bsn, cmp_name, ceo_name, webDriver, console_print,secondWebDriver=secondWebDriver,start_date=start_date,end_date=end_date)

                if es_save:
                    es = get_es_conn(
                        es_info["es_host"],
                        es_info["es_port"],
                        (es_info["es_user"],es_info["es_password"]),
                        es_info["es_timeout"],
                        es_info["es_max_retries"],
                        es_info["es_retry_on_timeout"],
                    )
                    
                    es_flag = True

                    for data in result:
                        flag = save_data_to_es("source_data", data, "bax", es)
                        es_flag = es_flag and (flag=="Success")
                    
                    if es_flag:
                        update_logs(f"{tag} {str(process_num).zfill(2)}.txt", str(bsn))
                    else:
                        update_logs(f"{tag} {str(process_num).zfill(2)}.txt", str(bsn), "./logs(failure)")

                if file_save:
                    folder_path = get_dir()
                    """
                    file_lst = os.listdir(folder_path)
                    num_lst = re.findall(tag + "_\d{1,2}_(\d{5}).","\n".join(file_lst))
                    file_name = tag + f"_{process_num}_"
                    if len(num_lst) < 1:
                        file_name = file_name + "00001"
                    else:
                        file_name = file_name + str(max([int(x) for x in num_lst])+1).zfill(5)
                    with open(f"{folder_path}/{file_name}.json","w",encoding="utf8") as f:
                        json.dump(result,f,ensure_ascii=False)
                    """
                    file_name = f"{tag}_{process_num}"

                    with open(f"{folder_path}/{file_name}.json","a+",encoding="utf8") as f:
                        if type(result) == list:
                            for res in result:
                                f.write(",\n"+f"{json.dumps(res,ensure_ascii=False)}")
                        else:
                            f.write(",\n"+f"{json.dumps(result,ensure_ascii=False)}")
                            
            except Exception as e:
                print("error:", e)
                folder_path = get_fail_log_dir()
                with open(f"{folder_path}/{tag}.txt","+a",encoding="utf8") as f:
                    f.write("\n"+f"from {tag}, {str(bsn)} {cmp_name}")
            
            finally:
                first_run = False
            
    finally:
        if webDriver is not None:
            webDriver.quit()
        if secondWebDriver is not None:
            secondWebDriver.quit()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", dest="tag",action="store", default="nice_biz_info")
    parser.add_argument("-p", "--process", dest="process",action="store", default="1")
    parser.add_argument("-d", dest="display_browser",action="store", default=False)

    args = parser.parse_args()
    tag = args.tag
    process_num = int(args.process)
    display_browser = args.display_browser

    bsn_set = make_set(tag)

    with open("./envs.json", "r", encoding="utf-8") as f:
        envs = json.load(f)
    
    console_print = envs.get("console_print",True)

    conn = get_mysql_conn(
        my_host=envs.get("db_host"),
        my_user=envs.get("db_user"),
        my_passwd=envs.get("db_password"),
        my_database=envs.get("db_database"),
        my_connect_timeout=envs.get("db_connect_timeout"),
    )
    biz_no_lst = list(get_biz_no_list(conn))

    biz_no_lst = list(filter(lambda o: not o[0] in bsn_set, biz_no_lst))

    # skip past works
    bsn_set_from_json = set()
    if "data_files" in os.listdir("./"):
        folder_lst = os.listdir(f"{os.getcwd()}/data_files")
        for folder_path in folder_lst:
            file_lst = os.listdir(f"{os.getcwd()}/data_files/{folder_path}")
            file_lst = list(filter(lambda o: tag in o, file_lst))
            for file_name in file_lst:
                with open(f"{os.getcwd()}/data_files/{folder_path}/{file_name}", "r", encoding="utf8") as f:
                    finded_bsn_lst = re.findall(r"\"BusinessNum\" ?: ?\"([^\"]+)\"", f.read())
                    bsn_set_from_json = bsn_set_from_json.union(finded_bsn_lst)
        biz_no_lst = list(filter(lambda o: o[0] not in bsn_set_from_json, biz_no_lst))

    # # fncsp 데이터베이스 컬럼 번호로 저장시
    # bsn_lst = [x[0] for x in biz_no_lst]
    # cmp_lst = [x[1] for x in biz_no_lst]
    # ceo_lst = [x[2] for x in biz_no_lst]

    # kms 데이터베이스 컬럼 번호로 저장시
    bsn_lst = [x[1] for x in biz_no_lst]
    cmp_lst = [x[3] for x in biz_no_lst]
    ceo_lst = [x[5] for x in biz_no_lst]
    
    es_info = {x:envs[x] for x in list(filter(lambda o: "es_"in o, envs.keys()))}

    end_date = datetime.now().strftime("%Y.%m.%d")
    """
    last_month = (datetime.strptime(f"{end_date[:-3]}.01","%Y.%m.%d") - timedelta(1)).month
    start_date_date = datetime.strptime(f"{end_date[:4]}.{str(last_month).zfill(2)}.{end_date[-2:]}","%Y.%m.%d")
    """
    if envs.get("start_date",None) is None:
        start_date_date = (datetime.strptime(end_date, "%Y.%m.%d") - timedelta(days=28))
        start_date = start_date_date.strftime("%Y.%m.%d")
    else:
        start_date = envs['start_date']

    head = 0
    gap = len(bsn_lst) // process_num
    tail = gap
    etc = len(bsn_lst) % process_num
    paramerers = []
    for i in range(process_num):
        paramerers.append(
            (
                bsn_lst[head:tail],
                cmp_lst[head:tail],
                ceo_lst[head:tail],
                tag,
                console_print,
                es_info,
                start_date,
                end_date,
                envs['file_save'],
                envs['es_save'],
                i,
                display_browser
            )
        )
        head = tail
        tail += gap

    if etc > 0:
        tmp = gap + etc
        paramerers.pop(-1)
        paramerers.append((bsn_lst[-tmp:], cmp_lst[-tmp:], ceo_lst[-tmp:], tag, console_print, es_info, start_date, end_date,envs['file_save'],envs['es_save'],(process_num-1),display_browser))

    # paramerers = list(map(lambda o: tuple(list(o[1]) + [o[0]+1]), enumerate(paramerers)))

    try:
        with pool.Pool() as pl:
            pl.starmap(async_group_work,paramerers)
    except Exception as e:
        print(e)

if __name__=="__main__":
    main()
    """
    crawl_kisti_article(biz_no="1130267960", company_name="신흥볼트",ceo_name=None,console_print=None,end_date=None,secondWebDriver=None,start_date=None,webDriver=None)
    crawl_kisti_patent(biz_no="1051081608", company_name="삼은이엔지",ceo_name=None,console_print=None,end_date=None,secondWebDriver=None,start_date=None,webDriver=None)
    """
