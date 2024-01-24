"""
Moduel version :
elasticsearch                      7.15.2
elasticsearch-dsl                  7.4.0
mysql                              0.0.3
mysql-connector                    2.2.9
mysql-connector-python             8.0.27
mysqlclient                        2.1.0
urllib3                            1.26.7
"""
# Global moduel
import socket
import datetime
import re
import copy
import datetime
import os
import functools
import pymysql

# Elastic moduel
from elasticsearch_dsl import Search, A, Q
from elasticsearch import Elasticsearch
from elasticsearch import helpers

# Mysql modeul
import MySQLdb


class func_para:
    # 기술연구소 사설ip 목록
    IP_LIST = [
        "192.168.120.155",
        "192.168.120.157",
        "192.168.120.156",
        "192.168.120.158",
        "192.168.120.159",
        "192.168.120.160",
        "192.168.120.161",
        "192.168.120.162",
        "192.168.120.163",
        "127.0.1.1",
    ]

    es_host1 = "192.168.120.159"  # 외부서버용
    es_host2 = "61.78.63.51"  # local 실행시
    es_port = 9200
    es_http_auth = ("sycns", "rltnfdusrnth")
    es_timeout = 100
    es_max_retries = 5
    es_retry_on_timeout = True
    es_scroll = "1m"
    es_scroll_size = 1000

    # mysql 외부 실행시 #
    sql_host1 = "192.168.120.160"
    sql_user1 = "nia"
    sql_pw1 = "nia123!!"

    # mysql local 실행시 #
    sql_host2 = "61.78.63.52"
    sql_user2 = "root"
    sql_pw2 = "[1SycnsDev20220404!@#]"
    sql_db = "nia"
    sql_connect_timeout = 36000


class Mysql_function:
    def __init__(self):
        ip_list = func_para.IP_LIST  # 기술연구소 사설 ip 목록
        ip = socket.gethostbyname(socket.gethostname())
        if ip in ip_list:
            self.my_host = func_para.sql_host1
            self.my_user = func_para.sql_user1
            self.my_passwd = func_para.sql_pw1
        else:
            self.my_host = func_para.sql_host2
            self.my_user = func_para.sql_user2
            self.my_passwd = func_para.sql_pw2

        self.my_database = func_para.sql_db
        self.my_connect_timeout = func_para.sql_connect_timeout

    def connect_mysql(self):  # mysql 연결 함수 , 5회까지 접속 재시도
        count = 0
        while True:
            try:
                if count > 5:
                    print(code)
                    return None
                else:
                    conn = pymysql.connect(
                        host=self.my_host,
                        user=self.my_user,
                        passwd=self.my_passwd,
                        database=self.my_database,
                        connect_timeout=self.my_connect_timeout,
                        cursorclass=pymysql.cursors.SSCursor,
                    )
                    return conn
            except Exception as e:
                code = e
                count += 1

    def get_mysql_col_name(self, table_name):  # mysql 컬럼 이름 출력하는 함수
        conn = self.connect_mysql()
        sql = f"SELECT * FROM {table_name} LIMIT 0"
        try:
            cur = conn.cursor()
            cur.execute(sql)

            result = [i[0] for i in cur.description]
            cur.close()
            conn.close()
            # result = [i for i in result if i != "COMPANY_NAME" and i != "CEO_NAME"]
            return result
        except:
            return []

    def get_bizNo_mysql(self, DataType, sql_=None):  # mysql 에서 사업자 번호 가져오는 함수
        try:
            conn = self.connect_mysql()
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
            # for i in cur:
            #     yield i
            res = cur.fetchall()
            cur.close()
            conn.close()
            return res
        except Exception as e:
            return None

    def get_biz_no_list(self):  # get_bizNo_mysql 함수에서 받아온 리스트 전처리 하는 함수
        try:
            sql = """SELECT BIZ_NO FROM SOURCE_DATA_STATUS ORDER BY BIZ_NO ASC"""
            biz_no_list = self.get_bizNo_mysql(DataType="", sql_=sql)
            biz_no_list = [str(i[0]) for i in biz_no_list]
            return biz_no_list
        except:
            return []

    def update_searchDate_mysql(self, DataType, biz_no_lst: list):
        try:
            # conn = mysql.connector.connect(
            conn = MySQLdb.connect(
                host=self.my_host,
                user=self.my_user,
                passwd=self.my_passwd,
                database=self.my_database,
                connect_timeout=self.my_connect_timeout,
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

        except Exception as e:
            print(e)
            # cur.close()
            # conn.close()
            return "False"

