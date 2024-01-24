# 데이터 패턴의 유효성 체크
import sys
from os import path
# 상위 폴더 위치
dir = path.abspath('../../..')
sys.path.append(dir)

from libraries import *

### 자료형 ##############################################################################################
def type_int(data):
    try:
        data = int(data)
        if not str(data).isdecimal():
            data = None
    except:
        pass
    return data

### 최소/최대 조건 ##############################################################################################
def gte_0(data):
    try:
        if float(data) < 0:
            data = None
    except:
        pass
    return data


### 날짜 ##############################################################################################
# 날짜패턴 : yyyy
def date_yyyy(data):
    try:
        regex = r'\d{4}'
        if data and not bool(re.match(regex, data)):
            data = None
    except:
        pass
    return data
# 날짜패턴 : yyyy-mm
def date_yyyymm(data):
    try:
        regex = r'\d{4}-\d{2}'
        if data and not bool(re.match(regex, data)):
            data = None
    except:
        pass
    return data
# 날짜패턴 : yyyy-mm-dd
def date_yyyymmdd(data):
    try:
        regex = r'\d{4}-\d{2}-\d{2}'
        if data and not bool(re.match(regex, data)):
            data = None
    except:
        pass
    return data
