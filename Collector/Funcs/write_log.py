# import sys
# from os import path
# # 상위 폴더 위치
# dir = path.abspath('../../..')
# dir = path.abspath('./')
# dir = path.abspath('../')
# sys.path.append(dir)
import platform
from libraries import *
"""
if platform.system()=="Windows":
    import funcs
else:
    from Funcs import funcs
"""
from . import funcs
# BIZ_NO			사업자번호
# DATA_TYPE			데이터타입
# ERR_DATE			에러발생날짜
# ERR_LOG			에러명세
# HOST_NAME			호스트명
# HOST_IP			호스트 IP
# SYS_INFO			시스템 운영체제
# CHECK_YN			작업자 확인여부
# CHECK_DATE		작업자 확인날짜
# COMMENT			코멘트

### 로그 남기기 ###
Flagwritelog = True


def write_log(BIZ_NO, DATA_TYPE, ERR_LOG):
    if Flagwritelog:
        try:
            # log 작성시간
            ERR_DATE = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # 호스트명
            HOST_NAME = socket.gethostname()
            # 호스트(IP)
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(("8.8.8.8", 80))  # ip, port로 연결 테스트
                HOST_IP = s.getsockname()[0]
            except:
                HOST_IP = socket.gethostbyname(socket.gethostname())
            # 시스템 운영체제
            SYS_INFO = platform.platform()

            # mysql에 로그작성
            sql = "INSERT INTO SOURCE_ERR_LOG (BIZ_NO, DATA_TYPE, ERR_DATE, ERR_LOG, HOST_NAME, HOST_IP, SYS_INFO) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            val = (
                BIZ_NO,
                DATA_TYPE,
                ERR_DATE,
                str(ERR_LOG),
                HOST_NAME,
                HOST_IP,
                SYS_INFO,
            )
            conn = funcs.get_mysql_conn()
            if conn:
                cur = conn.cursor()
                cur.execute(sql, val)
            conn.commit()
            conn.close()
        except Exception as e:
            flag = "fail"
            print("!!! ERROR LOG WRITE !!! : {} > {}".format(flag, e))


# if __name__ == "__main__":
#     write_log("2208736743", "test", "this error is made for testing")
