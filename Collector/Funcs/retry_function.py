# RETRY DECORATOR
import platform
from functools import wraps
from typing import Any
# if platform.system()=="Windows":
#     from . import write_log
# else:
#     from Funcs import write_log
if platform.system()=="Linux":
    from . import write_log
else:
    from Funcs import write_log



def retry(number_of_retry, match_type):  # -> 함수의 return or None 반환
    assert number_of_retry > 0, "최대 반복회수는 양수를 입력하시오"

    def deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            def new_func():
                try:
                    ret = func(*args, **kwargs)
                    return ret
                except Exception as e:
                    return e

            count = 0
            while True:
                if count > number_of_retry:
                    write_log.write_log(
                        BIZ_NO=args[0], DATA_TYPE=func.__module__, ERR_LOG=str(ret)
                    )
                    return None
                else:
                    ret = new_func()
                    if not isinstance(ret, match_type):
                        count += 1
                    else:
                        return ret

        return wrapper

    return deco
