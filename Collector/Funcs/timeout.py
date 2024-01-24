from threading import Thread
from functools import wraps


def timeout(seconds_before_timeout):
    def deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            res = [
                Exception(
                    "function [%s] timeout [%s seconds] exceeded!"
                    % (func.__name__, seconds_before_timeout)
                )
            ]

            def newFunc():
                try:
                    res[0] = func(*args, **kwargs)
                except Exception as e:
                    res[0] = e

            t = Thread(target=newFunc)
            t.name = "Time_check"
            t.daemon = True
            try:
                t.start()
                t.join(seconds_before_timeout)
            except Exception as e:
                print("error starting thread")
                raise e
            ret = res[0]
            if isinstance(ret, BaseException):
                if t._tstate_lock is not None:
                    t._tstate_lock.release()
                    t._stop()
                    raise Exception(ret)
            return ret

        return wrapper

    return deco
