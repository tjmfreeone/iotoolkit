# @Author : taojinmin
# @Time : 2023/2/7 18:42
from functools import wraps
from types import FunctionType
from . import LogKit
import inspect
import random
import asyncio
        
min_secs = 60
hour_secs = 3600
day_secs = 3600 * 24


def x2humansTime(secs):
    """
    transform seconds to a string that human can easily comprehend
    :param secs: seconds
    :return: time string that human can comprehend
    """
    h_time = ""
    if secs < min_secs:
        secs = "%.3f" % secs
        h_time = "{}s".format(secs)
    elif min_secs <= secs < hour_secs:
        mins, secs = secs // min_secs, secs % min_secs
        secs = "%.3f" % secs
        h_time = "{}m{}s".format(int(mins), secs)
    elif hour_secs <= secs < day_secs:
        hours, secs = secs // hour_secs, secs % hour_secs
        mins, secs = secs // min_secs, secs % min_secs
        secs = "%.3f" % secs
        h_time = "{}h{}m{}s".format(int(hours), int(mins), secs)
    else:
        days, secs = secs // day_secs, secs % day_secs
        hours, secs = secs // hour_secs, secs % hour_secs
        mins, secs = secs // min_secs, secs % min_secs
        secs = "%.3f" % secs
        h_time = "{}d{}h{}m{}s".format(int(days), int(hours), int(mins), secs)
    return h_time


async def retry(func, times: int = 3, interval_tup: tuple = (1.0, 1.0), logger=None):
    if not logger:
        raise ValueError("logger must not be None!")
    is_coro_func = inspect.iscoroutinefunction(func)

    @wraps(func)
    async def wrapper(*args, **kwargs):
        for t in range(times):
            try:
                if is_coro_func:
                    result = await func(*args, **kwargs)
                    return result
                else:
                    result = func(*args, **kwargs)
                    return result
            except Exception as e:
                logger.error(f"exec func {func.__name__} fail, try:{t}")
                interval = random.uniform(interval_tup[0], interval_tup[1])
                if is_coro_func:
                    await asyncio.sleep(interval)
                else:
                    result = func(*args, **kwargs)
                    return result
        else:
            logger.error(f"retry times exceed, give up.")
    return wrapper


def ensure_connected(method):
    @wraps(method)
    async def wrapper(method_this, *args, **kwargs):
        if not inspect.iscoroutinefunction(method):
            raise ValueError("method must be coroutine function...")
        if not method_this.is_ready():
            method_this.logger.info(
                "{}[{}]'s connection building...".format(method_this.scheme, method_this.conn_config.get("db")))
            await method_this._build_connect()
        result = await method(method_this, *args, **kwargs)
        return result
    return wrapper