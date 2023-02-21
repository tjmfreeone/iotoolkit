# @Author : taojinmin
# @Time : 2023/2/21 15:10
from iotoolkit.util import LogKit, DefaultValue, FuncSet
from itertools import cycle

import aiohttp
import time
import random
import asyncio


class StatusError(Exception):
    def __init__(self, error_code):
        self.error_code = error_code

    def __str__(self):
        return repr(f"status code error:{self.error_code}")


class Grabber(LogKit):
    session_pool = list()

    def __init__(self, session_count: int = 2):
        if session_count < 1:
            raise ValueError("session count must be greater than 1!")
        self.new_logger(self.__class__.__name__)
        self.session_count = session_count
        self._init_session_pool()

    def _init_session_pool(self):
        for _ in range(self.session_count):
            self.session_pool.append(aiohttp.ClientSession())
        self.sess_cycel_iter = cycle(self.session_pool)
        self.logger.info(f"Grabber init success, create {self.session_count} sessions.")

    async def _destroy_session_pool(self):
        while self.session_pool:
            sess = self.session_pool.pop()
            await sess.close()

    async def request(self, method: str = "GET", url: str = None, headers: dict = None,
                      body=None, timeout=None, proxy=None,
                      retry_times: int = 3, interval_tup: tuple = (1.0, 1.0)):
        for t in range(retry_times):
            start_ts = time.time()
            try:
                sess: aiohttp.ClientSession = next(self.sess_cycel_iter)
                resp = await sess.request(url=url, method=method, headers=headers,
                                          data=body, proxy=proxy,
                                          timeout=timeout or DefaultValue.grab_time_out,
                                          )
                if str(resp.status)[0] != "2":
                    raise StatusError(resp.status)
                resp_body = await resp.read()
                msg = self.grabber_succ_msg_tmpl.format(method, url, resp.status,
                                                        len(resp_body), FuncSet.x2humansTime(time.time() - start_ts))
                self.logger.info(msg)
                return resp_body
            except Exception as e:
                msg = self.grabber_fail_msg_tmpl.format(method, url, e, FuncSet.x2humansTime(time.time() - start_ts), t)
                self.logger.error(msg)
                await asyncio.sleep(random.uniform(*interval_tup))
        else:
            msg = self.grabber_give_up_msg_tmpl.format(method, url)
            self.logger.error(msg)

    async def destroy(self):
        await self._destroy_session_pool()

