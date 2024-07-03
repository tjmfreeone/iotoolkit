# @Author : taojinmin
# @Time : 2023/2/21 15:10
from iotoolkit.util import LogKit, DefaultValue, FuncSet, SuccessRateCounter
from iotoolkit import ProxyProvider
from itertools import cycle
from faker import Factory

import aiohttp
import time
import random
import asyncio


class StatusError(Exception):
    def __init__(self, error_code):
        self.error_code = error_code

    def __str__(self):
        return repr(f"status code error:{self.error_code}")


class ResponseIsNoneError(Exception):
    def __str__(self):
        return repr(f"got a null response")


class Grabber(LogKit):
    session_pool = list()
    user_agent_factory = Factory.create(providers=["faker.providers.user_agent", "faker.providers.date_time"])
    proxy_provider: ProxyProvider = None
    succ_counter = SuccessRateCounter(1000)

    def __init__(self, session_count: int = 2):
        if session_count < 1:
            raise ValueError("session count must be greater than 0!")
        self.session_count = session_count
        self._init_session_pool()

    def _init_session_pool(self):
        for i in range(self.session_count):
            self.session_pool.append((aiohttp.ClientSession(), i))
        self.sess_cycle_iter = cycle(self.session_pool)
        self.logger.info(f"Grabber init success, create {self.session_count} sessions.")

    async def _destroy_session_pool(self):
        while self.session_pool:
            sess, _ = self.session_pool.pop()
            await sess.close()

    async def request(self, method: str = "GET", url: str = None, headers: dict = None,
                      body=None, timeout=None, use_proxy=False, allow_redirects=True,
                      retry_times: int = 3, interval_tup: tuple = (1.0, 1.0), ssl=None):
        _timeout = timeout or DefaultValue.grab_time_out
        sess, sess_id = next(self.sess_cycle_iter)
        for t in range(retry_times):
            if use_proxy and self.proxy_provider:
                proxy = await self.proxy_provider.get_proxy()
            else:
                proxy = None
            start_ts = time.time()
            try:
                if not headers:
                    headers = {"User-Agent": self.user_agent_factory.user_agent()}
                resp = await sess.request(url=url, method=method, headers=headers,
                                          data=body, proxy=proxy, allow_redirects=allow_redirects,
                                          timeout=_timeout,
                                          ssl=ssl or False
                                          )
                if str(resp.status)[0] not in "23":
                    raise StatusError(resp.status)
                if resp is None:
                    raise ResponseIsNoneError()
                self.succ_counter.success()
                msg = self.grabber_succ_msg_tmpl.format(sess_id, method, url, resp.status,
                                                        FuncSet.x2humansTime(time.time() - start_ts), self.succ_counter.rate())
                self.logger.info(msg)
                return resp
            except Exception as e:
                self.succ_counter.fail()
                msg = self.grabber_fail_msg_tmpl.format(sess_id, t + 1, method, url, e.__repr__(),
                                                        FuncSet.x2humansTime(time.time() - start_ts), self.succ_counter.rate())
                self.logger.error(msg)
                await asyncio.sleep(random.uniform(*interval_tup))
                sess, sess_id = next(self.sess_cycle_iter)
        else:
            msg = self.grabber_give_up_msg_tmpl.format(sess_id, method, url)
            self.logger.error(msg)
            
    def set_proxy_provider(self, provider: ProxyProvider):
        self.proxy_provider = provider

    async def destroy(self):
        await self._destroy_session_pool()
        msg = "Grabber object has been destroyed."
        self.logger.info(msg)
