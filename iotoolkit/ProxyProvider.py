# @Author : taojinmin
# @Time : 2023/3/28 17:23
from abc import abstractmethod, ABC


class ProxyProvider(ABC):
    @abstractmethod
    async def get_proxy(self) -> str:
        """
        :return: should return "http://auth_name:auth_pwd@ip:port"
        """
        ...

