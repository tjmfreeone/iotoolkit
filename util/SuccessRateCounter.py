# @Author : taojinmin
# @Time : 2023/4/4 12:36
from collections import deque


class SuccessRateCounter:
    """
    ->in ｜...｜0｜1｜0｜0｜...｜ ->out
    num in stat_q:
        0 for success
        1 for fail
    """
    stat_q: deque = None
    capacity = None

    def __init__(self, capacity: int = 100):
        self.stat_q = deque()
        self.capacity = capacity

    def success(self):
        if len(self.stat_q) >= self.capacity:
            self.stat_q.pop()
        self.stat_q.appendleft(0)

    def fail(self):
        if len(self.stat_q) >= self.capacity:
            self.stat_q.pop()
        self.stat_q.appendleft(1)

    def rate(self) -> str:
        if len(self.stat_q) > 0:
            return "%.2f%%" % (self.stat_q.count(0) * 100 / len(self.stat_q))
        return "0%"
