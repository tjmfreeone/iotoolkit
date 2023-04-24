# @Author : taojinmin
# @Time : 2023/2/7 16:37
import logging


class LogKit:
    fmstr = "%(asctime)s pid:%(process)d [%(levelname)s]: %(name)s | %(message)s |"
    datefm = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmstr, datefm)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger = logging.getLogger("Default")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)

    getter_batch_msg_tmpl = "src: {} | fetch: {} | progress: {}/{}, {}% | cost: {} | left: {}"
    getter_finish_msg_tmpl = "finished report | src: {} | total: {} | cost: {}"
    writer_batch_msg_tmpl = "dest: {} | write: {} | written: {} | cost: {}"

    grabber_succ_msg_tmpl = "SESS-{} | SUCCESS | {} | {} | status: {} | cost: {} | succ rate: {}"
    grabber_fail_msg_tmpl = "SESS-{} | FAIL({}) | {} | {} | cause: {} | cost: {} | succ rate: {}"
    grabber_give_up_msg_tmpl = "SESS-{} | GIVE UP | {} | {} | exceed retry times."

    class LevelNames:
        CRITICAL = logging.CRITICAL
        FATAL = logging.FATAL
        ERROR = logging.ERROR
        WARNING = logging.WARNING
        WARN = logging.WARN
        INFO = logging.INFO
        DEBUG = logging.DEBUG
        NOTSET = logging.NOTSET

    def set_logger_level(self, level):
        self.logger.setLevel(level)

    def set_logger_name(self, name: str):
        self.logger.name = name

    def __init_subclass__(cls, **kwargs):
        cls.logger = logging.getLogger(cls.__name__)
        cls.logger.setLevel(cls.LevelNames.DEBUG)
        cls.logger.addHandler(cls.stream_handler)
