# @Author : taojinmin
# @Time : 2023/2/7 16:37
import logging


class LogKit:
    fmstr = "%(asctime)s pid:%(process)d [%(levelname)s]: | %(name)s | %(message)s |"
    datefm = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmstr, datefm)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger = logging.getLogger("DefaultLogger")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)
    getter_batch_msg_tmpl = "src: {} | fetch: {} | progress: {}/{}, {}% | cost: {} | left: {}"
    getter_finish_msg_tmpl = "finished report | src: {} | total: {} | cost: {}"

    writer_batch_msg_tmpl = "dest: {} | write: {} | written: {} | cost: {}"

    def new_logger(self, name: str):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(self.stream_handler)
