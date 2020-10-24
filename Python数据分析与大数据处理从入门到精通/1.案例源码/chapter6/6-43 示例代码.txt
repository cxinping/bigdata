import logging
from logging import handlers


class Logger(object):
    def __init__(self, log_name, level):
        self.logger = logging.getLogger(log_name)
        format = logging.Formatter("""
        %(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s
        """)
        self.logger.setLevel(level)
        tfh = handlers.TimedRotatingFileHandler(filename=log_name,
                                                when="D",
                                                backupCount=2,
                                                encoding="utf-8")
        tfh.setFormatter(format)
        self.logger.addHandler(tfh)


if __name__ == "__main__":
    log = Logger("python_log.log", level=logging.ERROR)
    try:
        a = 1 / 0
    except Exception as e:
        log.logger.error("错误信息是：{0}".format(e))
