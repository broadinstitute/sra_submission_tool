import logging


class LevelFilter(logging.Filter):
    def __init__(self, level):
        self.level = level

    def filter(self, record, ):
        if record.levelno >= self.level:
            return record
