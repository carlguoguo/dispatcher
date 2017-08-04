#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time


logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
logger.propagate = False


class Job:

    def __init__(self, command, dispatcher):
        self.command = command
        self.dispatcher = dispatcher

    # 强制重写
    def map_job(self, worker):
        raise NotImplementedError

    # 强制重写
    def reduce_job(self, workers):
        raise NotImplementedError

    def do(self):
        current = time.time()
        logger.info("[DISPATCHER] start doing job")
        self.dispatcher.execute(self.command, self.map_job, self.reduce_job)
        logger.info("[DISPATCHER] finish job within: {0}s".format(time.time() - current))
