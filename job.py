#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import logging
import time
import hashlib


logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
logger.propagate = False


class Job:

    def __init__(self, command, dispatcher):
        result_filename = hashlib.md5(command).hexdigest()
        self.remote_result_filepath = '/tmp/{0}'.format(result_filename)
        self.local_result_filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), result_filename)
        self.command = command + ' > {0}'.format(self.remote_result_filepath)
        self.dispatcher = dispatcher

    # 强制重写
    def map_job(self, worker):
        raise NotImplementedError

    # 强制重写
    def reduce_job(self):
        raise NotImplementedError

    def do(self):
        current = time.time()
        logger.info("[DISPATCHER] start doing job")
        self.dispatcher.execute(self.command, self.map_job, self.reduce_job)
        logger.info("[DISPATCHER] finish job within: {0}s".format(time.time() - current))
