#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
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
        command_type = command.split(' ')[0]
        self.remote_result_filepath = '/tmp/{0}.result'.format(command_type)
        self.local_result_filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), command_type)
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
        logger.info("[DISPATCHER] finish jobs within: {0}s".format(time.time() - current))
