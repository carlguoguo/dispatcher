#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from functools import partial
from multiprocessing import Process, Manager, Pool
import select

from worker import Worker

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
logger.propagate = False


class Dispatcher(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Dispatcher, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, nodes):
        self.workers = []
        for node in nodes:
            if len(node) == 3:
                ip, username, password = node
                self.workers.append(Worker(ip, username, password))
            else:
                self.workers.append(Worker(node))
        self.unready_fd_workers_dict = {}
        self.to_do_workers = []

    def is_all_workers_finish(self):
        return len(self.unready_fd_workers_dict) == 0

    def execute(self, command, map_job, reduce_job):
        for worker in self.workers:
            try:
                worker.connect()
                channel = worker.exec_command(command)
                if channel:
                    self.to_do_workers.append(channel)
                self.unready_fd_workers_dict[channel] = worker
            except OSError, e:
                logger.error(e)

        while True:
            if not self.to_do_workers:
                logger.warn("[DISPATCHER] no workers ")
                break
            if self.is_all_workers_finish():
                logger.info("[DISPATCHER] all workers done")
                reduce_job()
                break

            readables, writables, exceptionals = select.select(self.unready_fd_workers_dict.keys(), [], [])
            finished_workers = [self.unready_fd_workers_dict.pop(readable) for readable in readables]
            logger.info("[DISPATCHER] received data from {0}".format([worker.ip for worker in finished_workers]))
            map(map_job, finished_workers)


def _connect_and_exec(command, unready_fd_workers_dict, worker):
    try:
        worker.connect()
        channel = worker.exec_command(command)
        unready_fd_workers_dict.put((channel, worker))
    except OSError, e:
        logger.error(e)


# TODO多进程Dispatcher
class MultiProcessDispatcher(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(MultiProcessDispatcher, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, nodes):
        self.workers = [Worker(ip, username, password) for ip, username, password in nodes]
        self.manager = Manager()
        self.unready_fd_workers_dict = self.manager.Queue()

    def is_all_workers_finish(self):
        return len(self.unready_fd_workers_dict) == 0

    def execute(self, command, map_job, reduce_job):
        import time
        pool = Pool()
        func = partial(_connect_and_exec, command, self.unready_fd_workers_dict)
        pool.map(func, self.workers)
        # while True:
        #     print 'aa'
            # if self.is_all_workers_finish():
            #     reduce_job()
            #     break
            # print self.unready_fd_workers_dict.get()
            # time.sleep(1)
            # readables, writables, exceptionals = select.select(self.unready_fd_workers_dict.keys(), [], [])
            # unready_fd_workers_dict =
            #
            # finished_workers = [self.unready_fd_workers_dict.pop(readable) for readable in readables]
            # map(map_job, finished_workers)
