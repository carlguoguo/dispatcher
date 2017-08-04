#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import sys
import logging

from dispatcher import Dispatcher
from job import Job
from utils import load_cfg

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False


class Count(Job):

    def __init__(self, command, dispatcher):
        self.occurrences = 0
        Job.__init__(self, command, dispatcher)

    def map_job(self, worker):
        pass

    def reduce_job(self, workers):
        for worker in workers:
            try:
                count = int(worker.get_result())
                self.occurrences += count
            except:
                logger.error("[DISPATCHER] data error form [{0}]".format(worker.ip))
        logger.info("[DISPATCHER] total count: {0}".format(self.occurrences))


def init_args():
    parser = argparse.ArgumentParser(description='Count specific keyword among remote server logs')
    parser.add_argument('-k', help='keyword')
    parser.add_argument('-f', help='filename e.g.tomcat1-httpClient.log')
    parser.add_argument('-c', help='config file')
    args = parser.parse_args()
    return args.k, args.f, args.c or 'config.json'


def count(command, servers):
    job = Count(command, Dispatcher(servers))
    job.do()
    return job.occurrences


if __name__ == '__main__':
    keyword, log_file, cfg_file = init_args()
    cfg = load_cfg(cfg_file)
    if not cfg:
        sys.exit(0)
    servers = cfg.get("servers", [])
    _servers = [(server.split(' ')[0], server.split(' ')[1], server.split(' ')[2])
                for server in servers if len(server.split(' ')) == 3]
    servers = _servers if _servers else servers
    if not servers:
        logger.error("miss required configuration")
        sys.exit(0)
    command = "zgrep -c '{0}' {1}".format(keyword, log_file)
    job = Count(command, Dispatcher(servers))
    job.do()
