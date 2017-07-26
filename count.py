#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import sys
import logging

from dispatcher import Dispatcher
from job import Job
from utils import delete_files, load_cfg

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False


class ZGrep(Job):

    def __init__(self, command, dispatcher):
        self.local_result_filepaths = []
        self.occurrences = 0
        Job.__init__(self, command, dispatcher)

    def map_job(self, worker):
        # 各worker结果暂存文件路径
        temp_result_filepath = self.local_result_filepath + '-' + str(worker.ip)
        # 获取worker结果文件到本地temp_result_filepath
        worker.get(self.remote_result_filepath, temp_result_filepath)
        self.local_result_filepaths.append(temp_result_filepath)

    def reduce_job(self):
        for filepath in self.local_result_filepaths:
            occurrence = open(filepath).readline()
            try:
                self.occurrences += int(occurrence)
            except:
                logger.error("data from [{0}] is not normal".format(filepath))
        logger.info("[DISPATCHER] merge temp data from all workers".format(self.local_result_filepath))
        logger.info("[DISPATCHER] total count: {0}".format(self.occurrences))
        delete_files(self.local_result_filepaths)


def init_args():
    parser = argparse.ArgumentParser(description='Count specific keyword among remote server logs')
    parser.add_argument('-k', help='keyword')
    parser.add_argument('-f', help='filename e.g.tomcat1-httpClient.log')
    parser.add_argument('-c', help='config file')
    args = parser.parse_args()
    return args.k, args.f, args.c or 'config.json'


def zgrep(command, servers):
    job = ZGrep(command, Dispatcher(servers))
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
    job = ZGrep(command, Dispatcher(servers))
    job.do()
