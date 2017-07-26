#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import sys
import logging

import datetime

from dispatcher import Dispatcher
from job import Job
from utils import delete_files, send_mail, load_cfg

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
logger.propagate = False


class ZGrep(Job):

    def __init__(self, command, dispatcher, date_str):
        self.local_result_filepaths = []
        self.date_str = date_str
        self.occurrences = 0
        Job.__init__(self, command, dispatcher)

    def map_job(self, worker):
        # 各worker结果暂存文件路径
        temp_result_filepath = self.local_result_filepath + '-' + str(id(worker))
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Count specific keyword among remote server logs')
    parser.add_argument('-c', help='configuration file name')
    parser.add_argument('-d', help='date string; e.g 2017-09-01')
    args = parser.parse_args()
    cfg_file = args.c
    date_str = args.d

    cfg = load_cfg(cfg_file)
    if not cfg:
        sys.exit(0)
    _command = cfg.get("job", {}).get("command", '')
    command = _command.format('.' + date_str + '.gz') if date_str else _command.format('')
    servers = cfg.get("job", {}).get("servers", '')
    _servers = [(server.split(' ')[0], server.split(' ')[1], server.split(' ')[2])
               for server in servers if len(server.split(' ')) == 3]
    servers = _servers if _servers else servers
    if not command or not servers:
        logger.error("miss required configuration")
        sys.exit(0)
    if not date_str:
        date_str = datetime.datetime.now().strftime('%Y-%m-%d')
    job = ZGrep(command, Dispatcher(servers), date_str=date_str)
    job.do()
    mail = cfg.get("mail", {})
    if mail and 'subject' in mail and 'content' in mail:
        send_mail(mail.get('subject'), mail.get('content').format(date_str, job.occurrences))
