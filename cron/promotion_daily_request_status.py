#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import sys
import datetime

from utils import logger, send_mail, load_cfg
from count import zgrep

cfg = {
    "job": {
        "commands": [
            "zgrep -c 'promotion' /letv/logs/tomcat/iptv/tomcat1-httpClient.log{0}",
            "zgrep 'promotion' /letv/logs/tomcat/iptv/tomcat1-httpClient.log{0} | grep -vc '|200|'"
        ],
    },
    "mail": {
        "subject": u"观星失败请求量",
        "content": u"{0}当天总请求次数：{1} 失败请求次数：{2}",
        "to": "guoyunfeng@le.com;dengliwei@le.com;maning5@le.com;caidongfang@le.com",
        "cc": "zhuyi3@le.com"
    }
}


def init_args():
    parser = argparse.ArgumentParser(description='Count specific keyword among remote server logs')
    parser.add_argument('-d', help='date string; e.g 2017-09-01')
    parser.add_argument('-c', help='config file')
    args = parser.parse_args()
    return args.d, args.c or 'config.json'

if __name__ == '__main__':
    date_str, cfg_file = init_args()
    _commands = cfg.get("job", {}).get("commands", [])
    commands = [_command.format('.' + date_str + '.gz') if date_str else _command.format('') for _command in _commands]
    global_cfg = load_cfg(cfg_file)
    if not global_cfg:
        sys.exit(0)
    servers = global_cfg.get("servers", [])
    _servers = [(server.split(' ')[0], server.split(' ')[1], server.split(' ')[2])
                for server in servers if len(server.split(' ')) == 3]
    servers = _servers if _servers else servers
    if not commands or not servers:
        logger.error("miss required configuration")
        sys.exit(0)
    if not date_str:
        date_str = datetime.date.today() - datetime.timedelta(days=1)

    total_requests = zgrep(commands[0], servers)
    failed_requests = zgrep(commands[1], servers)

    mail = cfg.get("mail", {})
    if mail and 'subject' in mail and 'content' in mail and 'to' in mail:
        subject = mail.get('subject')
        content = mail.get('content', '').format(date_str, total_requests, failed_requests)
        to = mail.get('to')
        cc = mail.get('cc')
        send_mail(subject, content, to, cc)
