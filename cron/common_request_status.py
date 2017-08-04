#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import sys
import datetime

from utils import logger, send_mail, load_cfg
from count import count

cfg = {
    "job": {
        "commands": [
            "wc -l /letv/logs/nginx/host.access.log-{0}.gz | awk '{{print $1}}'",
        ],
    },
    # "mail": {
    #     "subject": u"请求统计",
    #     "content": u"{0}当天VV：{1}",
    #     "to": "guoyunfeng@le.com",
    # }
}


def init_args():
    parser = argparse.ArgumentParser(description='Count specific keyword among remote server logs')
    parser.add_argument('-d', help='date string; e.g 20170901')
    parser.add_argument('-c', help='config file')
    args = parser.parse_args()
    return args.d, args.c or 'config.json'

if __name__ == '__main__':
    date_str, cfg_file = init_args()
    # 如果不给日期，视为前一天
    if not date_str:
        date_str = datetime.date.today() - datetime.timedelta(days=1)
        date_str = date_str.strftime('%Y%m%d')
    _commands = cfg.get("job", {}).get("commands", [])
    commands = [_command.format(date_str) if date_str else _command.format('') for _command in _commands]
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

    vv = count(commands[0], servers)

    mail = cfg.get("mail", {})
    if mail and 'subject' in mail and 'content' in mail and 'to' in mail:
        subject = mail.get('subject')
        content = mail.get('content', '').format(date_str, vv)
        to = mail.get('to')
        cc = mail.get('cc')
        send_mail(subject, content, to, cc)
