import argparse
import sys
import datetime

from utils import load_cfg, logger, send_mail
from zgrep import zgrep

cfg = {
    "job": {
        "servers": [
            "10.87.180.22 root ott@20170601"
        ],
        "commands": [
            "zgrep -c 'promotion' /letv/logs/tomcat/iptv/tomcat1-httpClient.log{0}",
            "zgrep 'promotion' /letv/logs/tomcat/iptv/tomcat1-httpClient.log{0} | grep -vc '|200|'"
        ]
    },
    "mail": {
        "subject": "观星请求量统计",
         "content": "{0}当天总请求次数：{1} 失败请求次数：{2}",
        "to": "guoyunfeng@le.com",
        "cc": "780647243@qq.com"
    }
}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Count specific keyword among remote server logs')
    parser.add_argument('-d', help='date string; e.g 2017-09-01')
    args = parser.parse_args()
    date_str = args.d
    _commands = cfg.get("job", {}).get("commands", [])
    commands = [_command.format('.' + date_str + '.gz') if date_str else _command.format('') for _command in _commands]
    servers = cfg.get("job", {}).get("servers", '')
    _servers = [(server.split(' ')[0], server.split(' ')[1], server.split(' ')[2])
                for server in servers if len(server.split(' ')) == 3]
    servers = _servers if _servers else servers
    if not commands or not servers:
        logger.error("miss required configuration")
        sys.exit(0)
    if not date_str:
        date_str = datetime.datetime.now().strftime('%Y-%m-%d')
    total_requests = zgrep(commands[0], servers, date_str)
    failed_requests = zgrep(commands[1], servers, date_str)
    mail = cfg.get("mail", {})
    if mail and 'subject' in mail and 'content' in mail and 'to' in mail:
        subject = mail.get('subject')
        content = mail.get('content').format(date_str, total_requests, failed_requests)
        to = mail.get('to')
        cc = mail.get('cc')
        send_mail(subject, content, to, cc)
