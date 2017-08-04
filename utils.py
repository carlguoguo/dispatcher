#!/usr/bin/env python
# -*- coding: utf-8 -*-
import subprocess
import logging
import smtplib
import os
import json
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
logger.propagate = False


def delete_files(filepaths):
    delete_files_command = ["rm", "-rf"]
    delete_files_command.extend(filepaths)
    logger.debug("execute command [{0}]".format(' '.join(delete_files_command)))
    p = subprocess.Popen(delete_files_command, stdout=subprocess.PIPE)
    ret_code = p.wait()
    if ret_code:
        logger.error("failed to delete files [{0}]".format(','.join(filepaths)))
    else:
        logger.info("successfully delete files [{0}]".format(','.join(filepaths)))
    return ret_code


def load_cfg(cfg):
    if not cfg.endswith(".json"):
        cfg = cfg + '.json'
    config_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'configs')
    cfg_path = os.path.join(config_folder, cfg)
    if not os.path.isfile(cfg_path):
        logger.error("configuration file not found [{0}]".format(cfg_path))
        return None
    with open(cfg_path) as data_file:
        data = json.load(data_file)
    return data


def send_mail(subject, content, mailto, cc):
    mail_host = "smtp.letv.cn"
    mail_user = "letv_monitor"  # 用户名
    mail_pass = "!@s20170731"  # 密码
    mail_from = "letv_monitor@le.com"
    msg = MIMEText(content, 'plain', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = mail_from
    msg['To'] = mailto  # 将收件人列表以‘；’分隔
    msg['CC'] = cc
    mailto_list = mailto.split(';') if ';' in mailto else [mailto]
    if cc:
        cc_list = cc.split(';') if ';' in cc else [cc]
        mailto_list = mailto_list + cc_list
    try:
        server = smtplib.SMTP()
        server.connect(mail_host)  # 连接服务器
        server.login(mail_user, mail_pass)  # 登录操作
        server.sendmail(mail_from, mailto_list, msg.as_string())
        server.close()
        logger.info("successfully send email to [{0}]".format(','.join(mailto_list)))
    except Exception, e:
        logger.error("failed to send email [{0}]".format(e))
        return False
