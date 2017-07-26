#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 对Paramiko简单封装
import logging

import paramiko
from paramiko.ssh_exception import AuthenticationException, SSHException

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
logger.propagate = False


class Worker:
    def __init__(self, ip, username=None, password=None):
        self.ip = ip
        self.username = username if username else 'root'
        self.password = password
        self.connection = None
        logger.debug("[DISPATCHER] init dispatcher [{0}]".format(self.ip))

    def _get_sftp(self):
        """延迟建立，减少开销，公用sftp实例"""
        if not self.is_connected:
            raise RuntimeError("[NODE {0}] connection error".format(self.ip))
        return getattr(self, '_sftp', self.connection.open_sftp())

    def _need_auth(self):
        return self.username and self.password

    def connect(self, timeout=3):
        try:
            _connect = paramiko.SSHClient()
            _connect.load_system_host_keys()
            if self._need_auth():
                _connect.connect(self.ip, username=self.username, password=self.password, timeout=timeout)
            else:
                _connect.connect(self.ip, timeout=timeout)
            self.connection = _connect
        except AuthenticationException, e:
            logger.error(e)
            self.connection = None
        except SSHException, e:
            logger.error(e)
            self.connection = None

    def is_connected(self):
        return self.connection is not None

    def exec_command(self, command):
        if not self.is_connected():
            logger.error("[NODE {0}] connection error".format(self.ip))
            return None
        _channel = self.connection.get_transport().open_session()
        _channel.exec_command(command)
        logger.info('[NODE {0}] start executing command [{1}]'.format(self.ip, command))
        return _channel

    def get(self, filepath, local_path):
        sftp = self._get_sftp()
        logger.info('[NODE {0}] fetch resource [{1}]'.format(self.ip, filepath))
        sftp.get(filepath, local_path)
        logger.info('[NODE {0}] copy to local path [{1}]'.format(self.ip, local_path))