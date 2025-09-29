#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
#
# Author: Komal Thareja (kthare10@renci.org)
import logging
import os
import threading
from logging.handlers import RotatingFileHandler
from pathlib import Path


class LogHelper:
    @staticmethod
    def make_logger(*, log_dir: Path = ".", log_file: str, log_level, log_retain: int, log_size: int, logger: str,
                    log_format: str = None):
        """
        Detects the path and level for the log file from the actor config and sets
        up a logger. Instead of detecting the path and/or level from the
        config, a custom path and/or level for the log file can be passed as
        optional arguments.

       :param log_dir: Log directory
       :param log_file
       :param log_level
       :param log_retain
       :param log_size
       :param logger
       :param log_format
       :return: logging.Logger object
        """
        log_path = log_dir / log_file
        print(log_path)

        if log_path is None:
            raise RuntimeError('The log file path must be specified in config or passed as an argument')

        if log_level is None:
            log_level = logging.INFO

        # Set up the root logger
        log = logging.getLogger(logger)
        log.setLevel(log_level)
        default_log_format = \
            '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s-%(thread_id)s]- %(levelname)s - %(message)s'
        if log_format is not None:
            default_log_format = log_format

        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        file_handler = RotatingFileHandler(log_path, backupCount=int(log_retain), maxBytes=int(log_size))
        file_handler.addFilter(LogHelper.thread_id_filter)
        file_handler.setFormatter(logging.Formatter(default_log_format))
        log.addHandler(file_handler)

        console_log = logging.getLogger()
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.CRITICAL)
        console_log.addHandler(console_handler)

        #logging.basicConfig(handlers=[file_handler], format=log_format, force=True)
        #file_handler.addFilter(LogHelper.thread_id_filter)

        # Disable console logging to prevent /var partition from filling up with container logs
        console_log = logging.getLogger()
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.CRITICAL)
        console_log.addHandler(console_handler)
        return log

    @staticmethod
    def thread_id_filter(record):
        """Inject thread_id to log records"""
        record.thread_id = threading.get_native_id()
        return record
