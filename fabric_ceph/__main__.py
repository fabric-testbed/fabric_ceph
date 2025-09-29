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
import time
import traceback
from json import encoder

import connexion
import waitress

from fabric_ceph.common.config import Config
from fabric_ceph.common.graceful_interrupt_handler import GracefulInterruptHandler


def main():

    try:
        with GracefulInterruptHandler() as h:

            cfg = Config.load_from_file("./config.yml")
            cfg.logging.apply()
            log = logging.getLogger(cfg.logging.logger)
            port = cfg.runtime.port
            if port is None:
                raise Exception("Invalid configuration rest port not specified")

            print("Starting REST")
            # start swagger
            app = connexion.App(__name__, specification_dir='openapi_server/openapi/')
            app.json = encoder.JSONEncoder
            app.add_api('openapi.yaml', arguments={'title': 'Fabric CEPH API'}, pythonic_params=True)

            # Start up the server to expose the metrics.
            waitress.serve(app, port=int(port), threads=8)

            while True:
                time.sleep(0.0001)
                if h.interrupted:
                    break
    except Exception:
        traceback.print_exc()


if __name__ == '__main__':
    main()
