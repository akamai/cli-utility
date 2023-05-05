from __future__ import annotations

import json
import logging.config
import os
import time
from pathlib import Path

import coloredlogs
from utils.parser import Parser

origin_config = 'bin/config/logging.json'
Path('logs').mkdir(parents=True, exist_ok=True)


def setup_logger(logfile: str | None = None):
    args = Parser.get_args()
    with open(origin_config) as f:
        log_cfg = json.load(f)
    logging.config.dictConfig(log_cfg)
    logging.Formatter.converter = time.gmtime

    if args.log_file:
        logfile = f'{args.log_file}.log'

    if logfile:
        logger = logging.getLogger(logfile)
        file_handler = logging.FileHandler(f'logs/{logfile}', 'w')
        logFormatter = logging.Formatter('%(asctime)s %(process)d %(filename)-17s %(lineno)-3d %(levelname)s: %(message)s')
        file_handler.setFormatter(fmt=logFormatter)
        logger.addHandler(file_handler)
    else:
        logger = logging.getLogger(__name__)
    coloredlogs.install(logger=logger, fmt='%(levelname)-7s: %(message)s')

    # https://github.com/xolox/python-coloredlogs/issues/54
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        for h in logging.getLogger().handlers:
            h.setLevel(logging.NOTSET)  # reset colored handler level

    # allow override log filename
    '''
        LOG_FILE = Path(f'logs/{args.log_file}.log').absolute()
        LOG_FILE.touch(exist_ok=True)
        file_handler = logging.FileHandler(LOG_FILE, 'a')
        log = logging.getLogger()                     # root logger
        for handler in log.handlers[:]:               # remove all old handlers
            log.removeHandler(handler)
        log.addHandler(file_handler)                  # set the new handler
        logFormatter = "%(asctime)s %(process)d %(filename)-17s %(lineno)-3d %(levelname)s: %(message)s"
        file_handler.Formatter(logFormatter)
    '''

    return logger
