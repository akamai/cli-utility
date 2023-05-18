from __future__ import annotations

import json
import logging
import os
import shutil
import sys
import time
from logging.config import dictConfig
from pathlib import Path
from time import gmtime
from time import strftime

import coloredlogs


def setup_logger():
    # Create folders and copy config json when running via Akamai CLI
    Path('logs').mkdir(parents=True, exist_ok=True)
    Path('config').mkdir(parents=True, exist_ok=True)

    docker_path = os.path.expanduser(Path('/cli'))
    local_home_path = os.path.expanduser(Path('~/.akamai-cli'))

    if Path(docker_path).exists():
        origin_config = f'{docker_path}/.akamai-cli/src/cli-utility/config/logging.json'
    elif Path(local_home_path).exists():
        origin_config = f'{local_home_path}/src/cli-utility/config/logging.json'
        origin_config = os.path.expanduser(origin_config)
    else:
        raise FileNotFoundError('Could not find logging.json')

    try:
        shutil.copy2(origin_config, 'bin/config/logging.json')
    except FileNotFoundError as e:
        origin_config = 'bin/config/logging.json'

    with open(origin_config) as f:
        log_cfg = json.load(f)
    dictConfig(log_cfg)
    logging.Formatter.converter = time.gmtime

    logger = logging.getLogger(__name__)
    coloredlogs.install(logger=logger, fmt='%(levelname)-7s: %(message)s')

    return logger


def get_cli_root_directory():
    docker_path = os.path.expanduser(Path('/cli'))
    local_home_path = os.path.expanduser(Path('~/.akamai-cli'))
    if Path(docker_path).exists():
        return Path(f'{docker_path}/.akamai-cli/src/cli-utility')
    elif Path(local_home_path).exists():
        return Path(f'{local_home_path}/src/cli-utility')
    else:
        return os.getcwd()


def _elapse_time(start_time: time, msg: str) -> None:
    end_time = time.perf_counter()
    elapse_time = str(strftime('%H:%M:%S', gmtime(end_time - start_time)))
    setup_logger().info(f'{msg}: {elapse_time}')


def _log_error(msg: str) -> None:
    if msg is not None:
        sys.exit(setup_logger().error(msg))


# https://medium.com/@rahulkumar_33287/logger-error-versus-logger-exception-4113b39beb4b
def _log_exception(msg: str) -> None:
    sys.exit(setup_logger().exception(msg))
