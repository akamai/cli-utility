# https://medium.com/@rahulkumar_33287/logger-error-versus-logger-exception-4113b39beb4b
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
from time import perf_counter
from time import strftime

import coloredlogs
from utils.cli_formatter import CLIFormatter


custom_level_styles = {
    'debug': {'color': 'blue'},
    'info': {'color': 'white'},
    'warning': {'color': 'yellow'},
    'error': {'color': 'red'},
    'critical': {'color': 'magenta'},
}


def setup_logger(args):
    # Create folders and copy config json when running via Akamai CLI
    Path('logs').mkdir(parents=True, exist_ok=True)
    Path('config').mkdir(parents=True, exist_ok=True)

    load_local_config_file(config_file='ghost_r.txt')
    load_local_config_file(config_file='ghost_f.txt')
    origin_config = load_local_config_file(config_file='logging.json')

    with open(origin_config) as f:
        log_cfg = json.load(f)

    log_cfg['handlers']['file_handler']['filename'] = 'logs/utility.log'
    log_cfg['formatters']['long']['()'] = 'utils.cli_formatter.CLIFormatter'
    dictConfig(log_cfg)
    logging.Formatter.converter = time.gmtime

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Set up colored console logs using coloredlogs library
    coloredlogs.install(
        logger=logger,
        level=args.log_level.upper(),
        level_styles=custom_level_styles,
        # fmt='%(asctime)s %(levelname)-8s: %(message)s',
        fmt='%(levelname)-8s: %(message)s',
        field_styles={
            'asctime': {'color': 'black'},
            'levelname': {'color': 'black', 'bold': True},
        },
    )

    # Use a RotatingFileHandler instead of FileHandler
    log_handler = logging.handlers.RotatingFileHandler('logs/utility.log', maxBytes=1024*1024, backupCount=5)
    log_handler.setLevel(logging.DEBUG)
    logger.addHandler(log_handler)

    return logger


def load_local_config_file(config_file: str) -> str:
    docker_path = os.path.expanduser(Path('/cli'))
    local_home_path = os.path.expanduser(Path('~/.akamai-cli'))

    if Path(docker_path).exists():
        origin_config = f'{docker_path}/.akamai-cli/src/cli-utility/bin/config/{config_file}'
    elif Path(local_home_path).exists():
        origin_config = f'{local_home_path}/src/cli-utility/bin/config/{config_file}'
        origin_config = os.path.expanduser(origin_config)
    else:
        raise FileNotFoundError(f'Could not find {config_file}')

    try:
        shutil.copy2(origin_config, f'config/{config_file}')
    except FileNotFoundError as e:
        origin_config = f'config/{config_file}'

    return origin_config


def get_cli_root_directory():
    docker_path = os.path.expanduser(Path('/cli'))
    local_home_path = os.path.expanduser(Path('~/.akamai-cli'))
    if Path(docker_path).exists():
        return Path(f'{docker_path}/.akamai-cli/src/cli-utility')
    elif Path(local_home_path).exists():
        return Path(f'{local_home_path}/src/cli-utility')
    else:
        return os.getcwd()


def countdown(time_sec: int, msg: str, logger=None):
    time_min = int(time_sec / 60)
    msg = f'{msg} {time_min} minutes count down'
    logger.critical(msg)
    while time_sec:
        mins, secs = divmod(time_sec, 60)
        timeformat = f'{mins:02d}:{secs:02d}'
        print(f'\t\t\t\t{timeformat}', end='\r')
        time.sleep(1)
        time_sec -= 1


def log_cli_timing(start_time) -> None:
    print()
    end_time = perf_counter()
    elapse_time = str(strftime('%H:%M:%S', gmtime(end_time - start_time)))
    msg = f'End Akamai CLI utility, TOTAL DURATION: {elapse_time}'
    return msg


if __name__ == '__main__':
    pass
