# -*- coding: utf-8 -*-

import argparse
import glob
import os
import shutil
import subprocess

import psutil

from atm.api import create_app
from atm.config import (
    add_arguments_aws_s3, add_arguments_datarun, add_arguments_logging, add_arguments_sql)
from atm.models import ATM


def _work(args):
    atm = ATM(**vars(args))
    atm.work(
        datarun_ids=args.dataruns,
        choose_randomly=args.choose_randomly,
        save_files=args.save_files,
        cloud_mode=args.cloud_mode,
        total_time=args.time,
        wait=True
    )


def _check_server_status(args, kill=False):

    try:
        with open('atm_gunicorn.pid', 'r') as f:
            pid = int(f.read())

        process = psutil.Process(pid)

        command = process.as_dict().get('cmdline')

        if kill:
            process.kill()
            print('ATM server stopped.')

        if ('atm-gunicorn' in command) and not kill:
            print('ATM server is running at http://{}'.format(command[-1]))

            return True

    except (FileNotFoundError, psutil.NoSuchProcess) as e:

        if kill:
            print('ATM server not running.')

        return False


def _server_start(args):
    """Start server."""

    if not _check_server_status(args):
        host_port = '{}:{}'.format(args.host or '127.0.0.1', args.port or '8000')
        atm = ATM(**vars(args))

        with open('db_url.cfg', 'w') as f:
            f.write(atm.db.engine.url.database)

        gunicorn_process = [
            'gunicorn',
            '--name',
            'atm-gunicorn',
            '--pid',
            'atm_gunicorn.pid',
            '--log-file',
            'atm_gunicorn.log',
            'atm.api:create_app',
            '--bind',
            host_port
        ]

        process = psutil.Popen(gunicorn_process)

        print('ATM server started at http://{}'.format(host_port))


def _server_stop(args):
    """Stop server."""
    _check_server_status(args, kill=True)


def _server_status(args):
    """Check server status."""
    if not _check_server_status(args):
        print('ATM server not running')



def _enter_data(args):
    atm = ATM(**vars(args))
    atm.enter_data()


def _make_config(args):
    config_templates = os.path.join('config', 'templates')
    config_dir = os.path.join(os.path.dirname(__file__), config_templates)
    target_dir = os.path.join(os.getcwd(), config_templates)
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    for template in glob.glob(os.path.join(config_dir, '*.yaml')):
        target_file = os.path.join(target_dir, os.path.basename(template))
        print('Generating file {}'.format(target_file))
        shutil.copy(template, target_file)


# load other functions from config.py
def _add_common_arguments(parser):
    add_arguments_sql(parser)
    add_arguments_aws_s3(parser)
    add_arguments_logging(parser)


def _get_parser():
    parent = argparse.ArgumentParser(add_help=False)

    parser = argparse.ArgumentParser(description='ATM Command Line Interface')

    subparsers = parser.add_subparsers(title='action', help='Action to perform')
    parser.set_defaults(action=None)

    # Enter Data Parser
    enter_data = subparsers.add_parser('enter_data', parents=[parent])
    enter_data.set_defaults(action=_enter_data)
    _add_common_arguments(enter_data)
    add_arguments_datarun(enter_data)
    enter_data.add_argument('--run-per-partition', default=False, action='store_true',
                            help='if set, generate a new datarun for each hyperpartition')

    # Worker
    worker = subparsers.add_parser('worker', parents=[parent])
    worker.set_defaults(action=_work)
    _add_common_arguments(worker)
    worker.add_argument('--cloud-mode', action='store_true', default=False,
                        help='Whether to run this worker in cloud mode')

    worker.add_argument('--dataruns', help='Only train on dataruns with these ids', nargs='+')
    worker.add_argument('--time', help='Number of seconds to run worker', type=int)
    worker.add_argument('--choose-randomly', action='store_true',
                        help='Choose dataruns to work on randomly (default = sequential order)')

    worker.add_argument('--no-save', dest='save_files', default=True,
                        action='store_const', const=False,
                        help="don't save models and metrics at all")

    # Server
    server = subparsers.add_parser('server', parents=[parent])
    add_arguments_sql(server)  # add sql
    server.add_argument('--debug-mode',
                        help='Start the server in debug mode.', action='store_true')

    server_subparsers = server.add_subparsers(
    )

    server_subparsers.required = True

    server_start = server_subparsers.add_parser('start')
    server_start.set_defaults(action=_server_start)
    server_start.add_argument('--host', help='IP to listen at')
    server_start.add_argument('--port', help='Port to listen at', type=int)

    server_status = server_subparsers.add_parser('status')
    server_status.set_defaults(action=_server_status)

    server_stop = server_subparsers.add_parser('stop')
    server_stop.set_defaults(action=_server_stop)


    # Make Config
    make_config = subparsers.add_parser('make_config', parents=[parent])
    make_config.set_defaults(action=_make_config)

    return parser


def main():
    parser = _get_parser()
    args = parser.parse_args()

    if not args.action:
        parser.print_help()
        parser.exit()

    args.action(args)
