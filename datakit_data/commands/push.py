import argparse
import os
from cliff.command import Command
from datakit import CommandHelpers

from datakit.utils import write_json

from ..extra_flags import ExtraFlags
from ..project_mixin import ProjectMixin
from ..s3 import S3


def _parsed_list(parsed_args, name):
    value = getattr(parsed_args, name, None)
    return value if isinstance(value, list) else []


class Push(ProjectMixin, CommandHelpers, Command):

    "Push local data to S3"

    def get_parser(self, prog_name):
        parser = super(Push, self).get_parser(prog_name)
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help="One or more boolean flags without leading dashes: delete, dryrun, force"
        )
        parser.add_argument(
            '--force',
            action='store_true',
            default=False,
            help="Push every file, ignoring sync status checks"
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            default=False,
            help="Log per-file skipped output during push"
        )
        parser.add_argument(
            '--sync-status-in-data',
            action='store_true',
            default=False,
            help="Create sync status files in data/ instead of the configured location"
        )
        parser.add_argument(
            '--path',
            action='append',
            default=[],
            help="Only push files under this data/ path. May be repeated."
        )
        parser.add_argument(
            '--include',
            action='append',
            default=[],
            help="Only push relative data paths matching this glob. May be repeated."
        )
        parser.add_argument(
            '--exclude',
            action='append',
            default=[],
            help="Skip relative data paths matching this glob. May be repeated."
        )
        parser.add_argument(
            '--jobs',
            type=int,
            default=1,
            help="Number of parallel upload workers to use. Defaults to 1."
        )
        return parser

    def take_action(self, parsed_args):
        user_profile = self.project_configs['aws_user_profile']
        bucket = self.project_configs['s3_bucket']
        if not os.path.exists("config/datakit-data.json"):
            self.log.info("No config file found - have you run `datakit data init`?")
            return
        if bucket == "":
            self.log.info("No bucket specified in config - no data pushed")
            return
        s3 = S3(user_profile, bucket)
        clean_flags = ExtraFlags.convert(parsed_args.args)
        if getattr(parsed_args, 'force', False) is True and '--force' not in clean_flags:
            clean_flags.append('--force')
        if getattr(parsed_args, 'verbose', False) is True and '--verbose' not in clean_flags:
            clean_flags.append('--verbose')
        unsupported = ExtraFlags.unsupported(parsed_args.args)
        if unsupported:
            self.log.info(f"Ignoring unsupported flag(s): {', '.join(unsupported)}")
        dryrun = '--dryrun' in clean_flags or '--dry-run' in clean_flags
        if parsed_args.sync_status_in_data:
            sync_status_dir = 'data/'
            if not dryrun:
                configs = self.project_configs.copy()
                configs['sync_status_location'] = 'data/'
                write_json(self.project_config_path, configs)
        else:
            sync_status_dir = self.project_configs.get('sync_status_location')
        push_kwargs = {
            'extra_flags': clean_flags,
            'sync_status_dir': sync_status_dir,
        }
        paths = _parsed_list(parsed_args, 'path')
        include_patterns = _parsed_list(parsed_args, 'include')
        exclude_patterns = _parsed_list(parsed_args, 'exclude')
        if paths:
            push_kwargs['paths'] = paths
        if include_patterns:
            push_kwargs['include_patterns'] = include_patterns
        if exclude_patterns:
            push_kwargs['exclude_patterns'] = exclude_patterns
        jobs = getattr(parsed_args, 'jobs', 1)
        if isinstance(jobs, int) and jobs != 1:
            push_kwargs['jobs'] = jobs
        failures = s3.push(
            'data/',
            self.project_configs['s3_path'],
            **push_kwargs
        )
        if failures:
            self.log.info(f"{failures} file(s) failed to transfer")
            return 1
