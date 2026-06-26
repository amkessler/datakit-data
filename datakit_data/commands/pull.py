import argparse
import os
from cliff.command import Command
from datakit import CommandHelpers

from ..extra_flags import ExtraFlags
from ..project_mixin import ProjectMixin
from ..s3 import S3


def _parsed_list(parsed_args, name):
    value = getattr(parsed_args, name, None)
    return value if isinstance(value, list) else []


class Pull(ProjectMixin, CommandHelpers, Command):

    "Pull data from S3"

    def get_parser(self, prog_name):
        parser = super(Pull, self).get_parser(prog_name)
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help="One or more boolean flags without leading dashes: delete, dryrun, force"
        )
        parser.add_argument(
            '--force',
            action='store_true',
            default=False,
            help="Pull every S3 object, ignoring sync status checks"
        )
        parser.add_argument(
            '--archive',
            action='store_true',
            default=False,
            help="Restore one archived snapshot selected by --path"
        )
        parser.add_argument(
            '--path',
            action='append',
            default=[],
            help="Archive path to restore. Required with --archive."
        )
        parser.add_argument(
            '--expand-archives',
            action='store_true',
            default=False,
            help="After pulling, extract downloaded local archive files with matching manifests"
        )
        return parser

    def take_action(self, parsed_args):
        user_profile = self.project_configs['aws_user_profile']
        bucket = self.project_configs['s3_bucket']
        if not os.path.exists("config/datakit-data.json"):
            self.log.info("No config file found - have you run `datakit data init`?")
            return
        if bucket == "":
            self.log.info("No bucket specified in config - no data pulled")
            return
        s3 = S3(user_profile, bucket)
        clean_flags = ExtraFlags.convert(parsed_args.args)
        if getattr(parsed_args, 'force', False) is True and '--force' not in clean_flags:
            clean_flags.append('--force')
        unsupported = ExtraFlags.unsupported(parsed_args.args)
        if unsupported:
            self.log.info(f"Ignoring unsupported flag(s): {', '.join(unsupported)}")
        sync_status_dir = self.project_configs.get('sync_status_location')
        pull_kwargs = {
            'extra_flags': clean_flags,
            'sync_status_dir': sync_status_dir,
        }
        paths = _parsed_list(parsed_args, 'path')
        if paths:
            pull_kwargs['paths'] = paths
        if getattr(parsed_args, 'archive', False) is True:
            pull_kwargs['archive'] = True
        if getattr(parsed_args, 'expand_archives', False) is True:
            pull_kwargs['expand_archives'] = True
        failures = s3.pull(
            'data/',
            self.project_configs['s3_path'],
            **pull_kwargs
        )
        if failures:
            self.log.info(f"{failures} file(s) failed to transfer")
            return 1
