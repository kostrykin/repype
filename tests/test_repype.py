import contextlib
import io
import os
import pathlib
import re
import tempfile
import unittest
import urllib.request
import zipfile

import repype.cli
import repype.pipeline
import repype.stage
import repype.status
from repype.typing import (
    DataDictionary,
    Optional,
)
from . import testsuite


class Download(repype.stage.Stage):
     
    outputs = ['download']

    def process(
            self,
            pipeline: repype.pipeline.Pipeline,
            config: repype.config.Config,
            log_root_dir: Optional[pathlib.Path] = None,
            status: Optional[repype.status.Status] = None,
        ) -> DataDictionary:
        url = config['url']
        with urllib.request.urlopen('http://www.example.com/') as file:
            data = file.read()
        return dict(
            download = data
        )
     

class Unzip(repype.stage.Stage):
     
    inputs = ['input']
    consumes = ['download']
    outputs = ['image']

    def process(
            self,
            input,
            download,
            pipeline: repype.pipeline.Pipeline,
            config: repype.config.Config,
            log_root_dir: Optional[pathlib.Path] = None,
            status: Optional[repype.status.Status] = None,
        ) -> DataDictionary:
        contents = zipfile.ZipFile(download)
        with contents.open(input) as file:
            data = file.read()
        return dict(
            image = data
        )


class repype_segmentation(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root_path = pathlib.Path(self.tempdir.name)
        self.testsuite_pid = os.getpid()
        self.stdout_buf = io.StringIO()
        self.ctx = contextlib.redirect_stdout(self.stdout_buf)
        self.ctx.__enter__()

        testsuite.create_task_file(
            self.root_path / 'task',
            'runnable: true' '\n'
            'pipeline:' '\n'
            '  - tests.test_repype.Download' '\n'
            '  - tests.test_repype.Unzip' '\n'
            'config:' '\n'
            '  download:' '\n'
            '    url: https://zenodo.org/record/3362976/files/B2.zip' '\n'
            'scopes:' '\n'
            '  segmentation: seg/%s.png' '\n'
            'inputs:' '\n'
            '  - B2--W00026--P00001--Z00000--T00000--dapi.tif'
        )

    def tearDown(self):
        if os.getpid() == self.testsuite_pid:
            self.tempdir.cleanup()
            self.ctx.__exit__(None, None, None)

    @property
    def stdout(self):
        return re.sub(r'\033\[K', '', self.stdout_buf.getvalue())

    def test(self):
        ret = repype.cli.run_cli_ex(self.root_path, run = True)
        import sys; print(self.stdout, file = sys.stderr)
        self.assertTrue(ret)