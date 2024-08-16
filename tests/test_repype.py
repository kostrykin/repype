import contextlib
import io
import os
import pathlib
import re
import tempfile
import unittest
import urllib.request
import zipfile

import skimage.segmentation

import repype.cli
import repype.pipeline
import repype.stage
import repype.status
from repype.typing import (
    DataDictionary,
    Optional,
)
import scipy.ndimage as ndi
import skimage
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
        with urllib.request.urlopen(url) as file:
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
        contents = zipfile.ZipFile(io.BytesIO(download))
        with contents.open(input) as file:
            data = file.read()
        return dict(
            image = skimage.io.imread(io.BytesIO(data))
        )
     

class Segmentation(repype.stage.Stage):
     
    inputs = ['image']
    outputs = ['segmentation']

    def process(
            self,
            image,
            pipeline: repype.pipeline.Pipeline,
            config: repype.config.Config,
            log_root_dir: Optional[pathlib.Path] = None,
            status: Optional[repype.status.Status] = None,
        ) -> DataDictionary:
        image = skimage.filters.gaussian(image, sigma = config.get('sigma', 1.))
        threshold = skimage.filters.threshold_otsu(image)
        return dict(
            segmentation = skimage.util.img_as_ubyte(image > threshold)
        )
    

class Output(repype.stage.Stage):

    inputs = ['input', 'segmentation']

    def process(
            self,
            input,
            segmentation,
            pipeline: repype.pipeline.Pipeline,
            config: repype.config.Config,
            log_root_dir: Optional[pathlib.Path] = None,
            status: Optional[repype.status.Status] = None,
        ) -> DataDictionary:
        filepath = pipeline.resolve('segmentation', input)
        skimage.io.imsave(filepath, segmentation)
        return dict()


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
            '  - tests.test_repype.Segmentation' '\n'
            '  - tests.test_repype.Output' '\n'
            'config:' '\n'
            '  download:' '\n'
            '    url: https://zenodo.org/record/3362976/files/B2.zip' '\n'
            'scopes:' '\n'
            '  segmentation: seg/%s.png' '\n'
            'inputs:' '\n'
            '  - B2--W00026--P00001--Z00000--T00000--dapi.tif'
        )
        (self.root_path / 'task' / 'seg').mkdir()

    def tearDown(self):
        if os.getpid() == self.testsuite_pid:
            self.tempdir.cleanup()
            self.ctx.__exit__(None, None, None)

    @property
    def stdout(self):
        return re.sub(r'\033\[K', '', self.stdout_buf.getvalue())

    def test(self):
        ret = repype.cli.run_cli_ex(self.root_path, run = True)
        if not ret:
            import sys; print(self.stdout, file = sys.stderr)
        self.assertTrue(ret)
        self.assertEqual(
            self.stdout,
            f'\n'
            f'1 task(s) selected for running' '\n'
            f'  ' '\n'
            f'  (1/1) Entering task: {self.root_path.resolve()}/task' '\n'
            f'  Starting from scratch' '\n'
            f'    ' '\n'
            f'    (1/1) Processing input: B2--W00026--P00001--Z00000--T00000--dapi.tif' '\n'
            f'    Starting stage: download' '\r'
            f'    Starting stage: unzip   ' '\r'
            f'    Starting stage: segmentation' '\r'
            f'    Starting stage: output      ' '\r'
            f'                                ' '\n'
            f'  Results have been stored ✅' '\n'
        )

        # Load and verify the segmentation result
        segmentation = skimage.io.imread(self.root_path / 'task' / 'seg' / 'B2--W00026--P00001--Z00000--T00000--dapi.tif.png')
        n_onjects = ndi.label(segmentation)[1]
        #import shutil; shutil.copy(self.root_path / 'task' / 'seg' / 'B2--W00026--P00001--Z00000--T00000--dapi.tif.png', './output.png')
        self.assertEqual(n_onjects, 463)