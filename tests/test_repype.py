import io
import pathlib
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
            status: Optional[repype.status.Status] = None,
        ) -> DataDictionary:
        filepath = pipeline.resolve('segmentation', input)
        if not filepath.parent.is_dir():
            filepath.parent.mkdir(parents = True)
        skimage.io.imsave(filepath, segmentation)
        return dict()


class repype_segmentation(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root_path = pathlib.Path(self.tempdir.name)
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
        testsuite.create_task_file(
            self.root_path / 'task' / 'sigma=2',
            'config:' '\n'
            '  segmentation:' '\n'
            '    sigma: 2' '\n'
        )

    def tearDown(self):
        self.tempdir.cleanup()

    def test(self):
        with testsuite.CaptureStdout() as stdout:
            ret = repype.cli.run_cli_ex(self.root_path, run = True)
            self.assertTrue(ret)
            self.assertEqual(
                str(stdout),
                f'\n'
                f'2 task(s) selected for running' '\n'
                f'  ' '\n'
                f'  (1/2) Entering task: {self.root_path.resolve()}/task' '\n'
                f'  Starting from scratch' '\n'
                f'    ' '\n'
                f'    (1/1) Processing input: B2--W00026--P00001--Z00000--T00000--dapi.tif' '\n'
                f'    Starting stage: download' '\r'
                f'    Starting stage: unzip   ' '\r'
                f'    Starting stage: segmentation' '\r'
                f'    Starting stage: output      ' '\r'
                f'                                ' '\n'
                f'  Results have been stored ✅' '\n'
                f'  ' '\n'
                f'  (2/2) Entering task: {self.root_path.resolve()}/task/sigma=2' '\n'
                f'  Picking up from: {self.root_path.resolve()}/task (segmentation)' '\n'
                f'    ' '\n'
                f'    (1/1) Processing input: B2--W00026--P00001--Z00000--T00000--dapi.tif' '\n'
                f'    Starting stage: segmentation' '\r'
                f'    Starting stage: output      ' '\r'
                f'                                ' '\n'
                f'  Results have been stored ✅' '\n'
            )

        # Load and verify the segmentation result for `sigma=1`
        segmentation = skimage.io.imread(self.root_path / 'task' / 'seg' / 'B2--W00026--P00001--Z00000--T00000--dapi.tif.png')
        n_onjects = ndi.label(segmentation)[1]
        self.assertEqual(n_onjects, 463)

        # Load and verify the segmentation result for `sigma=2`
        segmentation = skimage.io.imread(self.root_path / 'task' / 'sigma=2' / 'seg' / 'B2--W00026--P00001--Z00000--T00000--dapi.tif.png')
        n_onjects = ndi.label(segmentation)[1]
        self.assertEqual(n_onjects, 435)