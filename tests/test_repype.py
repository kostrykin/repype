import hashlib
import io
import pathlib
import tempfile
import time
import unittest
import urllib.request
import zipfile

import scipy.ndimage as ndi
import skimage
import skimage.segmentation

import repype.benchmark
import repype.cli
import repype.pipeline
import repype.stage
import repype.status
from repype.typing import (
    Optional,
    PipelineData,
)

from . import testsuite


class Download(repype.stage.Stage):
     
    outputs = ['download']

    def process(
            self,
            pipeline: repype.pipeline.Pipeline,
            config: repype.config.Config,
            status: Optional[repype.status.Status] = None,
        ) -> PipelineData:
        url = config['url']

        # Employ a cache to speed-up tests and reduce bandwith usage
        cache_hit = False
        cache_filepath = pathlib.Path('.cache')
        cache_entry = cache_filepath / hashlib.sha1(url.encode('utf8')).hexdigest()
        if cache_entry.is_file():
            with cache_entry.open('rb') as file:
                data = file.read()
                cache_hit = True
                time.sleep(1)

        # Only perform the download if the cache is not hit
        if not cache_hit:
            with urllib.request.urlopen(url) as file:
                data = file.read()

            # Store the downloaded data in the cache
            cache_filepath.mkdir(exist_ok = True)
            with cache_entry.open('wb') as file:
                file.write(data)

        return dict(
            download = data
        )
     

class Unzip(repype.stage.Stage):
     
    inputs   = ['input_id']
    consumes = ['download']
    outputs  = ['image']

    def process(
            self,
            input_id,
            download,
            pipeline: repype.pipeline.Pipeline,
            config: repype.config.Config,
            status: Optional[repype.status.Status] = None,
        ) -> PipelineData:
        contents = zipfile.ZipFile(io.BytesIO(download))
        with contents.open(input_id) as file:
            data = file.read()
        return dict(
            image = skimage.io.imread(io.BytesIO(data))
        )
     

class Segmentation(repype.stage.Stage):
     
    inputs  = ['image']
    outputs = ['segmentation']

    def process(
            self,
            image,
            pipeline: repype.pipeline.Pipeline,
            config: repype.config.Config,
            status: Optional[repype.status.Status] = None,
        ) -> PipelineData:
        image = skimage.filters.gaussian(image, sigma = config.get('sigma', 1.))
        threshold = skimage.filters.threshold_otsu(image)
        return dict(
            segmentation = skimage.util.img_as_ubyte(image > threshold)
        )
    

class Output(repype.stage.Stage):

    inputs = ['input_id', 'segmentation']

    def process(
            self,
            input_id,
            segmentation,
            pipeline: repype.pipeline.Pipeline,
            config: repype.config.Config,
            status: Optional[repype.status.Status] = None,
        ) -> PipelineData:
        filepath = pipeline.resolve('segmentation', input_id)
        filepath.parent.mkdir(parents = True, exist_ok = True)
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
            'input_ids:' '\n'
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
                f'    (1/1) Processing: B2--W00026--P00001--Z00000--T00000--dapi.tif' '\n'
                f'    Running stage: download' '\r'
                f'    Running stage: unzip   ' '\r'
                f'    Running stage: segmentation' '\r'
                f'    Running stage: output      ' '\r'
                f'                               ' '\n'
                f'  Results have been stored ✅' '\n'
                f'  ' '\n'
                f'  (2/2) Entering task: {self.root_path.resolve()}/task/sigma=2' '\n'
                f'  Picking up from: {self.root_path.resolve()}/task (segmentation)' '\n'
                f'    ' '\n'
                f'    (1/1) Processing: B2--W00026--P00001--Z00000--T00000--dapi.tif' '\n'
                f'    Running stage: segmentation' '\r'
                f'    Running stage: output      ' '\r'
                f'                               ' '\n'
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

        # Load and verify the times for `sigma=1`
        times1 = repype.benchmark.Benchmark(self.root_path / 'task' / 'times.csv')
        self.assertEqual(times1.df.shape, (4, 1))
        self.assertGreater(float(times1['download', 'B2--W00026--P00001--Z00000--T00000--dapi.tif']), 0)
        self.assertGreater(float(times1['unzip', 'B2--W00026--P00001--Z00000--T00000--dapi.tif']), 0)
        self.assertGreater(float(times1['segmentation', 'B2--W00026--P00001--Z00000--T00000--dapi.tif']), 0)
        self.assertGreater(float(times1['output', 'B2--W00026--P00001--Z00000--T00000--dapi.tif']), 0)

        # Load and verify the times for `sigma=2`
        times2 = repype.benchmark.Benchmark(self.root_path / 'task' / 'sigma=2' / 'times.csv')
        self.assertEqual(times1.df.shape, (4, 1))
        self.assertEqual(
            times1['download', 'B2--W00026--P00001--Z00000--T00000--dapi.tif'],
            times2['download', 'B2--W00026--P00001--Z00000--T00000--dapi.tif'],
        )
        self.assertEqual(
            times1['unzip', 'B2--W00026--P00001--Z00000--T00000--dapi.tif'],
            times2['unzip', 'B2--W00026--P00001--Z00000--T00000--dapi.tif'],
        )
        self.assertGreater(float(times2['segmentation', 'B2--W00026--P00001--Z00000--T00000--dapi.tif']), 0)
        self.assertGreater(float(times2['output', 'B2--W00026--P00001--Z00000--T00000--dapi.tif']), 0)
        self.assertNotEqual(
            times1['segmentation', 'B2--W00026--P00001--Z00000--T00000--dapi.tif'],
            times2['segmentation', 'B2--W00026--P00001--Z00000--T00000--dapi.tif'],
        )
        self.assertNotEqual(
            times1['output', 'B2--W00026--P00001--Z00000--T00000--dapi.tif'],
            times2['output', 'B2--W00026--P00001--Z00000--T00000--dapi.tif'],
        )