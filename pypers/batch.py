import sys
import os
import pathlib
import json
import gzip
import dill
import csv
import tarfile
import shutil
import time
import re

from typing import Union, Type

from .pipeline import ProcessingControl
from .output import get_output, Text
from .config import Config


PathType = Union[pathlib.Path, str]


def _format_runtime(seconds):
    seconds = int(round(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f'{hours:02}:{minutes:02}:{seconds:02}'


def resolve_pathpattern(pathpattern, fileid):
    if pathpattern is None: return None
    else: return str(pathpattern) % fileid


def copy_dict(d):
    """Returns a deep copy of a dictionary.
    """
    assert isinstance(d, dict), 'not a "dict" object'
    return {item[0]: copy_dict(item[1]) if isinstance(item[1], dict) else item[1] for item in d.items()}


def _is_subpath(path, subpath):
    if isinstance(   path, str):    path = pathlib.Path(   path)
    if isinstance(subpath, str): subpath = pathlib.Path(subpath)
    try:
        subpath.relative_to(path)
        return True
    except ValueError:
        return False
    

def _mkdir(dir_path):
    pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)


def _process_file(dry, *args, out=None, **kwargs):
    if dry:
        out = get_output(out)
        kwargs_serializable = copy_dict(kwargs)
        if 'cfg' in kwargs_serializable:
            kwargs_serializable['cfg'] = kwargs_serializable['cfg'].entries
        out.write(f'{_process_file.__name__}: {json.dumps(kwargs_serializable)}')
        return None, {}
    else:
        return __process_file(*args, out=out, **kwargs)


def __process_file(loader, pipeline, data, input, log_filepath, cfg_filepath, cfg, first_stage, last_stage, out=None, **kwargs):
    if log_filepath is not None: _mkdir(pathlib.Path(log_filepath).parents[0])
    if cfg_filepath is not None: _mkdir(pathlib.Path(cfg_filepath).parents[0])

    out = get_output(out)

    timings = {}
    if first_stage != '':
        out.intermediate('Creating configuration…')
        t0 = time.time()
        cfg = pipeline.configurator.configure(cfg, input)
        timings['configuration'] = time.time() - t0
        if cfg_filepath is not None:
            loader.dump(cfg_filepath, cfg.entries)

    result_data, _, _timings = pipeline.process(input, data=data, cfg=cfg, first_stage=first_stage, last_stage=last_stage, log_root_dir=log_filepath, out=out, **kwargs)
    timings.update(_timings)

    return result_data, timings


def _resolve_timings_key(key, candidates):
    for c in candidates:
        if str(c) == key: return c
    raise ValueError(f'cannot resolve key "{key}"')


def _find_task_rel_path(task):
    if task.parent_task is not None:
        return _find_task_rel_path(task.parent_task)
    else:
        return task.path.parents[0]


def _compress_logs(log_dir):
    if log_dir is None: return
    log_dir_path = pathlib.Path(log_dir)
    if not log_dir_path.exists(): return
    assert log_dir_path.is_dir()
    compressed_logs_filepath = f'{log_dir}.tgz'
    with tarfile.open(compressed_logs_filepath, 'w:gz') as tar:
        tar.add(log_dir, arcname=os.path.sep)
    shutil.rmtree(str(log_dir))


def _decode_file_ids(spec):
    if isinstance(spec, str):
        m = re.match(r'^([0-9]+)-([0-9]+)$', spec.replace(' ', ''))
        if m is None or len(m.groups()) != 2:
            raise ValueError(f'cannot decode file_ids: "{spec}"')
        else:
            return list(range(int(m.group(1)), int(m.group(2)) + 1))
    else:
        return sorted(frozenset(spec))
    

def _estimate_processed_stages(pipeline, first_stage, last_stage):
    stages = []
    ctrl = ProcessingControl(first_stage, last_stage)
    for stage in pipeline.stages:
        if ctrl.step(stage.cfgns):
            stages.append(stage.cfgns)
    return frozenset(stages)


class TaskLoader:

    suffix_hint = ''

    def filepath(self, filepath):
        convert_to_path_obj = isinstance(filepath, pathlib.Path)
        filepath = str(filepath)
        filepath = filepath if any([filepath.lower().endswith(f'.{suffix.lower()}') for suffix in self.suffixes]) else f'{filepath}.{self.default_suffix}'
        return pathlib.Path(filepath) if convert_to_path_obj else filepath
    
    @property
    def default_suffix(self):
        return self.suffixes[0]
    
    @property
    def suffixes(self):
        return self.suffix_hint.split(',')

    def load(self, filepath):
        raise NotImplementedError()
    
    def dump(self, filepath, data):
        raise NotImplementedError()
    

class JSONLoader(TaskLoader):

    suffix_hint = 'json'

    def load(self, filepath):
        with open(self.filepath(filepath), 'r') as file:
            return json.load(file)
    
    def dump(self, filepath, data):
        with open(self.filepath(filepath), 'w') as file:
            json.dump(data, file)


class YAMLLoader(TaskLoader):

    suffix_hint = 'yml,yaml'

    def load(self, filepath):
        import yaml
        with open(self.filepath(filepath), 'r') as file:
            return yaml.load(file, Loader = yaml.FullLoader)

    def dump(self, filepath, data):
        import yaml
        with open(self.filepath(filepath), 'w') as file:
            file.write(yaml.dump(data))


class Task:
    """Represents a batch processing task (see :ref:`batch_task_spec`).

    :param path: The path of the directory where the task specification resides.
    :param data: Dictionary corresponding to the task specification (JSON data).
    :param parent_task: The parent task or ``None`` if there is no parent task.
    """

    outputs = list()

    def __init__(self, loader, path, data, parent_task=None):
        self.runnable    = 'runnable' in data and bool(data['runnable']) == True
        self.parent_task = parent_task
        self.loader      = loader
        self.path = path
        self.data = Config(data) if parent_task is None else Config(parent_task.data).derive(data)
        self.rel_path = _find_task_rel_path(self)
        self.file_ids = _decode_file_ids(self.data.entries['file_ids']) if 'file_ids' in self.data else None
        self.input_pathpattern = self.data.update('input_pathpattern', lambda pathpattern: str(self.resolve_path(pathpattern)))

        if 'base_config_path' in self.data:
            base_config_path = self.resolve_path(self.data['base_config_path'])
            with base_config_path.open('r') as base_config_fin:
                base_config = json.load(base_config_fin)
            parent_config = parent_task.data.get('config', {})
            self.data['config'] = parent_config.derive(base_config).merge(data.get('config', {}))
            del self.data.entries['base_config_path']

        if self.runnable:

            assert self.file_ids          is not None
            assert self.input_pathpattern is not None

            self.  log_pathpattern = (path / self.data.entries['log_pathpattern']) if 'log_pathpattern' in self.data.entries else None
            self.  cfg_pathpattern = (path / self.data.entries['cfg_pathpattern']) if 'cfg_pathpattern' in self.data.entries else None
            self.      result_path =  path / 'data.dill.gz'
            self.     timings_path =  path / 'timings.csv'
            self.timings_json_path =  path / '.timings.json'
            self.      digest_path =  path / '.digest'
            self.  digest_cfg_path =  path / '.digest.cfg.json'
            self.           config = self.data.get('config', {})
            self.       last_stage = self.data.entries.get('last_stage', None)
            self.          environ = self.data.entries.get('environ', {})

            for output in type(self).outputs:
                output_pathpattern =  f'{output}_pathpattern'
                setattr(self, output_pathpattern, (path / self.data.entries[output_pathpattern]) if output_pathpattern in self.data.entries else None)

    @property
    def available_outputs(self):
        for output in type(self).outputs:
            output_pathpattern_key =  f'{output}_pathpattern'
            output_pathpattern = getattr(self, output_pathpattern_key, None)
            if output_pathpattern is not None:
                yield (output, output_pathpattern)

    def __eq__(self, other):
        return isinstance(other, Task) and self.data == other.data

    def reset(self):
        if not self.runnable: return
        for file_id in self.file_ids:
            if self.log_pathpattern is not None:
                self._remove_from_filesystem(resolve_pathpattern(self.log_pathpattern, file_id) + '.tgz')
            if self.cfg_pathpattern is not None:
                self._remove_from_filesystem(self.loader.filepath(resolve_pathpattern(self.cfg_pathpattern, file_id)))
        self._remove_from_filesystem(self.result_path)
        self._remove_from_filesystem(self.timings_path)
        self._remove_from_filesystem(self.timings_json_path)
        self._remove_from_filesystem(self.digest_path)
        self._remove_from_filesystem(self.digest_cfg_path)
        for _, output_pathpattern in self.available_outputs:
            for file_id in self.file_ids:
                output_path = resolve_pathpattern(output_pathpattern, file_id)
                self._remove_from_filesystem(output_path)

    def _remove_from_filesystem(self, path):
        if path is None: return
        assert _is_subpath(self.path, path), f'{path} is not a sub-path of {self.path}'
        path = pathlib.Path(path)
        if not path.exists(): return
        if path.is_file():
            path.unlink()
            if not any(path.parent.iterdir()):
                self._remove_from_filesystem(path.parent)
        else: path.rmdir()

    def resolve_path(self, path):
        if path is None: return None
        path = pathlib.Path(os.path.expanduser(str(path))
            .replace('{DIRNAME}', self.path.name)
            .replace('{ROOTDIR}', str(self.root_path)))
        if path.is_absolute():
            return path.resolve()
        else:
            return path.resolve().relative_to(os.getcwd())
    
    @property
    def root_path(self):
        """The root path of the task (see :ref:`batch_system`)."""
        if self.parent_task is not None: return self.parent_task.root_path
        else: return self.path

    def _fmt_path(self, path):
        if isinstance(path, str): path = pathlib.Path(path)
        if self.rel_path is None: return str(path)
        else: return str(path.relative_to(self.rel_path))

    def _initialize(self, dry):
        for key, val in self.environ.items():
            os.environ[key] = str(val)
        return self.create_pipeline(dry)
    
    def create_pipeline(self, dry):
        raise NotImplementedError()

    def cleanup(self, dry):
        pass

    def _load_timings(self):
        if self.timings_json_path.exists():
            with self.timings_json_path.open('r') as fin:
                timings = json.load(fin)
            return {_resolve_timings_key(key, self.file_ids): timings[key] for key in timings}
        else:
            return {}
        
    @property
    def config_digest(self):
        """Hash code of the hyperparameters of this task.
        """
        return self.config.md5.hexdigest()
        
    @property
    def is_pending(self):
        """``True`` if the task needs to run, and ``False`` if the task is completed or not runnable.
        """
        return self.runnable and not (self.digest_path.exists() and self.digest_path.read_text() == self.config_digest)
    
    def is_stage_marginal(self, stage):
        return False
    
    def get_marginal_fields(self, pipeline):
        marginal_fields = set()
        for stage in pipeline.stages:
            if self.is_stage_marginal(stage.cfgns):
                marginal_fields |= stage.outputs
        return marginal_fields

    def run(self, task_info=None, dry=False, verbosity=0, force=False, one_shot=False, report=None, pickup=True, return_full_data=True, out=None):
        out = get_output(out)
        if not self.runnable: return
        if not force and not self.is_pending:
            out.write(f'\nSkipping task: {self._fmt_path(self.path)} {"" if task_info is None else f"({task_info})"}')
            return
        if self.last_stage is not None:
            if task_info is not None: task_info = f'{task_info}, '
            else: task_info = ''
            task_info = task_info + f'last stage: {self.last_stage}'
        out.write(Text.style(f'\nEntering task: {self._fmt_path(self.path)} {"" if task_info is None else f"({task_info})"}', Text.BLUE))
        out2 = out.derive(margin=2)
        pipeline = self._initialize(dry)
        assert self.last_stage is None or self.last_stage == '' or pipeline.find(self.last_stage, None) is not None, f'unknown stage "{self.last_stage}"'
        try:
            first_stage, data = self.pickup_previous_task(pipeline, dry, pickup, out=out2)
            out3 = out2.derive(margin=2, muted = (verbosity <= -int(not dry)))
            timings = self._load_timings()
            processed_stages = set()
            for file_idx, file_id in enumerate(self.file_ids):
                input_filepath = str(self.input_pathpattern) % file_id
                progress = file_idx / len(self.file_ids)
                if report is not None: report.update(self, progress)
                out3.write(Text.style(f'\n[{self._fmt_path(self.path)}] ', Text.BLUE + Text.BOLD) + Text.style(f'Processing file: {input_filepath}', Text.BOLD) + f' ({100 * progress:.0f}%)')
                kwargs = dict(       input = input_filepath,
                              log_filepath = resolve_pathpattern(self.log_pathpattern, file_id),
                              cfg_filepath = resolve_pathpattern(self.cfg_pathpattern, file_id),
                                last_stage = self.last_stage,
                                       cfg = self.config.copy())
                for output, output_pathpattern in self.available_outputs:
                    output_path = resolve_pathpattern(output_pathpattern, file_id)
                    _mkdir(pathlib.Path(output_path).parents[0])
                    kwargs[f'{output}_filepath'] = output_path
                if file_id not in data: data[file_id] = None
                data_chunk, _timings = _process_file(dry, self.loader, pipeline, data[file_id], first_stage=first_stage, out=out3, **kwargs)
                if not dry:
                    data[file_id] = data_chunk if return_full_data else self._strip_marginal_fields(pipeline, data_chunk, is_chunk=True)
                processed_stages |= _estimate_processed_stages(pipeline, first_stage, self.last_stage) if dry else set(_timings.keys())
                if not dry: _compress_logs(kwargs['log_filepath'])
                if file_id not in timings: timings[file_id] = {}
                timings[file_id].update(_timings)
            out2.write('')
            if report is not None: report.update(self, 'active')
            
            skip_writing_results_conditions = [
                one_shot,
                all([self.is_stage_marginal(stage) for stage in processed_stages]) and not self.result_path.exists(),
            ]
            if any(skip_writing_results_conditions):
                out2.write('Skipping writing results')
            else:
                if not dry:
                    self.write_results(pipeline, data, timings, out = out2)
                out2.write(Text.style('Results written to: ', Text.BOLD) + self._fmt_path(self.result_path))
            if not dry and not one_shot: self.digest_path.write_text(self.config_digest)
            return locals().get('data', None)
        except:
            out.write(Text.style(f'\nError while processing task: {self._fmt_path(self.path)}', Text.RED))
            raise
        finally:
            self.cleanup(dry)

    def _strip_marginal_fields(self, pipeline, data, is_chunk):
        marginal_fields = self.get_marginal_fields(pipeline)
        if is_chunk:
            return {field: data[field] for field in data.keys() if field not in marginal_fields}
        else:
            return {file_id: self._strip_marginal_fields(pipeline, data[file_id], is_chunk=True) for file_id in data.keys()}

    def write_results(self, pipeline, data, timings, out = None):
        out = get_output(out)
        self._write_timings(timings)
        out.intermediate(f'Writing results… {self._fmt_path(self.result_path)}')
        data_without_marginals = self._strip_marginal_fields(pipeline, data, is_chunk=False)
        with gzip.open(self.result_path, 'wb') as fout:
            dill.dump(data_without_marginals, fout, byref=True)
        with self.digest_cfg_path.open('w') as fout:
            self.config.dump_json(fout)

    def find_runnable_parent_task(self):
        if self.parent_task is None: return None
        elif self.parent_task.runnable: return self.parent_task
        else: return self.parent_task.find_runnable_parent_task()

    def find_parent_task_with_result(self):
        runnable_parent_task = self.find_runnable_parent_task()
        if runnable_parent_task is None: return None
        elif runnable_parent_task.result_path.exists(): return runnable_parent_task
        else: return runnable_parent_task.find_parent_task_with_result()

    def find_pickup_candidates(self, pipeline):
        pickup_candidates = []
        previous_task = self.find_parent_task_with_result()
        if previous_task is not None:
            first_stage = pipeline.configurator.first_differing_stage(self.config, previous_task.config)
            pickup_candidates.append((previous_task, first_stage.cfgns if first_stage else ''))
        if self.result_path.exists() and self.digest_cfg_path.exists():
            with self.digest_cfg_path.open('r') as fin:
                config = json.load(fin)
            first_stage = pipeline.configurator.first_differing_stage(self.config, config)
            pickup_candidates.append((self, first_stage.cfgns))
        return pickup_candidates

    def find_best_pickup_candidate(self, pipeline):
        pickup_candidates = self.find_pickup_candidates(pipeline)
        if len(pickup_candidates) == 0: return None, None
        pickup_candidate_scores = [pipeline.find(first_stage) for _, first_stage in pickup_candidates]
        return pickup_candidates[pickup_candidate_scores.index(max(pickup_candidate_scores))]

    def pickup_previous_task(self, pipeline, dry=False, pickup=True, out=None):
        out = get_output(out)
        pickup_task, stage_name = self.find_best_pickup_candidate(pipeline) if pickup else (None, None)
        if pickup_task is None or (len(stage_name) > 0 and all([self.is_stage_marginal(stage.cfgns) for stage in pipeline.stages[:pipeline.find(stage_name)]])):
            return None, {}
        else:
            out.write(f'Picking up from: {self._fmt_path(pickup_task.result_path)} ({stage_name if stage_name != "" else "load"})')
            if not dry:
                with gzip.open(pickup_task.result_path, 'rb') as fin:
                    data = dill.load(fin)
                return stage_name, data
            else:
                return stage_name, {}

    def _write_timings(self, timings):
        file_ids = timings.keys()
        stage_names = sorted(list(timings.values())[0].keys())
        rows = [[str(self.path)], ['ID'] + stage_names + ['total']]
        totals = [0] * (len(stage_names) + 1)
        for file_id in file_ids:
            vals  = [timings[file_id][stage_name] for stage_name in stage_names]
            vals += [sum(vals)]
            row   = [file_id] + [_format_runtime(val) for val in vals]
            rows.append(row)
            totals = [t + v for t, v in zip(totals, vals)]
        rows.append([''] + [_format_runtime(val) for val in totals])
        with self.timings_path.open('w', newline='') as fout:
            csv_writer = csv.writer(fout, delimiter=';', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for row in rows:
                csv_writer.writerow(row)
        with self.timings_json_path.open('w') as fout:
            json.dump(timings, fout)

    def __str__(self):
        return str(self.path)

    def __repr__(self):
        return f'<{type(self).__name__}, path: {self.path}>'


class BatchLoader:
    """Loads all tasks from a given directory (see :ref:`batch_task_spec`).

    :param inject: Dictionary of task specification settings which are to be added (before parsing)
    :param override: Dictionary of task specification settings which are to be overwritten (after parsing).
    """

    def __init__(self, task_cls: Type[Task], task_loader: 'TaskLoader', inject: dict = {}, override: dict = {}):
        self.tasks       = []
        self.task_cls    = task_cls
        self.task_loader = task_loader
        self.inject      = inject
        self.override    = override

    def load(self, path):
        """Loads all task from the root directory ``path``.
        """
        root_path = pathlib.Path(path)
        self._process_directory(root_path)
        return self

    @property
    def task_list(self):
        return [str(task.path) for task in self.tasks]

    def task(self, path):
        for task in self.tasks:
            if str(path) == str(task.path):
                return task
        return None

    def _process_directory(self, current_dir, parent_task=None):
        task = self._create_from_directory(current_dir, parent_task)
        if task is not None:
            self.tasks.append(task)
            parent_task = task
        for d in os.listdir(current_dir):
            f = current_dir / d
            if f.is_dir():
                self._process_directory(f, parent_task)

    def _create_from_directory(self, task_dir: PathType, parent_task: 'Task'):
        """Instantiates the task from the specification in a directory (see :ref:`batch_task_spec`).

        :param task_dir: The path of the directory which contains a task specification file.
        :param parent_task: The parent task (or ``None`` if this a root task).
        :param override: Dictionary of task specification settings which are to be overwritten.
        """
        task_dir  = pathlib.Path(task_dir)
        task_file = self.task_loader.filepath(task_dir / 'task')
        if task_file.exists():
            try:
                task_data = self.task_loader.load(str(task_file))
                task_data.update(self.inject)
                task = self.task_cls(loader = self.task_loader, path = task_dir, data = task_data, parent_task = parent_task)
                for key, value in self.override.items():
                    setattr(task, key, value)
                return task
            except:
                raise ValueError(f'error processing: "{task_file}"')
        return None


def _get_path(root_path, path):
    if isinstance(root_path, str): root_path = pathlib.Path(root_path)
    if isinstance(     path, str):      path = pathlib.Path(     path)
    if path.is_absolute(): return path
    return pathlib.Path(root_path) / path


class StatusReport:

    def __init__(self, scheduled_tasks, filepath=None):
        self.scheduled_tasks = scheduled_tasks
        self.filepath        = filepath
        self.status          = dict()
        self.task_progress   = None
        
    def get_task_status(self, task):
        return self.status.get(str(task.path), 'skipped')
    
    def update(self, task, status, save=True):
        if isinstance(status, float):
            self.task_progress = status
            status = 'active'
        else:
            self.task_progress = None
        assert status in ('pending', 'done', 'active', 'error')
        if status in ('done', 'active') and self.get_task_status(task) == 'skipped': return
        self.status[str(task.path)] = status
        if save: self.save()
    
    def save(self):
        if self.filepath is None: return
        with open(str(self.filepath), 'w') as fout:
            skipped_tasks = []
            for task in self.scheduled_tasks:
                status = self.get_task_status(task)
                prefix, suffix = '', ''
                if status == 'skipped':
                    skipped_tasks.append(task)
                    continue
                elif status == 'pending': prefix = ' o '
                elif status ==    'done': prefix = ' ✓ '
                elif status ==  'active': prefix = '-> '
                elif status ==   'error': prefix = 'EE '
                if status == 'active' and self.task_progress is not None:
                    suffix = f' ({100 * self.task_progress:.0f}%)'
                fout.write(f'{prefix}{task.path}{suffix}\n')
            if len(skipped_tasks) > 0:
                fout.write('\nSkipped tasks:\n')
                for task in skipped_tasks:
                    fout.write(f'- {str(task.path)}\n')


def run_cli(task_cls, task_loader = JSONLoader(), parser = None):

    if parser is None:
        import argparse
        parser = argparse.ArgumentParser()

    parser.add_argument('path', help='root directory for batch processing')
    parser.add_argument('--run', help='run batch processing', action='store_true')
    parser.add_argument('--verbosity', help='postive (negative) is more (less) verbose', type=int, default=0)
    parser.add_argument('--force', help='do not skip tasks', action='store_true')
    parser.add_argument('--oneshot', help='do not save results or mark tasks as processed', action='store_true')
    parser.add_argument('--last-stage', help='override the "last_stage" setting', type=str, default=None)
    parser.add_argument('--no-pickup', help='do not pick up previous results', action='store_true')
    parser.add_argument('--task', help='run only the given task', type=str, default=[], action='append')
    parser.add_argument('--task-dir', help='run only the given task and those from its sub-directories', type=str, default=[], action='append')
    parser.add_argument('--report', help='report current status to file', type=str, default=None)
    args = parser.parse_args()

    if args.last_stage is not None and not args.oneshot:
        parser.error('Using "--last-stage" is only allowed if "--oneshot" is used')

    override = dict()
    if args.last_stage is not None:
        override['last_stage'] = args.last_stage
        
    loader = BatchLoader(task_cls = task_cls, task_loader = task_loader, override = override)
    loader.load(args.path)

    args.task     = [_get_path(args.path,     task_path) for     task_path in args.task    ]
    args.task_dir = [_get_path(args.path, task_dir_path) for task_dir_path in args.task_dir]

    dry = not args.run
    out = get_output()
    runnable_tasks = [task for task in loader.tasks if task.runnable]
    out.write(f'Loaded {len(runnable_tasks)} runnable task(s)')
    if dry: out.write(f'DRY RUN: use "--run" to run the tasks instead')
    scheduled_tasks     = []
    run_task_count      =  0
    pending_tasks_count =  0
    report = StatusReport(scheduled_tasks, filepath=None if dry else args.report)
    for task in runnable_tasks:
        if (len(args.task) > 0 or len(args.task_dir) > 0) and all(task.path != path for path in args.task) and all(not _is_subpath(path, task.path) for path in args.task_dir): continue
        scheduled_tasks.append(task)
        if task.is_pending or args.force:
            pending_tasks_count += 1
            report.update(task, 'pending', save=False)
    for task in scheduled_tasks:
        if task.is_pending or args.force:
            run_task_count += 1
            task_info = f'{run_task_count} of {pending_tasks_count}'
        else:
            task_info = None
        report.update(task, 'active')
        newpid = os.fork()
        if newpid == 0:
            try:
                task.run(task_info, dry, args.verbosity, args.force, args.oneshot, report, not args.no_pickup, False, out)
            except:
                report.update(task, 'error')
                raise
            os._exit(0)
        else:
            if os.waitpid(newpid, 0)[1] != 0:
                out.write('An error occurred: interrupting')
                sys.exit(1)
            else:
                report.update(task, 'done')
    out.write(f'\nRan {run_task_count} task(s) out of {len(runnable_tasks)} in total')
