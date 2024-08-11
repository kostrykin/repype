import json
import os
import pathlib
from .typing import (
    Iterable,
    Iterator,
    Optional,
    PathLike,
    Self,
    Union,
)
import uuid

from watchdog.observers import Observer
from watchdog.events import (
    FileModifiedEvent,
    FileSystemEventHandler,
)


class Status:

    def __init__(self, parent: Optional[Self] = None, path: Optional[PathLike] = None):
        assert (parent is None) != (path is None), 'Either parent or path must be provided'
        self.id = uuid.uuid4()
        self.path = pathlib.Path(path) if path else None
        self.parent = parent
        self.data = list()
        self._intermediate = None

    @property
    def root(self) -> Optional[pathlib.Path]:
        return self.parent.root if self.parent else self

    @property
    def filepath(self) -> pathlib.Path:
        return self.root.path / f'{self.id}.json'
    
    def update(self) -> None:
        if self._intermediate:
            data = self.data + [
                dict(
                    expand = str(self._intermediate.filepath),
                    scope = 'intermediate',
                ),
            ]
        else:
            data = self.data
        with open(self.filepath, 'w') as file:
            json.dump(data, file)

    def derive(self) -> Self:
        child = Status(self)
        self.data.append(
            dict(
                expand = str(child.filepath),
            )
        )
        self.update()
        return child
    
    def write(self, status: Union[str, dict, list]) -> None:
        self._intermediate = None
        self.data.append(status)
        self.update()

    def intermediate(self, status: Optional[str] = None) -> None:
        if self._intermediate is None:
            self._intermediate = Status(self)
        if status is not None:
            self._intermediate.data.clear()
            self._intermediate.write(status)
            self._intermediate.update()
        else:
            self._intermediate = None
        self.update()

    def progress(self, description: str, iterable: Iterable, len_override: Optional[int] = None) -> Iterator[dict]:
        max_steps = len_override or len(iterable)
        try:
            for step, item in enumerate(iterable):
                assert step < max_steps
                self.intermediate(
                    dict(
                        description = description,
                        progress = step / max_steps,
                        step = step,
                        max_steps = max_steps,
                    )
                )
                yield item
        finally:
            self.intermediate(None)

    @staticmethod
    def get(status: Optional[Self] = None) -> Self:
        if status is None:
            path = pathlib.Path('.status')
            assert not path.is_file()
            path.mkdir(exist_ok = True)
            status = Status(path = path)
            print(f'Status written to: {status.filepath.resolve()}')
        return status
    

class StatusReader(FileSystemEventHandler):

    def __init__(self, filepath: PathLike):
        self.filepath = pathlib.Path(filepath).resolve()
        self.data = list()
        self.data_frames = {self.filepath: self.data}
        self.update(self.filepath)

    def __enter__(self) -> dict:
        # ================================================================================== #
        # There is an issue with WatchDog on GitHub Actions, for which this is a workaround. #
        # Details: https://github.com/kostrykin/pypers/pull/8#issuecomment-2282883353        #
        if os.environ.get('PYPERS_WATCHDOG_OBSERVER') == 'polling':
            from watchdog.observers.polling import PollingObserver
            self.observer = PollingObserver()
        else:
            self.observer = Observer()
        # ================================================================================== #
        self.observer.schedule(self, self.filepath.parent, recursive = False)
        self.observer.start()
        return self.data
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.observer.stop()
        self.observer.join()

    def update(self, filepath: pathlib.Path) -> None:
        data_frame = self.data_frames.get(filepath)

        if data_frame is None:
            return
        
        else:
            try:
                with open(filepath) as file:
                    data_frame_backup = data_frame.copy()
                    data_frame.clear()
                    data_frame.extend(json.load(file))
            except json.decoder.JSONDecodeError:
                data_frame.extend(data_frame_backup)

            for item_idx, item in enumerate(data_frame):
                if isinstance(item, dict) and 'expand' in item:
                    filepath = pathlib.Path(item['expand']).resolve()

                    child_data_frame = self.data_frames.get(filepath)
                    if child_data_frame is None:
                        child_data_frame = list()
                        self.data_frames[filepath] = child_data_frame

                    scope = item.get('scope')
                    if scope is not None:
                        data_frame[item_idx] = dict(
                            scope = scope,
                            content = child_data_frame,
                        )
                    else:
                        data_frame[item_idx] = child_data_frame
                    self.update(filepath)

    def on_modified(self, event) -> None:
        if isinstance(event, FileModifiedEvent):
            filepath = pathlib.Path(event.src_path).resolve()
            self.update(filepath)