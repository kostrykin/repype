import json
import pathlib
from .typing import (
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
    def root(self):
        return self.parent.root if self.parent else self

    @property
    def filepath(self):
        return self.root.path / f'{self.id}.json'
    
    def update(self):
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
    
    def write(self, status: Union[str, dict, list]):
        self._intermediate = None
        self.data.append(status)
        self.update()

    def intermediate(self, status: str):
        if self._intermediate is None:
            self._intermediate = Status(self)
        self._intermediate.data.clear()
        self._intermediate.write(status)
        self._intermediate.update()
        self.update()

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

    def __enter__(self):
        self.observer = Observer()
        self.observer.schedule(self, self.filepath.parent, recursive = False)
        self.observer.start()
        return self.data
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.observer.stop()
        self.observer.join()

    def update(self, filepath):
        data_frame = self.data_frames.get(filepath)

        if data_frame is None:
            return
        
        else:
            with open(filepath) as file:
                data_frame.clear()
                data_frame.extend(json.load(file))

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

    def on_modified(self, event):
        if isinstance(event, FileModifiedEvent):
            filepath = pathlib.Path(event.src_path).resolve()
            self.update(filepath)