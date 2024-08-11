import json
import pathlib
import uuid

from .typing import (
    Optional,
    PathLike,
    Self,
    Union,
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

    @staticmethod
    def get(status: Optional[Self] = None) -> Self:
        if status is None:
            path = pathlib.Path('.status')
            assert not path.is_file()
            path.mkdir(exist_ok = True)
            status = Status(path = path)
            print(f'Status written to: {status.filepath.resolve()}')
        return status