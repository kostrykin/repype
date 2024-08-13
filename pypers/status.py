import json
import pathlib
from .typing import (
    Iterable,
    Iterator,
    List,
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
                    content_type = 'intermediate',
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
    

class Cursor:
    """
    A cursor to navigate a nested list structures.
    """

    def __init__(self, data: Optional[list] = None, other: Optional[Self] = None):
        assert (data is None) != (other is None)
        if other is None:
            self.data = data
            self.path = [-1]
        else:
            self.data = other.data
            self.path = list(other.path)

    def increment(self) -> Optional[Self]:
        """
        Move the cursor to the next sibling.

        Returns:
            Cursor: The cursor, if it points to a valid element, or None otherwise.
        """
        self.path[-1] += 1
        if self.valid:
            return self
        else:
            return None
        
    def find_next_child_or_sibling(self) -> Optional[Self]:
        """
        Return a cursor to the next child or sibling.

        This cursor is not changed, but a new cursor is returned.

        Returns:
            Cursor: The cursor to the next child or sibling, if such exists, or None otherwise.
        """
        cursor = Cursor(other = self)
        if not cursor.increment():
            return None
        
        else:
            new_element = cursor.get_elements()[-1]
            if isinstance(new_element, list):
                cursor.path.append(-1)
                return cursor.find_next_child_or_sibling()
            
            else:
                return cursor
            
    def find_next_element(self) -> Optional[Self]:
        """
        Return a cursor to the next element.

        Precedentially, the next element is the next child or subling.
        If no next child or subling exists, then the next element is the next sibling of the parent.
        If no next sibling of the parent exists, then the next element is the next sibling of the grandparent, and so on.

        This cursor is not changed, but a new cursor is returned.

        Returns:
            Cursor: The cursor to the next element, if such exists, or None otherwise.
        """
        cursor = self.find_next_child_or_sibling()
        if cursor:
            return cursor
        
        for parent in self.parents:
            cursor = parent.find_next_child_or_sibling()
            if cursor:
                return cursor
            
        return None

    def get_elements(self) -> Optional[List[list]]:
        """
        Get the sequence of elements which represent the path to the element, that this cursor points to.

        Returns:
            List[list]: The sequence of elements, if the cursor points to a valid element, or None otherwise.
        """
        elements = [self.data]
        for pos in self.path:
            try:
                elements.append(elements[-1][pos])
            except IndexError:
                return None
        return elements
    
    @property
    def valid(self) -> bool:
        """
        Check if the cursor points to a valid element.
        """
        return self.get_elements() is not None
    
    @property
    def parent(self) -> Self:
        """
        Get the cursor to the parent element.
        """
        if len(self.path) > 1:
            parent = Cursor(other = self)
            parent.path.pop()
            return parent
        else:
            return None
    
    @property
    def parents(self) -> Iterator[Self]:
        """
        List of cursors to the parent elements.
        """
        cursor = self.parent
        while cursor is not None:
            yield cursor
            cursor = cursor.parent

    

class StatusReader(FileSystemEventHandler):

    def __init__(self, filepath: PathLike):
        self.filepath = pathlib.Path(filepath).resolve()
        self.data = list()
        self.data_frames = {self.filepath: self.data}
        self.cursor = Cursor(self.data)
        self.update(self.filepath)
        self.check_new_data()

    def __enter__(self) -> dict:
        self.observer = Observer()
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
            # Load the data from the file, and revert in case of JSON decoding errors
            # These can occur due to the file being written to while being read, or unfavorable buffer sizes
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

                    content_type = item.get('content_type')
                    if content_type is not None:
                        data_frame[item_idx] = dict(
                            content_type = content_type,
                            content = child_data_frame,
                        )
                    else:
                        data_frame[item_idx] = child_data_frame
                    self.update(filepath)

    def on_modified(self, event) -> None:
        if isinstance(event, FileModifiedEvent):
            filepath = pathlib.Path(event.src_path).resolve()
            self.update(filepath)
            self.check_new_data()

    def check_new_data(self) -> None:
        while (cursor := self.cursor.find_next_element()):
            self.cursor = cursor
            elements = self.cursor.get_elements()
            self.handle_new_data(elements[:-1], self.cursor.path, elements[-1])

    def handle_new_data(self, parents: List[Union[str, dict]], positions, element):
        pass
    

# Define some shortcuts

def update(status, intermediate = False, **kwargs):
    if status is not None:
        if intermediate:
            status.intermediate(dict(**kwargs))
        else:
            status.write(dict(**kwargs))

def derive(status):
    if status is not None:
        return status.derive()
    
def progress(status, iterable, *args, **kwargs):
    if status is None:
        return iterable
    else:
        return status.progress(iterable, *args, **kwargs)