import asyncio
import copy
import hashlib
import json
import pathlib
import tempfile
import uuid

from watchdog.events import (
    DirModifiedEvent,
    FileModifiedEvent,
    FileSystemEventHandler,
)
from watchdog.observers import Observer

from repype.typing import (
    ContextManager,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    PathLike,
    Self,
    Union,
)

# `hashlib.file_digest`` is available in Python 3.11+
if hasattr(hashlib, 'file_digest'):
    file_digest = hashlib.file_digest
else:
    def file_digest(file, hash_cls):
        hash = hash_cls()
        while (chunk := file.read(4096)):
            hash.update(chunk)
        return hash


class Status:
    """
    A status object that can be used to report the progress of a computation.

    Status updates should be made via the :func:`repype.status.update`, :func:`repype.status.progress`, and
    :func:`repype.status.derive` shortcuts. The updates can be monitored by a :class:`StatusReader` object.

    Status objects can be nested, so that the progress of a sub-computation can be reported within the progress of a
    parent computation. In addition, since each status object fosters its own status file, the amount of I/O operations
    required to write and read status updates is reduced.
    """

    id: uuid.UUID
    """
    The unique identifier of the status object.
    """

    path: Optional[pathlib.Path]
    """
    The path to the directory where the status file is written by this status object,
    or `None` if the path of the :attr:`parent` status object is adopted.
    """

    parent: Optional[Self]
    """
    The parent status object, if this status object is nested within another status object.
    """

    data: list
    """
    The data structure that represents the progress of the computation.
    """

    def __init__(self, parent: Optional[Self] = None, path: Optional[PathLike] = None):
        assert (
            (parent is None) != (path is None)
        ), f'Either `parent` or `path` must be provided (parent: {parent}, path: {path})'

        self.id = uuid.uuid4()
        self.path = pathlib.Path(path) if path else None
        self.parent = parent
        self.data = list()
        self._intermediate = None

    @property
    def root(self) -> Optional[pathlib.Path]:
        """
        The path to the directory where the status file is written by this status object.
        """
        return self.parent.root if self.parent else self

    @property
    def filepath(self) -> pathlib.Path:
        """
        The path to the status file written by this status object.
        """
        return self.root.path / f'{self.id}.json'

    def update(self) -> None:
        """
        Write the status data to the status file.
        """
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
        """
        Create a child status object that is nested within this status object.
        """
        self.intermediate(None)
        child = Status(self)
        self.data.append(
            dict(
                expand = str(child.filepath),
            )
        )
        child.update()
        self.update()
        return child

    def write(self, status: Union[str, dict, list]) -> None:
        """
        Write a permanent status update to the status object.
        """
        self._intermediate = None
        self.data.append(status)
        self.update()

    def intermediate(self, status: Optional[Union[str, dict]] = None) -> None:
        """
        Write an intermediate status update to the status object.

        Intermediate status updates are overwritten by subsequent status updates (intermediate or permanent). If
        `status` is `None`, any previous intermediate status is cleared without writing a new one.
        """
        # An intermediate status object is created, and then linked within this status object. The order of the two
        # operations is crucial, because otherwise an empty intermediate object might be detected initially.
        if status is not None:

            # If the intermediate object doesn't exist yet, create it
            if self._intermediate is None:
                self._intermediate = Status(self)
                update_required = True  # The intermediate object needs to be linked in this status object
            else:
                update_required = False  # The intermediate object is already linked

            # Write the intermediate status to the intermediate status object
            self._intermediate.data.clear()
            self._intermediate.write(status)

            # If the intermediate object was newly created, link it in this status object
            if update_required:
                self.update()

        # Clear the intermediate status
        else:
            self._intermediate = None
            self.update()

    def progress(
            self,
            iterable: Iterable,
            iterations: Optional[int] = None,
            details: Optional[Union[str, dict]] = None,
        ) -> Iterator[dict]:
        """
        Write an intermediate progress update for each item in the iterable.

        The intermediate status is cleared after yielding the last item from the `iterable`, after exiting the
        generator (e.g., breaking the loop), or if an error is raised.

        Arguments:
            iterable: The iterable to be processed.
            iterations: The number of iterations to make (e.g., if this cannot be determined by calling `len` on the
                `iterable`). Defaults to the `len` of the `iterable`.
            details: Additional status details.

        Yields:
            The items from the `iterable`, while making intermediate progress updates to the status object.

        Raises:
            AssertionError: If the `iterable` has more items than the number of `iterations`.
        """
        max_steps = len(iterable) if iterations is None else iterations
        try:
            for step, item in enumerate(iterable):
                assert step < max_steps
                self.intermediate(
                    dict(
                        info = 'progress',
                        details = details,
                        progress = step / max_steps,
                        step = step,
                        max_steps = max_steps,
                    )
                )
                yield item
        finally:
            self.intermediate(None)


def create() -> ContextManager[Status]:
    """
    Create a status object associated with a temporary directory.

    .. runblock:: pycon

        >>> import repype.status
        >>> with repype.status.create() as status:
        ...    status.write('Hello, World!')
        ...    print(status.filepath.read_text())
    """
    class ContextManager:

        def __enter__(self) -> Status:
            self.path_directory = tempfile.TemporaryDirectory()
            return Status(path = self.path_directory.name)

        def __exit__(self, exc_type, exc_val, exc_tb) -> None:
            self.path_directory.cleanup()
            self.path_directory = None

    return ContextManager()


class Cursor:
    """
    A cursor to navigate nested list structures.
    """

    data: list
    """
    The data structure that the cursor navigates.
    """

    path: List[int]
    """
    Sequence of elements along the path to where this cursor points, represented by the positions of the elements
    within the parent lists.
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
            The cursor, if it points to a valid element, or `None` otherwise.
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
            The cursor to the next child or sibling, if such exists, or `None` otherwise.
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

        Precedentially, the next element is the next child or subling. If no next child or subling exists, then the
        next element is the next sibling of the parent. If no next sibling of the parent exists, then the next element
        is the next sibling of the grandparent, and so on.

        This cursor is not changed, but a new cursor is returned.

        Returns:
            The cursor to the next element, if such exists, or `None` otherwise.
        """
        cursor = self.find_next_child_or_sibling()
        if cursor:
            return cursor

        for parent in self.parents:
            cursor = parent.find_next_child_or_sibling()
            if cursor:
                return cursor

        return None

    def has_subsequent_non_intermediate(self) -> bool:
        """
        Check if there is a subsequent non-intermediate element.

        Returns:
            `True` if calling :meth:`find_next_element` once or repeatedly will yield a non-intermediate element, and
            `False` otherwise.
        """
        cursor = self
        while cursor := cursor.find_next_element():
            if not cursor.intermediate:
                return True
        return False

    def get_elements(self) -> Optional[List[list]]:
        """
        Get the sequence of elements which represent the path to the element, that this cursor points to.

        Returns:
            The sequence of elements along the path to where this cursor points, if the cursor points to a valid
            element, or `None` otherwise.
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
        Check if the cursor points to an existing element.
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

    @property
    def intermediate(self) -> Optional[bool]:
        """
        Check if the cursor points to an intermediate element.

        The value is `None` if the cursor is invalid, `True` if the cursor points to an intermediate element, and
        `False` otherwise.
        """
        if self.valid:
            element = self.get_elements()[-1]
            return isinstance(element, dict) and element.get('content_type') == 'intermediate'
        else:
            return None


class StatusReader(FileSystemEventHandler):
    """
    A status reader that can be used to monitor the progress of a computation by tracking the updates of a
    :class:`Status` object, including its nested status objects.

    The monitored status object can reside in a different process and is accessed by reading the corresponding status
    file. The progress of the computation is represented by a nested list structure, where each list directly
    corresponds to the state of a nested status object.

    Arguments:
        filepath: The status file written by the status object to be monitored.
        loop: The event loop to be used for processing the status updates (usually the loop of the main thread).
            Defaults to the event loop of the thread used to create the status reader object.
        blocking: If `True`, the status updates are processed on a separate thread (the main thread is assumed to be
            used for long-running, blocking operations). If `False`, the status updates are posted to the main thread.

    See also:
        :attr:`repype.status.Status.filepath` is the status file written by a status object.

    See also:
        The implementation :class:`repype.cli.StatusReaderConsoleAdapter` writes status updates to the standard output.
    """

    filepath: PathLike
    """
    The status file written by the monitored status object.
    """

    data: list
    """
    The data structure that represents the progress of the computation.
    """

    data_frames: Dict[pathlib.Path, list]
    """
    The data structures that represent the progress of the nested status objects, indexed by the paths to the
    corresponding status files. This also contains the progress of the status object that corresponds to the
    :attr:`filepath` attribute.
    """

    file_hashes: Dict[pathlib.Path, str]
    """
    The hashes of the status files when they were last read.
    """

    cursor: Cursor
    """
    Points to the latest permanent (i.e. non-intermediate) status update within :attr:`data`.
    """

    loop: asyncio.AbstractEventLoop
    """
    The event loop to be used for processing the status updates.
    """

    blocking: bool
    """
    If `True`, the status updates are processed on a separate thread (the main thread is assumed to be used for
    long-running, blocking operations). If `False`, the status updates are posted to the main thread.
    """

    def __init__(self, filepath: PathLike, loop: asyncio.AbstractEventLoop = None, blocking = False):
        self.loop = loop if loop else asyncio.get_running_loop()
        self.blocking = blocking
        self.filepath = pathlib.Path(filepath).resolve()
        self.data = list()
        self.data_frames = {self.filepath: self.data}
        self.file_hashes = dict()
        self.cursor = Cursor(self.data)
        self._intermediate = None
        self.update(self.filepath)
        self.check_new_status()

    async def __aenter__(self) -> dict:
        self.observer = Observer()
        self.observer.schedule(self, self.filepath.parent, recursive = False)
        self.observer.start()
        return self.data

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await asyncio.sleep(1)  # Give the WatchDog observer some extra time
        self.observer.stop()
        self.observer.join()

    def update(self, filepath: pathlib.Path) -> bool:
        """
        Update the nested list structure that represents the progress of the computation.

        Only the list in :attr:`data_frames` is updated that corresponds to the status object that writes the status
        file at `filepath`. The status file is only read if its content has changed according to the
        :attr:`file_hashes`.

        Arguments:
            filepath: The path to the status file to be read.

        Returns:
            `True` if the status data has changed, and `False` otherwise.
        """
        data_frame = self.data_frames.get(filepath)

        if data_frame is None:
            return False  # No update has been performed

        else:
            # Check the file hash, whether the content of the file has changed at all
            # Opening the file can fail due to race conditions, so we need to handle that
            try:
                with filepath.open('rb') as file:
                    sha = file_digest(file, hashlib.sha1).hexdigest()
                    if self.file_hashes.get(filepath) == sha:
                        return False  # No update has been performed
                    else:
                        self.file_hashes[filepath] = sha

                    # Load the data from the file, and revert in case of JSON decoding errors
                    # These can occur due to race conditions, or unfavorable buffer sizes
                    try:
                        file.seek(0)
                        data_frame_backup = data_frame.copy()
                        data_frame.clear()
                        data_frame.extend(json.load(file))
                    except json.decoder.JSONDecodeError:
                        data_frame.extend(data_frame_backup)

            except FileNotFoundError:
                return False  # No update has been performed

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

            return True

    def on_modified(self, event: Union[DirModifiedEvent, FileModifiedEvent]) -> None:
        """
        Handle status file updates within the directory of the monitored status file.

        If the monitored or a nested status file is updated, the status data is reloaded by the :meth:`update` method.
        Only if the status data has changed according to the :attr:`file_hashes`, the new status data is processed by
        the :meth:`check_new_status` method.

        Arguments:
            event: The file modification event.
        """
        if isinstance(event, FileModifiedEvent):
            filepath = pathlib.Path(event.src_path).resolve()

            def update(filepath):
                if self.update(filepath):
                    self.check_new_status()

            if self.blocking:
                update(filepath)
            else:
                self.loop.call_soon_threadsafe(update, filepath)

    def check_new_status(self) -> None:
        """
        Check the :attr:`data` for new status updates, based on the current position of the :attr:`cursor`.

        The :attr:`cursor` is advanced to the next element (if any), and the new status update is processed by the
        :meth:`handle_new_status` method. This procedure is repated until the :attr:`cursor` points to the end of
        :attr:`data`. If the :attr:`cursor` then points to an intermediate status, it is rewinded to the last
        non-intermediate position (i.e. permanent). This assures that future intermediate status updates will be again
        proclaimed to the :meth:`handle_new_status` method.
        """
        new_data = False
        while (cursor := self.cursor.find_next_element()):
            elements = cursor.get_elements()
            new_data = True

            # If the element is an intermediate, but it didn't actually change, skip it
            if not (cursor.intermediate and self._intermediate is not None and self._intermediate[-1] == elements[-1]):
                self._unwrap_new_status(list(cursor.path), copy.deepcopy(elements[-1]))

            # If the element is an intermediate, leave the cursor on the last non-intermediate position
            # Unless there is a subsequent non-intermediate element
            if cursor.intermediate and not cursor.has_subsequent_non_intermediate():
                self._intermediate = (list(cursor.path), copy.deepcopy(elements[-1]))
                break
            else:
                self._intermediate = None
                self.cursor = cursor

        # If there was no new data, but there was supposed to be an intermediate, handle the closed intermediate
        if not new_data and self._intermediate:
            self._intermediate[-1]['content'] = None
            self._unwrap_new_status(*self._intermediate)
            self._intermediate = None

    def _unwrap_new_status(
            self,
            positions: List[int],
            element: Union[str, dict],
        ) -> None:
        # Check if the element is an intermediate status update
        if isinstance(element, dict) and element.get('content_type') == 'intermediate':

            # If the intermediate status is cleared, handle it accordingly
            if not element['content']:
                self.handle_new_status(positions, status = None, intermediate = True)

            # Otherwise, handle the intermediate status update
            else:
                self.handle_new_status(positions, status = element['content'][0], intermediate = True)

        # Handle an non-intermediate status update (permanent)
        else:
            self.handle_new_status(positions, status = element, intermediate = False)

    def handle_new_status(
            self,
            positions: List[int],
            status: Optional[Union[str, dict]],
            intermediate: bool,
        ) -> None:
        """
        Process a new status update.

        Arguments:
            positions: The sequence of elements along the path, represented by the positions of the elements within the
                parent lists.
            status: The new status update. Can only be `None` if `intermediate` is True, indicating that the
                intermediate status is cleared.
            intermediate: True if the status update is intermediate, and `False` otherwise.
        """
        pass


# Define some shortcuts

def update(status: Optional[Status], plain_text = None, intermediate: bool = False, **kwargs) -> None:
    """
    Shortcut for :meth:`Status.write` and :meth:`Status.intermediate`.

    Does nothing if `status` is `None`.

    Raises:
        AssertionError: If both `plain_text` and `kwargs` are provided (or neither, and `intermediate` is True).
    """
    assert plain_text is None or len(kwargs) == 0, 'Cannot specify both `plain_text` and `kwargs`'
    if status is None:
        return None
    else:
        if intermediate:
            status.intermediate(dict(**kwargs) if kwargs else plain_text)
        else:
            assert plain_text is not None or len(kwargs) > 0, 'Either `plain_text` or `kwargs` must be provided'
            status.write(dict(**kwargs) if kwargs else plain_text)


def derive(status: Optional[Status]) -> Optional[Status]:
    """
    Shortcut for :meth:`Status.derive`.

    Does nothing if `status` is `None`.
    """
    if status is None:
        return None
    else:
        return status.derive()


def progress(
        status: Optional[Status],
        iterable: Iterable,
        iterations: Optional[int] = None,
        details: Optional[Union[str, dict]] = None,
    ) -> Iterator[dict]:
    """
    Shortcut for :meth:`Status.progress`.

    Yields the items from the `iterable` directly if `status` is `None`.
    """
    if status is None:
        return iterable
    else:
        return status.progress(iterable, iterations, details)
