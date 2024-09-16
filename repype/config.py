import hashlib
import json

from repype.typing import (
    Any,
    Callable,
    Optional,
    Self,
    Union,
)


def _cleanup_value(value):
    return value.entries if isinstance(value, Config) else value


class Config:
    """
    Represents a set of hyperparameters.

    Hyperparameters can be worked with as follows:

    .. runblock:: pycon

       >>> import repype.config
       >>> config = repype.config.Config()
       >>> config['stage1/param1'] = 1000
       >>> config['stage2/param2'] = 5
       >>> print(config.entries)

    Arguments:
        other: A dictionary to be wrapped (no copying occurs), or another :py:class:`Config` object (a deep copy is
            created). Defaults to `None`, for which a blank configuration is created.
    """

    entries: dict
    """
    Nested dictionaries of hyperparameters.
    """

    def __init__(self, other: Optional[Union[dict, Self]] = None):
        if other is None:
            other = dict()
        if isinstance(other, dict):
            self.entries = other
        elif isinstance(other, Config):
            self.entries = json.loads(json.dumps(other.entries))
        else:
            raise ValueError(f'Unknown argument: {other}')

    @property
    def yaml(self) -> str:
        """
        YAML representation of this configuration.

        .. runblock:: pycon

            >>> import repype.config
            >>> config = repype.config.Config()
            >>> config['stage1/param1'] = 1000
            >>> config['stage2/param2'] = 5
            >>> config['stage1/sub/param1'] = 'xyz'
            >>> print(config.yaml)
        """
        return '\n'.join(self._as_yaml())

    def _as_yaml(self, indent = 0):
        for key, value in self.entries.items():
            prefix = '  ' * indent
            if isinstance(value, dict):
                yield prefix + f'{key}:'
                yield from Config(value)._as_yaml(indent + 1)
            else:
                yield prefix + f'{key}: {repr(value)}'

    def pop(self, key: str, default: Any) -> Any:
        """
        Removes a hyperparameter from this configuration.

        Arguments:
            key: The hyperparameter to be removed.
            default: Returned if the hyperparameter `key` is not set.

        Returns:
            The value of the hyperparameter `key`, or the `default` if `key` is not set.
        """
        if '/' in key:
            keys = key.split('/')
            config = self
            for key in keys[:-1]:
                config = config.get(key, {})
            return config.pop(keys[-1], default)
        else:
            return self.entries.pop(key, default)

    def set_default(self, key: str, default: Any, override_none: bool = False):
        """
        Sets a hyperparameter if it is not set yet.

        Arguments:
            key: The hyperparameter to be set.
            default: Returned if the hyperparameter `key` is not set.
            override_none: `True` if a hyperparameter set to `None` should be treated as not set.

        Returns:
            The new or unmodified value of the hyperparameter `key`.
        """
        if '/' in key:
            keys = key.split('/')
            config = self
            for key in keys[:-1]:
                config = config.set_default(key, {}, override_none)
            return config.set_default(keys[-1], default, override_none)
        else:
            if key not in self.entries or (override_none and self.entries[key] is None):
                self.entries[key] = _cleanup_value(default)
            return self[key]

    def get(self, key: str, default: Any) -> Any:
        """
        Returns the value of a hyperparameter.

        Arguments:
            key: The hyperparameter to be queried.
            default: Returned if the hyperparameter `key` is not set.

        Returns:
            The value of the hyperparameter `key`, or `default` if `key` is not set.
        """
        if '/' in key:
            keys = key.split('/')
            config = self
            for key in keys[:-1]:
                config = config.get(key, {})
            return config.get(keys[-1], default)
        else:
            if key not in self.entries:
                self.entries[key] = _cleanup_value(default)
            value = self.entries[key]
            return Config(value) if isinstance(value, dict) else value

    def __getitem__(self, key: str) -> Any:
        """
        Returns the value of a hyperparameter.

        Arguments:
            key: The hyperparameter to be queried.

        Returns:
            The value of the hyperparameter `key`, or `default` if `key` is not set.

        Raises:
            KeyError: Raised if the hyperparameter `key` is not set.
        """
        if '/' in key:
            keys = key.split('/')
            config = self
            for key in keys[:-1]:
                config = config[key]
            return config[keys[-1]]
        else:
            value = self.entries[key]
            return Config(value) if isinstance(value, dict) else value

    def __contains__(self, key: str) -> bool:
        """
        Checks whether a hyperparameter is set.

        Arguments:
            key: The hyperparameter to be queried.

        Returns:
            `True` if the hyperparameter `key` is set and `False` otherwise.
        """
        try:
            self.__getitem__(key)
            return True
        except KeyError:
            return False

    def update(self, key: str, func: Callable[[Any], Any]) -> Any:
        """
        Updates a hyperparameter by mapping it to a new value.

        Arguments:
            key: The hyperparameter to be updated.
            func: Function which maps the previous value to the new value.

        Returns:
            The new value.
        """
        if '/' in key:
            keys = key.split('/')
            config = self
            for key in keys[:-1]:
                config = config.get(key, {})
            return config.update(keys[-1], func)
        else:
            self.entries[key] = _cleanup_value(func(self.entries.get(key, None)))
            return self.entries[key]

    def __setitem__(self, key: str, value: Any) -> Self:
        """
        Sets the value of a hyperparameter.

        Arguments:
            key: The hyperparameter to be set.
            value: The new value of the hyperparameter.

        Returns:
            Itself.
        """
        self.update(key, lambda *args: value)
        return self

    def merge(self, other: Self) -> Self:
        """
        Updates this configuration using the hyperparameters from another configuration.

        The hyperparameters of this configuration are set to the values from the `other` configuration. If a
        hyperparameter was previously not set in this configuration, it is set to the value from the `other`
        configuration.

        Arguments:
            other: The configuration which is to be merged into this configuration.

        Returns:
            Itself.
        """
        for key, val in _cleanup_value(other).items():
            if not isinstance(val, dict):
                self.entries[key] = val
            else:
                self.get(key, {}).merge(val)
        return self

    def copy(self):
        """
        Returns a deep copy.
        """
        return Config(self)

    @property
    def sha(self):
        """The SHA1 hash code associated with the hyperparameters set in this configuration.
        """
        return hashlib.sha1(json.dumps(self.entries).encode('utf8'))

    def __str__(self):
        """
        Readable representation of this configuration.
        """
        return json.dumps(self.entries, indent=2)

    def __repr__(self):
        return f'<{type(self).__name__}, {str(self.entries)}>'

    def __eq__(self, other: object):
        return isinstance(other, Config) and str(self) == str(other)
