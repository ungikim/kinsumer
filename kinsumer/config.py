""":mod:`kinsumer.config` --- Implements the configuration related objects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""
import errno
import os
import types
from typing import Dict, Any

from typeguard import typechecked
from werkzeug.datastructures import ImmutableDict
from werkzeug.utils import import_string


class ConfigAttribute(object):
    """Make an attribute forward to the config"""

    def __init__(self, name: str, get_converter=None):
        self.__name__ = name
        self.get_converter = get_converter

    def __get__(self, obj: object, type_=None) -> object:
        if obj is None:
            return self
        rv = obj.config[self.__name__]
        if self.get_converter is not None:
            rv = self.get_converter(rv)
        return rv

    def __set__(self, obj: object, value: object) -> None:
        obj.config[self.__name__] = value


class Config(dict):
    @typechecked
    def __init__(self, root_path: str, defaults: ImmutableDict = None) -> None:
        super().__init__(defaults or {})
        self.root_path = root_path

    @typechecked
    def from_envvar(self, variable_name: str, silent: bool = False) -> bool:
        rv = os.environ.get(variable_name)
        if not rv:
            if silent:
                False
            raise RuntimeError()
        return self.from_pyfile(rv, silent=silent)

    @typechecked
    def from_pyfile(self, filename: str, silent: bool = False) -> bool:
        filename = os.path.join(self.root_path, filename)
        d = types.ModuleType('config')
        d.__file__ = filename
        try:
            with open(filename, mode='rb') as config_file:
                exec(compile(config_file.read(), filename, 'exec'), d.__dict__)
        except IOError as e:
            if silent and e.errno in (errno.ENOENT, errno.EISDIR):
                return False
            e.strerror = 'Unable to load configuration file (%s)' % e.strerror
            raise
        self.from_object(d)
        return True

    @typechecked
    def from_object(self, obj) -> None:
        if isinstance(obj, str):
            obj = import_string(obj)
        for key in dir(obj):
            if key.isupper():
                self[key] = getattr(obj, key)

    @typechecked
    def get_namespace(self,
                      namespace: str,
                      lowercase=True,
                      trim_namespace=True) -> Dict[str, Any]:
        rv = {}
        for k, v in self.items():
            if not k.startswith(namespace):
                continue
            if trim_namespace:
                key = k[len(namespace):]
            else:
                key = k
            if lowercase:
                key = key.lower()
            rv[key] = v
        return rv
