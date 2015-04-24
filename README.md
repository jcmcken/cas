# cas

A simple content-addressable storage (CAS) system, written in Python.

## Command-Line

Manage files:

```console
$ export CAS_ROOT=/path/to/somedir
$ cas add /path/to/some/file
7335999eb54c15c67566186bdfc46f64e0d5a1aa
$ cas ls
7335999eb54c15c67566186bdfc46f64e0d5a1aa
$ cas path 7335999eb54c15c67566186bdfc46f64e0d5a1aa
/path/to/somedir/storage/73/35/999eb54c15c67566186bdfc46f64e0d5a1aa
$ cas rm 7335999eb54c15c67566186bdfc46f64e0d5a1aa
$ cas ls
$
```

Examine storage metadata:

```console
$ cas meta
{
  "uuid": "e898dab64b224f5992e174e2b2e84b7b",
  "created": "2015-04-18T16:12:37.691159",
  "updated": "2015-04-18T16:12:54.851297",
  "shard": {
    "width": 2,
    "height": 2
  }
}
```

Attach metadata to a file:

```console
$ cas add /path/to/some/file --type rpm
$ cas match type rpm
7335999eb54c15c67566186bdfc46f64e0d5a1aa
$ cas match --exact rpm.version 0.1.2
$
$ cas match --exact rpm.version 0.1.1
7335999eb54c15c67566186bdfc46f64e0d5a1aa
```

## API

### Implementing Custom File Types

To implement your own filetypes, just subclass ``cas.storage.CASFileType``,
filling in the required methods.

For example:

```python
from cas.files import CASFileType, register_type, InvalidFileType
import my

__name__ = 'myplugin'

class MyType(CASFileType):
    type = 'my'

    def verify(self):
        if not self.filename.startswith('my'):
            raise InvalidFileType(self.filename, self.type)

    def meta(self):
        return my.meta(self.filename)

register_type(MyType)
```

(Note that the ``verify`` method must raise an ``InvalidFileType`` if the
file's type is found to be incorrect)

Next, just stick the ``py`` file in the plugins directory. By default,
this is ``cas/plugins`` wherever you have ``cas`` installed. (You can
override this via an environment variable or using the CLI.)
