class InvalidFileType(TypeError):
    def __init__(self, filename, typename, exception):
        self.filename = filename
        self.type = typename
        self.exception = exception

class CASFileType(object):
    """
    Verify a file is of a given type, and then compute metadata
    (key/value pairs) about the file, attaching that metadata
    to the CAS version of the file.

    The general principal here is that any computed metadata
    should be invariant for a given CAS file (i.e. as long as
    the checksum doesn't change, these metadata shouldn't change
    either).
    """
    def __init__(self, filename):
        self.filename = filename

        if not self.type:
            raise NotImplementedError('must set class param type')

    def meta(self):
        """
        Calculate type-specific metadata about the given file.
        """
        raise NotImplementedError

    def verify(self):
        """
        Is the given filename actually of the correct type?
        """
        raise NotImplementedError

class NullType(CASFileType):
    """
    The default file type. Don't verify the file or compute any
    metadata about it.
    """
    type = 'none'

    def verify(self):
        pass

    def meta(self):
        return {}

TYPE_MAP = {}

def register_type(type_cls):
    TYPE_MAP[type_cls.type] = type_cls

def get_type(type_str):
    return TYPE_MAP.get(type_str, None)

def types():
    return TYPE_MAP.keys()

register_type(NullType)

DEFAULT_TYPE = NullType.type
