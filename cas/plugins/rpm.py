from cas.files import CASFileType, register_type
import rpm

__name__ = 'rpm'

class RpmType(CASFileType):
    type = 'rpm'

    def verify(self):
        pass

    def meta(self):
        pass

register_type(RpmType)
