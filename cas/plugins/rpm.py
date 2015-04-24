from cas.files import CASFileType, register_type, InvalidFileType
import rpm
import logging

LOG = logging.getLogger(__name__)

__name__ = 'rpm'

class Rpm(object):
    def __init__(self, filename):
        self.filename = filename
        self._ts = rpm.TransactionSet()
        self._ts.setVSFlags(rpm._RPMVSF_NOSIGNATURES)

    @property
    def header(self):
        fd = open(self.filename)
        raw = self._ts.hdrFromFdno(fd)
        fd.close()
        return {
          'name': raw['name'],
          'version': raw['version'],
          'release': raw['release'],
          'arch': raw['arch'],
          'epoch': raw['epoch'] or 0,
          'group': raw['group'],
          'license': raw['license'],
          'vendor': raw['vendor'],
          'summary': raw['summary'],
          'description': raw['description'],
        }

    def verify(self):
        try:
            self.header
        except rpm.error, e:
            LOG.exception('failed to parse "%s" as an RPM, exception was:' % self.filename)
            raise InvalidFileType(self.filename, RpmType.type, e)

class RpmType(CASFileType):
    type = 'rpm'

    def __init__(self, filename):
        super(RpmType, self).__init__(filename)

        self._rpm = Rpm(self.filename)

    def verify(self):
        self._rpm.verify()

    def meta(self):
        keys = (
          'name', 'version', 'release',
          'arch', 'epoch', 'group', 'license', 
          'vendor', 'description', 'summary'
        )

        return dict(("rpm.%s" % k, v) for k, v in self._rpm.header.iteritems())

register_type(RpmType)
