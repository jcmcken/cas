from __future__ import absolute_import

from cas.config import DEBUG
import logging

LOG = logging.getLogger()

if DEBUG:
    logging.basicConfig()
    LOG.setLevel(logging.DEBUG)
