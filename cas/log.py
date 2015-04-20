from __future__ import absolute_import

from cas.config import DEBUG
import logging

LOG = logging.getLogger()
logging.basicConfig()
LOG.setLevel(logging.CRITICAL)

def enable_debug():
    LOG.setLevel(logging.DEBUG)

if DEBUG:
    enable_debug()
