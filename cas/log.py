from __future__ import absolute_import

from cas.config import DEBUG
import logging

LOG = logging.getLogger()

def enable_debug():
    logging.basicConfig()
    LOG.setLevel(logging.DEBUG)

if DEBUG:
    enable_debug()
