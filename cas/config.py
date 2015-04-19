import os

DEBUG = os.environ.get('CAS_DEBUG', 'false') == 'true'

CAS_ROOT = os.environ.get('CAS_ROOT', None)

