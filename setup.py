from setuptools import setup
import cas

setup(
  name='cas',
  version=cas.__version__,
  author=cas.__author__,
  py_modules=['cas'],
  install_requires=open('requirements.txt').readlines(),
  entry_points="""
    [console_scripts]
    cas=cas.cli:main
  """,
)
