from setuptools import setup, find_packages

import pip

pip.main(['install', 'SQLAlchemy==1.3.3'])

setup(name='neuro_python',
      packages=find_packages())
