from setuptools import setup, find_packages

import subprocess
subprocess.check_call(["python", '-m', 'pip', 'install', 'SQLAlchemy==1.3.3']) # install pkg
subprocess.check_call(["python", '-m', 'pip', 'install', 'scikit-learn==0.21.3']) # install pkg

setup(name='neuro_python',
      packages=find_packages())
