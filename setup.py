from setuptools import setup, find_packages

import subprocess
subprocess.check_call(["python", '-m', 'pip', 'install', 'SQLAlchemy==2.0.27']) # install pkg
subprocess.check_call(["python", '-m', 'pip', 'install', 'scikit-learn==1.4.0']) # install pkg
subprocess.check_call(["python", '-m', 'pip', 'install', 'retrying==1.3.4']) # install pkg

setup(name='neuro_python',
      packages=find_packages())
