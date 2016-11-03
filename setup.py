#!/usr/bin/env python

from distutils.core import setup
from glob import glob

setup(
    name='condor-comforter',
    version='0.1',
    description="Some helper scripts for running HTCondor jobs",
    author='Robin Aggleton',
    author_email='',
    url='https://github.com/BristolComputing/condor-comforter',
    py_modules=['cmsRunCondor', 'haddaway'],
    scripts=['cmsRunCondor/cmsRunCondor.py', 'haddaway/haddaway.py'],
    include_package_data=True,
)