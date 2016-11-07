#!/usr/bin/env python

from distutils.core import setup

setup(
    name='condor-comforter',
    version='0.2',
    description="Some helper scripts for running HTCondor jobs",
    author='Robin Aggleton',
    author_email='',
    url='https://github.com/BristolComputing/condor-comforter',
    py_modules=['cmsRunCondor.cmsRunCondor', 'haddaway.haddaway'],
    scripts=['cmsRunCondor/cmsRunCondor.py', 'haddaway/haddaway.py'],
    install_requires=['htcondenser>=0.3.0'],
    dependency_links=['git+https://github.com/raggleton/htcondenser#egg=htcondenser-0.3.0'],
    data_files=[
        ('bin', ['cmsRunCondor/cmsRun_worker.sh'])
    ]
)
