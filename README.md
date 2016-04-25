# condor-comforter
Helper routines for using condor at Bristol, particularly aimed at CMS users.

These will probably be a little hacky, but should offer some inspiration for other users.

Please report any issues, and add any helpful scripts for other users!

I take no responsibility for your results using these - with great power comes great potential for DDOS'ing DAS.

Robin Aggleton

##cmsRunCondor

This holds example code for running CMSSW jobs on condor. Like CRAB3, but on condor.
Currently supports output to /hdfs only at the moment.

Start from: [cmsRunCondor.py](cmsRun/cmsRunCondor.py) for running over one dataset with a config file.

Brief example:

```
./cmsRunCondor.py --config pset_tutorial_analysis.py --outputDir /hdfs/user/$LOGNAME/test --dataset /ttHTobb_M125_13TeV_powheg_pythia8/RunIIFall15DR76-25nsPUfixed30NzshcalRaw_76X_mcRun2_asymptotic_v12-v1/AODSIM --totalUnits 10 --untisPerJob 5 --splitByFiles --dag /storage/$LOGNAME/test/cms.dag --log /storage/$LOGNAME/test
```

You can then monitor job progress with [`DAGstatus.py`](cmsRun/DAGstatus.py).

See all options by doing `cmsRunCondor.py --help`.

Features currently supported:

- Run using LumiMask, and/or specified run numbers

- Run over all or part of a dataset

- Split into jobs by # files or # lumisections

- Run with a secondary dataset to do "2-file solution" (e.g. mixing RECO with RAW)

- Run with a specified set of files

- Specify additional input files needed for running (e.g. calibration files)

- Easy monitoring of jobs using [`DAGstatus.py`](cmsRun/DAGstatus.py)

- Profile cmsRun jobs with valgrind or callgrind

##exampleDAG

This holds a simple example of a DAG (directed acyclic graph), i.e. a nice way to schedule various jobs, each of which can depend on other jobs.
It shows how to setup and 'connect' jobs with parent-child relationships, and how to pass variables to the condor job file.

Start from: [diamond.dag](exampleDAG/diamond.dag)

Also includes a neat little monitoring script for DAG jobs, [DAGstatus.py](exampleDAG/DAGstatus.py)

##simpleJob

A very simple condor job file to run a script on the worker node.

Start from: [script.job](simpleJob/script.job)
