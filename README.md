# condor-comforter
Helper routines for using condor at Bristol, particularly aimed at CMS users.

These will probably be a little hacky, but should offer some inspiration for other users.

Please report any issues, and add any helpful scripts for other users!

Most things require the [`htcondenser`](https://github.com/raggleton/htcondenser) Python package.

Robin Aggleton

##cmsRunCondor

This holds example code for running CMSSW jobs on condor. Like CRAB3, but on condor.
Only supports output to `/hdfs`.

Start from: [cmsRunCondor.py](cmsRun/cmsRunCondor.py) for running over one dataset with a config file.

Brief example (_requires CMSSW 80X release_):

```
./cmsRunCondor.py pset_tutorial_analysis.py \
--dataset /QCD_HT500to700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM \
--totalUnits 10 --unitsPerJob 5 --splitByFiles \
--outputDir /hdfs/user/$LOGNAME/cmsRunCondor \
--dag
```

You can then monitor job progress with `DAGstatus` (part of `htcondenser` package)

See all options by doing `cmsRunCondor.py --help`.

Features currently supported:

- Run using a LumiMask, and/or specified run numbers

- Run over all or part of a dataset

- Split into jobs by # files or # lumisections

- Run with a secondary dataset to do "2-file solution" (e.g. mixing RECO with RAW)

- Run with a specified list of files

- Specify additional input files needed for running (e.g. calibration files)

- Easy monitoring of jobs using `DAGstatus`

- Profile cmsRun jobs with valgrind or callgrind

##haddaway

Simple script to put `hadd` jobs onto HTCondor, splitting them up into smaller parallel groups to speed things up (possibly).

It creates a series of intermediate hadd jobs, then does a final hadd over all of the intermediate files.

You can specify the intermediate group size, and also specify the standard hadd options (e.g. for compression).

##examples

There are 2 simple condor job examples included, one for a single job, another for a DAG.
Note that these are only for reference/a basic understanding of how htcondor operates.
For your own work, I would recommended using the [`htcondenser`](https://github.com/raggleton/htcondenser) package to avoid ever having to write these, and solving other hassles.

###simpleJob

A very simple condor job file to run a script on the worker node.

Start from: [script.job](examples/simpleJob/script.job)

###exampleDAG

This holds a simple example of a DAG (directed acyclic graph), i.e. a nice way to schedule various jobs, each of which can depend on other jobs.
It shows how to setup and 'connect' jobs with parent-child relationships, and how to pass variables to the condor job file.

Start from: [diamond.dag](examples/exampleDAG/diamond.dag)

