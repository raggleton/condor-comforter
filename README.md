# condor-comforter
Helper routines for using condor at Bristol, particularly aimed at CMS users.
Most things require the [`htcondenser`](https://github.com/raggleton/htcondenser) Python package.

Please report any issues, and add any helpful scripts for other users!

Robin Aggleton

## Installation

Easiest way is via `pip`. If you don't have `pip`, you can use the one in `/software/miniconda/bin/pip`.

```
pip install -U --process-dependency-links --user git+https://github.com/BristolComputing/condor-comforter.git
```
The same command can also be used to update the package.

Note that if you see:

```
  DEPRECATION: Dependency Links processing has been deprecated and will be removed in a future release.
```

it can be safely ignored.

After this, `cmsRunCondor.py` and `haddaway.py` should be available like any other command.
One can also import them as python modules for further extension.

For possible arguments, do `cmsRunCondor.py -h` or `haddaway.py -h`

##cmsRunCondor

This runs `cmsRun` jobs on HTCondor (like CRAB3), outputting to `/hdfs`.

Brief example (_requires CMSSW 80X release_):

```
cmsRunCondor.py pset_tutorial_analysis.py \
--dataset /QCD_HT500to700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM \
--totalUnits 10 --unitsPerJob 5 --splitByFiles \
--outputDir /hdfs/user/$LOGNAME/cmsRunCondor \
--dag
```

You can then monitor job progress with `DAGstatus` (part of [`htcondenser`](https://github.com/raggleton/htcondenser) package)

Features currently supported:

- Run using a LumiMask, and/or specified run numbers

- Run over all or part of a dataset

- Split into jobs by # files or # lumisections

- Run with a secondary dataset to do "2-file solution" (e.g. mixing RECO with RAW)

- Run with a specified list of files instead of a dataset

- Specify additional input files needed for running (e.g. calibration files)

- Easy monitoring of jobs using `DAGstatus`

- Profile cmsRun jobs with valgrind or callgrind

- Just run with whatever is in your config (`--asIs`) e.g. to stop hogging resources on `soolin`

- hadd the output from jobs (need to specify which module's output you want to hadd)

##haddaway

Simple script to perform `hadd` jobs on HTCondor, splitting them up into smaller parallel groups to speed things up (possibly, YMMV).
It creates a series of intermediate hadd jobs, then does a final hadd over all of the intermediate files.
You can specify the intermediate group size, and also specify the standard hadd options (e.g. for compression).

Example usage:

```
# passing the input filenames
haddaway.py --output final.root --size 3 --input file1.root file2.root file3.root --haddArgs="-f7"

# passing a text file (filelist.txt) with locations of files
haddaway.py --output final.root --size 3 --inputList filelist.txt --haddArgs="-f7"
```

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

