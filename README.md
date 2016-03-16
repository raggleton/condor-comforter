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
./cmsRunCondor.py --config pset_tutorial_analysis.py --outputDir /hdfs/user/$LOGNAME/test --dataset /QCD_Pt-15to7000_TuneCUETP8M1_Flat_13TeV_pythia8/RunIISpring15DR74-AsymptFlat0to50bx25Reco_MCRUN2_74_V9-v3/GEN-SIM-RECO --totalFiles 10 --filesPerJob 5 --dag /storage/$LOGNAME/test/cms.dag --log /storage/$LOGNAME/test
```

You can then monitor job progress with [`DAGstatus.py`](cmsRun/DAGstatus.py).

##exampleDAG

This holds a simple example of a DAG (directed acyclic graph), i.e. a nice way to schedule various jobs, each of which can depend on other jobs.
It shows how to setup and 'connect' jobs with parent-child relationships, and how to pass variables to the condor job file.

Start from: [diamond.dag](exampleDAG/diamond.dag)

Also includes a neat little monitoring script for DAG jobs, [DAGstatus.py](exampleDAG/DAGstatus.py)

##simpleJob

A very simple condor job file to run a script on the worker node.

Start from: [script.job](simpleJob/script.job)
