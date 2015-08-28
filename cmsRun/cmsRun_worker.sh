#!/bin/bash -e

#################################################
# Script to run cmsRun on condor worker node
#################################################

# Store args
# TODO: replace with getopts
script=$1
filelist=$2
outputDir=$3
ind=$4 # ind is the job number

worker="$PWD" # top level of worker node
export HOME=$worker # need this if getenv = false

######################
# First setup CMSSW
######################
# cmssw_version=CMSSW_7_4_6_patch6
cmssw_version=CMSSW_7_4_2
echo "Setting up ${cmssw_version} ..."
echo "... sourcing CMS default environment from CVMFS"
source /cvmfs/cms.cern.ch/cmsset_default.sh
echo "... creating CMSSW project area"
scramv1 project CMSSW ${cmssw_version}
cd ${cmssw_version}/src
eval `scramv1 runtime -sh`
echo "${cmssw_version} has been set up"

# Setup your git config, otherwise cannot check stuff out of git
git config --global user.github "raggleton"
git config --global user.name "Robin Aggleton"
git config --global user.email "robin.aggleton@cern.ch"
git config -l
export CMSSW_GIT_REFERENCE="${worker}/.cmsgit-cache"

######################
# Setup packages
# Do whatever you need to make sure all necessary packages are checked out, etc
# git cms-addpkg doesn't work for now :(
######################
# git cms-addpkg L1Trigger/L1TCalorimeter
git clone https://github.com/cms-l1-dpg/L1Ntuples.git L1TriggerDPG/L1Ntuples
git clone https://github.com/raggleton/L1JetEnergyCorrections.git L1Trigger/L1JetEnergyCorrections
# Get Golden JSON
# wget https://cms-service-dqm.web.cern.ch/cms-service-dqm/CAF/certification/Collisions15/13TeV/Cert_246908-251883_13TeV_PromptReco_Collisions15_JSON_v2.txt

######################
# Make a wrapper config
# This will setup the input files, output file, and number of events
# Means that the user doesn't have to do anything special to their config file
######################
pyfile="wrapper.py"

echo "import FWCore.ParameterSet.Config as cms" >> $pyfile
echo "import "${script%.py}" as myscript" >> $pyfile
echo "import ${filelist%.py} as filelist" >> $pyfile

# Write more overriding things to wrapper
echo "process = myscript.process" >> $pyfile
echo "process.source.fileNames = cms.untracked.vstring(filelist.fileNames[$ind])" >> $pyfile
echo "process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(-1))" >> $pyfile
echo "process.TFileService.fileName = cms.string(process.TFileService.fileName.value().replace('.root', '_${ind}.root'))" >> $pyfile
echo "process.output.fileName = cms.string(process.output.fileName.value().replace('.root', '_${ind}.root'))" >> $pyfile

echo ""
echo "Wrapper script:"
echo ""
cat $pyfile
echo ""


######################
# Move the cms config script * list of files from top level
# to CMSSW area and build everything
######################
mv $worker/$script .
mv $worker/fileList* .
scram b
ls

######################
# Now finally run script!
# TODO: some automated retry system
######################
cmsRun $pyfile
echo "CMS JOB OUTPUT" $?

ls -l

######################
# Copy across to hdfs. Don't need /hdfs bit when using hadoop tools
# TODO: add in check as to whether the user wants it on hdfs or not...
######################
for f in $(find . -name "*.root" -maxdepth 1)
do
    output=$(basename $f)
    hadoop fs -copyFromLocal -f $output ${outputDir///hdfs}/$output
done
