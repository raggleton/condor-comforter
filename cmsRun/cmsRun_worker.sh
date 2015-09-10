#!/bin/bash -e

###############################################################################
# Script to run cmsRun on condor worker node
#
# Briefly, it:
# - setups up environment & CMSSW
# - extracts all the user's libs, header files, etc from a sandbox zip
# - makes a wrapper script for the CMSSW config file,
# so that it uses the correct input/output files
# - runs cmsRun
# - moves the output to /hdfs
###############################################################################

worker=$PWD # top level of worker node
export HOME=$worker # need this if getenv = false

###############################################################################
# Store args
###############################################################################
script="config.py" # cmssw config script filename
filelist="filelist.py" # py file with dict of input file
outputDir="" # output directory for result
ind="" # ind is the job number
arch="" # architecture
cmssw_version="" # cmssw version
sandbox="" # sandbox location

while getopts ":s:f:o:i:a:c:S:" opt; do
    case $opt in
        \?)
            echo "Invalid option $OPTARG" >&2
            exit 1
            ;;
        :)
            echo "Option -$OPTARG requires an argument." >&2
            exit 1
            ;;
        s)
            echo "Config filename: $OPTARG"
            script=$OPTARG
            ;;
        f)
            echo "filelist: $OPTARG"
            filelist=$OPTARG
            ;;
        o)
            echo "outputDir: $OPTARG"
            outputDir=$OPTARG
            ;;
        i)
            echo "Index: $OPTARG"
            ind=$OPTARG
            ;;
        a)
            echo "ARCH: $OPTARG"
            arch=$OPTARG
            ;;
        c)
            echo "CMSSW: $OPTARG"
            cmssw_version=$OPTARG
            ;;
        S)
            echo "Sandbox location: $OPTARG"
            sandbox=$OPTARG
            ;;
    esac
done

###############################################################################
# TODO: Make a private area for the job incase there are multiple on the same worker?
###############################################################################

###############################################################################
# Setup CMSSW
###############################################################################
export SCRAM_ARCH=${arch}
echo "Setting up ${cmssw_version} ..."
echo "... sourcing CMS default environment from CVMFS"
source /cvmfs/cms.cern.ch/cmsset_default.sh
echo "... creating CMSSW project area"
scramv1 project CMSSW ${cmssw_version}
cd ${cmssw_version}/src
eval `scramv1 runtime -sh`  # cmsenv
echo "${cmssw_version} has been set up"

###############################################################################
# Extract sandbox of user's libs, headers, and python files
###############################################################################
cd ..
cp $sandbox sandbox.tgz
tar xvzf sandbox.tgz

###############################################################################
# Make a wrapper config
# This will setup the input files, output file, and number of events
# Means that the user doesn't have to do anything special to their config file
###############################################################################
wrapper="wrapper.py"

echo "import FWCore.ParameterSet.Config as cms" >> $wrapper
echo "import "${script%.py}" as myscript" >> $wrapper
echo "import ${filelist%.py} as filelist" >> $wrapper
echo "process = myscript.process" >> $wrapper
echo "process.source.fileNames = cms.untracked.vstring(filelist.fileNames[$ind])" >> $wrapper
echo "process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(-1))" >> $wrapper
echo "if hasattr(process, 'TFileService'): process.TFileService.fileName = "\
"cms.string(process.TFileService.fileName.value().replace('.root', '_${ind}.root'))" >> $wrapper
echo "process.output.fileName = cms.untracked.string("\
"process.output.fileName.value().replace('.root', '_${ind}.root'))" >> $wrapper

echo "==== Wrapper script ====="
echo ""
cat $wrapper
echo ""
echo "========================="

###############################################################################
# Log the modified script
###############################################################################

echo "=== CMS config script ==="
echo ""
cat $script
echo ""
echo "========================="

###############################################################################
# Now finally run script!
# TODO: some automated retry system
###############################################################################
cmsRun $wrapper
echo "CMS JOB OUTPUT" $?

ls -l

###############################################################################
# Copy across to output directory.
# Check if hdfs or not.
# Don't need /hdfs bit when using hadoop tools
###############################################################################
for f in $(find . -name "*.root" -maxdepth 1)
do
    output=$(basename $f)
    echo "Copying $output to $outputDir"
    if [[ "$outputDir" == /hdfs* ]]; then
        hadoop fs -copyFromLocal -f $output ${outputDir///hdfs}/$output
    elif [[ "$outputDir" == /storage* ]]; then
        cp $output $outputDir
    fi
done
