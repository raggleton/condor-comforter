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
doProfile=0 # run in profiling mode - use whatever files specified in config, don't override
doCallgrind=0  # do profiling - runs with callgrind
doValgrind=0  # do memcheck - runs with valgrind
while getopts ":s:f:o:i:a:c:S:p;m" opt; do
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
        p)
            echo "Running callgrind"
            doCallgrind=1
            doProfile=1
            ;;
        m)
            echo "Running valgrind memcheck"
            doValgrind=1
            doProfile=1
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
hadoop fs -copyToLocal ${sandbox#/hdfs} sandbox.tgz  # assumes this is on HDFS!
tar xvzf sandbox.tgz

# cp $script src/

# if [ $doProfile == 0 ]; then
    # only need filelist if not profiling - in profile mode use file in config
    # cp $filelist src/
# fi

cd src # run everything inside CMSSW_BASE/src

# Setup new libs to point to local ones
# export LD_LIBRARY_PATH=$CMSSW_BASE/biglib/${SCRAM_ARCH}:$CMSSW_BASE/lib/${SCRAM_ARCH}:$CMSSW_BASE/external/${SCRAM_ARCH}/lib:$LD_LIBRARY_PATH
# export PYTHONPATH=$CMSSW_BASE/python:$CMSSW_BASE/lib/${SCRAM_ARCH}:$PYTHONPATH
# export ROOT_INCLUDE_PATH=$CMSSW_BASE/src:${ROOT_INCLUDE_PATH}
# export SRT_ROOT_INCLUDE_PATH_SCRAMRTDEL=$CMSSW_BASE/src:${SRT_ROOT_INCLUDE_PATH_SCRAMRTDEL}
# export SRT_RIVET_ANALYSIS_PATH_SCRAMRTDEL=$CMSSW_BASE/src:${SRT_RIVET_ANALYSIS_PATH_SCRAMRTDEL}
# export PATH=$CMSSW_BASE/bin/slc6_amd64_gcc493:$CMSSW_BASE/external/slc6_amd64_gcc493/bin:${PATH}
# export SRT_CMSSW_SEARCH_PATH_SCRAMRTDEL=$CMSSW_BASE/src:$CMSSW_BASE/external/slc6_amd64_gcc493/data:${SRT_CMSSW_SEARCH_PATH_SCRAMRTDEL}
# export SRT_PATH_SCRAMRT=$CMSSW_BASE/bin/slc6_amd64_gcc493:$CMSSW_BASE/external/slc6_amd64_gcc493/bin:${SRT_PATH_SCRAMRT}
# export RIVET_ANALYSIS_PATH=$CMSSW_BASE/lib/slc6_amd64_gcc493:${RIVET_ANALYSIS_PATH}
# export SRT_LOCALRT_SCRAMRTDEL=$CMSSW_BASE
# export LOCALRT=$CMSSW_BASE
# export SRT_LOCALRT_SCRAMRTDEL=$CMSSW_BASE
# export SRT_CMSSW_BASE_SCRAMRTDEL=$CMSSW_BASE
# export SRT_PYTHONPATH_SCRAMRT=$CMSSW_BASE/python:$CMSSW_BASE/lib/$SCRAM_ARCH:$SRT_PYTHONPATH_SCRAMRT
# export CMSSW_SEARCH_PATH=$CMSSW_BASE/src:$CMSSW_BASE/external/$SCRAM_ARCH/data
# export SRT_LD_LIBRARY_PATH_SCRAMRT=$CMSSW_BASE/biglib/$SCRAM_ARCH:$CMSSW_BASE/lib/$SCRAM_ARCH:$CMSSW_BASE/external/slc6_amd64_gcc493/lib:$SRT_LD_LIBRARY_PATH_SCRAMRT

echo "========================="
echo "New env vars"
printenv
echo "========================="

###############################################################################
# Make a wrapper config
# This will setup the input files, output file, and number of events
# Means that the user doesn't have to do anything special to their config file
###############################################################################
wrapper="wrapper.py"

echo "import FWCore.ParameterSet.Config as cms" >> $wrapper
echo "import "${script%.py}" as myscript" >> $wrapper
echo "process = myscript.process" >> $wrapper
# if we're profiling then don't override the input files
if [ $doProfile == 0 ]; then
    echo "import ${filelist%.py} as filelist" >> $wrapper
    echo "process.source.fileNames = cms.untracked.vstring(filelist.fileNames[$ind])" >> $wrapper
    echo "process.source.secondaryFileNames = cms.untracked.vstring(filelist.secondaryFileNames[$ind])" >> $wrapper
    echo "process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(-1))" >> $wrapper
fi
echo "if hasattr(process, 'TFileService'): process.TFileService.fileName = "\
"cms.string(process.TFileService.fileName.value().replace('.root', '_${ind}.root'))" >> $wrapper
echo "for omod in process.outputModules.itervalues():" >> $wrapper
echo "    omod.fileName = cms.untracked.string(process.output.fileName.value().replace('.root', '_${ind}.root'))" >> $wrapper
echo ""

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
if [[ $doCallgrind == 1 ]]; then
    echo "Running with callgrind"
    valgrind --tool=callgrind cmsRun $wrapper
elif [[ $doValgrind == 1 ]]; then
    echo "Running with valgrind"
    valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all cmsRun $wrapper
else
    /usr/bin/time -v cmsRun $wrapper
fi
cmsResult=$?
echo "CMS JOB OUTPUT" $cmsResult
if [ "$cmsResult" -ne 0 ]; then
    exit $cmsResult
fi
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

# Copy callgrind output
for f in $(find . -name "callgrind.out.*")
do
    output=$(basename $f)
    echo "Copying $output to $outputDir"
    if [[ "$outputDir" == /hdfs/* ]]; then
        hadoop fs -copyFromLocal -f $output ${outputDir///hdfs}/$output
    elif [[ "$outputDir" == /storage* ]]; then
        cp $output $outputDir
    fi
done

exit $?