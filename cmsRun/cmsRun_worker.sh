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

echo "START: $(date)"

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
reportFile="" # job report XMl file
sandbox="" # sandbox location
overrideConfig=1 # override the files and num events in the config
doCallgrind=0  # do profiling - runs with callgrind
doValgrind=0  # do memcheck - runs with valgrind
lumiMaskSrc=""  # filename or URL for lumi mask
lumiMaskType="filename"  # source type (filename or url)
while getopts ":s:f:o:i:a:c:r:p:m:l:" opt; do
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
        r)
            echo "Job framework report XML: $OPTARG"
            reportFile=$OPTARG
            ;;
        p)
            echo "Running callgrind profiling"
            doCallgrind=1
            overrideConfig=0
            ;;
        m)
            echo "Running valgrind memcheck"
            doValgrind=1
            overrideConfig=0
            ;;
        l)
            lumiMaskSrc=$OPTARG
            urlRegex="^(https?|www)"
            if [[ "$lumiMaskSrc" =~ $urlRegex ]]; then
                lumiMaskType="url"
            fi
            if [ ! -z $lumiMaskSrc ]; then
                echo "Running with lumiMask $lumiMaskType $lumiMaskSrc"
            fi
            ;;
    esac
done

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
tar xvzf ../sandbox.tgz

cd src # run everything inside CMSSW_BASE/src

echo "==== New env vars ===="
printenv
echo "======================"

###############################################################################
# Make a wrapper config
# This will setup the input files, output file, and number of events
# Means that the user doesn't have to do anything special to their config file
###############################################################################
wrapper="wrapper.py"

echo "import FWCore.ParameterSet.Config as cms" >> $wrapper
echo "import "${script%.py}" as myscript" >> $wrapper
echo "import FWCore.PythonUtilities.LumiList as LumiList" >> $wrapper
echo "process = myscript.process" >> $wrapper
# override the input files
if [ $overrideConfig == 1 ]; then
    echo "import ${filelist%.py} as filelist" >> $wrapper
    echo "process.source.fileNames = cms.untracked.vstring(filelist.fileNames[$ind])" >> $wrapper
    echo "process.source.secondaryFileNames = cms.untracked.vstring(filelist.secondaryFileNames[$ind])" >> $wrapper
    echo "process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(-1))" >> $wrapper
    if [ ! -z "$lumiMaskSrc" ]; then
        # should we choose type for local file if .py or .json?
        if [ "$lumiMaskType" ==  "filename" ]; then
            echo "import ${lumiMaskSrc%.py} as lumilist" >> $wrapper
            echo "process.source.lumisToProcess = lumilist.lumis[$ind]" >> $wrapper
        elif [ "$lumiMaskType" == "url" ]; then
            echo "process.source.lumisToProcess = LumiList.LumiList(${lumiMaskType}='${lumiMaskSrc}').getVLuminosityBlockRange()" >> $wrapper
        fi
    fi
fi
echo "if hasattr(process, 'TFileService'): process.TFileService.fileName = "\
"cms.string(process.TFileService.fileName.value().replace('.root', '_${ind}.root'))" >> $wrapper
echo "for omod in process.outputModules.itervalues():" >> $wrapper
echo "    omod.fileName = cms.untracked.string(omod.fileName.value().replace('.root', '_${ind}.root'))" >> $wrapper
echo ""

echo "==== Wrapper script ===="
echo ""
cat $wrapper
echo ""
echo "========================"

###############################################################################
# Log the modified script
###############################################################################

echo "==== CMS config script ===="
echo ""
cat $script
echo ""
echo "==========================="

###############################################################################
# Now finally run script!
# TODO: some automated retry system
###############################################################################
if [[ $doCallgrind == 1 ]]; then
    echo "Running with callgrind"
    valgrind --tool=callgrind cmsRun -j $reportFile $wrapper
elif [[ $doValgrind == 1 ]]; then
    echo "Running with valgrind"
    valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all cmsRun -j $reportFile $wrapper
else
    # cmsRun args MUST be in this order otherwise complains it doesn't know -j
    /usr/bin/time -v cmsRun -j $reportFile $wrapper
fi
cmsResult=$?
echo "CMS JOB OUTPUT" $cmsResult
if [ "$cmsResult" -ne 0 ]; then
    exit $cmsResult
fi
echo "In" $PWD ":"
ls -l

###############################################################################
# Copy across to output directory.
# Check if hdfs or not.
# Don't need /hdfs bit when using hadoop tools
###############################################################################
# for f in $(find . -name "*.root" -maxdepth 1)
# do
#     output=$(basename $f)
#     echo "Copying $output to $outputDir"
#     if [[ "$outputDir" == /hdfs* ]]; then
#         hadoop fs -copyFromLocal -f $output ${outputDir///hdfs}/$output
#     elif [[ "$outputDir" == /storage* ]]; then
#         cp $output $outputDir
#     fi
# done

# # Copy framework report
# hadoop fs -copyFromLocal -f $reportFile ${outputDir///hdfs}/$reportFile

# Copy callgrind output
# for f in $(find . -name "callgrind.out.*")
# do
#     output=$(basename $f)
#     echo "Copying $output to $outputDir"
#     if [[ "$outputDir" == /hdfs/* ]]; then
#         hadoop fs -copyFromLocal -f $output ${outputDir///hdfs}/$output
#     elif [[ "$outputDir" == /storage* ]]; then
#         cp $output $outputDir
#     fi
# done

echo "END: $(date)"

exit $?
