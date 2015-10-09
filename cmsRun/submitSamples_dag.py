"""
Equivalent of the crab3 scripts, this creates jobs on the HTCondor system
running over various datasets. It submits them as a DAG, which means you can
a) easily monitor them with DAGstatus.py, and b) easily resubmit failed ones.

User must select the correct config file, outputDir, and dataset(s).

For the dataset(s), add in an entry to the samples dict. The key is a shorthand
identifier, used for script names and output directory. The value is a Dataset
namedtuple, which just needs the actual dataset names, the units (i.e. files)
per job, and the total number of units to run over. The last number can be:
-1: run over all files in the dataset
0 - 1: run over this fraction of the dataset
>= 1: run over this many files.
"""

import sys
import os
from cmsRunCondor import cmsRunCondor
from collections import namedtuple
from time import strftime, sleep
from subprocess import call

# handy data structure to store some attributes for each dataset
Dataset = namedtuple("Dataset", "inputDataset unitsPerJob totalUnits")

# EDIT THESE
config = "my_config_cfg.py"

outputDir = "/hdfs/user/REPLACEME/testing"

samples = {
    "QCDFlatSpring15BX25PU10to30HCALFix": Dataset(inputDataset='/QCD_Pt-15to3000_TuneCUETP8M1_Flat_13TeV_pythia8/RunIISpring15DR74-NhcalZSHFscaleFlat10to30Asympt25ns_MCRUN2_74_V9-v1/GEN-SIM-RAW',
                                                  unitsPerJob=20, totalUnits=-1),

    "QCDFlatSpring15BX25FlatNoPUHCALFix": Dataset(inputDataset='/QCD_Pt-15to3000_TuneCUETP8M1_Flat_13TeV_pythia8/RunIISpring15DR74-NhcalZSHFscaleNoPUAsympt25ns_MCRUN2_74_V9-v1/GEN-SIM-RAW',
                                                  unitsPerJob=5, totalUnits=-1)
}


def check_dataset_exists(dataset):
    """Check dataset exists in DAS.

    TODO: raise an Error?

    dataset: str.
        Name of dataset as it appears in DAS.
    """
    cmd = ['das_client.py', '--query', 'summary dataset=%s' % dataset]
    output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    return 'nfiles' in output


if __name__ == "__main__":

    status_names = []

    for dset, dset_opts in samples.iteritems():

        if not check_dataset_exists(dset_opts.inputDataset):
            raise RuntimeError("Dataset cannot be found in DAS: %s" % dset_opts.inputDataset)

        print "*"*80
        print "Dataset key:", dset

        # Make the condor submit script for this dataset
        scriptName = '%s_%s_%s.condor' % (os.path.basename(config).replace(".py", ""),
                                          dset,
                                          strftime("%H%M%S"))
        print "Script Name:", scriptName
        job_dict = cmsRunCondor(['--config', config,
                                 '--outputDir', outputDir+"/"+dset,
                                 '--dataset', dset_opts.inputDataset,
                                 '--filesPerJob', str(dset_opts.unitsPerJob),
                                 '--totalFiles', str(dset_opts.totalUnits),
                                 '--outputScript', scriptName,
                                 '--dry',
                                 '--dag',  # important!
                                 # '--verbose'
                                 ])

        # Setup DAG file for this dataset
        dag_name = "jobs_%s_%s.dag" % (dset, strftime("%d_%b_%y_%H%M%S"))
        with open(dag_name, "w") as dag_file:
            dag_file.write("# DAG for dataset %s\n" % dset_opts.inputDataset)
            for job_ind in xrange(job_dict['totalNumJobs']):
                jobName = "%s_%d" % (dset, job_ind)
                dag_file.write('JOB %s %s\n' % (jobName, scriptName))
                dag_file.write('VARS %s index="%d"\n' % (jobName, job_ind))
            status_file = dag_name.replace(".dag", ".status")
            status_names.append(status_file)
            dag_file.write("NODE_STATUS_FILE %s 30\n" % status_file)

        # Submit DAG
        call(['condor_submit_dag', dag_name])

        print "Check DAG status:"
        print "./DAGstatus.py", dag_name

        if dset != samples.keys()[-1]:
            print "Sleeping for 60s to avoid hammering the queue..."
            sleep(60)

    print "To view the status of all DAGs:"
    print "./DAGstatus.py", " ".join(status_names)