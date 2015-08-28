"""
Equivalent of the crab3 scripts, this creates jobs on the HTCondor system running
over various datasets. It submits them as a DAG, which means you can a) easily
monitor them with DAGstatus.py, and b) easily resubmit failed ones.

User must select the correct config file, outputDir, and dataset(s).

The datasets must be the name of their keys in the samples dict (in mc_scamples or data_samples)
"""

import sys
import os
from cmsRunCondor import cmsRunCondor
import mc_samples as samples
from time import strftime, sleep
from subprocess import call


config = "my_config_cfg.py"
outputDir = "/hdfs/user/REPLACEME/testing"

datasets = ['QCD_Pt-80to120_Spring15_AVE20BX25', 'QCD_Pt-120to170_Spring15_AVE20BX25']

if __name__ == "__main__":
    # Run through datasets once to check all fine
    for dset in datasets:
        if not dset in samples.samples.keys():
            raise KeyError("Wrong dataset key name:", dset)
        if not samples.check_dataset(samples.samples[dset].inputDataset):
            raise RuntimeError("Dataset cannot be found in DAS: %s" % samples.samples[dset].inputDataset)

    status_names = []

    for dset in datasets:

        dset_opts = samples.samples[dset]
        print "*"*80
        print "Dataset key:", dset

        # Make the condor submit script for this dataset
        scriptName = '%s_%s_%s.condor' % (os.path.basename(config).replace(".py", ""), dset, strftime("%H%M%S"))
        print "Script Name:", scriptName
        job_dict = cmsRunCondor(['--config', config,
                                 '--outputDir', outputDir+"/"+dset,
                                 '--dataset', dset_opts.inputDataset,
                                 '--filesPerJob', str(dset_opts.unitsPerJob),
                                 '--totalFiles', str(dset_opts.totalUnits),
                                 '--outputScript', scriptName,
                                 '--dry',
                                 '--dag', # important!
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

        if dset != datasets[-1]:
            print "Sleeping for 60s to avoid hammering the queue..."
            sleep(60)

    print "To view the status of all DAGs:"
    print "./DAGstatus.py", " ".join(status_names)