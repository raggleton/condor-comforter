"""
This creates jobs on the HTCondor system for several datasets.
Basically a batch wrapper around cmsRunCondor

User must select the correct config file, outputDir, and dataset(s).
"""

import sys
import os
from cmsRunCondor import cmsRunCondor
import mc_samples as samples
from time import strftime, sleep

# SPECIFY YOUR CMSSW CONFIG FILE HERE
config = "my_config.py"

# SPECIFIY YOUR OUTPUT DIR HERE
outputDir = "/hdfs/user/ab12345/my_output_dir"

# SPECIFY DATASET NAMES HERE.
# Use the shorthand key names, see mc_samples.py for examples
datasets = ['QCDFlatSpring15BX50', 'TTbarSpring15AVE30BX50']

if __name__ == "__main__":

    # Run through datasets once to check all fine
    for dset in datasets:
        if not dset in samples.samples.keys():
            raise KeyError("Wrong dataset key name:", dset)
        if not samples.check_dataset_exists(samples.samples[dset].inputDataset):
            raise RuntimeError("Dataset cannot be found in DAS:", samples.samples[dset].inputDataset)

    for dset in datasets:

        print "Dataset key:", dset
        dset_opts = samples.samples[dset]
 
        # to calculate the total number of files to run over
        if dset_opts.totalUnits > 1:
            totalUnits = dset_opts.totalUnits
        elif 0 < dset_opts.totalUnits <= 1:
            totalUnits = int(samples.get_number_files(dset_opts.inputDataset) * dset_opts.totalUnits)
        else:
            totalUnits = int(samples.get_number_files(dset_opts.inputDataset))  # make sure we reset
        print "Total units:", totalUnits

        # create a condor submission file for each dataset with a unique name
        scriptName = '%s_%s_%s.condor' % (os.path.basename(config).replace(".py", ""), dset, strftime("%H%M%S"))
        print "Condor submit script written to", scriptName
        cmsRunCondor(['--config', config,
                      '--outputDir', outputDir+"/"+dset,
                      '--dataset', dset_opts.inputDataset,
                      '--filesPerJob', str(dset_opts.unitsPerJob),
                      '--totalFiles', str(totalUnits),
                      '--outputScript', scriptName,
                      "--verbose"])

        print "Sleeping for 60s to avoid hammering the queue..."
        sleep(60)
