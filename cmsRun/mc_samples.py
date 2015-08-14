"""
MC samples to be used in submitSamples.py or CRAB jobs.

For each sample, we have a simple Dataset namedtuple. Fields are inputDataset,
unitsPerJob and totalUnits. We then store all samples in a dict.

Usage:

from mc_samples import samples

dataset = "TTbarSpring15AVE30BX50"
config.General.requestName = dataset
config.Data.inputDataset = samples[dataset].inputDataset
config.Data.unitsPerJob = samples[dataset].unitsPerJob

totalUnits can take the values:
-1: run over all files in the dataset
0 - 1: run over this fraction of the dataset
>1: run over this many files
"""


from collections import namedtuple
import re
import subprocess


# some helper functions

def get_number_files(dataset):
    """Get total number of files in dataset

    dataset: string. Name of dataset as it appears in DAS.
    """
    output = subprocess.check_output(['das_client.py','--query', 'summary dataset=%s' % dataset], stderr=subprocess.STDOUT)
    return int(re.search(r'nfiles +: (\d*)', output).group(1))


def check_dataset_exists(dataset):
    """Check dataset exists in DAS.

    TODO: raise an Error?

    dataset: string. Name of dataset as it appears in DAS.
    """
    output = subprocess.check_output(['das_client.py','--query', 'summary dataset=%s' % dataset], stderr=subprocess.STDOUT)
    return 'nflies' in output


# handy data structure to store some attributes for each dataset
Dataset = namedtuple("Dataset", "inputDataset unitsPerJob totalUnits")

# This dict holds ALL samples
samples = {

    "TTbarSpring15AVE30BX50": Dataset(inputDataset='/TTJets_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISpring15Digi74-AVE_30_BX_50ns_tsg_MCRUN2_74_V6-v1/GEN-SIM-RAW',
                                      unitsPerJob=6, totalUnits=-1),

    "QCDFlatSpring15BX50": Dataset(inputDataset='/QCD_Pt-15to3000_TuneCUETP8M1_Flat_13TeV_pythia8/RunIISpring15Digi74-Flat_10_50_50ns_tsg_MCRUN2_74_V6-v1/GEN-SIM-RAW',
                                   unitsPerJob=6, totalUnits=-1)
}

# Add in QCD pt binned samples here easily
ptbins = [15, 30, 50, 80, 120, 170, 300, 470, 600, 800, 1000]
for i, pt_min in enumerate(ptbins[:-1]):
    pt_max = ptbins[i+1]

    # Spring15 AVEPU20 25ns
    key = "QCD_Pt-%dto%d_Spring15_AVE20BX25" % (pt_min, pt_max)
    ver = "-v1"
    if pt_min == 80:
        ver = "-v2"
    elif pt_min == 15:
        ver = "_ext1-v1"
    samples[key] = Dataset(inputDataset="/QCD_Pt_%dto%d_TuneCUETP8M1_13TeV_pythia8/RunIISpring15Digi74-AVE_20_BX_25ns_tsg_MCRUN2_74_V7%s/GEN-SIM-RAW" % (pt_min, pt_max, ver),
                            unitsPerJob=20, totalUnits=1)


# adhoc mini samples for diff QCD sets
samples_qcd_Spring15_AVE20BX25 = dict((k, samples[k]) for k in samples.keys() if re.match(r"QCD_Pt-[\dto]*_Spring15_AVE20BX25", k))
