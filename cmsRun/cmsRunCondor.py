#!/usr/bin/env python

"""
Script to allow you to run cmsRun jobs on HTCondor.

Bit rudimentary. See options with:

cmsRunCondor.py --help

Note that it automatically sets the correct process.source, process.maxEvents,
and output filenames in the CMSSW config.

Robin Aggleton 2015, in a rush
"""


import sys
import argparse
import os
from time import strftime
import subprocess
import re
import math
import logging
from itertools import izip_longest


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)


def cmsRunCondor(in_args):
    """Main routine for running cmsRun on condor"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", help="CMSSW config file you want to run.")
    parser.add_argument("--outputDir", help="Where you want your output to be stored. /hdfs is recommended")
    parser.add_argument("--dataset", help="Name of dataset you want to run over")
    parser.add_argument("--filesPerJob", help="Number of files to run over, per job.", type=int, default=5)
    parser.add_argument("--totalFiles", help="Total number of files to run over. Default is ALL (-1)", type=int, default=-1)
    parser.add_argument("--outputScript", help="Optional: name of condor submission script. Default is <config>_<time>.condor")
    parser.add_argument("--verbose", help="Extra printout to clog up your screen.", action='store_true')
    parser.add_argument("--dry", help="Dry-run: only make condor submission script, don't submit to queue.", action='store_true')
    args = parser.parse_args(args=in_args)

    if args.verbose:
        log.setLevel(logging.DEBUG)

    log.debug(args)

    # do some checking
    if not os.path.isfile(args.config):
        log.error("Cannot find config file %s" % args.config)
        raise IOError

    if not os.path.exists(args.outputDir):
        log.error("Output directory doesn't exists, trying to make it: %s" % args.outputDir)
        try:
            os.makedirs(args.outputDir)
        except OSError as e:
            log.error("Cannot make output dir %s" % args.outputDir)
            raise

    if args.filesPerJob > args.totalFiles and args.totalFiles != -1:
        log.error("You can't have --filesPerJob > --totalFiles!")
        raise RuntimeError

    output_summary = subprocess.check_output(['das_client.py','--query',
                                              'summary dataset=%s' % args.dataset],
                                              stderr=subprocess.STDOUT)
    log.debug(output_summary)

    if 'nfiles' not in output_summary:
        log.error("Dataset doesn't exist in DAS - check your spelling")
        raise RuntimeError

    # make an output directory for log files
    if not os.path.exists(args.outputDir):
        os.mkdir('jobs')

    # get the total number of files for this dataset using das_client
    if args.totalFiles == -1:
        total_num_files = int(re.search(r'nfiles +: (\d*)', output_summary).group(1))
        args.totalFiles = total_num_files

    # Figure out correct number of jobs
    total_num_jobs = int(math.ceil(args.totalFiles / float(args.filesPerJob)))

    # Make a list of files for each job to avoid doing it on worker node side:
    output_files = subprocess.check_output(['das_client.py','--query',
                                            'file dataset=%s' % args.dataset,
                                            '--limit=%d' % args.totalFiles],
                                            stderr=subprocess.STDOUT)

    list_of_files = ['"%s"' % line for line in output_files.splitlines() if line.lower().startswith("/store")]

    def grouper(iterable, n, fillvalue=None):
        args = [iter(iterable)] * n
        return izip_longest(fillvalue=fillvalue, *args)

    fileListName = "fileList%s.py" % args.dataset.replace("/", "_").replace("-", "_")
    with open(fileListName, "w") as file_list:
        file_list.write("fileNames = {")
        for n, chunk in enumerate(grouper(list_of_files, args.filesPerJob)):
            file_list.write("%d: [%s]," % (n, ', '.join(filter(None, chunk))))
        file_list.write("}")
    log.info("List of files for each jobs written to %s" % fileListName)


    # Make a condor submission script
    log.debug("Will be submitting %d jobs, running over %d files" % (total_num_jobs, args.totalFiles))

    with open('cmsRun_template.condor') as template:
        job_template = template.read()

    config_filename = os.path.basename(args.config)
    job_filename = '%s_%s.condor' % (config_filename.replace(".py", ""), strftime("%H%M%S"))

    if not args.outputScript:
        args.outputScript = job_filename
    job_description = job_template.replace("SEDINITIAL", "")  # for not, keept initialdir local, otherwise tonnes of files on hdfs
    job_description = job_description.replace("SEDNAME", args.outputScript.replace(".condor", ""))
    args_str = "%s %s %s $(process)" % (config_filename, fileListName, args.outputDir)
    job_description = job_description.replace("SEDARGS", args_str)
    job_description = job_description.replace("SEDEXE", 'cmsRun_worker.sh')
    job_description = job_description.replace("SEDNJOBS", str(total_num_jobs))
    job_description = job_description.replace("SEDINPUTFILES", "%s, %s" % (os.path.abspath(args.config), fileListName))

    with open(args.outputScript, 'w') as submit_script:
        submit_script.write(job_description)
    log.info('New condor submission script written to %s' % args.outputScript)

    # submit to queue
    if not args.dry:
        subprocess.call(['condor_submit', args.outputScript])


if __name__ == "__main__":
    cmsRunCondor(sys.argv[1:])