#!/usr/bin/env python

"""
Script to allow you to run cmsRun jobs on HTCondor.

Bit rudimentary. See options with:

cmsRunCondor.py --help

Note that it automatically sets the correct input files, providing you give it a
dataset and specify filesPerJob, and totalFiles.

Robin Aggleton 2015, in a rush
"""


import os
import re
import sys
import json
import math
import logging
import tarfile
import argparse
import subprocess
from time import strftime
from itertools import izip_longest


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)


def cmsRunCondor(in_args=sys.argv[1:]):
    """Creates a condor job description file with the correct arguments,
    and optionally submit it.

    Returns a dict of information about the job.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config",
                        help="CMSSW config file you want to run.")
    parser.add_argument("--outputDir",
                        help="Where you want your output to be stored. "
                        "/hdfs is recommended")
    parser.add_argument("--dataset",
                        help="Name of dataset you want to run over")
    parser.add_argument("--filesPerJob",
                        help="Number of files to run over, per job.",
                        type=int, default=5)
    parser.add_argument("--totalFiles",
                        help="Total number of files to run over. "
                        "Default is ALL (-1). Also acceptable is a fraction of "
                        "the whole dataset (0-1), or an integer number of files.",
                        type=float, default=-1)
    parser.add_argument("--outputScript",
                        help="Optional: name of condor submission script. "
                        "Default is <config>_<time>.condor")
    parser.add_argument("--verbose", "-v",
                        help="Extra printout to clog up your screen.",
                        action='store_true')
    parser.add_argument("--dry",
                        help="Dry-run: only make condor submission script, "
                        "don't submit to queue.",
                        action='store_true')
    parser.add_argument("--dag",
                        help="If you want to run as a condor DAG. ",
                        action='store_true')
    parser.add_argument('--log',
                        help="Location to store job stdout/err/log files. "
                        "Default is $PWD/logs.",
                        default='logs')
    args = parser.parse_args(args=in_args)

    if args.verbose:
        log.setLevel(logging.DEBUG)

    log.debug(args)

    ###########################################################################
    # Do some preliminary checking
    ###########################################################################
    if not args.config:
        raise RuntimeError('You must specify a CMSSW config file')

    if not os.path.isfile(args.config):
        err_msg = "Cannot find config file %s" % args.config
        log.error(err_msg)
        raise IOError(err_msg)

    # for now, restrict output dir to /hdfs
    if not args.outputDir:
        raise RuntimeError('You must specify an output directory')

    if not args.outputDir.startswith('/hdfs'):
        log.error('Output directory not on /hdfs')
        raise RuntimeError('Output directory not on /hdfs')

    if not os.path.exists(args.outputDir):
        print "Output directory doesn't exists, making it:", args.outputDir
        try:
            os.makedirs(args.outputDir)
        except OSError as e:
            log.error("Cannot make output dir %s", args.outputDir)
            raise

    if args.filesPerJob > args.totalFiles and args.totalFiles >= 1:
        log.error("You can't have filesPerJob > totalFiles!")
        raise RuntimeError

    # make an output directory for log files
    if not os.path.exists(args.log):
        os.mkdir(args.log)

    ###########################################################################
    # Lookup dataset with das_client to determine number of files/jobs
    ###########################################################################
    if not args.dataset:
        raise RuntimeError('You must specify a dataset')

    # TODO: use das_client API
    cmds = ['das_client.py',
            '--query',
            'summary dataset=%s' % args.dataset,
            '--format=json']
    output_summary = subprocess.check_output(cmds, stderr=subprocess.STDOUT)
    log.debug(output_summary)
    summary = json.loads(output_summary)

    # check to make sure dataset is valid
    if summary['status'] == 'fail':
        log.error('Error querying dataset with das_client:')
        log.error(summary['reason'])
        raise RuntimeError('Error querying dataset with das_client')

    # get required number of files
    # can either have:
    # < 0 : all files
    # 0 - 1 : use that fraction of the dataset
    # >= 1 : use that number of files
    num_dataset_files = int(summary['data'][0]['summary'][0]['nfiles'])
    if args.totalFiles < 0:
        args.totalFiles = num_dataset_files
    elif args.totalFiles < 1:
        args.totalFiles = math.ceil(args.totalFiles * num_dataset_files)
    elif args.totalFiles > num_dataset_files:
        log.warning("You specified more files than exist. Using all %d files.",
                    num_dataset_files)

    # Figure out correct number of jobs
    total_num_jobs = int(math.ceil(args.totalFiles / float(args.filesPerJob)))

    ###########################################################################
    # Make a list of input files for each job to avoid doing it on worker node
    ###########################################################################
    cmds = ['das_client.py',
            '--query',
            'file dataset=%s' % args.dataset,
            '--limit=%d' % args.totalFiles]
    output_files = subprocess.check_output(cmds, stderr=subprocess.STDOUT)

    list_of_files = ['"{0}"'.format(line) for line in output_files.splitlines()
                     if line.lower().startswith("/store")]

    def grouper(iterable, n, fillvalue=None):
        """
        Iterate through iterable in groups of size n.
        If < n values available, pad with fillvalue.

        Taken from the itertools cookbook.
        """
        args = [iter(iterable)] * n
        return izip_longest(fillvalue=fillvalue, *args)

    dset_uscore = args.dataset[1:]
    dset_uscore = dset_uscore.replace("/", "_").replace("-", "_")
    input_file_list = "fileList_%s.py" % dset_uscore
    with open(input_file_list, "w") as file_list:
        file_list.write("fileNames = {")
        for n, chunk in enumerate(grouper(list_of_files, args.filesPerJob)):
            file_list.write("%d: [%s],\n" % (n, ', '.join(filter(None, chunk))))
        file_list.write("}")

    log.info("List of files for each jobs written to %s", input_file_list)

    ###########################################################################
    # Make sandbox of user's libs/c++/py files
    ###########################################################################
    sandbox_file = "sandbox.tgz"
    sandbox_dirs = ['biglib', 'lib', 'module', 'python']
    tar = tarfile.open(sandbox_file, mode="w:gz", dereference=True)
    cmssw_base = os.environ['CMSSW_BASE']
    for directory in sandbox_dirs:
        fullPath = os.path.join(cmssw_base, directory)
        if os.path.isdir(fullPath):
            log.debug('Adding %s to tar', fullPath)
            tar.add(fullPath, directory, recursive=True)

    # special case for /src - need to include src/package/sub_package/data
    # and src/package/sub_package/interface
    src_dirs = ['data', 'interface']
    src_path = os.path.join(cmssw_base, 'src')
    for root, dirs, files in os.walk(os.path.join(cmssw_base, 'src')):
        if os.path.basename(root) in src_dirs:
            d = root.replace(src_path, 'src')
            log.debug('Adding %s to tar', d)
            tar.add(root, d, recursive=True)

    # add in the config file and input filelist
    tar.add(args.config, arcname="config.py")
    tar.add(input_file_list, arcname="filelist.py")

    # TODO: add in any other files the user wants

    tar.close()

    # copy to /hdfs or /storage to avoid transfer/copying issues
    sandbox_location = os.path.join(args.outputDir, sandbox_file)
    if args.outputDir.startswith('/hdfs'):
        subprocess.call(['hadoop', 'fs', '-copyFromLocal', '-f',
                         sandbox_file, args.outputDir.replace("/hdfs", "")])
    else:
        raise Exception("Not a valid output dir for sandbox - not /hdfs")

    ###########################################################################
    # Make a condor submission script
    ###########################################################################
    log.debug("Will be submitting %d jobs, running over %d files",
              total_num_jobs, args.totalFiles)

    script_dir = os.path.dirname(__file__)
    with open(os.path.join(script_dir, 'cmsRun_template.condor')) as template:
        job_template = template.read()

    config_filename = os.path.basename(args.config)

    time = strftime("%H%M%S")
    date = strftime("%d_%b_%y")
    if not args.outputScript:
        args.outputScript = '%s_%s.condor' % (config_filename.replace(".py", ""),
                                              time)

    job = job_template.replace("SEDINITIAL", "")  # don't use initialdir for now
    log_dir = "%s/%s/%s" % (args.log, date, dset_uscore)
    log_str = "%s/%s" % (log_dir, args.outputScript.replace(".condor", ""))
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log.info('Logs for each job will be written to %s', log_dir)
    job = job.replace("SEDLOG", log_str)

    # Construct args to pass to cmsRun_worker.sh on the worker node
    args_dict = dict(output=args.outputDir,
                     ind="index" if args.dag else "process",
                     sandbox=sandbox_location)
    args_str = "-o {output} -i $({ind}) -a $ENV(SCRAM_ARCH) " \
               "-c $ENV(CMSSW_VERSION) -S {sandbox}".format(**args_dict)
    job = job.replace("SEDARGS", args_str)
    job = job.replace("SEDEXE", os.path.join(script_dir, 'cmsRun_worker.sh'))
    job = job.replace("SEDNJOBS", str(1) if args.dag else str(total_num_jobs))
    # transfers = [os.path.abspath(args.config), input_file_list, sandbox_file]
    transfers = []
    job = job.replace("SEDINPUTFILES", ", ".join(transfers))

    with open(args.outputScript, 'w') as submit_script:
        submit_script.write(job)
    log.info('New condor submission script written to %s', args.outputScript)

    ###########################################################################
    # Setup DAG file if needed
    ###########################################################################
    if args.dag:
        dag_name = "jobs_%s_%s_%s.dag" % (dset_uscore, date, time)
        status_file = dag_name.replace(".dag", ".status")
        print "DAG Name:", dag_name
        with open(dag_name, "w") as dag_file:
            dag_file.write("# DAG for dataset %s\n" % args.dataset)
            dag_file.write("# Using config file %s\n" % args.config)
            for job_ind in xrange(total_num_jobs):
                jobName = "%s_%d" % (dset_uscore, job_ind)
                dag_file.write('JOB %s %s\n' % (jobName, args.outputScript))
                dag_file.write('VARS %s index="%d"\n' % (jobName, job_ind))
                dag_file.write('RETRY %s 3\n' % jobName)
            dag_file.write("NODE_STATUS_FILE %s 30\n" % status_file)

    ###########################################################################
    # submit to queue unless dry run
    ###########################################################################
    if not args.dry:
        if not args.dag:
            subprocess.call(['condor_submit', args.outputScript])

        if args.dag:
            subprocess.call(['condor_submit_dag', dag_name])
            print "Check DAG status:"
            print "DAGstatus.py", status_file

    # Return job properties
    return dict(dataset=args.dataset,
                jobFile=args.outputScript,
                totalNumJobs=total_num_jobs,
                totaNumFiles=args.totalFiles,
                filesPerJob=args.filesPerJob,
                fileList=input_file_list,
                config=args.config,
                condorScript=args.outputScript
                )


if __name__ == "__main__":
    cmsRunCondor()
