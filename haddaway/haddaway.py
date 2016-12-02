#!/usr/bin/env python

"""
Script and modules to perform more efficient hadd-ing using HTCondor.

Requires htcondenser

TODO:

- clever RAM/disk requirements using estimate
- recursive algo - avoid hardcoding in just 2 layers - somehow allow for maximum?

[Refers to either the 80s classic "What is love",
or the more modern "Hideaway" by Kiesza.]
"""

import sys
import os
import argparse
import logging
from distutils.spawn import find_executable
from itertools import izip_longest, chain
import math
import string
import random
from time import strftime


import htcondenser as ht


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)


class CustomFormatter(argparse.ArgumentDefaultsHelpFormatter,
                      argparse.RawDescriptionHelpFormatter):
    """For better argparse output"""
    pass


class ArgParser(argparse.ArgumentParser):
    """Class to handle arg parsing"""
    def __init__(self, *args, **kwargs):
        super(ArgParser, self).__init__(*args, **kwargs)
        self.add_arguments()

    def add_arguments(self):
        self.add_argument("--output",
                          help="Output filename, must be on HDFS",
                          required=True)

        self.add_argument("--inputList",
                          help="Text file with list of input files")
        self.add_argument("--input",
                          nargs="+",
                          help="Input file[s]")

        self.add_argument("--size", type=int, default=20,
                          help="Number of files for an intermediate hadd job")

        self.add_argument("--haddArgs",
                          help="Arguments to pass to hadd")
        self.add_argument("--verbose", "-v",
                          help="Extra printout to clog up your screen.",
                          action='store_true')


def check_hadd_exists():
    hadd_path = find_executable("hadd")
    if hadd_path is None:
        raise RuntimeError("Cannot find hadd in PATH")
    return True


def grouper(iterable, n, fillvalue=None):
    """Collect data into fixed-length chunks or blocks.

    If iterable does not divide by n with modulus 0, then the remaining entries
    in the last iterable of grouper() will be padded with fillvalue.

    Taken from https://docs.python.org/2/library/itertools.html#recipes
    e.g.
    >>> grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    """
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


def arrange_hadd_files(input_files, group_size):
    """Groups `input_files` into groups of `group_size`.

    Parameters
    ----------
    input_files : list[]
        List of input files to be grouped

    group_size : int
        Ideal size of each group. If one file would be left over,
        the group_size is reduced by 1.

    Returns
    -------
    list[list[str]]
        List of groups of filenames
    """
    group_size = int(group_size)
    if group_size < 2:
        raise RuntimeError("group_size must be > 1")  # TODO: change error type

    # adjust to avoid hadding 1 file by itself
    if len(input_files) % group_size == 1 and group_size > 2:
        group_size -= 1

    # calculate number of intermediate hadd jobs required
    n_inter_jobs = int(math.ceil(len(input_files) * 1. / group_size))
    if n_inter_jobs == 1:
        return [input_files]
    else:
        intermediate_jobs = []
        for i, job_group in enumerate(grouper(input_files, group_size)):
            job_group = list(filter(None, job_group))
            intermediate_jobs.append(job_group)
    return intermediate_jobs


def rand_str(length=3):
    """Generate a random string of user-specified length"""
    return ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase)
                   for _ in range(length))


def create_hadd_jobs(input_files, group_size, final_filename, hadd_args=None):
    """Create htcondenser.Job objects for intermediate and final hadd jobs.

    Parameters
    ----------
    input_files : list[str]
        List of input files
    group_size : int
        Size of each intermediate hadd job
    final_filename : str
        Final filename
    hadd_args : str, optional
        Optional args to pass to hadd

    Returns
    -------
    list[htcondenser.Job], htcondenser.Job
        List of intermediate jobs, and a final hadd job

    """
    hadd_file_groups = arrange_hadd_files(input_files, group_size)

    hadd_args = hadd_args or ""

    inter_jobs = []
    final_file_input = hadd_file_groups[0]  # when only 1 intermediate job

    # add intermediate files if needed
    if len(hadd_file_groups) > 1:
        final_file_input = []

        if len(hadd_file_groups) > 255:
            raise RuntimeError("Final hadd cannot cope with %d intermediate jobs" % len(hadd_file_groups))

        final_dir = os.path.dirname(final_filename)

        for ind, group in enumerate(hadd_file_groups):
            inter_output_file = os.path.join(final_dir, 'haddInter_%d_%s.root' % (ind, rand_str(5)))
            this_hadd_args = hadd_args.split() + [inter_output_file] + group
            inter_job = ht.Job(name="interHadd_%d" % ind,
                               args=this_hadd_args,
                               input_files=group,
                               output_files=[inter_output_file])
            inter_jobs.append(inter_job)
            final_file_input.append(inter_output_file)

    # add final job
    this_hadd_args = hadd_args.split() + [final_filename] + final_file_input
    final_hadd_job = ht.Job(name="finalHadd",
                            args=this_hadd_args,
                            input_files=final_file_input,
                            output_files=[final_filename])

    return (inter_jobs, final_hadd_job)


def create_intermediate_cleanup_jobs(inter_hadd_jobs):
    """Create htcondenser.Job objects to cleanup intermediate hadd files.

    Parameters
    ----------
    inter_hadd_jobs : list[htcondenser.Job]
        List of intermediate hadd Jobs

    Returns
    -------
    name : list[htcondenser.Job]
        List of removal Jobs
    """
    rm_jobs = []
    for ind, job in enumerate(inter_hadd_jobs):
        rm_job = ht.Job(name="rm_%d" % ind,
                        args=" fs -rm -skipTrash %s" % job.output_files[0].replace("/hdfs", ""))
        rm_jobs.append(rm_job)
    return rm_jobs


def haddaway(in_args=sys.argv[1:]):
    parser = ArgParser(description=__doc__, formatter_class=CustomFormatter)
    args = parser.parse_args(args=in_args)

    if args.verbose:
        log.setLevel(logging.DEBUG)

    log.debug(args)

    # Check hadd exists
    check_hadd_exists()

    if not args.input and not args.inputList:
        raise RuntimeError("Need to specify --input or --inputFiles")

    final_filename = args.output
    if not final_filename.startswith("/hdfs"):
        raise RuntimeError("Output file MUST be on HDFS")

    # Get list of input files, do checks
    input_files = []

    if args.inputList:
        if not os.path.isfile(args.inputList):
            raise IOError("%s does not exist" % args.inputList)
        with open(args.inputList) as f:
            input_files = f.readlines()
    else:
        input_files = args.input[:]

    if len(input_files) < 2:
        raise RuntimeError("Fewer than 2 input files - hadd not needed")

    # sanitise paths, check existance
    for i, f in enumerate(input_files):
        input_files[i] = os.path.abspath(f).strip().strip("\n").strip()
        if not os.path.isfile(input_files[i]):
            raise IOError("Input %s does not exist" % input_files[i])

    log.debug('Input:', input_files)

    # Arrange into jobs
    inter_hadd_jobs, final_hadd_job = create_hadd_jobs(input_files, args.size, final_filename, hadd_args=args.haddArgs)

    log.info("Creating %d intermediate jobs", len(inter_hadd_jobs))

    # Add to JobSet and DAG
    user_dict = {
        "username": os.environ['LOGNAME'],
        'datestamp': strftime("%d_%b_%y"),
        'timestamp': strftime("%H%M%S")
    }

    log_dir = "/storage/{username}/haddaway/{datestamp}/".format(**user_dict)
    dag_file = os.path.join(log_dir, "haddaway_{timestamp}.dag".format(**user_dict))
    status_file = os.path.join(log_dir, "haddaway_{timestamp}.status".format(**user_dict))

    hadd_dag = ht.DAGMan(filename=dag_file,
                         status_file=status_file)

    condor_file = os.path.join(log_dir, "haddaway_{timestamp}.condor".format(**user_dict))
    log_stem = "hadd.$(cluster).$(process)"

    # TODO: clever estimate of RAM/disk size required

    hadd_jobset = ht.JobSet(exe='hadd', copy_exe=False,
                            filename=condor_file,
                            out_dir=os.path.join(log_dir, 'logs'), out_file=log_stem + '.out',
                            err_dir=os.path.join(log_dir, 'logs'), err_file=log_stem + '.err',
                            log_dir=os.path.join(log_dir, 'logs'), log_file=log_stem + '.log',
                            cpus=1, memory='1GB', disk='1.95GB',
                            transfer_hdfs_input=False,
                            share_exe_setup=True,
                            hdfs_store=os.path.dirname(final_filename))

    for job in inter_hadd_jobs:
        hadd_jobset.add_job(job)
        hadd_dag.add_job(job)

    hadd_jobset.add_job(final_hadd_job)
    hadd_dag.add_job(final_hadd_job, requires=inter_hadd_jobs if inter_hadd_jobs else None)

    # Add removal jobs if necessary
    rm_jobs = create_intermediate_cleanup_jobs(inter_hadd_jobs)

    if len(rm_jobs) > 0:
        condor_file = os.path.join(log_dir, "rm_{timestamp}.condor".format(**user_dict))
        log_stem = "rm.$(cluster).$(process)"

        rm_jobset = ht.JobSet(exe="hadoop", copy_exe=False,
                              filename=condor_file,
                              out_dir=os.path.join(log_dir, 'logs'), out_file=log_stem + '.out',
                              err_dir=os.path.join(log_dir, 'logs'), err_file=log_stem + '.err',
                              log_dir=os.path.join(log_dir, 'logs'), log_file=log_stem + '.log',
                              cpus=1, memory='100MB', disk='10MB',
                              transfer_hdfs_input=False,
                              share_exe_setup=False,
                              hdfs_store=os.path.dirname(final_filename))
        for job in rm_jobs:
            rm_jobset.add_job(job)
            hadd_dag.add_job(job, requires=final_hadd_job)

    # add jobs to remove copies from HDFS if they weren't there originally
    for job_ind, job in enumerate(inter_hadd_jobs):
        for m_ind, mirror in enumerate(job.input_file_mirrors):
            if not mirror.original.startswith('/hdfs'):
                condor_file = os.path.join(log_dir, "rmCopy_{timestamp}.condor".format(**user_dict))
                log_stem = "rmCopy.$(cluster).$(process)"
                rm_job = ht.Job(name="rmCopy_%d_%d" % (job_ind, m_ind),
                                args=" fs -rm -skipTrash %s" % mirror.hdfs.replace("/hdfs", ""))
                rm_jobset.add_job(rm_job)
                hadd_dag.add_job(rm_job, requires=job)

    # Submit jobs
    hadd_dag.submit()

    return 0


if __name__ == '__main__':
    sys.exit(haddaway())
