#!/usr/bin/env python

"""
Script to allow you to run cmsRun jobs on HTCondor.

See options with:

    cmsRunCondor.py --help

Robin Aggleton 201[5|6]
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
from itertools import izip_longest, izip, product
import FWCore.PythonUtilities.LumiList as LumiList


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)


class CustomFormatter(argparse.ArgumentDefaultsHelpFormatter,
                      argparse.RawDescriptionHelpFormatter):
    """For better argparse output"""
    pass


class DatasetFile(object):
    """Hold info about a file in a dataset"""

    def __init__(self, name, lumi_list):
        """
        Parameters
        ----------
        name : str
            Filename
        lumi_list : LumiList object
            Info about run numbers/lumisections
        """
        self.name = name
        self.lumi_list = lumi_list
        self.parents = []

    def __repr__(self):
        return ('DatasetFile(name={name:s}, lumi_list={lumi_list:s}, '
                'parents={parents:s})'.format(**self.__dict__))


def find_matching_run_ls_range(raw_files, run, ls_range):
    """Find all files that have lumisections that fully cover ls_range.

    Parameters
    ----------
    raw_files : list[DatasetFile]
        List of files to match against.
    run : int
        Run number
    ls_range : list[int, int]
        Edges of lumisection range to match, e.g. [610, 621]

    Returns
    -------
    list[DatasetFile]
        List of unique DatasetFiles that cover ls_range.
    """
    matching_files = []
    for ls in xrange(ls_range[0], ls_range[1] + 1):
        matching_files.extend([f for f in raw_files if f.lumi_list.contains(run=run, lumiSection=ls)])
    return list(set(matching_files))


def find_matching_files(raw_files, lumi_list):
    """Find all files in raw_files that cover all runs/lumisections in lumi_list

    Parameters
    ----------
    raw_files : list[DatasetFile]
        List of files to match against.
    lumi_list : LumiList.LumiList
        LumiList holding {run: lumisections}

    Returns
    -------
    list[DatasetFile]
        List of unique DatasetFiles that cover lumi_list.

    Raises
    ------
    RuntimeError
        If no files in `raw_files` match the lumisection.
    """
    matching_files = []
    for run, lumis in lumi_list.compactList.iteritems():
        for lsr in lumis:
            res = find_matching_run_ls_range(raw_files, run, lsr)
            if not res:
                print lsr
                print len(raw_files)
                for rf in raw_files:
                    print rf
                raise RuntimeError('No matching RAW file for this LS %s' % lsr)
            matching_files.extend(res)
    return list(set(matching_files))


def generate_filelist_filename(dataset):
    """Generate a filelist filename from a dataset name."""
    dset_uscore = dataset[1:]
    dset_uscore = dset_uscore.replace("/", "_").replace("-", "_")
    return "fileList_%s.py" % dset_uscore


def generate_lumilist_filename(dataset):
    """Generate a lumilist filename from a dataset name."""
    dset_uscore = dataset[1:]
    dset_uscore = dset_uscore.replace("/", "_").replace("-", "_")
    return "lumiList_%s.py" % dset_uscore


def grouper(iterable, n, fillvalue=None):
    """
    Iterate through iterable in groups of size n.
    If < n values available, pad with fillvalue.

    Taken from the itertools cookbook.
    """
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


def filter_by_lumi_list(list_of_files, lumi_mask):
    """Filter list of files by run number and lumisection.

    Modifies each DatasetFile's LumiList to only the run:LS passing lumi mask.

    Parameters
    ----------
    list_of_files : lit[DatasetFile]
        List of DatasetFiles to be filtered
    lumi_mask : LumiList.LumiList or None
        LumiList of {run:[lumisections]} to filter against.

    Returns
    -------
    list[DatasetFile]
        List of files that have run:LS in lumi_mask
    """
    filtered = []
    for f in list_of_files:
        overlap = f.lumi_list & lumi_mask
        if len(overlap) > 0:
            f.lumi_list = overlap
            filtered.append(f)
    return filtered


def filter_by_run_num(list_of_files, run_list):
    """Filter list of files by list of runs.
    Modifies each DatasetFile's LumiList to only the run:LS in run_list.

    Parameters
    ----------
    list_of_files : list[DatasetFile]
        List of DatasetFiles to be filtered
    run_list : list[int]
        List of run numbers to keep.

    Returns
    -------
    list[DatasetFile]
        List of files that have run number in run_list
    """

    for f in list_of_files:
        f.lumi_list.selectRuns(run_list)
    return [f for f in list_of_files if f.lumi_list.compactList]


def group_files_by_lumis_per_job(list_of_lumis, lumis_per_job):
    """Makes groups of files, splitting based on lumis_per_job.

    Parameters
    ----------
    list_of_lumis : {(run, LS) : DatasetFile}
        List of run/LS with corresponding file
    lumis_per_job : int
        Number of LS per job

    Returns
    -------
    list[list[DatasetFile]], list[LumiList]
        List of list of files for each job, and list of LumiList obj for each job
    """
    group_files, group_lumis = [], []
    for keylist in grouper(list_of_lumis.keys(), lumis_per_job):
        group_keys = filter(None, list(keylist))
        group_lumis.append(LumiList.LumiList(lumis=group_keys))
        group_f = set([list_of_lumis[k] for k in group_keys])
        group_files.append(group_f)
    return group_files, group_lumis


def group_files_by_files_per_job(list_of_files, files_per_job):
    """Makes groups of files, splitting into groups of files_per_job.

    Parameters
    ----------
    list_of_files : list[obj]
        List of files to be grouped
    files_per_job : int
        Number of files per group

    Returns
    -------
    list[list[obj]]
        List of file groups, one per job/group.
    """
    groups = []
    for flist in grouper(list_of_files, files_per_job):
        group = filter(None, list(flist))
        groups.append(group)
    return groups


def create_filelist(jobs_input_files, filelist_filename):
    """Write python dict to file with input files for each job.
    It can then be used in worker script to override the PoolSource.

    Parameters
    ----------
    jobs_input_files : list[list[DatasetFile]]
        List of input files for each cmsRun job.
    filelist_filename : str
        Filename to write python dict to.
    """
    with open(filelist_filename, "w") as file_list:
        file_list.write("fileNames = {")
        for n, flist in enumerate(jobs_input_files):
            file_list.write("%d: [%s],\n" % (n, ', '.join(["'%s'" % f.name for f in flist if f])))
        file_list.write("}\n")

        file_list.write("secondaryFileNames = {")
        for n, flist in enumerate(jobs_input_files):
            job_parents = []
            for f in flist:
                job_parents.extend([p.name for p in f.parents])
            file_list.write("%d: [%s],\n" % (n, ', '.join(["'%s'" % x for x in set(job_parents)])))
        file_list.write("}\n")

    log.info("List of files for each jobs written to %s", filelist_filename)


def create_lumilists(jobs_lumis, lumilist_filename):
    """Write python dict to file with lumis for each job.
    It can then be used in worker script to override the PoolSource.

    Parameters
    ----------
    jobs_lumis : list[LumiList]
        List of LumiList objects, one for each job.
    lumilist_filename : str
        Filename to write python dict to.
    """
    with open(lumilist_filename, "w") as file_lumis:
        file_lumis.write("import FWCore.ParameterSet.Config as cms\n")
        file_lumis.write("lumis = {")
        for n, lumi_list in enumerate(jobs_lumis):
            file_lumis.write("%d: %s, \n" % (n, lumi_list.getVLuminosityBlockRange()))
        file_lumis.write("}\n")
    log.info("List of lumis for each job written to %s", lumilist_filename)


def setup_sandbox(sandbox_filename, sandbox_dest_dir, config_filename,
                  input_filelist, additional_input_files):
    """Create sandbox gzip of libs/headers/py/config/input filelist, & copy to HDFS.

    Parameters
    ----------
    sandbox_filename : str
        Filename of sandbox
    sandbox_dest_dir : str
        Destination directory for sandbox. Must be on /hdfs
    config_filename : str
        Filename of CMSSW config file to be included.
    input_filelist : str or None
        Filename of list of files for worker. If None, will not be added, and
        worker will use whatever files are specified in config.
    additional_input_files : list[str]
        List of additional input files to add to sandbox

    Returns
    -------
    str
        Location of sandbox zip on /hdfs

    Raises
    ------
    Exception
        If sandbox_dest_dir is not on /hdfs
    """
    log.info('Creating sandbox')

    sandbox_filename = "sandbox.tgz"
    sandbox_dirs = ['biglib', 'lib', 'module', 'python']
    tar = tarfile.open(sandbox_filename, mode="w:gz", dereference=True)
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
    tar.add(config_filename, arcname="src/config.py")
    if input_filelist:
        log.debug('Adding %s to tar', input_filelist)
        tar.add(input_filelist, arcname="src/filelist.py")

    # add in any other files the user wants
    for input_file in additional_input_files:
        if not os.path.isfile(input_file):
            raise IOError('Cannot find additional file %s' % input_file)
            log.debug('Adding %s to tar', input_file)
        # we want it to end up in CMSSW_BASE/src, for now
        tar.add(input_file, arcname=os.path.join('src', os.path.basename(input_file)))

    tar.close()

    # copy to /hdfs or /storage to avoid transfer/copying issues
    sandbox_location = os.path.join(sandbox_dest_dir, sandbox_filename)
    if sandbox_dest_dir.startswith('/hdfs'):
        log.info("Copying %s to %s", sandbox_filename, sandbox_location)
        subprocess.check_call(['hadoop', 'fs', '-copyFromLocal', '-f',
                               sandbox_filename, sandbox_location.replace("/hdfs", "")])
    else:
        raise Exception("Not a valid output dir for sandbox - not /hdfs")
    return sandbox_location


def das_file_to_lumilist(data):
    """Extract LumiList object from DAS file entry"""
    lumi_dict = {}
    for rn, lumi in izip(data['run'], data['lumi']):
        run_num = str(rn['run_number'])
        lumis = lumi['number']
        lumi_dict[run_num] = lumis
    return LumiList.LumiList(compactList=lumi_dict)


def get_list_of_files_from_das(dataset, num_files):
    """Create list of num_files filenames for dataset using DAS.

    Parameters
    ----------
    dataset : str
        Name of dataset
    num_files : int
        Total number of files to get.

    Returns
    -------
    list[DatasetFile]
        List of DatasetFile obj with filename and lumisections for each file.

    Raises
    ------
    RuntimeError
        If DAS fails to find dataset

    """
    # TODO: use das_client API
    log.info("Querying DAS for dataset info, please be patient...")
    cmds = ['das_client.py', '--query',
            'summary dataset=%s' % dataset, '--format=json']
    output_summary = subprocess.check_output(cmds)
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
    if num_files < 0:
        num_files = num_dataset_files
    elif num_files < 1:
        num_files = math.ceil(num_files * num_dataset_files)
    elif num_files > num_dataset_files:
        num_files = num_dataset_files
        log.warning("You specified more files than exist. Using all %d files.",
                    num_dataset_files)

    # Make a list of input files for each job to avoid doing it on worker node
    log.info("Querying DAS for %d filenames, please be patient...", num_files)
    cmds = ['das_client.py', '--query',
            'file,run,lumi dataset=%s status=VALID' % dataset,
            '--limit=%d' % (num_files), '--format=json']
    log.debug(' '.join(cmds))
    das_output = subprocess.check_output(cmds)
    file_dict = json.loads(das_output)
    try:
        files = [DatasetFile(name=entry['file'][0]['name'], lumi_list=das_file_to_lumilist(entry))
                 for entry in file_dict['data']]
    except KeyError as e:
        print file_dict
        raise e
    return files


def write_condor_job_file(job_filename, log_dir, args_str, num_jobs):
    """Write condor job file.

    Parameters
    ----------
    job_filename : str
        filename of job file
    log_dir : str
        Dir for logs
    args_str : str
        Argument string to pass to worker script
    num_jobs : int
        Total number of jobs. For DAGs this should be 1 as the DAG should
        take care of the actual total number of jobs.
    """

    # Get job file template
    script_dir = os.path.dirname(__file__)
    with open(os.path.join(script_dir, 'cmsRun_template.condor')) as template:
        job_template = template.read()

    job = job_template.replace("SEDINITIAL", "")  # don't use initialdir for now
    log_filename = os.path.join(log_dir, os.path.basename(job_filename).replace(".condor", ""))
    log.info('Logs for each job will be written to %s', log_dir)
    job = job.replace("SEDLOG", log_filename)
    job = job.replace("SEDARGS", args_str)
    job = job.replace("SEDEXE", os.path.join(script_dir, 'cmsRun_worker.sh'))
    job = job.replace("SEDNJOBS", num_jobs)
    transfers = []
    job = job.replace("SEDINPUTFILES", ", ".join(transfers))

    with open(job_filename, 'w') as submit_script:
        submit_script.write(job)
    log.info('New condor submission script written to %s', job_filename)


def write_dag_file(dag_filepath, status_filename, condor_jobscript, total_num_jobs, job_name):
    """Write DAG description file.

    Parameters
    ----------
    dag_filepath : str
        Filepath for DAG file
    status_filename : str
        Filepath for DAG status file
    condor_jobscript : str
        Filepath for condor job submit file
    total_num_jobs : int
        Total number of jobs to submit
    job_name : str
        Name of job. An index will be added for each job
    """
    log.info("DAG Filename: %s", dag_filepath)
    with open(dag_filepath, "w") as dag_file:
        for job_ind in xrange(total_num_jobs):
            jobName = "%d_%s" % (job_ind, job_name)
            dag_file.write('JOB %s %s\n' % (jobName, condor_jobscript))
            dag_file.write('VARS %s index="%d"\n' % (jobName, job_ind))
            dag_file.write('RETRY %s 5\n' % jobName)
        dag_file.write("NODE_STATUS_FILE %s 30\n" % status_filename)


def check_create_dir(dirname, info_msg=None, debug_msg=None):
    """Check if directory exists, if not make it."""
    if not os.path.isdir(dirname):
        if os.path.abspath(dirname).startswith('/hdfs'):
            subprocess.check_call(['hadoop', 'fs', '-mkdir', '-p', os.path.abspath(dirname).replace('/hdfs', '')])
        else:
            os.makedirs(dirname)


def flag_mutually_exclusive_args(args, opts_a, opts_b):
    """Flag mutually exclusive args (i.e can't specify both A and B).

    Each of the opts in opts_a are incompatible with each of the opts in opts_b.
    """
    arg_dict = vars(args)
    for oa, ob in product(opts_a, opts_b):
        if arg_dict[oa] and arg_dict[ob]:
            raise RuntimeError("Cannot specify both --%s and --%s" % (oa, ob))


def flag_dependent_args(args, opts_a, opts_b):
    """Flag dependent args (i.e B require A to be set).

    Each of the opts in opts_b requires every opt in opts_a.
    """
    arg_dict = vars(args)
    if all([arg_dict[oa] for oa in opts_a]):
        for ob in opts_b:
            if not arg_dict[ob]:
                raise RuntimeError("--%s requires %s" % (oa, ob))


def check_args(args):
    """Check program arguments.

    Parameters
    ----------
    args : argparse.Namespace
        Args to check

    Raises
    ------
    IOError
        If it cannot find config file or filelist (if one specified)
    RuntimeError
        If outputDir not on HDFS, or incorrect --unitsPerJob

    """
    if not os.path.isfile(args.config):
        raise IOError("Cannot find config file %s" % args.config)

    flag_mutually_exclusive_args(args, ['filelist'], ['dataset', 'splitByLumis'])

    if args.filelist:
        args.filelist = os.path.abspath(args.filelist)
        if not os.path.isfile(args.filelist):
            raise IOError("Cannot find filelist %s" % args.filelist)

    # for now, restrict output dir to /hdfs
    if not args.outputDir.startswith('/hdfs'):
        raise RuntimeError('Output directory (--outputDir) not on /hdfs')

    check_create_dir(args.outputDir,
                     info_msg="Output directory doesn't exists, "
                              "making it: %s" % args.outputDir)

    if args.unitsPerJob > args.totalUnits and args.totalUnits >= 1:
        raise RuntimeError("You can't have unitsPerJob > totalUnits!")

    if args.secondaryDataset:
        flag_dependent_args(args, ['dataset'], ['secondaryDataset'])
        log.info("Running 2-file solution with secondary dataset %s", args.secondaryDataset)

    # make an output directory for log files
    check_create_dir(args.log,
                     info_msg="Log directory doesn't exist, "
                              "making it: %s" % args.log)

    if args.dag:
        args.dag = os.path.realpath(args.dag)
        check_create_dir(os.path.dirname(args.dag),
                         info_msg="DAG directory doesn't exist, "
                                  "making it: %s" % os.path.dirname(args.dag))

    if args.lumiMask and not is_url(args.lumiMask):
        args.lumiMask = os.path.abspath(args.lumiMask)

    flag_mutually_exclusive_args(args, ['callgrind', 'valgrind'], ['filelist', 'dataset'])


def is_url(path):
    """Test if path is URL or not"""
    # this is a pretty crap test, can do better?
    return path.startswith('http') or path.startswith('www')


def setup_lumi_mask(lumi_mask_source):
    """Produce LumiList.LumiList from lumi_mask_source.

    Parameters
    ----------
    lumi_mask_source : str
        File or URL of lumi mask JSON to be interpreted.

    Returns
    -------
    LumiList.LumiList
        LumiList object, with {run : [lumisections]} info
    """
    if is_url(lumi_mask_source):
        return LumiList.LumiList(url=lumi_mask_source)
    else:
        # leave file existence to LumiList
        return LumiList.LumiList(filename=lumi_mask_source)


def parse_run_range(range_str):
    """Parse run range string, result list of run numbers

    range_str can have individual runs, or ranges. They can also be combined,
    but must be separated by a comma.

    e.g.
    "234567,234568" -> [234567, 234568]
    "234567-234569" -> [234567, 234568, 234569]
    "234567,234569-234570" -> [234567, 234569, 234570]
    """
    if range_str == '':
        return []
    if range_str is None:
        return None

    run_list = []
    for entry in range_str.split(','):
        entry = entry.strip()
        if '-' in entry:
            start, end = entry.split('-')
            run_list.extend(range(int(start), int(end) + 1))
        else:
            run_list.append(int(entry))
    return run_list


def cmsRunCondor(in_args=sys.argv[1:]):
    """Creates a condor job description file with the correct arguments,
    and optionally submit it.

    Returns a dict of information about the job.
    """
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=CustomFormatter)
    parser.add_argument("--config",
                        help="CMSSW config file you want to run.",
                        required=True)
    parser.add_argument("--outputDir",
                        help="Where you want your output to be stored. "
                        "Must be on /hdfs.",
                        required=True)
    parser.add_argument("--dataset",
                        help="Name of dataset you want to run over")
    parser.add_argument("--secondaryDataset",
                        help="Name of secondary dataset. This allows you to do the "
                        "'2-file' solution, e.g.to run both RAW and RECO in the "
                        "same job. The --unitsPerJob and --totalUnits options "
                        "will then apply to the child dataset (specified with --dataset)")
    split_group = parser.add_mutually_exclusive_group(required=True)
    split_group.add_argument("--splitByFiles",
                             help='Unit = file',
                             action='store_true')
    split_group.add_argument("--splitByLumis",
                             help='Unit = lumisection',
                             action='store_true')
    parser.add_argument("--unitsPerJob",
                        help="Number of units to run over per job.",
                        type=int)
    parser.add_argument("--totalUnits",
                        help="Total number of units to run over. "
                        "Default is ALL (-1). Also acceptable is a fraction of "
                        "the whole dataset (0-1), or an integer number of files (>=1).",
                        type=float, default=-1)
    parser.add_argument("--filelist",
                        help="Pass in a list of filenames to run over. "
                        "This will ignore --dataset/--lumiMask/--runRange options.")
    parser.add_argument("--outputScript",
                        help="Name of condor submission script. "
                        "Default is <config>_<time>.condor, recommended to put it on /storage.")
    parser.add_argument("--verbose", "-v",
                        help="Extra printout to clog up your screen.",
                        action='store_true')
    parser.add_argument("--dry",
                        help="Dry-run: only make condor submission script, "
                        "don't submit to queue.",
                        action='store_true')
    parser.add_argument("--dag",
                        type=str,
                        help="Specify DAG filename if you want to run as a condor DAG."
                        "**Strongly recommended** to put it on /storage")
    parser.add_argument('--log',
                        help="Location to store job stdout/err/log files. "
                        "Default is $PWD/logs, but would recommend to put it on /storage",
                        default='logs')
    parser.add_argument('--inputFile',
                        help="Additional input file(s) needed by cmsRun.",
                        action='append')
    parser.add_argument('--lumiMask',
                        help='Specify file or URL with {run:lumisections} to run over')
    parser.add_argument('--runRange',
                        help='Specify run number(s) to run over. List, or range '
                        '(or combine). Must be comma separated. '
                        'e.g. 259700,269710-259720')
    parser.add_argument('--callgrind',
                        help='Run using callgrind. Note that in this mode, '
                        'it will use the files and # evts in the config. '
                        'You do not need to specify --unitsPerJob, --totalUnits, or --dataset. '
                        'You should recompile with `scram b clean; scram b USER_CXXFLAGS="-g"`',
                        action='store_true')
    parser.add_argument('--valgrind',
                        help='Run using valgrind to find mem leaks. Note that in this mode, '
                        'it will use the files and # evts in the config. '
                        'You do not need to specify --unitsPerJob, --totalUnits, or --dataset. '
                        'You should recompile with `scram b clean; scram b USER_CXXFLAGS="-g"`',
                        action='store_true')
    args = parser.parse_args(args=in_args)

    if args.verbose:
        log.setLevel(logging.DEBUG)

    log.debug(args)

    check_args(args)

    # Why not just use args.lumiMask to hold result?
    run_list = parse_run_range(args.runRange) if args.runRange else None
    lumi_mask = setup_lumi_mask(args.lumiMask) if args.lumiMask else None
    log.debug("Run range: %s", run_list)
    log.debug("Lumi mask: %s", lumi_mask)

    ###########################################################################
    # Lookup dataset with das_client to determine number of files/jobs
    # but only if we're not profiling
    ###########################################################################
    # placehold vars
    total_num_jobs = 1
    filelist_filename, lumilist_filename = None, None

    # This could probably be done better!

    if not args.valgrind and not args.callgrind:
        list_of_files, list_of_secondary_files = None, None
        list_of_lumis = None
        if not args.filelist and not args.dataset:
            raise RuntimeError('You must specify a dataset or a filelist')
        if args.unitsPerJob is None:
            raise RuntimeError('You must specify an integer number of --unitsPerJob')

        if args.filelist:
            # Get files from user's file
            with open(args.filelist) as flist:
                list_of_files = [DatasetFile(name=line.strip(), lumi_list=None) for line in flist if line.strip()]
            n_files = args.totalUnits
            if n_files < 0:
                n_files = None
            elif n_files < 1:
                n_files = int(round(n_files * len(list_of_files)))
            else:
                n_files = int(n_files)
                if n_files >= len(list_of_files):
                    raise IndexError("You cannot have more files than in the files:"
                                     " use -1 (the default) if you want them all")
            list_of_files = list_of_files[:n_files]
            filelist_filename = "filelist_user_%s.py" % (strftime("%H%M%S"))  # add time to ensure unique
        else:
            filelist_filename = generate_filelist_filename(args.dataset)
            lumilist_filename = generate_lumilist_filename(args.dataset)
            # Get list of files from DAS, also store corresponding lumis
            n_files = args.totalUnits if args.splitByFiles else -1
            list_of_files = get_list_of_files_from_das(args.dataset, n_files)
            log.debug("Pre lumi filter")
            log.debug(list_of_files)
            if run_list:
                list_of_files = filter_by_run_num(list_of_files, run_list)
            if lumi_mask:
                list_of_files = filter_by_lumi_list(list_of_files, lumi_mask)
            log.debug("After lumi filter")
            log.debug(list_of_files)
            if args.secondaryDataset:
                list_of_secondary_files = get_list_of_files_from_das(args.secondaryDataset, -1)
                # do lumisection matching between primary and secondary datasets
                for f in list_of_files:
                    f.parents = find_matching_files(list_of_secondary_files, f.lumi_list)

        # figure out job grouping
        if args.splitByFiles:
            job_files = group_files_by_files_per_job(list_of_files, args.unitsPerJob)
            total_num_jobs = len(job_files)
            create_filelist(job_files, filelist_filename)
            if lumilist_filename:
                # make an overall lumilist for all files in each job
                job_lumis = []
                for f in job_files:
                    tmp = f[0].lumi_list
                    for x in f[1:]:
                        tmp += x.lumi_list
                    job_lumis.append(tmp)
                create_lumilists(job_lumis, lumilist_filename)

        elif args.splitByLumis:
            # need to keep track of which files correspond with which lumi
            # this holds a map of {(run:LS) : DatasetFile}
            list_of_lumis = {}
            for f in list_of_files:
                for x in f.lumi_list.getLumis():
                    list_of_lumis[x] = f
            # choose the required number of lumis
            if 0 < args.totalUnits < 1:
                end = int(math.ceil(len(list_of_lumis) * args.totalUnits))
                list_of_lumis = {k:list_of_lumis[k] for k in list_of_lumis.keys()[0:end + 1]}
            elif args.totalUnits >= 1:
                list_of_lumis = {k:list_of_lumis[k] for k in list_of_lumis.keys()[0:int(args.totalUnits)]}

            # do job grouping
            job_files, job_lumis = group_files_by_lumis_per_job(list_of_lumis, args.unitsPerJob)
            total_num_jobs = len(job_files)
            create_filelist(job_files, filelist_filename)
            create_lumilists(job_lumis, lumilist_filename)

    log.debug("Will be submitting %d jobs", total_num_jobs)

    ###########################################################################
    # Create sandbox of user's files
    ###########################################################################
    sandbox_local = "sandbox.tgz"
    additional_input_files = args.inputFile or []
    if lumilist_filename and os.path.isfile(lumilist_filename):
        additional_input_files.append(lumilist_filename)

    sandbox_location = setup_sandbox(sandbox_local, args.outputDir,
                                     args.config, filelist_filename,
                                     additional_input_files)
    # rm local files
    if os.path.isfile(sandbox_local):
        os.remove(sandbox_local)
    if filelist_filename and os.path.isfile(filelist_filename):
        os.remove(filelist_filename)
    if lumilist_filename and os.path.isfile(lumilist_filename):
        os.remove(lumilist_filename)

    ###########################################################################
    # Make a condor submission script
    ###########################################################################
    config_filename = os.path.basename(args.config)
    if not args.outputScript:
        args.outputScript = '%s_%s.condor' % (config_filename.replace(".py", ""),
                                              strftime("%H%M%S"))
    args.outputScript = os.path.realpath(args.outputScript)
    check_create_dir(os.path.dirname(args.outputScript),
                     info_msg="Output condor script directory doesn't exist, "
                              "making it: %s" % os.path.dirname(args.outputScript))

    # Construct args to pass to cmsRun_worker.sh on the worker node
    args_dict = dict(output=args.outputDir,
                     ind="index" if args.dag else "process",
                     sandbox=sandbox_location)
    args_str = "-o {output} -i $({ind}) -a $ENV(SCRAM_ARCH) " \
               "-c $ENV(CMSSW_VERSION) -S {sandbox}".format(**args_dict)
    if args.lumiMask or args.runRange:
        if lumilist_filename:
            args_str += ' -l ' + os.path.basename(lumilist_filename)
        elif is_url(args.lumiMask):
            args_str += ' -l ' + args.lumiMask
    if args.valgrind:
        args_str += ' -m'
    if args.callgrind:
        args_str += ' -p'

    num_jobs = str(1) if args.dag else str(total_num_jobs)

    write_condor_job_file(args.outputScript, args.log, args_str, num_jobs)

    ###########################################################################
    # Setup DAG file if needed
    ###########################################################################
    if args.dag:
        if args.filelist:
            job_name = os.path.splitext(os.path.basename(args.filelist))[0][:20]
        elif args.callgrind:
            job_name = "callgrind"
        elif args.valgrind:
            job_name = "valgrind"
        else:
            job_name = args.dataset[1:].replace("/", "_").replace("-", "_")

        status_filename = args.dag.replace(".dag", ".status")
        write_dag_file(args.dag, status_filename, args.outputScript, total_num_jobs, job_name)

    ###########################################################################
    # submit to queue unless dry run
    ###########################################################################
    if not args.dry:
        if not args.dag:
            subprocess.check_call(['condor_submit', args.outputScript])

        if args.dag:
            subprocess.check_call(['condor_submit_dag', args.dag])
            print "Check DAG status:"
            print "DAGstatus.py", status_filename

    # Return job properties
    return dict(dataset=args.dataset,
                jobFile=args.outputScript,
                totalNumJobs=total_num_jobs,
                totaNumFiles=args.totalUnits,
                unitsPerJob=args.unitsPerJob,
                fileList=filelist_filename,
                config=args.config,
                condorScript=args.outputScript
                )


if __name__ == "__main__":
    cmsRunCondor()
