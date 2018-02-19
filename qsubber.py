import subprocess
import traceback
import time
from pathos import pools
import argparse

parser = argparse.ArgumentParser(description='')
parser.add_argument('-j','--jobs',nargs='+', required=True,
                    help='the paths of the pbs files to submit, as a list in the form "-j job1.pbs job2.pbs"')
parser.add_argument('-w','--workers',type=int, required=False,
                    help='the number of pathos workers to use for job submission')


def runCmd(exe):
    p = subprocess.Popen(exe,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while True:
        retcode = p.poll()
        line = p.stdout.readline()
        yield line
        if retcode is not None:
            break

def hasRQJob(jobNo):
    snum = str(jobNo).split('.')
    if len(snum)<1 or snum[0] == '':
        return False
    qstat = runCmd(['qstat',snum[0]])
    for line in qstat:
        columns = line.split()
        if len(columns) >= 2 and columns[-2] in ('Q','R'): return True
    return False

def submitJob(pbsPath):
    try:
        job = subprocess.Popen(["qsub", pbsPath],stdout=subprocess.PIPE)
        job.wait()
        out = job.communicate()[0]
        if len(out)<1:
            raise ValueError('qsub failed, it is likely that the file you specified does not exist')
        while(hasRQJob(out)):
            time.sleep(1)
    except Exception as err:
        traceback.print_exc()
        print('error in qsub of file: {}'.format(pbsPath))

def submitJobs(pbsPaths,workers):
    m = pools.ProcessPool(workers).map
    results = m(submitJob,pbsPaths)
    return results

def main():
    args = parser.parse_args()
    jobs = args.jobs
    workers = args.workers if args.workers > 0 else 10
    submitJobs(jobs,workers)


if __name__ == "__main__":
    main()