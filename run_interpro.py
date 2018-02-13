#!/usr/bin/env python

import os, sys, errno
import subprocess
import argparse
from dask_controller import distributed_single
from dask.distributed import Client
from time import sleep


parser = argparse.ArgumentParser(description='''

    Run Interpro or import

''', formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument('--fasta', metavar = '<input.fasta>', required=True,
help='''Input set to analyze.  Protein or nucleotide\n\n''')

parser.add_argument('--out_prefix', metavar = '</path/to/output>', 
required=True,
help='''Absolute or relative path to output directory\n\n''')

parser.add_argument('--chunk_input', metavar = '<INT>', 
required=True,
help='''Number of sequences per chunk file, will be used as inputs\n\n''')

parser._optionals.title = "Program Options"
args = parser.parse_args()


def dump_me(x):
    '''futures'''
    print(x)
    return x


def sleepy_test(i):
    '''sleeps for 10s and returns exit value, 0 success'''
    cmd = 'sleep 20'
    exit_val = subprocess.check_call(cmd, shell=True)
    return exit_val


def create_directories(dirpath):
    '''make directory path'''
    try:
        os.makedirs(dirpath)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def return_filehandle(open_me):
    magic_dict = {
                  '\x1f\x8b\x08': 'gz'
#                  '\x42\x5a\x68': 'bz2',
#                  '\x50\x4b\x03\x04': 'zip'
                 }
    max_bytes = max(len(t) for t in magic_dict)
    with open(open_me) as f:
        s = f.read(max_bytes)
    for m in magic_dict:
        if s.startswith(m):
            t = magic_dict[m]
            if t == 'gz':
                return gzip.open(open_me)
#            elif t == 'bz2':
#                return bz2.open(open_me)
#            elif t == 'zip':
#                return zipfile.open(open_me)
    return open(open_me)


def split_fasta(fasta, out_prefix, sequences):
    '''splits a multiple fasta file into chunks'''
    chunk_files = []
    chunk_prefix = out_prefix + '/chunks'
    create_directories(chunk_prefix)
    fh = return_filehandle(fasta)
    count = 0
    chunk = 0
    chunk_file = "{}/{:04d}.fasta".format(chunk_prefix, chunk)
    chunk_files.append(chunk_file)
    chunk_out = open(chunk_file, 'w')
    with fh as fopen:
        for line in fopen:
            line = line.rstrip()
            if line.startswith('>'):
                count += 1
                if count > sequences:
                    count = 1
                    chunk += 1
                    chunk_out.close()
                    chunk_file = "{}/{:04d}.fasta".format(chunk_prefix, chunk)
                    chunk_files.append(chunk_file)
                    chunk_out = open(chunk_file, 'w')
                chunk_out.write(line + '\n')
            else:
                chunk_out.write(line + '\n')
    chunk_out.close()
    return chunk_files
    

def init_interpro(fasta, interproscan, **kwargs):
    '''run subprocess for interproscan'''
    interproscan = os.path.abspath(interproscan + '/interproscan.sh')
    out_prefix = os.path.abspath(fasta)
    out_prefix += '.interproscan'
    fasta = os.path.abspath(fasta)
    molecule_type = False  # Change this later make kwargs
    lookup = True  # Change this later make kwargs default
    goterms = True  # Change this later make kwargs default
    outfmt = ['GFF3', 'JSON']  # Change this later make kwargs default
    outfmt_str = ','.join(outfmt)
    cmd = '''{} -i {} -b {} --cpu 64 --disable-precalc'''.format(interproscan, fasta, out_prefix) #change later correcly format line
    if molecule_type:
        cmd += ''' -t {}'''.format(molecule_type)
    if lookup:
        cmd += ' -iprlookup'
    if goterms:
        cmd += ' -goterms'
    cmd += ''' -f {};'''.format(outfmt_str)
    run_shell = 'export PATH=/home/ctc/ctc_e00/sw/interpro/interproscan-5.27-66.0/python2:$PATH;'
    exit_val = 'exit $?;'
    run_shell += cmd
    run_shell += exit_val
    return run_shell
    #subprocess.check_call(run_shell, shell=True)
    #return True


def submit_process(cmd):
    exit_val = subprocess.check_call(cmd, shell=True)
    return exit_val


if __name__ == '__main__':
#    client = Client('tcp://10.0.1.2:8786', processes=False)
    optionals = {}
    interproscan = '/fs/e00/ctc/sw/interpro/interproscan-5.27-66.0/' #Config Object
    sequences = int(args.chunk_input)
    chunks = []
    if sequences > 1:
        chunks = split_fasta(args.fasta, args.out_prefix, sequences)
    else:
        print('chunk_input must be greater than 1 not {}'.format(sequences))
        sys.exit(1)
#    for c in chunks:
    #out_prefix = '{}.interproscan'.format(c)
    cmds = []
    cluster = distributed_single()
    client = cluster.client
    for c in chunks:
    #    print(c)
        cmd = init_interpro(c, interproscan, **optionals)
        print('running {}'.format(cmd))
# resources={'THREADS' : 2}))
        ipr_futures = client.submit(submit_process, cmd, resources={'THREADS' : 64})
        print(ipr_futures.result())
#    t2 = client.submit(dump_me, ipr_futures)
#    t = client.map(sleepy_test, range(9))
#    print(t)
#    print(iprs_futures)
#    for f in iprs_futures:
#        print(f)
#        print(f.result())
#    print(ipr_futures.result())
#    t2 = client.submit(dump_me, iprs_futures)
#    print(t2.result())
    print('tasks completed')
#    print(iprs_futures)
