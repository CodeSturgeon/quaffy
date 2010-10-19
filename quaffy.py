#!/usr/bin/env python

import paramiko
import sqlite3
from stat import S_ISDIR
from yaml import load
from os.path import expanduser
from pprint import pprint as pp

def get_sftp(name='test'):
    cfg = load(open(expanduser('~/.quaffy')))
    host = cfg['test']['host']
    port = cfg['test']['port']
    user = cfg['test']['user']
    passwd = cfg['test']['pass']

    transport = paramiko.Transport((host,port))
    transport.connect(username=user, password=passwd)
    return paramiko.SFTPClient.from_transport(transport)

def scan(sftp, path):
    print " -> %s"%path
    files = []
    for f in sftp.listdir_attr(path):
        if f.filename.startswith('.'): continue
        if S_ISDIR(f.st_mode):
            files.extend(scan(sftp, path+'/'+f.filename))
        else:
            files.append({
                    'path': "/".join([path,f.filename]),
                    'size': f.st_size,
                    'mtime': f.st_mtime,
                    })
    return files

if __name__ == '__main__':
    sftp = get_sftp()
    pp(scan(sftp, '.'))

# cross check with db
# if there is any size/m_time differnce - WARN
# if it's flagged downloaded - skip
# if it's new insert it

# find first record without 'downloaded' flag ste
# download and set flag
