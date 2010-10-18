#!/usr/bin/env python

import paramiko
import sqlite3
import stat
from yaml import load
from os.path import expanduser

cfg = load(open(expanduser('~/.quaffy')))

host = cfg['test']['host']
port = cfg['test']['port']
user = cfg['test']['user']
passwd = cfg['test']['pass']

transport = paramiko.Transport((host,port))
transport.connect(username=user, password=passwd)
sftp = paramiko.SFTPClient.from_transport(transport)
for f in sftp.listdir_attr():
    print f.filename, stat.S_ISDIR(f.st_mode)


# recursive func doing ls
# [{path, size, m_time}, ...]

# cross check with db
# if there is any size/m_time differnce - WARN
# if it's flagged downloaded - skip
# if it's new insert it

# find first record without 'downloaded' flag ste
# download and set flag
