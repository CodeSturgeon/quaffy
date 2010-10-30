#!/usr/bin/env python

import paramiko
import httplib
import simplejson as json
from stat import S_ISDIR
from yaml import load
from os import makedirs
from os.path import expanduser, dirname, isdir, isfile
from pprint import pprint as pp

def get_cfg(filename='~/.quaffy'):
    # FIXME insert error handling and raising around this
    return load(open(expanduser(filename)))

def get_sftp(cfg):
    transport = paramiko.Transport((cfg['host'],cfg['port']))
    transport.connect(username=cfg['user'], password=cfg['pass'])
    return paramiko.SFTPClient.from_transport(transport)

def scan(sftp, path, ret_dict=True):
    files = []
    for f in sftp.listdir_attr(path):
        if f.filename.startswith('.'): continue
        if S_ISDIR(f.st_mode):
            files.extend(scan(sftp, path+'/'+f.filename, False))
        else:
            files.append({
                    'path': "/".join([path,f.filename]),
                    'size': f.st_size,
                    'mtime': f.st_mtime,
                    })

    if ret_dict: return dict([(f['path'], f) for f in files])
    return files

def download(sftp, cfg, path):
    local_filepath = expanduser(cfg['path_local'])
    local_filepath += path.split(cfg['path_remote'])[1]

    local_filedir = dirname(local_filepath)
    if isfile(local_filedir): raise "expecting %s to be a dir"%local_filedir
    if not isdir(local_filedir): makedirs(local_filedir)

    sftp.get(path, local_filepath)

if __name__ == '__main__':
    # a dict of files indexed by path
    cfg = get_cfg()['default']
    sftp = get_sftp(cfg)
    remote_files = scan(sftp, cfg['path_remote'])
    paths = remote_files.keys()

    couch = httplib.HTTPConnection('localhost',5984)

    # Get list of paths from DB
    body = json.dumps({"keys":paths})
    headers = {"Content-Type":'application/json'}
    uri = "/%s/_design/qafd/_view/paths?include_docs=true"%cfg['db']
    couch.request("POST", uri, body, headers)

    resp = couch.getresponse()
    # FIXME look for errors
    couch_ret = json.loads(resp.read())

    # Iter DB results by path
    for doc in couch_ret['rows']:
        paths.remove(doc['key'])

    for path in paths:
        print "downloading",path
        download(sftp, cfg, path)

        # update database
        couch = httplib.HTTPConnection('localhost',5984)
        body = json.dumps(remote_files[path])
        headers = {"Content-Type":'application/json'}
        couch.request("POST", "/%s/"%cfg['db'], body, headers)
        resp = couch.getresponse()
        print resp.status, resp.read()
        # output result
