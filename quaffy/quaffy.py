#!/usr/bin/env python

import paramiko
import httplib
import simplejson as json
from stat import S_ISDIR
from yaml import load
from os import makedirs
from os.path import expanduser, dirname, isdir, isfile
from pprint import pprint as pp
import logging
from optparse import OptionParser
from urllib import quote

log = logging.getLogger()
cfg = {}

def get_cfg(profile='default'):

    couch = httplib.HTTPConnection('localhost',5984)

    # Get list of paths from DB
    uri = "/quaffy/profile-%s"%profile
    couch.request("GET", uri)
    resp = couch.getresponse()
    couch_ret = json.loads(resp.read())

    return couch_ret

def get_sftp():
    transport = paramiko.Transport((cfg['host'],cfg['port']))
    transport.connect(username=cfg['user'], password=cfg['pass'])
    return paramiko.SFTPClient.from_transport(transport)

def scan_sftp(sftp, path, ret_dict=True):
    files = []
    for f in sftp.listdir_attr(path):
        if f.filename.startswith('.'): continue
        if S_ISDIR(f.st_mode):
            files.extend(scan_sftp(sftp, path+'/'+f.filename, False))
        else:
            files.append({
                    'path': "/".join([path,f.filename]),
                    'size': f.st_size,
                    'mtime': f.st_mtime,
                    })

    if ret_dict: return dict([(f['path'], f) for f in files])
    return files

def download(sftp, path):
    rel_path = path.split(cfg['path_remote'])[1]
    local_filepath = expanduser(cfg['path_local'])
    local_filepath += rel_path
    print "downloading", rel_path,

    local_filedir = dirname(local_filepath)
    if isfile(local_filedir): raise "expecting %s to be a dir"%local_filedir
    if not isdir(local_filedir): makedirs(local_filedir)

    sftp.get(path, local_filepath)
    print 'done'

def scan_and_dl():
    # a dict of files indexed by path
    sftp = get_sftp()
    remote_files = scan_sftp(sftp, cfg['path_remote'])
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
        download(sftp, path)

        # update database
        couch = httplib.HTTPConnection('localhost',5984)
        body = json.dumps(remote_files[path])
        headers = {"Content-Type":'application/json'}
        couch.request("POST", "/%s/"%cfg['db'], body, headers)
        resp = couch.getresponse()
        #print resp.status, resp.read()
        # FIXME error check
        # output result

def main():
    usage = "Usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                        default=False, help="turn on info messages")
    parser.add_option("-d", "--debug", dest="debug", action="store_true",
                        default=False, help="turn on debugging")
    parser.add_option("-n", "--no-download", dest="nodl", action="store_true",
                        default=False, help="adds to the db but does not dl")
    (options, args) = parser.parse_args()

    # Setup logging
    log.addHandler(logging.StreamHandler())
    log.setLevel(logging.WARN)
    if options.verbose: log.setLevel(logging.INFO)
    if options.debug: log.setLevel(logging.DEBUG)

    cfg.update(get_cfg('default'))
    scan_and_dl()

if __name__ == '__main__':
    main()
