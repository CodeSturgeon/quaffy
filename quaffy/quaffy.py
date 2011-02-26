#!/usr/bin/env python

import paramiko
import httplib
import simplejson as json
from stat import S_ISDIR
from os import makedirs, chmod
from os.path import expanduser, dirname, isdir, isfile
from pprint import pprint as pp
import logging
from optparse import OptionParser
from urllib import quote
import datetime

log = logging.getLogger()
cfg = {}

def get_cfg():
    couch = httplib.HTTPConnection(cfg['dbhost'], cfg['dbport'])

    # Get list of paths from DB
    uri = "/%s/quaffy-%s"%(cfg['dbname'], cfg['profile'])
    couch.request("GET", uri)
    log.debug('Getting profile')
    resp = couch.getresponse()
    couch_ret = json.loads(resp.read())
    log.debug('Got profile')

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
    print "downloading", rel_path.lstrip('/'),

    local_filedir = dirname(local_filepath)
    if isfile(local_filedir): raise "expecting %s to be a dir"%local_filedir
    if not isdir(local_filedir): makedirs(local_filedir)

    if not cfg['nodl']:
        sftp.get(path, local_filepath)
        chmod(local_filepath, 0664)
        print 'done'
    else:
        print 'skipped'

def scan_and_dl():
    # a dict of files indexed by path
    sftp = get_sftp()
    log.debug('scanning sftp')
    remote_files = scan_sftp(sftp, cfg['path_remote'])
    paths = remote_files.keys()
    log.debug('found %d remote files'%len(paths))

    couch = httplib.HTTPConnection(cfg['dbhost'], cfg['dbport'])

    # Get list of paths from DB
    body = json.dumps({"keys":paths})
    headers = {"Content-Type":'application/json'}
    #uri = "/%s/_design/quaffy/_view/paths"%cfg['dbname']
    #couch.request("POST", uri, body, headers)
    uri = "/%s/_design/quaffy/_view/paths?group=true"%cfg['dbname']
    uri += "&startkey=[\"%s\"]"%cfg['profile']
    uri += "&endkey=[\"%s\",{}]"%cfg['profile']
    couch.request("GET", uri)

    log.debug('requesting records')
    resp = couch.getresponse()
    # FIXME look for errors
    couch_ret = json.loads(resp.read())
    log.debug('%d records recived'%len(couch_ret['rows']))

    # Iter DB results by path
    for doc in couch_ret['rows']:
        # FIXME check size and mtime
        cur = remote_files[doc['key'][1]]
        if doc['value'][0] == cur['mtime'] and doc['value'][1] == cur['size']:
            paths.remove(doc['key'][1])

    downloaded = []
    for path in paths:
        download(sftp, path)
        downloaded.append(remote_files[path])

    if len(downloaded) > 0:
        # update database
        timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        body = json.dumps({
                'downloads': downloaded,
                'timestamp': timestamp,
                'profile': cfg['profile']
        })
        headers = {"Content-Type":'application/json'}
        couch = httplib.HTTPConnection(cfg['dbhost'], cfg['dbport'])
        couch.request("POST", "/%s/"%cfg['dbname'], body, headers)
        resp = couch.getresponse()
        #print resp.status, resp.read()
        # FIXME error check

    sftp.close()
    # output result
    print 'downloaded %d files'%len(downloaded)

def main():
    usage = "Usage: %prog [options]"
    parser = OptionParser(usage=usage)
    # host
    parser.add_option("", "--dbhost", dest="dbhost", action="store",
                        default='localhost', help='couchdb host')
    # port
    parser.add_option("", "--dbport", dest="dbport", action="store",
                        default=5984, help='couchdb port')
    # dbname
    parser.add_option("-b", "--dbname", dest="dbname", action="store",
                        default='quaffy', help='couchdb name')
    # profile
    parser.add_option("-p", "--profile", dest="profile", action="store",
                        default='default', help='profile to use')

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

    cfg.update({'dbhost':options.dbhost})
    cfg.update({'dbport':options.dbport})
    cfg.update({'dbname':options.dbname})
    cfg.update({'profile':options.profile})
    cfg.update({'nodl':options.nodl})
    cfg.update(get_cfg())
    scan_and_dl()

if __name__ == '__main__':
    main()
