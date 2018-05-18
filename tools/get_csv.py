import getopt
import sys
import urllib2, hashlib, random, time
from datetime import datetime

Usage = """
python get_csv.py [options] <URL> <folder>

    -t <time>           default = now
    -T <tag>
    -n                  do not retrieve data, just print the URL
"""

opts, args = getopt.getopt(sys.argv[1:], 't:nT:')
if not args:
    print Usage
    sys.exit(1)
t = time.time()
tag = None
do_retrieve = True

for opt, val in opts:
    if opt == '-t': 
        t = float(val)
    if opt == '-T': tag = val
    if opt == '-n': do_retrieve = False

folder = args[1]
url = args[0]
args = "f=%s&t=%f" % (folder, t)
if tag: args += "&tag=%s" % (tag,)
url = "%s/data?%s" % (url, args)

if do_retrieve:
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)
    print response.getcode(), response.read()
else:
    print url
