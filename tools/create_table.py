from IOVAPI import IOVDB
import sys, getopt, psycopg2

Usage = """
python create_table.py [options] <database name> <folder_name> <column>:<type> [...]
options:
    -h <host>
    -p <port>
    -U <user>
    -w <password>
    
    -t - time column type. Accepted values are:
         t  - time with the DB implied time zone (default)
         tz - time with explicit time zone
         f  - floating point
         i  - long integer
    -c - force create, drop existing table
    -R <user>,... - DB users to grant read permissions to
    -W <user>,... - DB users to grant write permissions to
"""

host = None
port = None
user = None
password = None
columns = []
grants_r = []
grants_w = []
drop_existing = False
time_type = "timestamp"

dbcon = []

opts, args = getopt.getopt(sys.argv[1:], 'h:U:w:p:cR:W:t:')

if len(args) < 3 or args[0] == 'help':
    print(Usage)
    sys.exit(0)

for opt, val in opts:
    if opt == '-h':         dbcon.append("host=%s" % (val,))
    elif opt == '-p':       dbcon.append("port=%s" % (int(val),))
    elif opt == '-U':       dbcon.append("user=%s" % (val,))
    elif opt == '-w':       dbcon.append("password=%s" % (val,))
    elif opt == '-c':       drop_existing = True
    elif opt == '-R':       grants_r = val.split(',')
    elif opt == '-W':       grants_w = val.split(',')
    elif opt == '-t':       time_type = val
                                
                            
    

dbcon.append("dbname=%s" % (args[0],))

dbcon = ' '.join(dbcon)
tname = args[1]

ctypes = []
for w in args[2:]:
    n,t = tuple(w.split(':',1))
    ctypes.append((n,t))
    
grants = {}
for u in grants_r:
    grants[u] = 'r'
for u in grants_w:
    grants[u] = grants.get(u, '') + 'w'

db = IOVDB(connstr=dbcon)
t = db.createFolder(tname, ctypes, 
    time_type = time_type, 
    grants = grants, 
    drop_existing = drop_existing)
print('Folder created')

