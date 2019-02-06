from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import os
import gzip
import re
import datetime
import sys


# Function to insert each line into cassandra as batches of 200
def log_insert(log_file, table):
    counter = 1
    insert_query = session.prepare("INSERT INTO " + table +
                                   " (host, id, datetime, path, bytes) VALUES (?, uuid(), ?, ?, ?)")
    batch = BatchStatement(consistency_level=1)
    for line in log_file:
        line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
        m = re.match(line_re, line)
        if m:  # Skip lines that do not satisfy parsing
            host = m.group(1)
            p_datetime = datetime.datetime.strptime(m.group(2), '%d/%b/%Y:%H:%M:%S')
            path = m.group(3)
            bytes_pro= int(m.group(4))
            counter += 1
            batch.add(insert_query, (host, p_datetime, path, bytes_pro))
            if counter == 200:
                session.execute(batch)
                batch.clear()
                counter = 1
    session.execute(batch)
    batch.clear()


input_directory = sys.argv[1]
user_id = sys.argv[2]
table_name = sys.argv[3]

cluster = Cluster(['199.60.17.171', '199.60.17.188'])  # Connect to the cluster
session = cluster.connect(user_id)  # Use the specified key space

session.execute('create table if not exists ' + table_name +
                '(id UUID, host TEXT, datetime TIMESTAMP, path TEXT, bytes INT, PRIMARY KEY(host, id));')

for f in os.listdir(input_directory):
    filename, file_extension = os.path.splitext(f)
    file = os.path.join(input_directory, f)
    if file_extension == '.gz':  # Check if the file type is gz or not
        with gzip.open(file, 'rt', encoding='utf-8', errors='ignore') as logfile:
            log_insert(logfile, table_name)
    else:
        log_insert(open(file), table_name)
