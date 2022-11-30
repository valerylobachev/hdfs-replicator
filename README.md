# HDFS Replicator

Tool to replicate files from an HDFS source path to a local 
destination, excluding previously copied files.

## Motivation

Many machine learning projects lack a tool to replicate files from 
an HDFS-based data store or data lake to local storage, with 
the ability to exclude previously copied files. Using Apache Flume, 
Apache Nifi or another big ETL tool would be overengineered for 
such tasks. HDFS Replicator - a lightweight tool for copying files 
from HDFS to local storage with tracking of previously copied files 
is just designed to close this gap.

### Build the application

Application is written in Go. Go environment version 1.19 is required. 
To build application run command 
```bash
go build -o replicator main.go  
```

## Usage

To copy files from HDFS to local file system run command:
```bash
replicator -s <source-url> -d <destination-dir> -c <control-file>
```

`-c <control-file>` - control file with a list of downloaded files (default "./files.txt"). Files included in the 
file are excluded from replication. After execution this file is updated with new copied files to exclude them 
in next runs.

`-d <destination-dir>` - local destination path (default "./")

`-s <source-url>` - source HDFS URL, for example `-s "hdfs://namenode:9000/user/data/parquet/passport"`


## Contributing

Contributions are always welcome! Just fork the project and create PR. 

## License

[Apache 2.0](http://www.apache.org/licenses/)