jobHarvest
==========

harvest and aggregate Lustre job stats from slurm clusters.

jobHarvest is a simple python script that acts as both a data gatherer on the Lustre servers and as aggregator of the data on a central host. it uses a bunch of logic and checks and timestamps to try to work out which jobs are still running, which are just being quiet for a while, and which are actually finished.

Lustre's job stats often reset back to zero when jobs are quite for an interval of 600s or so. jobHarvest's main feature is that it detects these zeroing events and corrects for them, meaning that it keeps an accurate total record of how many bytes and iops each job does.

various levels of detail in the aggregated data are available from jobHarvest.

Requirements
------------

jobHarvest requires pyslurm be available on the aggregation host in order to track the life cycle of slurm jobs. influxdb-client is required to enable writing to InfluxDB.

How it Works
------------

data gathering occurs on Lustre OSS/MDS server nodes eg.

    jobHarvest.py hostname home short

where ''hostname'' is the name of the machine to send data to, and ''home'' and ''short'' are the names of Lustre OSTs present.

the master aggregator process runs on ''hostname'' which is typically a management server. this needs no arguments. eg.

    jobHarvest.py

data is transferred by sending serialised python objects over simple TCP connections. client (OSS/MDS) sends are synchronised to improve coherence of statistics. data integrity is verified by md5 sums of the objects. authenticity is ensured by using a shared secret that is read from a file - typically the same one lustreHarvest uses.

jobHarvest transparently handles client and server process disconnections and restarts (eg. OSS reboots).

Writing to InfluxDB
-------------------
Creating an config file at `/root/.jobHarvest.influx` containing

    http://server_address:port
    org_name
    bucket_name
    token

will enable writing to InfluxDB.
