(c) Copyright 2012 WibiData, Inc.

bento-cluster ${project.version}
================================

bento-cluster is a tool for the Hadoop ecosystem that makes it easy to
run and interact with HDFS, MapReduce, and HBase clusters running on
your local machine. bento-cluster helps you setup a suitable
environment for testing and prototyping projects that use HDFS,
MapReduce, and HBase with little setup time.

Quick start
-----------

### Untar bento-cluster
Untar your bento-cluster distribution into a location convenient for
you. We suggest your home directory. We'll call the path to your
bento-cluster distribution `$BENTO_CLUSTER_HOME`.

> `tar xzvf bento-cluster-${project.version}-release.tar.gz.`
> `mv bento-cluster-${project.version} $BENTO_CLUSTER_HOME`

### Configure your system to use bento-cluster.
bento-cluster includes a script that will configure your
environment to use the HDFS, MapReduce and HBase clusters managed by
bento-cluster. Run:

> `source $BENTO_CLUSTER_HOME/bin/bento-env.sh`

to configure your environment. Add this line to your `~/.bashrc` file
to ensure your environment is always configured for bento-cluster.

`bento-env.sh` configures your environment to use the Hadoop
ecosystem distributions included with bento-cluster, as well as the
clusters we'll soon use bento-cluster to start. It also adds the
`hadoop`, `hbase`, and `bento` scripts to your `$PATH`, making them
available as top-level commands.

### Start bento-cluster.
Now that your system is configured to use bento-cluster, you can use
the `bento` script to start your local HDFS, MapReduce, and HBase
clusters.

> `bento start`

### Use the Hadoop ecosystem.
With the clusters started, you can run Hadoop ecosystem tools. To
list your home directory in HDFS, for example, run:

> `hadoop fs -ls`

The `hadoop` and `hbase` scripts are available for use. You can also
use other Hadoop ecosystem tools like kiji, hive, or pig and they
will use the clusters when run in an environment configured with
`bento-env.sh`.

### Stop bento-cluster.
When you're ready to call it a day, you can stop bento-cluster by
running:

> `bento stop`

This will shutdown the HBase, MapReduce, and HDFS clusters managed by
bento-cluster.

The bento script
-----------------
The `bento` script includes tools for starting and stopping
bento-cluster. Use:

> `bento help`

for full usage information.

Cluster data and logs.
-----------------------
The bento-cluster distribution contains a directory
`$BENTO_CLUSTER_HOME/state` that holds cluster data and logs. Logs
written by MapReduce jobs can be found in
`$BENTO_CLUSTER_HOME/state/userlogs`.

Cluster configuration
---------------------
The Hadoop and HBase clusters included with bento-cluster (in the
directory `$BENTO_CLUSTER_HOME/lib`) contain directories named `conf`
that contain the Hadoop and HBase XML configuration files. The first
time you start bento-cluster, bento-cluster will configure the ports
used by Hadoop and HBase and write configuration XML files to these
directories.

To manually run the port configuration utility, use:

> `bento config`

For more information on using the port configuration utility, use:

> `bento config --help`

For convenience, bento-cluster also creates a directory
`$BENTO_CLUSTER_HOME/cluster-conf` containing symlinks to the Hadoop
and HBase XML configuration files. You may edit the files
`core-site.xml`, `mapred-site.xml`, `hdfs-site.xml` and
`hbase-site.xml` linked in this directory to further configure the
clusters started by bento-cluster. Note that some standard Hadoop
and HBase configuration parameters are controlled by bento-cluster
and cannot be set by the user. See the XML files for a listing of
specific parameters that cannot be overridden.

Troubleshooting
----------------
When run on a local machine, HDFS, MapReduce, and HBase clusters may
fail or behave unexpectedly if the machine is suspended. Users who
encounter problems after a suspend should stop and then start
bento-cluster to recover.

