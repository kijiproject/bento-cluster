#!/usr/bin/env bash
#
#   (c) Copyright 2012 WibiData, Inc.
#
#   See the NOTICE file distributed with this work for additional
#   information regarding copyright ownership.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#   The bento-env.sh script configures your environment to use the HDFS, MapReduce, and
#   HBase clusters started by bento-cluster. Use:
#
#   bash> source $BENTO_CLUSTER_HOME/bin/bento-env.sh
#
#   to configure your environment. The environment variables set are:
#
#   BENTO_CLUSTER_HOME  Set to the parent of the directory this script is contained in.
#                       This should be the root of a bento-cluster distribution.
#
#   HADOOP_HOME         Set to the directory of the Hadoop distribution included with
#                       bento-cluster. This distribution should be located in
#                       $BENTO_CLUSTER_HOME/lib.
#
#   HBASE_HOME          Set to the directory of the HBase distribution included with
#                       bento-cluster. This distribution should be located in
#                       $BENTO_CLUSTER_HOME/lib.
#
#   PATH                The $PATH is modified so that $BENTO_CLUSTER_HOME/bin,
#                       $HADOOP_HOME/bin, and $HBASE_HOME/bin are on it.
#

# Get the directory this script is located in, no matter how the script is being
# run.
bin="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# The script is inside a bento-cluster distribution.
BENTO_CLUSTER_HOME="${bin}/.."

# Set the rest of the environment relative to the bento-cluster distribution.
libdir="${BENTO_CLUSTER_HOME}/lib"
HADOOP_HOME="${libdir}/hadoop-${hadoop.version}"
HBASE_HOME="${libdir}/hbase-${hbase.version}"
PATH="${BENTO_CLUSTER_HOME}/bin:${HADOOP_HOME}/bin:${HBASE_HOME}/bin:${PATH}"

if [ `uname` = "Darwin" ]; then
    export HADOOP_OPTS="$HADOOP_OPTS -Djava.security.krb5.realm= -Djava.security.krb5.kdc="
    export HBASE_OPTS="$HBASE_OPTS -Djava.security.krb5.realm= -Djava.security.krb5.kdc="
fi

export BENTO_CLUSTER_HOME
echo "Set BENTO_CLUSTER_HOME=${BENTO_CLUSTER_HOME}"
export HADOOP_HOME
echo "Set HADOOP_HOME=${HADOOP_HOME}"
export HBASE_HOME
echo "Set HBASE_HOME=${HBASE_HOME}"
export PATH
echo "Added Hadoop, HBase, and bento-cluster binaries to PATH."

