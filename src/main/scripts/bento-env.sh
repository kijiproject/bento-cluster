#!/usr/bin/env bash

# (c) Copyright 2012 WibiData, Inc.
#
# See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ------------------------------------------------------------------------------

# The bento-env.sh script configures your environment to use the HDFS,
# MapReduce, and HBase clusters started by bento-cluster. Use:
#
#     $ source $BENTO_CLUSTER_HOME/bin/bento-env.sh
#
# to configure your environment. The environment variables set are:
#
# BENTO_CLUSTER_HOME  Set to the parent of the directory this script is contained in.
#                     This should be the root of a bento-cluster distribution.
#
# HADOOP_HOME         Set to the directory of the Hadoop distribution included with
#                     bento-cluster. This distribution should be located in
#                     $BENTO_CLUSTER_HOME/lib.
#
# HBASE_HOME          Set to the directory of the HBase distribution included with
#                     bento-cluster. This distribution should be located in
#                     $BENTO_CLUSTER_HOME/lib.
#
# PATH                The $PATH is modified so that $BENTO_CLUSTER_HOME/bin,
#                     $HADOOP_HOME/bin, and $HBASE_HOME/bin are on it.

# ------------------------------------------------------------------------------

# Canonicalize a path into an absolute, symlink free path.
#
# Portable implementation of the GNU coreutils "readlink -f path".
# The '-f' option of readlink does not exist on MacOS, for instance.
#
# Args:
#   param $1: path to canonicalize.
# Stdout:
#   Prints the canonicalized path on stdout.
function resolve_symlink() {
  local target_file=$1

  if [[ -z "${target_file}" ]]; then
    echo ""
    return 0
  fi

  cd "$(dirname "${target_file}")"
  target_file=$(basename "${target_file}")

  # Iterate down a (possible) chain of symlinks
  local count=0
  while [[ -L "${target_file}" ]]; do
    if [[ "${count}" -gt 1000 ]]; then
      # Just stop here, we've hit 1,000 recursive symlinks. (cycle?)
      break
    fi

    target_file=$(readlink "${target_file}")
    cd $(dirname "${target_file}")
    target_file=$(basename "${target_file}")
    count=$(( ${count} + 1 ))
  done

  # Compute the canonicalized name by finding the physical path
  # for the directory we're in and appending the target file.
  local phys_dir=$(pwd -P)
  echo "${phys_dir}/${target_file}"
}

# ------------------------------------------------------------------------------

bento_env_path="${BASH_SOURCE:-$0}"
bento_env_path=$(resolve_symlink "${bento_env_path}")

BENTO_CLUSTER_HOME=$(dirname "$(dirname "${bento_env_path}")")
BENTO_CLUSTER_HOME=$(cd "${BENTO_CLUSTER_HOME}"; pwd -P)

source "${BENTO_CLUSTER_HOME}/conf/bento-cluster.version"

# Set the rest of the environment relative to the bento-cluster distribution.
libdir="${BENTO_CLUSTER_HOME}/lib"
HADOOP_HOME="${libdir}/hadoop-${hadoop_version}"
HBASE_HOME="${libdir}/hbase-${hbase_version}"
HADOOP_CONF_DIR="${HADOOP_HOME}/conf"
HBASE_CONF_DIR="${HBASE_HOME}/conf"
PATH="${BENTO_CLUSTER_HOME}/bin:${HADOOP_HOME}/bin:${HBASE_HOME}/bin:${PATH}"

if [[ "$(uname)" == "Darwin" ]]; then
  export HADOOP_OPTS="$HADOOP_OPTS -Djava.security.krb5.realm= -Djava.security.krb5.kdc="
  export HBASE_OPTS="$HBASE_OPTS -Djava.security.krb5.realm= -Djava.security.krb5.kdc="
fi

export BENTO_CLUSTER_HOME
export HADOOP_CONF_DIR
export HADOOP_HOME
export HBASE_CONF_DIR
export HBASE_HOME
export PATH

if [[ -z "${QUIET}" ]]; then
  cat <<EOF
Set BENTO_CLUSTER_HOME=${BENTO_CLUSTER_HOME}
Set HADOOP_HOME=${HADOOP_HOME}
Set HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
Set HBASE_HOME=${HBASE_HOME}
Set HBASE_CONF_DIR=${HBASE_CONF_DIR}
Added Hadoop, HBase, and bento-cluster binaries to PATH.
EOF
fi
