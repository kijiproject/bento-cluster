/**
 * (c) Copyright 2012 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.bento;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instances of this class encapsulate in-process HDFS, MapReduce, HBase,
 * and Zookeeper clusters that can be started and stopped as a group. The clusters are provided
 * through {@link BentoHBaseTestingUtility}.
 */
public class BentoClusters {
  private static final Logger LOG = LoggerFactory.getLogger(BentoClusters.class);

  /** For for the directory containing data for the mini zookeeper cluster. */
  private File mZookeeperDir;

  /** The testing utility that runs the minicluster. */
  private BentoHBaseTestingUtility mTestingUtil;

  /** The mini cluster started by the testing utility. */
  private MiniHBaseCluster mMiniCluster;


  /**
   * Constructs a new instance that stores its data in the path specified.
   *
   * @param clusterDir The path to the directory to be used by the clusters.
   * @param conf A configuration to use for the clusters started.
   * @throws Exception if there is a problem creating the testing utility.
   */
  public BentoClusters(String clusterDir, Configuration conf) throws Exception {
    // Initialize directories used by the mini-clusters.
    File dataTestDir = new File(clusterDir, "data");
    File clusterTestDir = new File(clusterDir, "dfscluster");
    mZookeeperDir = new File(clusterDir, "zookeeper");

    mTestingUtil = new BentoHBaseTestingUtility(conf, dataTestDir, clusterTestDir);

    // Set the location used by the mini clusters.
    System.setProperty(HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY, clusterDir);

    // Set various storage directories used by mapreduce to within the cluster directory.
    configureDir("hadoop.log.dir", clusterDir, "userlogs");
    configureDir("mapred.working.dir", clusterDir, "mapred-working");
    configureDir("mapred.output.dir", clusterDir, "mapred-output");
  }

  /**
   * Adds the path to a directory to this instance's configuration.
   *
   * @param key The configuration key to associate with the directory path.
   * @param parentDir The path to the parent dir of the directory.
   * @param subDir The name of the directory under the parent directory.
   */
  private void configureDir(String key, String parentDir, String subDir) {
    File dir = new File(parentDir, subDir);
    getConfiguration().set(key, dir.toString());
  }

  /**
   * Gets the configuration used by the clusters encapsulated by this instance.
   *
   * @return The configuration in use by clusters.
   */
  public Configuration getConfiguration() {
    return mTestingUtil.getConfiguration();
  }

  /**
   * Start the HDFS, Zookeeper, HBase, and MapReduce clusters encapsulated by this instance.
   *
   * @throws Exception If there is a problem starting the clusters.
   */
  public void startClusters() throws Exception {
    // Start a mini zookeeper cluster on a client-configured port, and use that zookeeper cluster
    // in the testing util.
    final MiniZooKeeperCluster zkCluster = new MiniZooKeeperCluster(getConfiguration());
    zkCluster.setDefaultClientPort(
        getConfiguration().getInt("hbase.zookeeper.property.clientPort", 2181));
    zkCluster.startup(mZookeeperDir);
    mTestingUtil.setZkCluster(zkCluster);
    mMiniCluster = mTestingUtil.startMiniCluster();
    mTestingUtil.startMiniMapReduceCluster(1);
  }

  /**
   * Wait (block) until the master of the HBase cluster encapsulated by this instance has stopped.
   */
  public void waitUntilStop() {
    mMiniCluster.waitOnMaster(0);
  }

  /**
   * Stop the MapReduce, HBase, Zookeeper, and HDFS clusters encapsulated by this instance.
   *
   * @throws Exception If there is a problem stopping the clusters.
   */
  public void stopClusters() throws Exception {
    mTestingUtil.shutdownMiniCluster();
  }
}
