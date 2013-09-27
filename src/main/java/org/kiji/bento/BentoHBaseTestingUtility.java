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

import static org.kiji.bento.AbstractionBarrierBulldozer.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>This class provides mini hadoop and hbase clusters that can be executed in a process on a
 * local machine. </p>
 *
 * <p>The actual functionality is mostly provided by extending {@link HBaseTestingUtility}.
 * Some changes are made to the testing utility to ensure cluster data is persistent between
 * runs of the cluster.</p>
 */
class BentoHBaseTestingUtility extends HBaseTestingUtility {
  private static final Logger LOG = LoggerFactory.getLogger(BentoHBaseTestingUtility.class);

  /** A directory containing the storage for the mini cluster. */
  private File mDataTestDir;

  /** File for the directory containing cluster data, including dfs data. */
  private File mClusterTestDir;

  /**
   * Constructs a new BentoHBaseTestingUtility and sets the location for its
   * data.
   *
   * @param conf The configuration to use for the started clusters.
   * @param dataTestDir The directory used for cluster storage.
   * @param clusterTestDir The directory used for cluster data, including dfs data.
   * @throws Exception If an error occurred while trying to set the data location.
   */
  public BentoHBaseTestingUtility(Configuration conf, File dataTestDir,
      File clusterTestDir) throws Exception {
    super(conf);
    mDataTestDir = dataTestDir;
    mClusterTestDir = clusterTestDir;
    // We need to set these private fields to ensure that dfs data will be preserved
    // across runs. Unfortunately this can't be done through subclassing, because all
    // methods related to the initialization of these fields are private.
    setField(HBaseTestingUtility.class, this, "dataTestDir", mDataTestDir);
    setField(HBaseTestingUtility.class, this, "clusterTestDir", mClusterTestDir);
  }

  /**
   * Stops mini hbase, zk, and hdfs clusters. We override this so we don't erase the
   * dfs contents.
   *
   * @throws Exception If a component of the minicluster failed to shutdown.
   * @see {@link #startMiniCluster(int)}
   */
  @Override
  public void shutdownMiniCluster() throws Exception {
    LOG.info("Shutting down bento cluster.");
    shutdownMiniHBaseCluster();
    LOG.info("bento cluster is down.");
  }

  /**
   * Start a mini dfs cluster. We override this method in our child class so we can
   * disable formatting the filesystem between runs and so we can pass configuration options for
   * the namenode port and namenode ui address.
   *
   * @param servers How many DNs to start.
   * @param hosts hostnames DNs to run on.
   * @throws Exception If an error occurs when starting up the cluster.
   * @see {@link #shutdownMiniDFSCluster()}
   * @return The mini dfs cluster created.
   */
  @Override
  public MiniDFSCluster startMiniDFSCluster(int servers, final String[] hosts)
      throws Exception {
    // Check that there is not already a cluster running
    isRunningCluster();

    // We have to set this property as it is used by MiniCluster
    System.setProperty("test.build.data", mClusterTestDir.toString());

    // Some tests also do this:
    //  System.getProperty("test.cache.data", "build/test/cache");
    // It's also deprecated
    System.setProperty("test.cache.data", mClusterTestDir.toString());

    // Use configuration provided values for the namenode port and namenode ui port, or use
    // accepted defaults.
    Configuration conf = getConfiguration();
    int nameNodePort = FileSystem.get(conf).getUri().getPort();
    int nameNodeUiPort = getPortFromConfiguration("dfs.http.address", 50070);
    MiniDFSCluster dfsCluster = null;
    MiniDFSCluster.Builder options = new MiniDFSCluster.Builder(conf)
        .nameNodePort(nameNodePort)
        .nameNodeHttpPort(nameNodeUiPort)
        .numDataNodes(servers)
        .manageNameDfsDirs(true)
        .manageDataDfsDirs(true)
        .hosts(hosts);

    // Ok, now we can start. First try it without reformatting.
    try {
      LOG.debug("Attempting to use existing cluster storage.");
      dfsCluster = options.format(false).build();
    } catch (InconsistentFSStateException e) {
      LOG.debug("Couldn't use existing storage. Attempting to format and try again.");
      dfsCluster = options.format(true).build();
    }

    // Set this just-started cluster as our filesystem.
    FileSystem fs = dfsCluster.getFileSystem();
    conf.set("fs.defaultFS", fs.getUri().toString());
    // Do old style too just to be safe.
    conf.set("fs.default.name", fs.getUri().toString());

    // Wait for the cluster to be totally up
    dfsCluster.waitClusterUp();

    // Save the dfsCluster in the private field of the parent class.
    setField(HBaseTestingUtility.class, this, "dfsCluster", dfsCluster);

    return dfsCluster;
  }

  /**
   * Starts a <code>MiniMRCluster</code>. We override this method so we can pass configuration
   * options for the jobtracker port.
   *
   * @param servers  The number of <code>TaskTracker</code>'s to start.
   * @throws IOException When starting the cluster fails.
   */
  @Override
  public void startMiniMapReduceCluster(final int servers) throws IOException {
    LOG.info("Starting mini mapreduce cluster...");
    // These are needed for the new and improved Map/Reduce framework
    Configuration conf = getConfiguration();
    String logDir = conf.get("hadoop.log.dir");
    String tmpDir = conf.get("hadoop.tmp.dir");
    if (logDir == null) {
      logDir = tmpDir;
    }
    System.setProperty("hadoop.log.dir", logDir);

    // Use the bento extension of MiniMRCluster that avoids overwriting some parameters we pass
    // in the configuration.
    MiniMRCluster mrCluster = new BentoMiniMRCluster(servers,
        FileSystem.get(conf).getUri().toString(), 1, conf);

    LOG.info("Mini mapreduce cluster started");
    // Save the mrCluster private field of the parent class.
    setField(HBaseTestingUtility.class, this, "mrCluster", mrCluster);
  }

  /**
   * Reads a URL from a key in a configuration, obtains the port from the URL,
   * and returns the result. Any exceptions that occur while parsing the URL are converted to
   * runtime exceptions.
   *
   * @param key The configuration key holding the URL.
   * @param defaultPort A default URL to use if the configuration is missing the specified key.
   * @return The port.
   */
  private int getPortFromConfiguration(String key, int defaultPort) {
    return getConfiguration().getSocketAddr(key, "localhost", defaultPort).getPort();
  }

  /**
   * An extension of {@link MiniMRCluster} that avoids overwriting the value of the job tracker
   * info port in the configuration used to start the job tracker.
   */
  static class BentoMiniMRCluster extends MiniMRCluster {
    /**
     * Constructs a new instance.
     *
     * @param numServers The number of task trackers to start.
     * @param namenode URI to the namenode.
     * @param numDir num dir
     * @param conf The configuration to use when starting the cluster.
     * @throws IOException If there is a problem starting the cluster.
     */
    public BentoMiniMRCluster(int numServers,
        String namenode, int numDir, Configuration conf) throws IOException {
      // Despite passing "0" for the job tracker and task tracker ports here, their values
      // will still be taken from the configuration, due to our changes to createJobConf.
      // If either of these are not set in the configuration, they will start on a random port.
      super(0, 0, numServers, namenode, numDir, null, getTaskTrackerHostNames(), null,
          new JobConf(conf));
    }

    /**
     * Creates a job conf for use with the started clusters. This implementation simply returns
     * the passed conf.
     *
     * @param conf The configuration to use when starting the clusters.
     * @return The same configuration.
     */
    @Override
    public JobConf createJobConf(JobConf conf) {
      return conf;
    }

    /**
     * Gets the hostnames of the tasktrackers in this mini cluster.
     * Since there is only one task tracker, this local one, this will
     * return a single-element array with the hostname according to
     * the machine, usually "localhost".
     *
     * @return An array of the tasktracker hostnames.
     */
    private static String[] getTaskTrackerHostNames() {
      try {
        return new String[] { InetAddress.getLocalHost().getHostName() };
      } catch (UnknownHostException e) {
        return new String[] { "localhost" };
      }
    }
  }
}
