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

package org.kiji.bento.tools;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.kiji.bento.CoreSiteConfBuilder;
import org.kiji.bento.HBaseSiteConfBuilder;
import org.kiji.bento.HadoopPorts;
import org.kiji.bento.HdfsSiteConfBuilder;
import org.kiji.bento.MapRedSiteConfBuilder;

/**
 * A tool that reports on the ports being used by bento-cluster.
 */
public final class PortReportingTool extends Configured implements Tool {

  /** The port the NameNode UI is configured to use. */
  private int mNameNodeUIPort;
  /** The port the JobTracker UI is configured to use. */
  private int mJobTrackerUIPort;
  /** The port the HMaster UI is configured to use. */
  private int mHMasterUIPort;
  /** The port the NameNode is configured to use. */
  private int mNameNodePort;
  /** The port the JobTracker is configured to use. */
  private int mJobTrackerPort;
  /** The Zookeeper client port. */
  private int mZookeeperPort;

  /**
   * Initializes fields holding port values for this tool using this instance's configuration.
   */
  private void initializePorts() {
    mNameNodePort = getConf().getSocketAddr(
        CoreSiteConfBuilder.NAME_NODE_ADDRESS_CONF,
        "localhost", HadoopPorts.NAME_NODE_PORT_CONVENTION).getPort();

    mNameNodeUIPort = getConf().getSocketAddr(
        HdfsSiteConfBuilder.NAME_NODE_UI_ADDRESS_CONF,
        "localhost", HadoopPorts.NAME_NODE_UI_PORT_CONVENTION).getPort();

    mJobTrackerPort = getConf().getSocketAddr(
        MapRedSiteConfBuilder.JOB_TRACKER_ADDRESS_CONF,
        "localhost", HadoopPorts.JOB_TRACKER_PORT_CONVENTION).getPort();

    mJobTrackerUIPort = getConf().getSocketAddr(
        MapRedSiteConfBuilder.JOB_TRACKER_UI_ADDRESS_CONF,
        "localhost", HadoopPorts.JOB_TRACKER_UI_PORT_CONVENTION).getPort();

    mHMasterUIPort = getConf().getInt(HBaseSiteConfBuilder.HMASTER_UI_PORT_CONF,
        HadoopPorts.HMASTER_UI_PORT_CONVENTION);

    mZookeeperPort = getConf().getInt(HBaseSiteConfBuilder.ZOOKEEPER_CLIENT_PORT_CONF,
        HadoopPorts.ZOOKEEPER_CLIENT_PORT_CONVENTION);
  }

  /**
   * Displays a message to the user detailing the web addresses for the various cluster UIs,
   * and the ports on which services are available.
   */
  private void reportPorts() {
    final String nameNodeUIAddress = "http://localhost:" + mNameNodeUIPort;
    final String jobTrackerUIAddress = "http://localhost:" + mJobTrackerUIPort;
    final String hMasterUIAddress = "http://localhost:" + mHMasterUIPort;

    System.out.println();
    System.out.println("Cluster webapps can be visited at these web addresses:");
    System.out.println("HDFS NameNode:         " + nameNodeUIAddress);
    System.out.println("MapReduce JobTracker:  " + jobTrackerUIAddress);
    System.out.println("HBase Master:          " + hMasterUIAddress);
    System.out.println();

    System.out.println("Cluster services are available on the following ports:");
    System.out.println("HDFS NameNode:        " + mNameNodePort);
    System.out.println("MapReduce JobTracker: " + mJobTrackerPort);
    System.out.println("Zookeeper:            " + mZookeeperPort);
    System.out.println();
  }

  /** {@inheritDoc} */
  @Override
  public int run(String[] args) throws Exception {
    // Add resource conf files that hold settings for mapreduce, hdfs, and hbase.
    getConf().addResource("hdfs-site.xml");
    getConf().addResource("mapred-site.xml");
    setConf(HBaseConfiguration.addHbaseResources(getConf()));

    // Initialize ports using the configuration and then report them.
    initializePorts();
    reportPorts();
    return 0;
  }

  /**
   * Java program entry point.
   *
   * @param args Command-line arguments.
   * @throws Exception on error.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PortReportingTool(), args);
  }
}
