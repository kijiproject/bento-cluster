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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to assist with checking / choosing ports for use with the Hadoop ecosystem.
 */
public class HadoopPorts {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopPorts.class);

  /** The conventional port value for the NameNode. */
  public static final int NAME_NODE_PORT_CONVENTION = 8020;
  /** The conventional port value for the NameNode UI. */
  public static final int NAME_NODE_UI_PORT_CONVENTION = 50070;
  /** The conventional port value for the JobTracker. */
  public static final int JOB_TRACKER_PORT_CONVENTION = 8021;
  /** The conventional port value for the JobTracker UI. */
  public static final int JOB_TRACKER_UI_PORT_CONVENTION = 50030;
  /** The conventional port value for the HMaster UI. */
  public static final int HMASTER_UI_PORT_CONVENTION = 60010;
  /** The conventional port value for the HMaster UI. */
  public static final int ZOOKEEPER_CLIENT_PORT_CONVENTION = 2181;

  /** The default to use for the NameNode port. */
  private int mNameNodePortDefault;
  /** The port for the NameNode. */
  private int mNameNodePort;
  /** The default to use for the NameNode UI port. */
  private int mNameNodeUIPortDefault;
  /** The port for the NameNode UI. */
  private int mNameNodeUIPort;
  /** The default to use for the JobTracker port. */
  private int mJTPortDefault;
  /** The port for the JobTracker. */
  private int mJTPort;
  /** The default port to use for the JobTracker UI. */
  private int mJTUIPortDefault;
  /** The port for the JobTracker UI. */
  private int mJTUIPort;
  /** The default to use for the HMaster UI port. */
  private int mHMasterUIPortDefault;
  /** The port for the HMaster UI. */
  private int mHMasterUIPort;
  /** The default to use for the Zookeeper client port. */
  private int mZookeeperClientPortDefault;
  /** The Zookeeper client port. */
  private int mZookeeperClientPort;

  /**
   * Constructs an instance that uses default values for ports taken from Hadoop/HBase conventions.
   */
  public HadoopPorts() {
    setDefaultsUsingConventions();
    initializeFromDefaults();
  }

  /**
   * Constructs an instance that uses default values for ports taken from bento-managed Hadoop XML
   * resource files present in the specified directories. If a particular resource does not exist,
   * or if a particular port is not set in the resources, then the conventional default for the
   * port will be used.
   *
   * @param hadoopConfDir A directory containing bento-managed Hadoop XML resource files
   *     (bento-core-site.xml, bento-mapred-site.xml, etc.).
   * @param hbaseConfDir  A directory containing bento-managed HBase XML resource files
   *     (bento-hbase-site.xml).
   */
  public HadoopPorts(File hadoopConfDir, File hbaseConfDir) {
    // Create a Hadoop configuration populated with configuration already existing in
    // bento-managed resources, and use it to initialize defaults and port values.
    Configuration confWithDefaults = getConfigurationWithResources(hadoopConfDir, hbaseConfDir);
    setDefaultsUsingConfiguration(confWithDefaults);
    initializeFromDefaults();
  }

  /**
   * Adds the properties defined in a bento-managed Hadoop XML resource to the specified
   * configuration. If the resource specified does not exist, no new properties will be added to
   * the configuration.
   *
   * @param conf The configuration to add properties to.
   * @param resource The Hadoop XMl resource whose properties should be added to the configuration.
   */
  private static void addResourceToConfiguration(Configuration conf, File resource) {
    try {
      FileInputStream inputStream = new FileInputStream(resource);
      // The input stream is closed as a side effect of this method.
      conf.addResource(inputStream);
    } catch (FileNotFoundException e) {
      // Do nothing if the specified resource file does not exist. We'll use conventional
      // defaults in this case.
    }
  }

  /**
   * Gets an {@link Configuration} with properties obtained from the bento-core-site.xml,
   * bento-mapred-site.xml, bento-hdfs-site.xml, and bento-hbase-site.xml present in the specified
   * directories.
   *
   * @param hadoopConfDir The Hadoop configuration directory.
   * @param hbaseConfDir The HBase configuration directory.
   * @return A configuration populated with properties from bento-managed Hadoop XML resources
   *     present in the specified directories.
   */
  private static Configuration getConfigurationWithResources(File hadoopConfDir,
      File hbaseConfDir) {
    Configuration conf = new Configuration(false);
    addResourceToConfiguration(conf,  new File(hadoopConfDir, "bento-core-site.xml"));
    addResourceToConfiguration(conf,  new File(hadoopConfDir, "bento-mapred-site.xml"));
    addResourceToConfiguration(conf,  new File(hadoopConfDir, "bento-hdfs-site.xml"));
    addResourceToConfiguration(conf,  new File(hbaseConfDir, "bento-hbase-site.xml"));
    return conf;
  }

  /**
   * Sets the defaults for ports using Hadoop/HBase conventions.
   */
  private void setDefaultsUsingConventions() {
    mNameNodePortDefault = NAME_NODE_PORT_CONVENTION;
    mNameNodeUIPortDefault = NAME_NODE_UI_PORT_CONVENTION;
    mJTPortDefault = JOB_TRACKER_PORT_CONVENTION;
    mJTUIPortDefault = JOB_TRACKER_UI_PORT_CONVENTION;
    mHMasterUIPortDefault = HMASTER_UI_PORT_CONVENTION;
    mZookeeperClientPortDefault = ZOOKEEPER_CLIENT_PORT_CONVENTION;
  }

  /**
   * Sets the defaults for ports using properties in the provided configuration. If a
   * property containing a port is not present in the provided configuration,
   * than the conventional default is used.
   *
   * @param confWithDefaults A configuration possibly containing default values for ports.
   */
  private void setDefaultsUsingConfiguration(Configuration confWithDefaults) {
    mNameNodePortDefault = confWithDefaults.getSocketAddr(
        CoreSiteConfBuilder.NAME_NODE_ADDRESS_CONF,
        "localhost", NAME_NODE_PORT_CONVENTION).getPort();

    mNameNodeUIPortDefault = confWithDefaults.getSocketAddr(
        HdfsSiteConfBuilder.NAME_NODE_UI_ADDRESS_CONF,
        "localhost", NAME_NODE_UI_PORT_CONVENTION).getPort();

    mJTPortDefault = confWithDefaults.getSocketAddr(
        MapRedSiteConfBuilder.JOB_TRACKER_ADDRESS_CONF,
        "localhost", JOB_TRACKER_PORT_CONVENTION).getPort();

    mJTUIPortDefault = confWithDefaults.getSocketAddr(
        MapRedSiteConfBuilder.JOB_TRACKER_UI_ADDRESS_CONF,
        "localhost", JOB_TRACKER_UI_PORT_CONVENTION).getPort();

    mHMasterUIPortDefault = confWithDefaults.getInt(HBaseSiteConfBuilder.HMASTER_UI_PORT_CONF,
        HMASTER_UI_PORT_CONVENTION);

    mZookeeperClientPortDefault = confWithDefaults.getInt(
        HBaseSiteConfBuilder.ZOOKEEPER_CLIENT_PORT_CONF, ZOOKEEPER_CLIENT_PORT_CONVENTION);
  }

  /**
   * @return <code>true</code> if all ports are set to their default value,
   *     <code>false</code> otherwise.
   */
  public boolean isAllDefaultsUsed() {
    return mNameNodePort == mNameNodePortDefault
        && mNameNodeUIPort == mNameNodeUIPortDefault
        && mJTPort == mJTPortDefault
        && mJTUIPort == mJTUIPort
        && mHMasterUIPort == mHMasterUIPortDefault
        && mZookeeperClientPort == mZookeeperClientPortDefault;
  }

  /**
   * Checks if a port has already been chosen as one of the Hadoop ports.
   *
   * @param port The port to check.
   * @return <code>true</code> if the port has been chosen as one of the Hadoop ports,
   *     <code>false</code> otherwise.
   */
  private boolean isChosen(int port) {
    return port == mNameNodePort
        || port == mNameNodeUIPort
        || port == mJTPort
        || port == mJTUIPort
        || port == mHMasterUIPort
        || port == mZookeeperClientPort;
  }

  /**
   * Close a server socket, catching and logging any exceptions.
   *
   * @param ss The server socket to close.
   */
  private static void closeQuietly(ServerSocket ss) {
    if (null != ss) {
      try {
        ss.close();
      } catch (IOException e) {
        LOG.warn("There was an error while closing a server socket: "
            + StringUtils.stringifyException(e));
      }
    }
  }

  /**
   * Check if the specified port is open.
   *
   * @param port The port to check.
   * @return <code>true</code> if the port is open, <code>false</code> otherwise.
   */
  public static boolean isOpen(int port) {
    ServerSocket ss = null;
    try {
      ss = new ServerSocket(port);
      ss.setReuseAddress(true);
    } catch (IOException e) {
      // The port isn't open.
      return false;
    } finally {
      closeQuietly(ss);
    }
    return true;
  }

  /**
   * Find an available port.
   *
   * @param startPort the starting port to check.
   * @return an open port number.
   * @throws IllegalArgumentException if it can't find an open port.
   */
  public int findOpenPort(int startPort) {
    if (startPort < 1024 || startPort > 65534) {
      throw new IllegalArgumentException("Invalid start port: " + startPort);
    }

    for (int port = startPort; port < 65534; port++) {
      // If the port is open and is the start port, or the port is open and has not been chosen,
      // return it.
      if (isOpen(port) && (port == startPort || !isChosen(port))) {
        return port;
      }
    }
    throw new IllegalArgumentException("No port available starting at " + startPort);
  }

  /**
   * Chooses values for all ports, trying default values first.
   */
  private void initializeFromDefaults() {
    mNameNodePort = findOpenPort(mNameNodePortDefault);
    mNameNodeUIPort = findOpenPort(mNameNodeUIPortDefault);
    mJTPort = findOpenPort(mJTPortDefault);
    mJTUIPort = findOpenPort(mJTUIPortDefault);
    mHMasterUIPort = findOpenPort(mHMasterUIPortDefault);
    mZookeeperClientPort = findOpenPort(mZookeeperClientPortDefault);
  }

  /**
   * @return The name node port.
   */
  public int getNameNodePort() {
    return mNameNodePort;
  }

  /**
   * Sets the name node port.
   *
   * @param port The port to use.
   */
  public void setNameNodePort(int port) {
    mNameNodePort = port;
  }

  /**
   *
   * @return The name node UI port.
   */
  public int getNameNodeUIPort() {
    return mNameNodeUIPort;
  }

  /**
   * Sets the name node UI port.
   *
   * @param port The port to use.
   */
  public void setNameNodeUIPort(int port) {
    mNameNodeUIPort = port;
  }

  /**
   * @return The job tracker port.
   */
  public int getJobTrackerPort() {
    return mJTPort;
  }

  /**
   * Sets the job tracker port.
   *
   * @param port The port to use.
   */
  public void setJobTrackerPort(int port) {
    mJTPort = port;
  }

  /**
   * @return The job tracker UI port.
   */
  public int getJobTrackerUIPort() {
    return mJTUIPort;
  }

  /**
   * Sets the job tracker UI port.
   *
   * @param port The port to use.
   */
  public void setJobTrackerUIPort(int port) {
    mJTUIPort = port;
  }

  /**
   * @return The HBase master UI port;
   */
  public int getHMasterUIPort() {
    return mHMasterUIPort;
  }

  /**
   * Sets the HBase master UI port.
   *
   * @param port The port to use.
   */
  public void setHMasterUIPort(int port) {
    mHMasterUIPort = port;
  }

  /**
   * @return The Zookeeper client port.
   */
  public int getZookeeperClientPort() {
    return mZookeeperClientPort;
  }

  /**
   * Sets the Zookeeper client port.
   *
   * @param port The port to use.
   */
  public void setZookeeperClientPort(int port) {
    mZookeeperClientPort = port;
  }

}
