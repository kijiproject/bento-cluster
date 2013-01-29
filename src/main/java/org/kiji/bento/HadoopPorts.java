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
  public static final Integer NAME_NODE_PORT_CONVENTION = 8020;
  /** The conventional port value for the NameNode UI. */
  public static final Integer NAME_NODE_UI_PORT_CONVENTION = 50070;
  /** The conventional port value for the JobTracker. */
  public static final Integer JOB_TRACKER_PORT_CONVENTION = 8021;
  /** The conventional port value for the JobTracker UI. */
  public static final Integer JOB_TRACKER_UI_PORT_CONVENTION = 50030;
  /** The conventional port value for the HMaster UI. */
  public static final Integer HMASTER_UI_PORT_CONVENTION = 60010;
  /** The conventional port value for the HMaster UI. */
  public static final Integer ZOOKEEPER_CLIENT_PORT_CONVENTION = 2181;

  /** The default to use for the NameNode port. */
  private Integer mNameNodePortDefault = null;
  /** The port for the NameNode. */
  private Integer mNameNodePort = null;
  /** The default to use for the NameNode UI port. */
  private Integer mNameNodeUIPortDefault = null;
  /** The port for the NameNode UI. */
  private Integer mNameNodeUIPort = null;
  /** The default to use for the JobTracker port. */
  private Integer mJTPortDefault = null;
  /** The port for the JobTracker. */
  private Integer mJTPort = null;
  /** The default port to use for the JobTracker UI. */
  private Integer mJTUIPortDefault = null;
  /** The port for the JobTracker UI. */
  private Integer mJTUIPort = null;
  /** The default to use for the HMaster UI port. */
  private Integer mHMasterUIPortDefault = null;
  /** The port for the HMaster UI. */
  private Integer mHMasterUIPort = null;
  /** The default to use for the Zookeeper client port. */
  private Integer mZookeeperClientPortDefault = null;
  /** The Zookeeper client port. */
  private Integer mZookeeperClientPort = null;

  /**
   * Constructs an instance that uses default values for ports taken from Hadoop/HBase conventions.
   */
  public HadoopPorts() {
    setDefaultsUsingConventions();
    initializeFromDefaults();
  }

  /**
   * Constructs an instance that uses default values for ports taken from the specified
   * configuration. If a particular port is not set in the configuration,
   * then the conventional default for the port will be used.
   *
   * @param confWithDefaults is the configuration used to get values for ports.
   */
  public HadoopPorts(Configuration confWithDefaults) {
    setDefaultsUsingConfiguration(confWithDefaults);
    initializeFromDefaults();
  }

  /**
   * Constructs an instance that uses default values for ports taken from bento-managed Hadoop XML
   * resource files present in the specified directories. If a particular resource does not exist,
   * or if a particular port is not set in the resources, then the conventional default for the
   * port will be used.
   *
   * @param hadoopConfDir is a directory containing bento-managed Hadoop XML resource files
   *     (bento-core-site.xml, bento-mapred-site.xml, etc.).
   * @param hbaseConfDir  is a directory containing bento-managed HBase XML resource files
   *     (bento-hbase-site.xml).
   */
  public HadoopPorts(File hadoopConfDir, File hbaseConfDir) {
    // Create a Hadoop configuration populated with configuration already existing in
    // bento-managed resources, and use it to initialize defaults and port values.
    this(getConfigurationWithResources(hadoopConfDir, hbaseConfDir));
  }

  /**
   * Adds the properties defined in a bento-managed Hadoop XML resource to the specified
   * configuration. If the resource specified does not exist, no new properties will be added to
   * the configuration.
   *
   * @param conf is the configuration to add properties to.
   * @param resource is the Hadoop XMl resource whose properties should be added to the
   *     configuration.
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
   * @param hadoopConfDir is the Hadoop configuration directory.
   * @param hbaseConfDir is the HBase configuration directory.
   * @return a configuration populated with properties from bento-managed Hadoop XML resources
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
   * @param confWithDefaults is a configuration possibly containing default values for ports.
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
    return mNameNodePortDefault.equals(mNameNodePort)
        && mNameNodeUIPortDefault.equals(mNameNodeUIPort)
        && mJTPortDefault.equals(mJTPort)
        && mJTUIPortDefault.equals(mJTUIPort)
        && mHMasterUIPortDefault.equals(mHMasterUIPort)
        && mZookeeperClientPortDefault.equals(mZookeeperClientPort);
  }

  /**
   * Prints a message to the user about a port's default value being taken.
   *
   * @param portName is the name of the port whose default is taken.
   * @param portDefault is the default value for the port that is in use.
   */
  private void reportOnTakenDefault(String portName, Integer portDefault) {
    String output = String.format("The %s port %d is taken.", portName, portDefault);
    System.out.println(output);

  }

  /**
   * Prints a message to the user reporting on which needed ports have used defaults.
   */
  public void reportOnTakenDefaults() {
    if (!isOpen(mNameNodePortDefault)) {
      reportOnTakenDefault("NameNode", mNameNodePortDefault);
    }
    if (!isOpen(mNameNodeUIPortDefault)) {
      reportOnTakenDefault("NameNode UI", mNameNodeUIPortDefault);
    }
    if (!isOpen(mJTPortDefault)) {
      reportOnTakenDefault("JobTracker", mJTPortDefault);
    }
    if (!isOpen(mJTUIPortDefault)) {
      reportOnTakenDefault("JobTracker UI", mJTUIPortDefault);
    }
    if (!isOpen(mHMasterUIPortDefault)) {
      reportOnTakenDefault("HMaster UI", mHMasterUIPortDefault);
    }
    if (!isOpen(mZookeeperClientPortDefault)) {
      reportOnTakenDefault("Zookeeper Client", mZookeeperClientPortDefault);
    }
  }

  /**
   * Checks if a port has already been chosen as one of the Hadoop ports.
   *
   * @param port is the port to check for being chosen.
   * @return <code>true</code> if the port has been chosen as one of the Hadoop ports,
   *     <code>false</code> otherwise.
   */
  private boolean isChosen(Integer port) {
    return port.equals(mNameNodePort)
        || port.equals(mNameNodeUIPort)
        || port.equals(mJTPort)
        || port.equals(mJTUIPort)
        || port.equals(mHMasterUIPort)
        || port.equals(mZookeeperClientPort);
  }

  /**
   * Close a server socket, catching and logging any exceptions.
   *
   * @param ss is the server socket to close.
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
   * @param port is the port to check for being open.
   * @return <code>true</code> if the port is open, <code>false</code> otherwise.
   */
  public static boolean isOpen(Integer port) {
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
   * @param startPort is the port from which the search is started.
   * @return an open port number.
   * @throws IllegalArgumentException if an open port is not found by the search.
   */
  public Integer findOpenPort(Integer startPort) {
    if (startPort < 1024 || startPort > 65534) {
      throw new IllegalArgumentException("Invalid start port: " + startPort);
    }

    for (Integer port = startPort; port < 65534; port++) {
      // If the port is open and not chosen, return it.
      if (isOpen(port) && !isChosen(port)) {
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
   * @return the NameNode port.
   */
  public Integer getNameNodePort() {
    return mNameNodePort;
  }

  /**
   * @return the default value for the NameNode port.
   */
  public Integer getNameNodePortDefault() {
    return mNameNodePortDefault;
  }

  /**
   * Sets the NameNode port.
   *
   * @param port is the port to use.
   */
  public void setNameNodePort(Integer port) {
    mNameNodePort = port;
  }

  /**
   *
   * @return the NameNode UI port.
   */
  public Integer getNameNodeUIPort() {
    return mNameNodeUIPort;
  }

  /**
   * @return the default value for the NameNode UI port.
   */
  public Integer getNameNodeUiPortDefault() {
    return mNameNodeUIPortDefault;
  }

  /**
   * Sets the NameNode UI port.
   *
   * @param port is the port to use.
   */
  public void setNameNodeUIPort(Integer port) {
    mNameNodeUIPort = port;
  }

  /**
   * @return the JobTracker port.
   */
  public Integer getJobTrackerPort() {
    return mJTPort;
  }

  /**
   * @return the default value for the JobTracker port.
   */
  public Integer getJobTrackerPortDefault() {
    return mJTPortDefault;
  }

  /**
   * Sets the JobTracker port.
   *
   * @param port is the port to use.
   */
  public void setJobTrackerPort(Integer port) {
    mJTPort = port;
  }

  /**
   * @return the JobTracker UI port.
   */
  public Integer getJobTrackerUIPort() {
    return mJTUIPort;
  }

  /**
   * @return the default value for the JobTracker port.
   */
  public Integer getJobTrackerUIPortDefault() {
    return mJTUIPortDefault;
  }

  /**
   * Sets the JobTracker UI port.
   *
   * @param port is the port to use.
   */
  public void setJobTrackerUIPort(Integer port) {
    mJTUIPort = port;
  }

  /**
   * @return the HMaster UI port;
   */
  public Integer getHMasterUIPort() {
    return mHMasterUIPort;
  }

  /**
   * @return the default value for the HMaster UI port.
   */
  public Integer getHMasterUIPortDefault() {
    return mHMasterUIPortDefault;
  }

  /**
   * Sets the HBase master UI port.
   *
   * @param port is the port to use.
   */
  public void setHMasterUIPort(Integer port) {
    mHMasterUIPort = port;
  }

  /**
   * @return the Zookeeper client port.
   */
  public Integer getZookeeperClientPort() {
    return mZookeeperClientPort;
  }

  /**
   * @return the default value for the Zookeeper client port.
   */
  public Integer getZookeeperClientPortDefault() {
    return mZookeeperClientPortDefault;
  }

  /**
   * Sets the Zookeeper client port.
   *
   * @param port is the port to use.
   */
  public void setZookeeperClientPort(Integer port) {
    mZookeeperClientPort = port;
  }

  /**
   * Sets all chosen points to <code>null</code>, thereby clearing any existing port configuration.
   */
  public void clearPortChoices() {
    mNameNodePort = null;
    mNameNodeUIPort = null;
    mJTPort = null;
    mJTUIPort = null;
    mHMasterUIPort = null;
    mZookeeperClientPort = null;
  }
}
