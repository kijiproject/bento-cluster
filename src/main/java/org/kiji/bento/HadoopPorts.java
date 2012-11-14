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

import java.io.IOException;
import java.net.ServerSocket;

import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to assist with checking / choosing ports for use with the Hadoop ecosystem.
 */
public class HadoopPorts {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopPorts.class);

  public static final int NAMENODE_PORT_DEFAULT = 8020;
  public static final int NAMENODE_UI_PORT_DEFAULT = 50070;
  public static final int JT_PORT_DEFAULT = 8021;
  public static final int JT_UI_PORT_DEFAULT = 50030;
  public static final int HMASTER_UI_PORT_DEFAULT = 60010;
  public static final int ZOOKEEPER_CLIENT_PORT_DEFAULT = 2181;

  private int mNameNodePort = NAMENODE_PORT_DEFAULT;
  private int mNameNodeUIPort = NAMENODE_UI_PORT_DEFAULT;
  private int mJTPort = JT_PORT_DEFAULT;
  private int mJTUIPort = JT_UI_PORT_DEFAULT;
  private int mHMasterUIPort = HMASTER_UI_PORT_DEFAULT;
  private int mZookeeperClientPort = ZOOKEEPER_CLIENT_PORT_DEFAULT;

  /**
   * @return <code>true</code> if all ports are set to their default value,
   *     <code>false</code> otherwise.
   */
  public boolean isAllDefaultsUsed() {
    return mNameNodePort == NAMENODE_PORT_DEFAULT
        && mNameNodeUIPort == NAMENODE_UI_PORT_DEFAULT
        && mJTPort == JT_PORT_DEFAULT
        && mJTUIPort == JT_UI_PORT_DEFAULT
        && mHMasterUIPort == HMASTER_UI_PORT_DEFAULT
        && mZookeeperClientPort == ZOOKEEPER_CLIENT_PORT_DEFAULT;
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
  public void initializeFromDefaults() {
    mNameNodePort = findOpenPort(NAMENODE_PORT_DEFAULT);
    mNameNodeUIPort = findOpenPort(NAMENODE_UI_PORT_DEFAULT);
    mJTPort = findOpenPort(JT_PORT_DEFAULT);
    mJTUIPort = findOpenPort(JT_UI_PORT_DEFAULT);
    mHMasterUIPort = findOpenPort(HMASTER_UI_PORT_DEFAULT);
    mZookeeperClientPort = findOpenPort(ZOOKEEPER_CLIENT_PORT_DEFAULT);
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
