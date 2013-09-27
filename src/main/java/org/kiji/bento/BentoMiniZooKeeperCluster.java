/**
 * (c) Copyright 2013 WibiData, Inc.
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

import static org.kiji.bento.AbstractionBarrierBulldozer.getField;
import static org.kiji.bento.AbstractionBarrierBulldozer.setField;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An in-memory ZooKeeper cluster. This class exists to customize some functionality of its
 * parent, {@link org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster},
 * so that it is suitable for use as part of BentoCluster.
 */
final class BentoMiniZooKeeperCluster extends MiniZooKeeperCluster {

  private static final Logger LOG = LoggerFactory.getLogger(BentoMiniZooKeeperCluster.class);

  /**
   * Constructs a new instance using the provided configuration.
   *
   * @param configuration to use to configure the ZooKeeper cluster started.
   */
  public BentoMiniZooKeeperCluster(Configuration configuration) {
    super(configuration);
  }

  /**
   * Starts a mini ZooKeeper cluster.
   *
   * <p>Note that this method is lifted from
   * {@link org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster}. The only difference in
   * implementation is that the ZooKeeper data directory is not cleared before startup.</p>
   *
   * @param baseDir for storing ZooKeeper cluster data.
   * @param numZooKeeperServers that will be part of the started cluster.
   * @return ClientPort server bound to.
   * @throws IOException if there is a problem starting up the cluster.
   * @throws InterruptedException if this thread is interrupted while waiting for a server to
   *    start-up.
   */
  @Override
  public int startup(File baseDir, int numZooKeeperServers) throws IOException,
      InterruptedException {
    if (numZooKeeperServers <= 0) {
      return -1;
    }

    setupTestEnv();
    shutdown();

    int tentativePort = selectClientPort();

    // List tracking values for the servers started.
    List<Integer> clientPortList = new ArrayList<Integer>();
    List<ZooKeeperServer> zooKeeperServers = new ArrayList<ZooKeeperServer>();
    List<NIOServerCnxnFactory> standaloneServerFactoryList = new ArrayList<NIOServerCnxnFactory>();

    // running all the ZK servers
    for (int i = 0; i < numZooKeeperServers; i++) {
      File dir = new File(baseDir, "zookeeper_" + i).getAbsoluteFile();
      // Don't delete the contents of the ZooKeeper data directory. This is the difference in the
      // implementation provided in the super class.
      if (!dir.exists()) {
        dir.mkdirs();
      }
      int tickTimeToUse;
      final Integer tickTimeSpecified = getField(MiniZooKeeperCluster.class, this, "tickTime");
      final Integer defaultTickTime = getField(MiniZooKeeperCluster.class, this, "TICK_TIME");
      if (tickTimeSpecified > 0) {
        tickTimeToUse = tickTimeSpecified;
      } else {
        tickTimeToUse = defaultTickTime;
      }
      ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTimeToUse);
      NIOServerCnxnFactory standaloneServerFactory;
      while (true) {
        try {
          Configuration configuration = getField(MiniZooKeeperCluster.class, this,
              "configuration");
          standaloneServerFactory = new NIOServerCnxnFactory();
          standaloneServerFactory.configure(
              new InetSocketAddress(tentativePort),
              configuration.getInt(HConstants.ZOOKEEPER_MAX_CLIENT_CNXNS,
                  1000));
        } catch (BindException e) {
          LOG.debug("Failed binding ZK Server to client port: "
              + tentativePort);
          // This port is already in use, try to use another.
          tentativePort++;
          continue;
        }
        break;
      }

      // Start up this ZK server
      standaloneServerFactory.startup(server);
      final Integer connectionTimeout = getField(MiniZooKeeperCluster.class,
          this, "CONNECTION_TIMEOUT");
      if (!waitForServerUp(tentativePort, connectionTimeout)) {
        throw new IOException("Waiting for startup of standalone server");
      }

      // We have selected this port as a client port.
      clientPortList.add(tentativePort);
      standaloneServerFactoryList.add(standaloneServerFactory);
      zooKeeperServers.add(server);
    }

    // Set the lists of info about zookeeper servers we just built in the parent class.
    setField(MiniZooKeeperCluster.class, this, "clientPortList", clientPortList);
    setField(MiniZooKeeperCluster.class, this, "standaloneServerFactoryList",
        standaloneServerFactoryList);
    setField(MiniZooKeeperCluster.class, this, "zooKeeperServers", zooKeeperServers);

    // set the first one to be active ZK; Others are backups
    setField(MiniZooKeeperCluster.class, this, "activeZKServerIndex", 0);
    setField(MiniZooKeeperCluster.class, this, "started", true);
    Integer clientPort = clientPortList.get(0);
    setField(MiniZooKeeperCluster.class, this, "clientPort", clientPort);
    LOG.info("Started MiniZK Cluster and connect 1 ZK server on client port: " + clientPort);
    return clientPort;
  }

  /**
   * Configures the pre-allocation size of logs. Taken from
   * {@link org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster}.
   */
  private static void setupTestEnv() {
    // during the tests we run with 100K prealloc in the logs.
    // on windows systems prealloc of 64M was seen to take ~15seconds
    // resulting in test failure (client timeout on first session).
    // set env and directly in order to handle static init/gc issues
    System.setProperty("zookeeper.preAllocSize", "100");
    FileTxnLog.setPreallocSize(100 * 1024);
  }

  /**
   * Selects a ZK client port. Returns the default port if specified.
   * Otherwise, returns a random port. The random port is selected from the
   * range between 49152 to 65535. These ports cannot be registered with IANA
   * and are intended for dynamic allocation (see http://bit.ly/dynports).
   *
   * @return the port selected.
   */
  private int selectClientPort() {
    final Integer defaultClientPort = getField(MiniZooKeeperCluster.class, this,
        "defaultClientPort");
    if (defaultClientPort > 0) {
      return defaultClientPort;
    }
    return 0xc000 + new Random().nextInt(0x3f00);
  }

  /**
   * Polls a particular port to determine if a ZooKeeper server has started.
   *
   * @param port to poll for a ZooKeeper server.
   * @param timeout until failure.
   * @return <code>true</code> if a ZooKeeper server was contacted at the port.
   */
  private static boolean waitForServerUp(int port, long timeout) {
    long start = System.currentTimeMillis();
    while (true) {
      try {
        Socket sock = new Socket("localhost", port);
        BufferedReader reader = null;
        try {
          OutputStream outstream = sock.getOutputStream();
          outstream.write("stat".getBytes());
          outstream.flush();

          Reader isr = new InputStreamReader(sock.getInputStream());
          reader = new BufferedReader(isr);
          String line = reader.readLine();
          if (line != null && line.startsWith("Zookeeper version:")) {
            return true;
          }
        } finally {
          sock.close();
          if (reader != null) {
            reader.close();
          }
        }
      } catch (IOException e) {
        // ignore as this is expected
        LOG.info("server localhost:" + port + " not up " + e);
      }

      if (System.currentTimeMillis() > start + timeout) {
        break;
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    return false;
  }
}
