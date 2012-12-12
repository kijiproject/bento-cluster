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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.bento.BentoClusters;
import org.kiji.common.flags.Flag;
import org.kiji.common.flags.FlagParser;

/**
 * <p>This tool starts or stops mini hadoop and hbase clusters for use in testing and prototyping
 * on a local machine. After starting these clusters, clients can use the hadoop and hbase
 * distributions included with bento to interact with the clusters. The mini clusters should not
 * be used in production. </p>
 *
 * <p>The mini clusters should be started with the *-site.xml configuration files located in
 * the hadoop and hbase distributions packaged with bento-cluster on the classpath. Users wishing
 * to alter the configuration of the mini clusters should edit these files. Note that some
 * configuration (such as the location of the mapreduce working directory,
 * the location where user logs are written, and the location used for cluster storage) cannot
 * be overwritten. </p>
 *
 * <p>The mini-cluster uses the path specified with the "--state-dir" flag for its state. This
 * flag must be specified when running this tool. The bento script uses the --state-dir flag to
 * specify the subdir "state" of the bento-cluster distribution as the cluster state dir.
 * </p>
 *
 * <p>The state dir contains cluster storage. When a cluster is running,
 * it also contains a pid file written when the cluster started and containing the process id of
 * the running cluster. This pid file is removed on cluster shutdown.</p>
 *
 * <p>Note that the way we determine the pid of the cluster is specific to the
 * behavior of {@link java.lang.management.RuntimeMXBean#getName() RuntimeMXBean.getName()}
 * and may not be portable on non-UNIX systems.</p>
 */
public class MiniClusterTool extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(MiniClusterTool.class);

  @Flag(name="state-dir",
      usage="The path used by bento-cluster for its state and configuration.")
  private String mClusterDir = "";

  /** A file containing a pid for a running cluster. */
  private File mPidFile;

  /** Encapsulates the mini clusters started by this tool. */
  private BentoClusters mBentoClusters;

  /**
   * Gets the pid for this process. This won't be portable on non-UNIX systems.
   *
   * @return The pid of this JVM.
   */
  private int getPid() {
    String processString = ManagementFactory.getRuntimeMXBean().getName();
    return Integer.valueOf(processString.split("@")[0]);
  }

  /**
   * Reads a pid from a file.
   *
   * @return The pid extracted from the file.
   * @throws IOException If the file couldn't be read.
   */
  private int readPidFile() throws IOException {
    int pid;
    Scanner scanner = new Scanner(mPidFile, "UTF-8");
    if (!scanner.hasNextInt()) {
      throw new IOException("Invalid pid file format.");
    }
    pid = scanner.nextInt();
    scanner.close();
    return pid;
  }

  /**
   * Writes the process's pid into a file.
   *
   * @throws IOException If an error occurs when writing the file.
   */
  private void writePidFile() throws IOException {
    OutputStreamWriter osw = null;
    try {
      osw = new OutputStreamWriter(new FileOutputStream(mPidFile), "UTF-8");
      osw.write(Integer.toString(getPid()));
    } finally {
      IOUtils.closeQuietly(osw);
    }
  }

  /**
   * Attempts to create a pid file. The file will be removed on exit.
   *
   * @throws IOException If the pid file already exists or if it couldn't be created.
   */
  private void createPidFile() throws IOException {
    if (mPidFile.exists()) {
      int pid = 0;
      try {
        pid = readPidFile();
      } catch (IOException e) {
        throw new IOException("Existing bento-cluster pid file found, but couldn't be read: "
            + mPidFile.getPath() + " Might need to be cleaned.", e);
      }
      throw new IOException("bento-cluster already running with pid: " + Integer.toString(pid)
            + " Stop cluster (or remove stale pid file).");
    } else {
      createFileParentDir(mPidFile);
      writePidFile();
      mPidFile.deleteOnExit();
    }
  }

  /**
   * Gets the parent directory of the specified file.  Creates the directory if it does not already
   * exist.
   *
   * @param file The File containing the path to extract the parent dir from.
   * @return The parent directory.
   * @throws IOException If there is an error getting or creating the parent directory.
   */
  private static File createFileParentDir(File file) throws IOException {
    File parentDir = file.getParentFile();
    if (null != parentDir && !parentDir.exists() && !parentDir.mkdirs()) {
      throw new IOException(
          "Unable to create or access parent directory of: "
              + file.getParent());
    }
    return parentDir;
  }


  /**
   * Ensures the user specified command-line arguments correctly.
   *
   * @throws IllegalArgumentException if the user failed to specify a required argument,
   *     or specified an argument of the wrong type.
   */
  private void validateArguments() {
    if (null == mClusterDir || mClusterDir.isEmpty()) {
      throw new IllegalArgumentException("You must specify a directory to store bento-cluster's "
          + "state with flag --state-dir=/path/to/cluster/dir");
    }
  }


  /**
   * Initializes this tool's configuration, as well as system properties for use with the mini
   * clusters.
   *
   * The tool's configuration is modified by adding hbase resources and setting cluster storage
   * and logging locations. A system property is also set that specifies the cluster storage
   * directory.
   */
  private void configure() {
    // Add resource conf files that hold settings for mapreduce, hdfs, and hbase.
    getConf().addResource("hdfs-site.xml");
    getConf().addResource("mapred-site.xml");
    setConf(HBaseConfiguration.addHbaseResources(getConf()));
  }

  /**
   * Initializes this tool and installs a shutdown hook that stops the clusters when the SIGTERM
   * signal is received.
   */
  private void setup() {
    mPidFile = new File(mClusterDir, "bento-cluster.pid");
    installShutdownHook();
  }

  /**
   * Installs a shutdown hook with the runtime that gracefully stops the miniclusters. The
   * shutdown hook should run when SIGTERM is caught by the jvm.
   */
  private void installShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOG.info("Received TERM signal, so shutting down bento-cluster.");
        try {
          mBentoClusters.stopClusters();
        } catch (Exception e) {
          LOG.warn("An exception occurred while shutting down bento-cluster:\n"
              + StringUtils.stringifyException(e));
        }
      }
    });
  }
  /**
   * Runs the MiniCluster tool.
   *
   * @param args Command-line arguments.
   * @return the command exit code.
   * @throws Exception on error.
   */
  public int run(String[] args) throws Exception {
    final List<String> unparsed = FlagParser.init(this, args);
    if (null == unparsed) {
      return 1;
    }

    validateArguments();
    setup();
    configure();

    mBentoClusters = new BentoClusters(mClusterDir, getConf());
    mBentoClusters.startClusters();
    createPidFile();
    mBentoClusters.waitUntilStop();
    return 0;
  }

  /**
   * Java program entry point.
   *
   * @param args Command-line arguments.
   * @throws Exception on error.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new MiniClusterTool(), args);
  }
}
