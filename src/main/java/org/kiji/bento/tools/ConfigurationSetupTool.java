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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import com.odiago.common.flags.Flag;
import com.odiago.common.flags.FlagParser;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.bento.CoreSiteConfBuilder;
import org.kiji.bento.HBaseSiteConfBuilder;
import org.kiji.bento.HadoopPorts;
import org.kiji.bento.HdfsSiteConfBuilder;
import org.kiji.bento.MapRedSiteConfBuilder;

/**
 * A tool to help the user setup their Hadoop *-site.xml files with correct ports and settings.
 */
public class ConfigurationSetupTool extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationSetupTool.class);

  /** A flag used to pass the HBase configuration directory. */
  @Flag(name="hadoop-conf-dir")
  private String mHadoopConfDirPath = "";
  private File mHadoopConfDir;

  /** A flag used to pass the Hadoop configuration directory. */
  @Flag(name="hbase-conf-dir")
  private String mHBaseConfDirPath = "";
  private File mHBaseConfDir;

  /** A reader for standard input. */
  private BufferedReader mReader;

  /**
   * Checks the specified file is a directory that exists, and throws an
   * {@link IllegalArgumentException} otherwise.
   *
   * @param file The file to check.
   */
  private void checkDirectory(File file) {
    if (!file.exists() || !file.isDirectory()) {
      throw new IllegalArgumentException("The path " + file.toString() + " does not exist or is "
          + "not a directory");
    }
  }

  /**
   * Setup directories used by this tool using command line arguments.
   *
   * @throws Exception If there is a problem setting up an input stream reader for standard input.
   */
  private void setup() throws Exception {
    mHadoopConfDir = new File(mHadoopConfDirPath);
    mHBaseConfDir = new File(mHBaseConfDirPath);
    checkDirectory(mHadoopConfDir);
    checkDirectory(mHBaseConfDir);
    mReader = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
  }

  /**
   * Release resources used by this tool.
   */
  private void cleanup() {
    IOUtils.closeQuietly(mReader);
  }

  /**
   * Writes Hadoop's core-site.xml to the Hadoop conf directory.
   *
   * @param ports The ports to use when writing the conf.
   * @throws IOException If there is an error writing the file.
   */
  private void writeCoreSiteConf(HadoopPorts ports) throws IOException {
    CoreSiteConfBuilder site = new CoreSiteConfBuilder()
        .withNamenodePort(ports.getNameNodePort());
    site.writeToDir(mHadoopConfDir);

  }

  /**
   * Writes Hadoop's mapred-site.xml to the Hadoop conf directory.
   *
   * @param ports The ports to use when writing the conf.
   * @throws IOException If there is an error writing the file.
   */
  private void writeMapRedConf(HadoopPorts ports) throws IOException {
    MapRedSiteConfBuilder site = new MapRedSiteConfBuilder()
        .withJobTrackerPort(ports.getJobTrackerPort())
        .withJobTrackerUIPort(ports.getJobTrackerUIPort());
    site.writeToDir(mHadoopConfDir);
  }

  /**
   * Writes Hadoop's hdfs-site.xml to the Hadoop conf directory.
   *
   * @param ports The ports to use when writing the conf.
   * @throws IOException If there is an error writing the file.
   */
  private void writeHdfsConf(HadoopPorts ports) throws IOException {
    HdfsSiteConfBuilder site = new HdfsSiteConfBuilder()
        .withNameNodeUIPort(ports.getNameNodeUIPort());
    site.writeToDir(mHadoopConfDir);
  }

  /**
   * Writes HBase's hbase-site.xml to the HBase conf directory.
   *
   * @param ports The ports to use when writing the conf.
   * @throws IOException If there is an error writing the file.
   */
  private void writeHBaseConf(HadoopPorts ports) throws IOException {
    HBaseSiteConfBuilder site = new HBaseSiteConfBuilder()
        .withMasterUIPort(ports.getHMasterUIPort())
        .withZookeeperClientPort(ports.getZookeeperClientPort())
        .withMaxZookeeperConnections(80)
        .withRegionServerUIPort(ports.findOpenPort(60030));
    site.writeToDir(mHBaseConfDir);
  }

  /**
   * Write all conf files to their appropriate directories.
   *
   * @param ports The ports to use when writing the configurations.
   * @throws IOException If there is an error writing the file.
   */
  private void writeConfFiles(HadoopPorts ports) throws IOException {
    writeCoreSiteConf(ports);
    writeMapRedConf(ports);
    writeHdfsConf(ports);
    writeHBaseConf(ports);
  }

  /**
   * Asks the user for a port.
   *
   * @param port A suggestion for the port.
   * @param portDescription A description for the port.
   * @return The user's string response.
   * @throws IOException If there is a problem reading from the console.
   */
  private String askUserForPort(int port, String portDescription) throws IOException {
    System.out.println("Please enter a port for " + portDescription);
    System.out.println("or press enter to use suggestion: " + port);
    return mReader.readLine();
  }

  /**
   * Prompts the user to choose a port until they enter a valid one.
   *
   * @param ports The Hadoop ports to use when checking ports.
   * @param port A suggestion for the port the user is choosing.
   * @param portDescription A description of the port of the user.
   * @return The port chosen by the user.
   * @throws IOException If there is a problem reading from the console.
   */
  private int askUserForPort(HadoopPorts ports, int port, String portDescription) throws
      IOException {
    boolean takePort = false;
    do {
      String response = askUserForPort(port, portDescription);
      if (!response.isEmpty()) {
        // User did not take suggested port. Let's double check their entry.
        try {
          port = Integer.parseInt(response);
          int suggestedPort = ports.findOpenPort(port);
          if (suggestedPort != port) {
            // the user did not choose an open port. continue with a suggestion chosen starting
            // from the port asked for.
            port = suggestedPort;
            System.out.println("That port is in use. Please try again.");
          } else {
            takePort = true;
          }
        } catch (NumberFormatException e) {
          System.out.println("Please enter a valid integer to use as the port.");
        }
      } else {
        takePort = true;
      }
    } while (!takePort);
    return port;
  }

  /**
   * Asks the user for confirmation on the Hadoop ports to use.
   *
   * @param ports The ports to set.
   * @throws IOException If there is a problem reading input from the console.
   */
  private void askUserAboutPorts(HadoopPorts ports) throws IOException {
    ports.setNameNodePort(askUserForPort(ports, ports.getNameNodePort(), "HDFS NameNode"));
    ports.setNameNodeUIPort(askUserForPort(ports, ports.getNameNodeUIPort(), "HDFS NameNode UI"));
    ports.setJobTrackerPort(askUserForPort(ports, ports.getJobTrackerPort(),
        "MapReduce JobTracker"));
    ports.setJobTrackerUIPort(askUserForPort(ports, ports.getJobTrackerUIPort(),
        "MapReduce JobTracker UI"));
    ports.setHMasterUIPort(askUserForPort(ports, ports.getHMasterUIPort(), "HBase Master UI"));
    ports.setZookeeperClientPort(askUserForPort(ports, ports.getZookeeperClientPort(),
        "Zookeeper client port"));
  }

  /**
   * Prints a message containing the web addresses of Hadoop web interfaces.
   *
   * @param ports The ports used to determine the web addresses.
   */
  private void tellUserAboutUIs(HadoopPorts ports) {
    final String nameNodeUIAddress = "http://localhost:" + ports.getNameNodeUIPort();
    final String jobTrackerUIAddress = "http://localhost:" + ports.getJobTrackerUIPort();
    final String hMasterUIAddress = "http://localhost:" + ports.getHMasterUIPort();

    System.out.println("After your clusters have (re)started, "
        + "you can visit their web interfaces:");
    System.out.println("HDFS NameNode:         " + nameNodeUIAddress);
    System.out.println("MapReduce JobTracker:  " + jobTrackerUIAddress);
    System.out.println("HBase Master:          " + hMasterUIAddress);
  }

  /**
   * Runs the tool.
   *
   * @param args Command line arguments.
   * @return Exit status.
   * @throws Exception If there is an error.
   */
  @Override
  public int run(String[] args) throws Exception {
    final List<String> unparsed = FlagParser.init(this, args);
    if (null == unparsed) {
      return 1;
    }

    setup();
    try {
      // Get a port helper and use it to initialize some port suggestions.
      HadoopPorts ports = new HadoopPorts();
      ports.initializeFromDefaults();
      System.out.println("Checking if the Hadoop/HBase default ports are open...");
      if (!ports.isAllDefaultsUsed()) {
        // Get confirmation from the user on all port values if the defaults aren't open.
        System.out.println("Some default ports were not available.");
        askUserAboutPorts(ports);
      } else {
        System.out.println("All default ports open. Using these in the Hadoop/HBase configuration "
            + "for your cluster.");
      }
      System.out.println("Writing Hadoop configuration files to: " + mHadoopConfDirPath);
      System.out.println("Writing HBase configuration files to: " + mHBaseConfDirPath);
      writeConfFiles(ports);
      System.out.println("Configuration files successfully written!");
      System.out.println();
      tellUserAboutUIs(ports);
    } finally {
      cleanup();
    }
    return 0;
  }

  /**
   * Java program entry point.
   *
   * @param args Command-line arguments.
   * @throws Exception on error.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ConfigurationSetupTool(), args);
  }
}
