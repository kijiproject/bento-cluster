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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;

import com.google.common.io.Resources;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.kiji.bento.CoreSiteConfBuilder;
import org.kiji.bento.HBaseSiteConfBuilder;
import org.kiji.bento.HadoopPorts;
import org.kiji.bento.HdfsSiteConfBuilder;
import org.kiji.bento.MapRedSiteConfBuilder;
import org.kiji.common.flags.Flag;
import org.kiji.common.flags.FlagParser;

/**
 * A tool to help the user create bento-managed Hadoop XML resource files with correct ports and
 * settings.
 */
public final class ConfigurationSetupTool extends Configured implements Tool {
  private static final String RESOURCES_ROOT = "org/kiji/bento/";

  /** A flag used to pass the HBase configuration directory. */
  @Flag(name = "hadoop-conf-dir", hidden = true)
  private String mHadoopConfDirPath = "";
  private File mHadoopConfDir;

  /** A flag used to pass the Hadoop configuration directory. */
  @Flag(name = "hbase-conf-dir", hidden = true)
  private String mHBaseConfDirPath = "";
  private File mHBaseConfDir;

  @Flag(name = "prompt",
      usage = "If true, the utility will always prompt for ports, even if defaults are open.")
  private boolean mAlwaysPrompt = false;

  @Flag(name = "use-hadoop-defaults",
      usage = "If true, default port values will follow Hadoop conventions, not existing "
          + "configuration.")
  private boolean mUseHadoopDefaults = false;

  @Flag(name = "reset",
      usage = "If true, ALL existing Hadoop/HBase configuration will be removed.")
  private boolean mResetConfiguration = false;

  /** A reader for standard input. */
  private BufferedReader mReader;

  /**
   * Checks the specified file is a directory that exists, and throws an
   * {@link IllegalArgumentException} otherwise.
   *
   * @param file is the file to check.
   */
  private void checkIsDirectory(File file) {
    if (!file.exists() || !file.isDirectory()) {
      throw new IllegalArgumentException("The path " + file.toString() + " does not exist or is "
          + "not a directory");
    }
  }

  /**
   * Checks if a file exists in a directory.
   *
   * @param directory is the directory that may contain the file.
   * @param fileName is the name of the file to check for.
   * @return <code>true</code> if the file exists, <code>false</code> otherwise.
   */
  private boolean isFileExists(File directory, String fileName) {
    File file = new File(directory, fileName);
    return file.exists() && file.isFile();
  }

  /**
   * Checks whether the directory specified contains a bento-managed XML resource with the
   * provided name. The filename used is the resource name specified prefixed with "bento-" and
   * suffixed with ".xml".
   *
   * @param directory is the directory to check for the resource file.
   * @param resourceName is a Hadoop XML resource name like "core-site" or "mapred-site."
   * @return <code>true</code> if the resource exists in the specified directory,
   *     <code>false</code> otherwise.
   */
  private boolean isExistingBentoResource(File directory, String resourceName) {
    return isFileExists(directory, "bento-" + resourceName + ".xml");
  }

  /**
   * Checks if any bento-managed Hadoop/HBase XML resources already exist in the Hadoop and HBase
   * configuration directories.
   *
   * @return <code>true</code> if any of bento-core-site.xml, bento-mapred-site.xml,
   *     bento-hdfs-site.xml, and bento-hbase-site.xml already exist, <code>false</code> otherwise.
   */
  private boolean isAnyBentoResourceExists() {
    return isExistingBentoResource(mHadoopConfDir, "core-site")
        || isExistingBentoResource(mHadoopConfDir, "mapred-site")
        || isExistingBentoResource(mHadoopConfDir, "hdfs-site")
        || isExistingBentoResource(mHBaseConfDir, "hbase-site");
  }

  /**
   * Setup directories used by this tool using command line arguments.
   *
   * @throws Exception if there is a problem setting up an input stream reader for standard input.
   */
  private void setup() throws Exception {
    mHadoopConfDir = new File(mHadoopConfDirPath);
    mHBaseConfDir = new File(mHBaseConfDirPath);
    checkIsDirectory(mHadoopConfDir);
    checkIsDirectory(mHBaseConfDir);
    System.out.println("Hadoop configuration directory is: " + mHadoopConfDirPath + "\n");
    System.out.println("HBase configuration directory is: " + mHBaseConfDirPath + "\n");
    mReader = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
  }

  /**
   * Release resources used by this tool.
   */
  private void cleanup() {
    IOUtils.closeQuietly(mReader);
  }

  /**
   * Writes the bento-managed bento-core-site.xml to the Hadoop conf directory.
   *
   * @param ports holds the port choices to use when writing the conf.
   * @throws IOException if there is an error writing the file.
   */
  private void writeBentoCoreSiteConf(HadoopPorts ports) throws IOException {
    CoreSiteConfBuilder site = new CoreSiteConfBuilder();
    site.withNameNodePort(ports.getNameNodePort());
    site.writeToDir(mHadoopConfDir);
  }

  /**
   * Writes the bento-managed bento-mapred-site.xml to the Hadoop conf directory.
   *
   * @param ports holds the port choices to use when writing the conf.
   * @throws IOException if there is an error writing the file.
   */
  private void writeBentoMapRedConf(HadoopPorts ports) throws IOException {
    MapRedSiteConfBuilder site = new MapRedSiteConfBuilder();
    site.withJobTrackerPort(ports.getJobTrackerPort())
        .withJobTrackerUIPort(ports.getJobTrackerUIPort());
    site.writeToDir(mHadoopConfDir);
  }

  /**
   * Writes the bento-managed bento-hdfs-site.xml to the Hadoop conf directory.
   *
   * @param ports are the port choices to use when writing the conf.
   * @throws IOException if there is an error writing the file.
   */
  private void writeBentoHdfsConf(HadoopPorts ports) throws IOException {
    HdfsSiteConfBuilder site = new HdfsSiteConfBuilder();
    site.withNameNodeUIPort(ports.getNameNodeUIPort());
    site.writeToDir(mHadoopConfDir);
  }

  /**
   * Writes the bento-managed bento-hbase-site.xml to the HBase conf directory.
   *
   * @param ports are the port choices to use when writing the conf.
   * @throws IOException if there is an error writing the file.
   */
  private void writeBentoHBaseConf(HadoopPorts ports) throws IOException {
    HBaseSiteConfBuilder site = new HBaseSiteConfBuilder();
    site.withMasterUIPort(ports.getHMasterUIPort())
        .withZookeeperClientPort(ports.getZookeeperClientPort())
        .withMaxZookeeperConnections(80)
        .withRegionServerUIPort(ports.findOpenPort(60030));
    site.writeToDir(mHBaseConfDir);
  }

  /**
   * Writes all bento-managed configuration files to their appropriate directories.
   *
   * @param ports are the port choices to use when writing the configurations.
   * @throws IOException if there is an error writing the files.
   */
  private void writeBentoConfFiles(HadoopPorts ports) throws IOException {
    System.out.println("Writing bento-managed configuration.");
    writeBentoCoreSiteConf(ports);
    writeBentoMapRedConf(ports);
    writeBentoHdfsConf(ports);
    writeBentoHBaseConf(ports);
  }

  /**
   * Writes a clean XML configuration file. The file is written if it does not exist or the
   * user requested a reset.
   *
   * @param directory is the directory the file should be written to.
   * @param confFileName is the filename of the clean configuration file to write.
   * @throws IOException if there is an error writing the file.
   */
  private void maybeWriteConfFile(File directory, String confFileName)
      throws IOException {
    if (!isFileExists(directory, confFileName) || mResetConfiguration) {
      System.out.println("Writing clean " + confFileName);
      URL resourceUrl = Resources.getResource(RESOURCES_ROOT + confFileName);
      FileOutputStream out = null;
      try {
        out = new FileOutputStream(new File(directory, confFileName));
        Resources.copy(resourceUrl, out);
      } finally {
        IOUtils.closeQuietly(out);
      }
    }
  }

  /**
   * Writes clean Hadoop/HBase configuration files. The files are written if they are
   * missing or the user requested a configuration reset.
   *
   * @throws IOException if there is an error writing the files.
   */
  private void maybeWriteConfFiles() throws IOException {
    maybeWriteConfFile(mHadoopConfDir, "core-site.xml");
    maybeWriteConfFile(mHadoopConfDir, "hdfs-site.xml");
    maybeWriteConfFile(mHadoopConfDir, "mapred-site.xml");
    maybeWriteConfFile(mHBaseConfDir, "hbase-site.xml");
  }

  /**
   * Asks the user for a port.
   *
   * @param portSuggestion is a suggestion for the port the user can accept.
   * @param portDescription is a description for the port displayed in the prompt to the user.
   * @return the user's string response.
   * @throws IOException if there is a problem reading from the console.
   */
  private String askUserForPort(Integer portSuggestion, String portDescription)
      throws IOException {
    System.out.println(portDescription + " [" + portSuggestion + "]");
    return mReader.readLine();
  }

  /**
   * Prompts the user to choose a port until they enter a valid one.
   *
   * @param ports is the port utility used to check and find ports.
   * @param port is a suggestion for the port the user can accept.
   * @param portDescription is a description of the port displayed in the prompt to the user.
   * @return the port chosen by the user.
   * @throws IOException if there is a problem reading from the console.
   */
  private Integer askUserForPort(HadoopPorts ports, Integer port, String portDescription) throws
      IOException {
    boolean takePort = false;
    do {
      String response = askUserForPort(port, portDescription).trim();
      if (!response.isEmpty()) {
        // User did not take suggested port. Let's double check their entry.
        try {
          port = Integer.parseInt(response);
          Integer suggestedPort = ports.findOpenPort(port);
          if (!suggestedPort.equals(port)) {
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
   * @param ports is the port utility used to check and find ports.
   * @throws IOException if there is a problem reading input from the console.
   */
  private void askUserForPorts(HadoopPorts ports) throws IOException {
    System.out.println("Please enter a value for each port, or press enter to use suggestion.");
    ports.setNameNodePort(askUserForPort(ports,
        ports.findOpenPort(ports.getNameNodePortDefault()), "HDFS NameNode"));

    ports.setNameNodeUIPort(askUserForPort(ports,
        ports.findOpenPort(ports.getNameNodeUiPortDefault()), "HDFS NameNode UI"));

    ports.setJobTrackerPort(askUserForPort(ports,
        ports.findOpenPort(ports.getJobTrackerPortDefault()), "MapReduce JobTracker"));

    ports.setJobTrackerUIPort(askUserForPort(ports,
        ports.findOpenPort(ports.getJobTrackerUIPortDefault()), "MapReduce JobTracker UI"));

    ports.setHMasterUIPort(askUserForPort(ports,
        ports.findOpenPort(ports.getHMasterUIPortDefault()), "HBase Master UI"));

    ports.setZookeeperClientPort(askUserForPort(ports,
        ports.findOpenPort(ports.getZookeeperClientPortDefault()), "Zookeeper client port"));
  }

  /**
   * Runs the tool.
   *
   * @param args are the command-line arguments.
   * @return an exit status.
   * @throws Exception if there is an error while running the tool.
   */
  @Override
  public int run(String[] args) throws Exception {
    final List<String> unparsed = FlagParser.init(this, args);
    if (null == unparsed) {
      return 1;
    }

    try {
      setup();

      // Get a port helper and use it to initialize some port suggestions.
      HadoopPorts ports;
      if (mUseHadoopDefaults) {
        ports = new HadoopPorts();
      } else {
        ports = new HadoopPorts(mHadoopConfDir, mHBaseConfDir);
      }

      // Inform the user that we are checking for open ports.
      if (!mAlwaysPrompt) {
        if (isAnyBentoResourceExists()) {
          System.out.println("Checking if already configured ports are still open...");
        } else {
          System.out.println("Checking if default Hadoop/HBase ports are open...");
        }
        if (!ports.isAllDefaultsUsed()) {
          System.out.println("Some ports are in use. \n");
        }
      }

      // Prompt for ports, or inform the user that defaults/existing configuration will be used.
      if (!ports.isAllDefaultsUsed() || mAlwaysPrompt) {
        // Ask the user for port values.
        ports.clearPortChoices();
        askUserForPorts(ports);
      } else {
        if (isAnyBentoResourceExists()) {
          System.out.println("Already configured ports are open.");
        } else {
          System.out.println("Default Hadoop/HBase ports are open.");
        }
        System.out.println("Using these in the Hadoop/HBase configuration for your cluster.\n");
      }

      writeBentoConfFiles(ports);
      maybeWriteConfFiles();
      System.out.println("Configuration complete.");
    } finally {
      cleanup();
    }
    return 0;
  }

  /**
   * Java program entry point.
   *
   * @param args are the command-line arguments.
   * @throws Exception if there is an error while running the tool.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new ConfigurationSetupTool(), args));
  }
}
