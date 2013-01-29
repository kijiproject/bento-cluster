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

import org.kiji.bento.HadoopPorts;

/**
 * A tool that checks if ports used by bento-cluster are in use.
 */
public final class PortCheckingTool extends Configured implements Tool {

  /** {@inheritDoc} */
  @Override
  public int run(String[] args) throws Exception {
    // Add resource conf files that hold settings for mapreduce, hdfs, and hbase.
    getConf().addResource("hdfs-site.xml");
    getConf().addResource("mapred-site.xml");
    setConf(HBaseConfiguration.addHbaseResources(getConf()));

    // Check if ports are open and report if they are not.
    HadoopPorts ports = new HadoopPorts(getConf());
    if (!ports.isAllDefaultsUsed()) {
      System.out.println("The following configured ports are in use. Please free the ports or "
          + "reconfigure bento-cluster by running `bento config` before trying to start the "
          + "clusters.");
      ports.reportOnTakenDefaults();
      return 1;
    }
    return 0;
  }

  /**
   * Java program entry point.
   *
   * @param args are the command-line arguments.
   * @throws Exception on error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new PortCheckingTool(), args));
  }
}
