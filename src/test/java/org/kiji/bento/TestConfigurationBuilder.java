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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests the functionality of {@link ConfigurationBuilder}.
 */
public class TestConfigurationBuilder {

  /** Temporary folder for test. */
  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  /**
   * Reads the contents of a file in the temporary folder for this test.
   *
   * @param fileName The name of the file to read.
   * @return The contents of the file.
   * @throws IOException If there is a problem reading the file.
   */
  private String readContents(String fileName) throws IOException {
    return FileUtils.readFileToString(new File(mTempFolder.getRoot(), fileName));
  }

  /**
   * Reads the given file from the temporary directory and checks that the given configuration
   * key is present.
   *
   * @param fileName The name of the file to read.
   * @param confKey The configuration key to check for.
   */
  private void checkFor(String fileName, String... confKey) throws IOException {
    String contents = readContents(fileName);
    for (String key : confKey) {
      assertTrue(contents.contains("<name>" + key + "</name>"));
    }
  }

  @Test
  public void testMapRedSite() throws IOException {
    MapRedSiteConfBuilder builder = new MapRedSiteConfBuilder()
        .withJobTrackerPort(1)
        .withJobTrackerUIPort(2);
    builder.writeToDir(mTempFolder.getRoot());
    checkFor("mapred-site.xml", "mapred.job.tracker", "mapred.job.tracker.http.address");
  }

  @Test
  public void testCoreSite() throws IOException {
    CoreSiteConfBuilder builder = new CoreSiteConfBuilder()
        .withNamenodePort(1);
    builder.writeToDir(mTempFolder.getRoot());
    checkFor("core-site.xml", "fs.defaultFS");
  }

  @Test
  public void testHdfsSite() throws IOException {
    HdfsSiteConfBuilder builder = new HdfsSiteConfBuilder()
        .withNameNodeUIPort(1);
    builder.writeToDir(mTempFolder.getRoot());
    checkFor("hdfs-site.xml", "dfs.http.address");
  }

  @Test
  public void testHBaseSite() throws IOException {
    HBaseSiteConfBuilder builder = new HBaseSiteConfBuilder()
        .withMasterUIPort(1)
        .withMaxZookeeperConnections(2)
        .withZookeeperClientPort(3);
    builder.writeToDir(mTempFolder.getRoot());
    checkFor("hbase-site.xml", "hbase.master.info.port", "hbase.zookeeper.property.clientPort",
        "hbase.zookeeper.property.maxClientCnxns");
  }
}
