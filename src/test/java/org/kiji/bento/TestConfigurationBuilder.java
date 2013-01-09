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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests the functionality of subclasses of {@link ConfigurationBuilder} specific to certain
 * Hadoop XML configuration resources managed by bento-cluster.
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
   * Reads the given file from the temporary directory and checks that the specified properties
   * (name/value pairs) are present.
   *
   * @param fileName The name of the file to read.
   * @param properties The property name/value pairs to check for.
   */
  private void checkFor(String fileName, Map<String, String> properties)
      throws IOException {
    final String contents = readContents(fileName);
    System.out.println(contents);
    final String xmlPropertyFormat =
        "  <property>\n"
            + "    <name>%s</name>\n"
            + "    <value>%s</value>\n"
            + "  </property>\n";
    for (final Map.Entry<String, String> property : properties.entrySet()) {
      final String expectedXml =
          String.format(xmlPropertyFormat, property.getKey(), property.getValue());
      assertTrue(contents.contains(expectedXml));
    }
  }

  @Test
  public void testMapRedSite() throws IOException {
    final MapRedSiteConfBuilder builder = new MapRedSiteConfBuilder()
        .withJobTrackerPort(1)
        .withJobTrackerUIPort(2);
    builder.writeToDir(mTempFolder.getRoot());

    final Map<String, String> expectedProperties = new HashMap<String, String>();
    expectedProperties.put("mapred.job.tracker", "localhost:1");
    expectedProperties.put("mapred.job.tracker.http.address", "localhost:2");
    checkFor("bento-mapred-site.xml", expectedProperties);
  }

  @Test
  public void testCoreSite() throws IOException {
    final CoreSiteConfBuilder builder = new CoreSiteConfBuilder()
        .withNameNodePort(1);
    builder.writeToDir(mTempFolder.getRoot());

    final Map<String, String> expectedProperties = new HashMap<String, String>();
    expectedProperties.put("fs.defaultFS", "hdfs://localhost:1");
    checkFor("bento-core-site.xml", expectedProperties);
  }

  @Test
  public void testHdfsSite() throws IOException {
    final HdfsSiteConfBuilder builder = new HdfsSiteConfBuilder()
        .withNameNodeUIPort(1);
    builder.writeToDir(mTempFolder.getRoot());

    final Map<String, String> expectedProperties = new HashMap<String, String>();
    expectedProperties.put("dfs.http.address", "localhost:1");
    checkFor("bento-hdfs-site.xml", expectedProperties);
  }

  @Test
  public void testHBaseSite() throws IOException {
    final HBaseSiteConfBuilder builder = new HBaseSiteConfBuilder()
        .withMasterUIPort(1)
        .withMaxZookeeperConnections(2)
        .withZookeeperClientPort(3);
    builder.writeToDir(mTempFolder.getRoot());

    final Map<String, String> expectedProperties = new HashMap<String, String>();
    expectedProperties.put("hbase.master.info.port", "1");
    expectedProperties.put("hbase.zookeeper.property.maxClientCnxns", "2");
    expectedProperties.put("hbase.zookeeper.property.clientPort", "3");
    checkFor("bento-hbase-site.xml", expectedProperties);
  }
}

