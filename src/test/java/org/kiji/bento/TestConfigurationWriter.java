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
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the functionality of {@link ConfigurationWriter}.
 */
public class TestConfigurationWriter {

  private static final String CONFIGURATION_COMMENT =
      "My awesome comment!\n"
      + "That is multiline!\n";

  /** The writer to use in tests. */
  private ConfigurationWriter mWriter;

  @Before
  public void setup() {
    mWriter = new ConfigurationWriter(CONFIGURATION_COMMENT);
  }

  /**
   * Tests that we can put values into the configuration to be written and that we can get them
   * back out.
   */
  @Test
  public void testGetPut() {
    // Make sure we can put some values and get them back out.
    for (int i = 0; i < 50; i++) {
      mWriter.put("key." + i, "value." + i);
    }
    for (int i = 0; i < 50; i++) {
      assertEquals("Retrieved incorrect value for key." + i, "value." + i,
          mWriter.get("key." + i));
    }
  }

  /**
   * Tests that getting a key out of the configuration writer that wasn't put there returns
   * <code>null</code>.
   */
  @Test
  public void testGetNonexistentKey() {
    assertNull(mWriter.get("not.in.configuration"));
  }

  /**
   *  Tests getting the XML string representing a key/value pair in the configuration.
   */
  @Test
  public void testGetPropertyXML() {
    final String expectedXML =
        "  <property>\n"
        + "    <name>hello.world</name>\n"
        + "    <value>how are you</value>\n"
        + "  </property>\n";
    assertEquals("Got unexpected property XML string.", expectedXML,
        mWriter.getPropertyXML("hello.world", "how are you"));
  }

  /**
   * Tests getting the XML configuration header.
   */
  @Test
  public void testGetHeaderXML() {
    final String expectedXML =
        "<?xml version=\"1.0\"?>\n"
        + "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n\n"
        + "<!--\n"
        + "  My awesome comment!\n"
        + "  That is multiline!\n"
        + "-->\n\n"
        + "<configuration>\n";
    assertEquals("Got wrong configuration XML header.", expectedXML, mWriter.getHeaderXml());
  }

  /**
   * Tests getting the expected XML for a configuration and writing that configuration to a file.
   */
  @Test
  public void testConfigurationXML() throws IOException {
    final String expectedXML =
        "<?xml version=\"1.0\"?>\n"
        + "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n\n"
        + "<!--\n"
        + "  My awesome comment!\n"
        + "  That is multiline!\n"
        + "-->\n\n"
        + "<configuration>\n\n"
        + "  <property>\n"
        + "    <name>key.1</name>\n"
        + "    <value>value.1</value>\n"
        + "  </property>\n\n"
        + "  <property>\n"
        + "    <name>key.2</name>\n"
        + "    <value>value.2</value>\n"
        + "  </property>\n\n"
        + "</configuration>\n";
    mWriter.put("key.1", "value.1");
    mWriter.put("key.2", "value.2");

    assertEquals("Got wrong configuration XML.", expectedXML, mWriter.getConfigurationXML());

    final File xmlFile = new File(FileUtils.getTempDirectory(), "bento-config.xml");
    xmlFile.deleteOnExit();
    mWriter.write(xmlFile);
    String contents = FileUtils.readFileToString(xmlFile);
    assertEquals("Got wrong configuration XML from file.", expectedXML, contents);
  }
}
