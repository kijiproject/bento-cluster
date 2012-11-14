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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;


/**
 * Instances of this class can write pretty-printed Hadoop configuration XML files containing
 * key/value pairs that have been added to the instance.
 */
public class ConfigurationWriter {

  /**
   * A format for the head of a configuration XML string. Can be formatted with a comment.
   */
  private static final String CONFIGURATION_HEADER_FORMAT =
      "<?xml version=\"1.0\"?>\n"
      + "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n\n"
      + "<!--\n"
      + "%s\n"
      + "-->\n\n"
      + "<configuration>\n";

  /** A footer for the configuration file. */
  private static final String CONFIGURATION_FOOTER =
      "</configuration>\n";

  /**
   * A format for the XML string representing a key/value pair in the configuration. Can be
   * formatted with a key and a value.
   */
  private static final String KEY_VALUE_PAIR_FORMAT =
      "  <property>\n"
      + "    <name>%s</name>\n"
      + "    <value>%s</value>\n"
      + "  </property>\n";

  /** A map holding the key/value pairs to include in the configuration. */
  private final Map<String, String> mConfiguration;

  /** A comment to include in the head of the configuration XML. */
  private final String mComment;

  /**
   * Constructs a new instance holding no key/value pairs, whose XML will contain the specified
   * comment as part of the header.
   *
   * @param comment A comment for the head of the configuration XML.
   */
  public ConfigurationWriter(String comment) {
    mConfiguration = new HashMap<String, String>();
    mComment = comment;
  }

  /**
   * Adds a key/value pair to the configuration to be written. If the key specified already
   * exists in the configuration, its value will be overwritten.
   *
   * @param key The key too add to the configuration.
   * @param value The value to associate with the key.
   */
  public void put(String key, String value) {
    mConfiguration.put(key, value);
  }

  /**
   * Gets the value for a key that has been added to the configuration to be written,
   * or <code>null</code> if the key does not exist in the configuration.
   *
   * @param key The key whose value should be retrieved.
   * @return The value, or <code>null</code> if the key does not exist in the configuration.
   */
  public String get(String key) {
    return mConfiguration.get(key);
  }

  /**
   * Gets an XML string encoding a key/value pair in a Hadoop configuration XML file.
   *
   * @param key The key to include in the XML.
   * @param value The value for the key.
   * @return The XML string.
   */
  protected String getPropertyXML(String key, String value) {
    return String.format(KEY_VALUE_PAIR_FORMAT, key, value);
  }

  /**
   * Indents all lines in the specified string by two spaces.
   *
   * @param str The string to indent.
   * @return The string with all lines indented by two spaces.
   */
  private String indent(String str) {
    final String[] lines = str.split("\n");
    for (int i = 0; i < lines.length; i++) {
      lines[i] = "  " + lines[i];
    }
    return StringUtils.join(lines, "\n");
  }

  /**
   * Gets an XML string holding the header for the configuration.
   *
   * @return An XML string holding the header for the configuration.
   */
  protected String getHeaderXml() {
    return String.format(CONFIGURATION_HEADER_FORMAT, indent(mComment));
  }

  /**
   * Gets an XML string holding the key/value pairs added to the configuration writer.
   *
   * @return An XML string containing the configuration added to this writer.
   */
  public String getConfigurationXML() {
    final StringBuilder xml = new StringBuilder().append(getHeaderXml()).append("\n");
    for (Map.Entry<String, String> entry : mConfiguration.entrySet()) {
      xml.append(getPropertyXML(entry.getKey(), entry.getValue())).append("\n");
    }
    return xml.append(CONFIGURATION_FOOTER).toString();
  }

  /**
   * Writes the configuration XML to the file at the specified path.
   *
   * @param file The file the configuration should be written to.
   * @throws IOException If there is an error writing the file.
   */
  public void write(File file) throws IOException {
    FileUtils.write(file, getConfigurationXML());
  }
}
