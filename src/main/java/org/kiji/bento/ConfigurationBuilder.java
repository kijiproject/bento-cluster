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
import java.net.URL;
import java.nio.charset.Charset;

import com.google.common.io.Resources;

/**
 * A builder for Hadoop XML site configuration files.
 */
public abstract class ConfigurationBuilder {
  /**
   * A configuration writer for the XML configuration file.
   */
  private final ConfigurationWriter mWriter;

  /**
   * The name of this configuration, for example, "mapred-site" or "core-site".
   */
  private final String mName;

  /**
   * Constructs a new configuration builder for a configuration with the specified name.
   *
   * @param name The name of the configuration, for example, "mapred-site" or "core-site."
   * @param commentResource The name of a resource that contains a usage comment for the
   *     configuration.
   */
  public ConfigurationBuilder(String name, String commentResource) {
    URL resourceURL = Resources.getResource(commentResource);
    String comment;
    try {
      comment = Resources.toString(resourceURL, Charset.forName("UTF-8"));
    } catch (IOException e) {
      throw new RuntimeException("Error when generating a Hadoop site XML file being: "
          + "comment could not be read from resource: " + commentResource);
    }
    mWriter = new ConfigurationWriter(comment);
    mName = name;
  }

  /**
   * @return An XML string encoding the configuration.
   */
  public String getConfigurationXML() {
    return mWriter.getConfigurationXML();
  }

  /**
   * Write the XML configuration to a file in the specified directory. The file will have name
   * equivalent to the configuration name and the <code>.xml</code> extension.
   *
   * @param directory The directory where the configuration should be written.
   * @throws IOException If there is a problem writing the configuration.
   */
  public void writeToDir(File directory) throws IOException {
    final File outputFile = new File(directory, mName + ".xml");
    mWriter.write(outputFile);
  }

  /**
   * Writes the value <code>localhost:PORT</code> to the configuration,
   * where <code>PORT</code> is the argument passed.
   *
   * @param key The key for the value.
   * @param port The port to include in the address.
   * @param <T> The type of this builder.
   * @return This builder, so configuration can be chained.
   */
  protected <T> T withLocalAddressAtPort(String key, int port) {
    return withValue(key, "localhost:" + port);
  }

  /**
   * Writes the specified integer value to the configuration.
   *
   * @param key The key to write.
   * @param value The value to write.
   * @param <T> The type of this builder.
   * @return This builder, so configuration can be chained.
   */
  protected <T> T withValue(String key, int value) {
    return withValue(key, Integer.toString(value));
  }

  /**
   * Writes the specified string value to the configuration.
   *
   * @param key The key to write.
   * @param value The value to write.
   * @param <T> The type of this builder.
   * @return This builder, so configuration can be chained.
   */
  @SuppressWarnings(value = "unchecked")
  protected <T> T withValue(String key, String value) {
    mWriter.put(key, value);
    return (T) this;
  }
}
