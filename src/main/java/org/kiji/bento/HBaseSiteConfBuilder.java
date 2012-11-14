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

/**
 * A builder for a HBase "hbase-site.xml" file.
 */
public class HBaseSiteConfBuilder extends ConfigurationBuilder {
  /** A resource containing a usage comment for the file. */
  private static final String HBASE_SITE_COMMENT_RESOURCE = "org/kiji/bento/hbase-site-comment";

  /**
   * Constructs a builder.
   */
  public HBaseSiteConfBuilder() {
    super("hbase-site", HBASE_SITE_COMMENT_RESOURCE);
  }

  /**
   * Uses the specified port for the master UI in the configuration.
   *
   * @param port The master UI port.
   * @return This builder, so configuration can be chained.
   */
  public HBaseSiteConfBuilder withMasterUIPort(int port) {
    return withValue("hbase.master.info.port", port);
  }

  /**
   * Uses the specified port for zookeeper clients in the configuration.
   *
   * @param port The zookeeper client port.
   * @return This builder, so configuration can be chained.
   */
  public HBaseSiteConfBuilder withZookeeperClientPort(int port) {
    return withValue("hbase.zookeeper.property.clientPort", port);
  }

  /**
   * Uses the specified value for the maximum number of zookeeper client connections in the
   * configuration.
   *
   * @param max The maximum number of zookeeper client connections.
   * @return This builder, so configuration can be chained.
   */
  public HBaseSiteConfBuilder withMaxZookeeperConnections(int max) {
    return withValue("hbase.zookeeper.property.maxClientCnxns", max);
  }

  /**
   * Uses the specified port for the regionserver web UI.
   *
   * @param port The regionserver UI port.
   * @return This builder, so configuration can be chained.
   */
  public HBaseSiteConfBuilder withRegionServerUIPort(int port) {
    return withValue("hbase.regionserver.info.port", port);
  }
}

